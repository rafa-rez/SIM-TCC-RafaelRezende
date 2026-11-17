"""
Guardião determinístico de SQL, Linhagem de Dados, Lacunas de Transparência e Proteção PII (LGPD).

Resolve os achados e blindagens (SIM v4.5):
- FIND-001 / Sandbox DuckDB 2.0: Execução arbitrária de SQL, leitura de sistema de arquivos, sleep/JSON expansion e format width DoS
- FIND-008: Extração determinística de anos (2018..2035) e linhagem `fontes_utilizadas` via AST
- FIND-011 / LGPD 2.0: Mascaramento de CPFs completos (11 dígitos), CPFs embutidos sem word-boundary e re-mascaramento
  de CPFs parcialmente mascarados (padrões Betha `046.244.***-**` e SICOM `164***156**`) impedindo reconstrução cruzada
- Step 7C Anti-Evasão LGPD: Bloqueio de fatiamento (`SUBSTR`/`LEFT`/`RIGHT`/`REGEXP_EXTRACT`/`SPLIT_PART`), `LIKE`/`SIMILAR TO`/`REGEXP`
  sobre colunas sensíveis e propagação de aliases em CTEs/subqueries, preservando expressões matemáticas sobre `COUNT(...)` (`SEC-P2-04`)
- RN-GOV-02 Multi-Cidade: Detecção dinâmica de lacunas de folha (`GAP-FOLHA`) e anos parciais (`partial_years`) por município
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope


BLOCKED_FUNCTIONS: Set[str] = {
    "read_csv",
    "read_csv_auto",
    "read_parquet",
    "read_json",
    "read_json_auto",
    "read_json_objects",
    "read_text",
    "read_blob",
    "read_xlsx",
    "glob",
    "parquet_scan",
    "csv_scan",
    "sqlite_scan",
    "postgres_scan",
    "mysql_scan",
    "iceberg_scan",
    "delta_scan",
    "excel_scan",
    "spatial_scan",
    "st_read",
    "sniff_csv",
    "json_execute_serialized_sql",
    "json_serialize_sql",
    "getenv",
    "current_setting",
    "query",
    "query_table",
    "system",
    "shell",
    "copy",
    "enable_profiling",
    "disable_profiling",
    "enable_logging",
    "disable_logging",
    "write_log",
    "truncate_duckdb_logs",
    "which_secret",
    "checkpoint",
    "force_checkpoint",
    "range",
    "generate_series",
    "generate_subscripts",
    "repeat_row",
    "repeat",
    "unnest",
    "explode",
    "explode_outer",
    "explodeouter",
    "list_resize",
    "regexp_split_to_table",
    "flatten",
    "pg_sleep",
    "sleep_ms",
    "json_each",
    "json_tree",
    "to_json",
    "row_to_json",
}

BLOCKED_SCHEMAS: Set[str] = {
    "information_schema",
    "pg_catalog",
    "sqlite_master",
    "sqlite_temp_master",
    "sys",
}

BLOCKED_TABLE_PREFIXES: Tuple[str, ...] = (
    "duckdb_",
    "pragma_",
    "sqlite_",
    "pg_",
    "read_",
    "glob",
    "parquet_",
    "csv_",
)

YEAR_COLUMNS: Set[str] = {
    "ano_referencia",
    "ano_exercicio",
    "ano",
    "ano_edicao",
    "ano_mes",
}

TABLES_2023_CORRUPTED: Set[str] = {
    "contrato_creditocontrato",
    "despesa_despesa",
    "despesa_liquidacaofonte",
    "despesa_pagamento",
    "empenho_empenhofonte",
    "empenho_movfontersp",
    "licitacao_reclicitacao",
    "receita_receita",
}

PII_SENSITIVE_COL_SUBSTRINGS: Tuple[str, ...] = (
    "cpf",
    "num_documento",
    "num_doc_credor",
    "cpf_cnpj_socio",
    "num_doc_fornecedor",
    "num_doc_resp",
    "num_doc_signatario",
    "num_doc_representante",
    "cgc_habilitado",
)

_NIS_TOKEN_RE = re.compile(r"(?:^|[\s_])nis(?:$|[\s_])", re.IGNORECASE)

AUDITABLE_PII_ALIAS_KEYWORDS: Tuple[str, ...] = (
    "documento",
    "cpf",
    "cgc_habilitado",
    "doc_",
    "cnpj",
)

PROMPT_INJECTION_PATTERNS: List[Tuple[str, re.Pattern[str]]] = [
    (
        "override_instructions",
        re.compile(
            r"(ignore\s+(?:(?:all|todas|todos|as|os)\s+)*(?:previous|prior|above|anteriores|instru[cç][oõ]es|regras|system|diretrizes)|"
            r"esque[cç]a\s+(?:todas\s+as\s+)?(?:regras|instru[cç][oõ]es)|"
            r"desconsidere\s+(?:todas\s+as\s+|as\s+)?(?:regras|instru[cç][oõ]es|diretrizes)|"
            r"disregard\s+(?:all\s+)?(?:previous|prior|system)\s+instructions|"
            r"novo\s+system\s+prompt|"
            r"you\s+are\s+now\s+(?:in\s+)?developer\s+mode|"
            r"reveal\s+(?:your\s+)?system\s+prompt|"
            r"mostre\s+(?:o\s+)?seu\s+prompt\s+de\s+sistema)",
            re.IGNORECASE,
        ),
    ),
    (
        "file_probe",
        re.compile(
            r"(/etc/passwd|/etc/shadow|(?:^|[\s/\\'\"`])\.env\b|id_rsa|read_csv_auto|read_text|read_parquet\s*\(|COPY\s+.*\s+TO)",
            re.IGNORECASE,
        ),
    ),
    (
        "sql_smuggling",
        re.compile(
            r"(;\s*(DROP|DELETE|UPDATE|INSERT|ALTER|COPY|ATTACH|DETACH|INSTALL|LOAD|PRAGMA)\b|"
            r";\s*SET\s+[A-Za-z_]+\s*(?:=|TO\b)|"
            r"\bUNION\s+ALL\s+SELECT\b.*read_)",
            re.IGNORECASE,
        ),
    ),
]


def _is_base_sensitive_col_name(col_name: str) -> bool:
    cl = (col_name or "").lower().strip().strip('"')
    if not cl:
        return False
    if any(sub in cl for sub in PII_SENSITIVE_COL_SUBSTRINGS):
        return True
    return bool(_NIS_TOKEN_RE.search(cl))


def _is_auditable_pii_alias(alias_name: str) -> bool:
    al = (alias_name or "").lower().strip().strip('"')
    if not al:
        return False
    if any(k in al for k in AUDITABLE_PII_ALIAS_KEYWORDS):
        return True
    return bool(_NIS_TOKEN_RE.search(al))


@dataclass
class TransparencyWarning:
    gap_id: str
    severity: str
    title: str
    message: str
    affected_tables: List[str]
    affected_years: List[int]
    available_years: List[int]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "gap_id": self.gap_id,
            "severity": self.severity,
            "title": self.title,
            "message": self.message,
            "affected_tables": self.affected_tables,
            "affected_years": self.affected_years,
            "available_years": self.available_years,
        }


@dataclass
class SQLValidationResult:
    is_valid: bool
    sanitized_sql: str = ""
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    referenced_tables: List[str] = field(default_factory=list)
    referenced_years: List[int] = field(default_factory=list)
    fontes_utilizadas: List[str] = field(default_factory=list)
    transparency_warnings: List[TransparencyWarning] = field(default_factory=list)
    limit_applied: bool = False


class SQLGuard:
    """
    Validador determinístico baseado em AST (sqlglot) para consultas DuckDB.
    """

    def __init__(
        self,
        allowed_tables: Optional[Set[str]] = None,
        table_years_map: Optional[Dict[str, List[int]]] = None,
        max_limit: int = 500,
        repaired_2023: bool = True,
        municipality_name: str = "Caeté",
        partial_years: Optional[Set[int]] = None,
    ) -> None:
        self.allowed_tables = {t.lower() for t in allowed_tables} if allowed_tables is not None else None
        self.table_years_map = {
            k.lower(): sorted(v) for k, v in (table_years_map or {}).items()
        }
        self.max_limit = max_limit
        self.repaired_2023 = repaired_2023
        self.municipality_name = municipality_name

        if partial_years is not None:
            self.partial_years: Set[int] = {int(y) for y in partial_years}
        else:
            all_yrs = [y for yrs in self.table_years_map.values() for y in yrs]
            if all_yrs:
                max_yr = max(all_yrs)
                if max_yr >= 2026:
                    self.partial_years = {max_yr}
                elif max_yr == 2025:
                    self.partial_years = {2025}
                else:
                    self.partial_years = set()
            else:
                is_caete_default = municipality_name.strip().lower() in ("caeté", "caete")
                self.partial_years = {2025} if is_caete_default else set()

    @staticmethod
    def clean_llm_sql(raw_text: str) -> str:
        """Remove blocos markdown ```sql ... ``` ou extrai SQL de texto bruto."""
        text = (raw_text or "").strip()
        fence_match = re.search(r"```(?:sql)?\s*(.*?)```", text, flags=re.DOTALL | re.IGNORECASE)
        if fence_match:
            text = fence_match.group(1).strip()
        return text.rstrip(";").strip()

    def validate_and_rewrite(
        self,
        raw_sql: str,
        enforce_limit: bool = True,
        role: str = "citizen",
    ) -> SQLValidationResult:
        sql = self.clean_llm_sql(raw_sql)
        if not sql:
            return SQLValidationResult(
                is_valid=False,
                error_code="EMPTY_SQL",
                error_message="A consulta SQL está vazia.",
            )

        # 1. Verificação léxica rápida para palavras-chave de sistema/administração do DuckDB
        forbidden_kw = re.search(
            r"^\s*(PRAGMA|INSTALL|LOAD|ATTACH|DETACH|COPY|EXPORT|IMPORT|CALL|SET|RESET|VACUUM|CHECKPOINT)\b",
            sql,
            flags=re.IGNORECASE,
        )
        if forbidden_kw:
            return SQLValidationResult(
                is_valid=False,
                error_code="FORBIDDEN_STATEMENT",
                error_message=f"Comando administrativo/sistema bloqueado pelo SQLGuard: {forbidden_kw.group(1).upper()}.",
            )

        # 2. Parsing AST via sqlglot (dialeto duckdb)
        try:
            statements = sqlglot.parse(sql, read="duckdb")
        except Exception as exc:
            return SQLValidationResult(
                is_valid=False,
                error_code="SQL_SYNTAX_ERROR",
                error_message=f"Erro de sintaxe SQL detectado pelo parser AST: {exc}",
            )

        statements = [stmt for stmt in statements if stmt is not None]
        if len(statements) != 1:
            return SQLValidationResult(
                is_valid=False,
                error_code="MULTIPLE_STATEMENTS",
                error_message="Múltiplas instruções SQL na mesma chamada são proibidas.",
            )

        root = statements[0]

        # 3. Somente SELECT / UNION / INTERSECT / EXCEPT são permitidos na raiz
        if not isinstance(root, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
            return SQLValidationResult(
                is_valid=False,
                error_code="NON_SELECT_STATEMENT",
                error_message=f"Apenas consultas de leitura (SELECT/WITH) são permitidas. Tipo recebido: {type(root).__name__}.",
            )

        # 4. Proibir qualquer nó DML/DDL embutido na AST
        forbidden_nodes = (
            exp.Insert,
            exp.Update,
            exp.Delete,
            exp.Drop,
            exp.Create,
            exp.Alter,
            exp.Command,
            exp.Copy,
            exp.Pragma,
            exp.Set,
            exp.LoadData,
        )
        for node in root.walk():
            if isinstance(node, forbidden_nodes):
                return SQLValidationResult(
                    is_valid=False,
                    error_code="FORBIDDEN_AST_NODE",
                    error_message=f"Operação não autorizada na AST: {type(node).__name__}.",
                )

        # 5. Bloquear CTEs recursivas (proteção anti-DoS / CPU-memory bomb) e identificar CTEs normais
        if re.search(r"\bWITH\s+RECURSIVE\b", sql, flags=re.IGNORECASE):
            return SQLValidationResult(
                is_valid=False,
                error_code="RECURSIVE_CTE_BLOCKED",
                error_message="CTEs recursivas (WITH RECURSIVE) são bloqueadas por proteção contra exaustão de CPU/memória.",
            )
        for with_node in root.find_all(exp.With):
            if with_node.args.get("recursive"):
                return SQLValidationResult(
                    is_valid=False,
                    error_code="RECURSIVE_CTE_BLOCKED",
                    error_message="CTEs recursivas (WITH RECURSIVE) são bloqueadas por proteção contra exaustão de CPU/memória.",
                )

        # Validar identificadores de CTEs contra shadowing de paths externos, prefixos de sistema ou tabelas reais
        for cte in root.find_all(exp.CTE):
            alias_raw = (cte.alias_or_name or "").strip()
            alias_low = alias_raw.lower().strip('"')
            if not alias_low or not re.fullmatch(r"[a-z_][a-z0-9_]*", alias_low):
                return SQLValidationResult(
                    is_valid=False,
                    error_code="INVALID_CTE_IDENTIFIER",
                    error_message=f"Identificador de CTE inválido ou potencialmente malicioso bloqueado: '{alias_raw}'.",
                )
            if alias_low.startswith(BLOCKED_TABLE_PREFIXES) or (
                self.allowed_tables is not None and alias_low in self.allowed_tables
            ):
                return SQLValidationResult(
                    is_valid=False,
                    error_code="CTE_SHADOWING_BLOCKED",
                    error_message=f"CTE '{alias_raw}' colide com tabela oficial do catálogo ou prefixo reservado.",
                )

        # Mapear exatamente quais exp.Table pertencem ao escopo léxico real de uma CTE (evitando CTE scope-shadowing)
        scoped_cte_table_ids: Set[int] = set()
        try:
            for scope_obj in traverse_scope(root):
                local_ctes = {k.lower().strip('"') for k in (scope_obj.cte_sources or {}).keys()}
                if not local_ctes:
                    continue
                for tb_in_scope in scope_obj.tables:
                    t_n = (tb_in_scope.name or "").lower().strip('"')
                    t_d = (tb_in_scope.db or "").lower().strip('"')
                    t_c = (tb_in_scope.catalog or "").lower().strip('"')
                    if t_n in local_ctes and not t_d and not t_c:
                        scoped_cte_table_ids.add(id(tb_in_scope))
        except Exception:
            scoped_cte_table_ids = set()

        # 6. Inspecionar todas as funções e expressões UNNEST/EXPLODE na AST contra funções de leitura/escrita/explosão
        if any(True for _ in root.find_all(exp.Unnest)) or any(True for _ in root.find_all(exp.Explode)):
            return SQLValidationResult(
                is_valid=False,
                error_code="BLOCKED_FUNCTION",
                error_message="Expressão tabular 'unnest' bloqueada por proteção contra amplificação de memória.",
            )
        for func_node in root.find_all(exp.Func):
            func_name = (func_node.sql_name() or "").lower()
            if isinstance(func_node, exp.Anonymous):
                func_name = (func_node.name or "").lower()
            # Proteção contra amplificação de strings (`lpad`/`rpad`/`repeat` com comprimento > 256 ou dinâmico abusivo)
            if isinstance(func_node, exp.Pad) or func_name in ("pad", "lpad", "rpad", "repeat"):
                len_arg = func_node.args.get("expression") or (
                    func_node.expressions[1] if getattr(func_node, "expressions", None) and len(func_node.expressions) >= 2 else None
                )
                len_ok = False
                if isinstance(len_arg, exp.Literal) and len_arg.is_int:
                    try:
                        len_ok = 0 <= int(len_arg.this) <= 256
                    except Exception:
                        len_ok = False
                if not len_ok:
                    return SQLValidationResult(
                        is_valid=False,
                        error_code="BLOCKED_FUNCTION",
                        error_message=f"Função '{func_name}' com comprimento > 256 ou não-literal bloqueada contra amplificação de memória.",
                    )
            if func_name in ("printf", "format"):
                fmt_arg = func_node.this or (func_node.expressions[0] if getattr(func_node, "expressions", None) else None)
                fmt_str = fmt_arg.name if isinstance(fmt_arg, exp.Literal) else (fmt_arg.sql() if fmt_arg is not None else "")
                if (
                    re.search(r"%[+\-0 #]*(?:\*|\d{3,})", fmt_str)
                    or re.search(r"\{:[^}]*\d{3,}", fmt_str)
                    or re.search(r"\{[^}]*:[^}]*\d{3,}", fmt_str)
                ):
                    return SQLValidationResult(
                        is_valid=False,
                        error_code="BLOCKED_FUNCTION",
                        error_message=f"Função '{func_name}' com especificador de largura elevado/dinâmico bloqueada contra amplificação de memória.",
                    )
            if (
                func_name in BLOCKED_FUNCTIONS
                or func_name.startswith("read_")
                or func_name.startswith("parquet_")
                or func_name.startswith("duckdb_")
                or func_name.startswith("pragma_")
                or func_name.startswith("sqlite_")
                or func_name.startswith("postgres_")
                or func_name.startswith("json_execute")
                or func_name.startswith("sniff_")
                or func_name.startswith("enable_")
                or func_name.startswith("disable_")
                or func_name.startswith("force_")
            ):
                return SQLValidationResult(
                    is_valid=False,
                    error_code="BLOCKED_FUNCTION",
                    error_message=f"Função de sistema/arquivo bloqueada por segurança: '{func_name}'.",
                )

        # 7. Extrair e validar todas as tabelas referenciadas (bloqueando paths, extensões e table-functions ANTES do skip de CTE)
        base_tables: List[str] = []
        for tbl_node in root.find_all(exp.Table):
            tbl_name = (tbl_node.name or "").lower().strip('"')
            tbl_db = (tbl_node.db or "").lower().strip('"')
            tbl_catalog = (tbl_node.catalog or "").lower().strip('"')

            if tbl_db in BLOCKED_SCHEMAS or tbl_catalog in BLOCKED_SCHEMAS:
                return SQLValidationResult(
                    is_valid=False,
                    error_code="BLOCKED_SCHEMA",
                    error_message=f"Acesso ao schema de sistema '{tbl_db or tbl_catalog}' é proibido.",
                )

            if not tbl_name:
                inner = tbl_node.this
                fn_label = ""
                if isinstance(inner, exp.Anonymous):
                    fn_label = (inner.name or "").lower()
                elif isinstance(inner, exp.Func):
                    fn_label = (inner.sql_name() or "").lower()
                return SQLValidationResult(
                    is_valid=False,
                    error_code="BLOCKED_FUNCTION",
                    error_message=f"Uso de função tabular como fonte em FROM/JOIN bloqueado por segurança: '{fn_label or tbl_node.sql()}'.",
                )

            if (
                tbl_name.startswith(BLOCKED_TABLE_PREFIXES)
                or "/" in tbl_name
                or "\\" in tbl_name
                or ":" in tbl_name
                or ".." in tbl_name
                or tbl_name.endswith((".csv", ".parquet", ".json", ".jsonl", ".db", ".duckdb", ".sqlite", ".txt", ".env", ".gz"))
            ):
                return SQLValidationResult(
                    is_valid=False,
                    error_code="BLOCKED_TABLE_OR_PATH",
                    error_message=f"Referência a tabela de sistema ou arquivo externo bloqueada: '{tbl_name}'.",
                )

            if id(tbl_node) in scoped_cte_table_ids:
                continue

            if self.allowed_tables is not None and tbl_name not in self.allowed_tables:
                return SQLValidationResult(
                    is_valid=False,
                    error_code="UNKNOWN_OR_UNAUTHORIZED_TABLE",
                    error_message=(
                        f"A tabela '{tbl_name}' não existe no catálogo autorizado deste município. "
                        f"Consulte apenas tabelas válidas do catálogo."
                    ),
                )

            if tbl_name not in base_tables:
                base_tables.append(tbl_name)

        if not base_tables:
            return SQLValidationResult(
                is_valid=False,
                error_code="NO_BASE_TABLE",
                error_message="A consulta deve referenciar pelo menos uma tabela do catálogo de dados públicos.",
            )

        # 7B. Proteção Semântica Anti-Fan-Out: impedir JOIN direto redundante entre Gold View pré-unificada e sua tabela-fato bruta no mesmo bloco SELECT
        gold_view_raw_children = {
            "vw_gold_frota_consolidada": {"frota_gastofrota", "frota_frota"},
            "vw_gold_folha_servidores": {"remuneracao_municipio", "pessoa_municipio"},
            "vw_gold_contratos_vigencia_aditivos": {"contrato_termocontrato", "contrato_contcontrato", "contrato_contratos"},
            "vw_gold_execucao_despesa_credor": {
                "empenho_empenho",
                "empenho_credorempenho",
                "despesa_liquidacao",
                "despesa_pagamento",
            },
            "vw_gold_orcamento_previsto_vs_realizado": {"receita_receita", "despesa_despesa"},
            "vw_gold_orcamento_por_orgao": {"receita_receita", "despesa_despesa", "orgao_orgao"},
            "vw_gold_fornecedores_qsa_360": {
                "despesa_pagamento",
                "licitacao_quadrosoclicitacao",
                "licitacao_hablicitacao",
            },
        }
        for sel_node in root.find_all(exp.Select):
            direct_tables_in_sel = set()
            from_clause = sel_node.args.get("from") or sel_node.args.get("from_")
            if from_clause is not None and isinstance(from_clause.this, exp.Table):
                if id(from_clause.this) not in scoped_cte_table_ids:
                    t_nm = (from_clause.this.name or "").lower()
                    if t_nm:
                        direct_tables_in_sel.add(t_nm)
            for j_node in sel_node.args.get("joins") or []:
                if isinstance(j_node.this, exp.Table):
                    if id(j_node.this) not in scoped_cte_table_ids:
                        t_nm = (j_node.this.name or "").lower()
                        if t_nm:
                            direct_tables_in_sel.add(t_nm)
            for gv_name, raw_set in gold_view_raw_children.items():
                overlap = direct_tables_in_sel.intersection(raw_set)
                if gv_name in direct_tables_in_sel and overlap:
                    bad_raw = ", ".join(sorted(overlap))
                    return SQLValidationResult(
                        is_valid=False,
                        error_code="REDUNDANT_GOLD_VIEW_JOIN",
                        error_message=(
                            f"ERRO SEMÂNTICO DE FAN-OUT (PRODUTO CARTESIANO): A Gold View '{gv_name}' JÁ INCORPORA "
                            f"os registros de ({bad_raw})! Fazer JOIN direto entre '{gv_name}' e ({bad_raw}) no mesmo SELECT "
                            f"multiplica as somas indevidamente. Reescreva consultando APENAS '{gv_name}' "
                            f"(ou, se precisar de `dsc_funcao`/`dsc_acao` ausentes da Gold View, consulte diretamente as tabelas "
                            f"base ou filtre via `WHERE EXISTS (SELECT 1 FROM ...)` sem JOIN direto)."
                        ),
                    )

        # 7C. Proteção Anti-Evasão de Mascaramento LGPD para perfil público ('citizen' e perfis não privilegiados)
        if role not in ("admin", "auditor", "controller"):
            pii_block = self._validate_pii_anti_evasion(root)
            if pii_block is not None:
                return pii_block

        # 8. Aplicar LIMIT máximo de proteção em SELECT, UNION, INTERSECT, EXCEPT e GROUP BY
        limit_applied = False
        if enforce_limit and self.max_limit > 0 and isinstance(root, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
            is_scalar_agg = False
            if isinstance(root, exp.Select):
                has_agg = any(
                    isinstance(n, exp.AggFunc)
                    for expr in root.expressions
                    for n in expr.walk()
                )
                has_group = root.args.get("group") is not None
                is_scalar_agg = has_agg and not has_group

            current_limit = root.args.get("limit")
            if current_limit is not None:
                cur_lim_sql = current_limit.sql(dialect="duckdb").upper()
                if current_limit.args.get("percent") or "PERCENT" in cur_lim_sql:
                    return SQLValidationResult(
                        is_valid=False,
                        error_code="PERCENT_LIMIT_BLOCKED",
                        error_message="Cláusula LIMIT ... PERCENT é bloqueada por proteção contra varredura integral de memória.",
                    )
                else:
                    lim_expr = current_limit.expression
                    lim_raw = (lim_expr.sql(dialect="duckdb") if lim_expr is not None else "").strip().upper()
                    try:
                        limit_val = int(lim_raw)
                        if limit_val <= 0 or limit_val > self.max_limit:
                            root = root.limit(self.max_limit)
                            limit_applied = True
                    except Exception:
                        root = root.limit(self.max_limit)
                        limit_applied = True
            elif not is_scalar_agg:
                root = root.limit(self.max_limit)
                limit_applied = True

        sanitized_sql = root.sql(dialect="duckdb")

        # 9. Extrair anos e linhagem determinística de fontes (fontes_utilizadas)
        years = self._extract_years_from_ast(root)
        fontes = self._resolve_source_files(base_tables, years)

        # 10. Detectar lacunas de transparência determinísticas (dinâmicas por município)
        warnings = self._detect_transparency_gaps(base_tables, years)

        return SQLValidationResult(
            is_valid=True,
            sanitized_sql=sanitized_sql,
            referenced_tables=base_tables,
            referenced_years=years,
            fontes_utilizadas=fontes,
            transparency_warnings=warnings,
            limit_applied=limit_applied,
        )

    @staticmethod
    def _is_col_strictly_inside_count(col_node: exp.Column, boundary: exp.Expression) -> bool:
        """
        Retorna True se `col_node` estiver estritamente envolvido por `exp.Count`
        (ex.: `COUNT(num_cpf)`, `COUNT(DISTINCT num_cpf)` ou `ROUND(COUNT(DISTINCT num_cpf) * 100.0 / COUNT(*), 2)`),
        sem `CASE`/`IF` intermediários que permitam oráculo booleano dígito-a-dígito (`SEC-P2-04`).
        Também permite `COUNT(*) FILTER (WHERE LENGTH(col) = 11)` onde apenas `LENGTH` da coluna é testado.
        """
        curr: Optional[exp.Expression] = col_node.parent
        saw_length = False
        while curr is not None:
            if isinstance(curr, exp.Count):
                return True
            if isinstance(curr, exp.Distinct):
                curr = curr.parent
                continue
            if isinstance(curr, exp.Length):
                saw_length = True
                curr = curr.parent
                continue
            if saw_length and isinstance(curr, exp.Filter) and isinstance(curr.this, exp.Count):
                return True
            if saw_length and isinstance(curr, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.And, exp.Or, exp.Where, exp.Paren)):
                curr = curr.parent
                continue
            # Qualquer outro nó (CASE, SUBSTR, CAST, operador binário fora de COUNT) invalida o caminho até COUNT
            return False
        return False

    def _validate_pii_anti_evasion(self, root: exp.Expression) -> Optional[SQLValidationResult]:
        """
        Step 7C (LGPD 2.0):
        1. Rastreia todas as colunas sensíveis (`PII_SENSITIVE_COL_SUBSTRINGS` + token `nis`) e propaga aliases derivados
           em CTEs e subqueries (ponto fixo).
        2. Bloqueia fatiamento/extração (`SUBSTR`/`SUBSTRING`/`LEFT`/`RIGHT`/`REGEXP_EXTRACT`/`SPLIT_PART`/hashes) sobre colunas sensíveis
           em qualquer parte da AST.
        3. Bloqueia `LIKE`/`ILIKE`/`SIMILAR TO`/`REGEXP` sobre colunas sensíveis em `WHERE`/`HAVING` (e predicados correlatos).
        4. Permite cálculos aritméticos como `ROUND(COUNT(DISTINCT num_cpf) * 100.0 / COUNT(*), 2)` quando todas as referências
           sensíveis na projeção estiverem estritamente envolvidas por `exp.Count` (`SEC-P2-04`).
        """
        derived_pii_aliases: Set[str] = set()

        def _is_sensitive_col(col_name: str) -> bool:
            cl = (col_name or "").lower().strip().strip('"')
            return _is_base_sensitive_col_name(cl) or cl in derived_pii_aliases

        # 1. Ponto fixo para propagar aliases derivados de colunas sensíveis em CTEs (incluindo aliases posicionais), subqueries e UNIONs
        select_nodes = list(root.find_all(exp.Select))
        cte_nodes = list(root.find_all(exp.CTE))
        subquery_nodes = list(root.find_all(exp.Subquery))
        setop_nodes = list(root.find_all((exp.Union, exp.Intersect, exp.Except)))

        def _proj_touches_sensitive(proj_node: exp.Expression) -> bool:
            inner_p = proj_node.this if isinstance(proj_node, exp.Alias) else proj_node
            p_cols = [c for c in inner_p.find_all(exp.Column) if _is_sensitive_col(c.name or "")]
            if not p_cols:
                return False
            return not all(self._is_col_strictly_inside_count(c, inner_p) for c in p_cols)

        changed = True
        while changed:
            changed = False
            for sel in select_nodes:
                for proj in sel.expressions:
                    if not _proj_touches_sensitive(proj):
                        continue
                    out_alias = (proj.alias_or_name or "").lower().strip().strip('"')
                    if out_alias and out_alias not in derived_pii_aliases:
                        derived_pii_aliases.add(out_alias)
                        changed = True

            # Propaga aliases posicionais em `WITH c(x) AS (...)` e `FROM (...) AS sub(x)`
            for container in cte_nodes + subquery_nodes:
                tbl_alias = container.args.get("alias")
                col_aliases = list(getattr(tbl_alias, "columns", None) or []) if tbl_alias else []
                if not col_aliases:
                    continue
                body = container.this
                branch_selects = list(body.find_all(exp.Select)) if body is not None else []
                for b_sel in branch_selects:
                    for idx_c, c_alias_node in enumerate(col_aliases):
                        if idx_c < len(b_sel.expressions) and _proj_touches_sensitive(b_sel.expressions[idx_c]):
                            pos_name = (c_alias_node.name or "").lower().strip().strip('"')
                            if pos_name and pos_name not in derived_pii_aliases:
                                derived_pii_aliases.add(pos_name)
                                changed = True

            # Propaga sensibilidade posicional entre ramos de `UNION / INTERSECT / EXCEPT`
            for sop in setop_nodes:
                left_sels = [sop.this] if isinstance(sop.this, exp.Select) else list(sop.this.find_all(exp.Select)) if sop.this else []
                right_sels = [sop.expression] if isinstance(sop.expression, exp.Select) else list(sop.expression.find_all(exp.Select)) if sop.expression else []
                for l_sel in left_sels:
                    for r_sel in right_sels:
                        n_proj = min(len(l_sel.expressions), len(r_sel.expressions))
                        for idx_p in range(n_proj):
                            l_sens = _proj_touches_sensitive(l_sel.expressions[idx_p])
                            r_sens = _proj_touches_sensitive(r_sel.expressions[idx_p])
                            if l_sens or r_sens:
                                for side_proj in (l_sel.expressions[idx_p], r_sel.expressions[idx_p]):
                                    s_alias = (side_proj.alias_or_name or "").lower().strip().strip('"')
                                    if s_alias and s_alias not in derived_pii_aliases:
                                        derived_pii_aliases.add(s_alias)
                                        changed = True

        # 2. Bloquear indexação/fatiamento por colchetes (`col[1:3]`, `col[1]`) e funções de extração/codificação sobre colunas sensíveis
        for br_node in root.find_all(exp.Bracket):
            br_cols = [
                (c.name or "").lower().strip('"')
                for c in br_node.find_all(exp.Column)
                if _is_sensitive_col(c.name or "")
            ]
            if br_cols:
                return SQLValidationResult(
                    is_valid=False,
                    error_code="PII_MASKING_EVASION_BLOCKED",
                    error_message=(
                        f"Indexação/fatiamento por colchetes sobre coluna protegida pela LGPD ({br_cols[0]}) "
                        "bloqueada para perfil público."
                    ),
                )

        slicing_expr_types = (
            exp.Substring,
            exp.Left,
            exp.Right,
            exp.RegexpExtract,
            exp.SplitPart,
        )
        slicing_func_names = {
            "substr",
            "substring",
            "left",
            "right",
            "regexp_extract",
            "regexp_extract_all",
            "regexp_replace",
            "split_part",
            "string_split",
            "string_to_array",
            "str_split",
            "array_slice",
            "list_slice",
            "ascii",
            "ord",
            "unicode",
            "strpos",
            "instr",
            "position",
            "strchr",
            "translate",
            "replace",
            "md5",
            "sha256",
            "sha1",
            "hash",
            "hex",
            "to_hex",
            "encode",
            "base64",
            "to_base64",
            "reverse",
        }
        for fn_node in root.find_all(exp.Func):
            fn_name = (fn_node.name if isinstance(fn_node, exp.Anonymous) else fn_node.sql_name() or "").lower()
            if isinstance(fn_node, slicing_expr_types) or fn_name in slicing_func_names:
                sens_cols = [
                    (c.name or "").lower().strip('"')
                    for c in fn_node.find_all(exp.Column)
                    if _is_sensitive_col(c.name or "")
                ]
                if sens_cols:
                    return SQLValidationResult(
                        is_valid=False,
                        error_code="PII_MASKING_EVASION_BLOCKED",
                        error_message=(
                            f"Função de fatiamento/extração '{fn_name or type(fn_node).__name__}' sobre coluna protegida pela LGPD "
                            f"({sens_cols[0]}) bloqueada para perfil público."
                        ),
                    )

        # 3. Bloquear LIKE / ILIKE / SIMILAR TO / REGEXP e busca binária lexicográfica (>, >=, <, <=, BETWEEN) sobre colunas sensíveis em WHERE / HAVING / FILTER
        pattern_pred_types = tuple(
            t
            for t in (
                getattr(exp, "Like", None),
                getattr(exp, "ILike", None),
                getattr(exp, "SimilarTo", None),
                getattr(exp, "RegexpLike", None),
                getattr(exp, "RegexpIMatch", None),
                getattr(exp, "RegexpFullMatch", None),
                getattr(exp, "Glob", None),
            )
            if t is not None
        )
        range_pred_types = (exp.GT, exp.GTE, exp.LT, exp.LTE, exp.Between)
        pattern_func_names = {
            "regexp_matches",
            "regexp_full_match",
            "regexp_like",
            "starts_with",
            "ends_with",
            "prefix",
            "suffix",
            "contains",
        }
        for clause in list(root.find_all(exp.Where)) + list(root.find_all(exp.Having)) + list(root.find_all(exp.Filter)):
            for pnode in clause.walk():
                is_pat = isinstance(pnode, pattern_pred_types)
                if not is_pat and isinstance(pnode, exp.Func):
                    p_fn = (pnode.name if isinstance(pnode, exp.Anonymous) else pnode.sql_name() or "").lower()
                    is_pat = p_fn in pattern_func_names
                if is_pat:
                    sens_cols = [
                        (c.name or "").lower().strip('"')
                        for c in pnode.find_all(exp.Column)
                        if _is_sensitive_col(c.name or "")
                    ]
                    if sens_cols:
                        return SQLValidationResult(
                            is_valid=False,
                            error_code="PII_MASKING_EVASION_BLOCKED",
                            error_message=(
                                f"Filtro de padrão parcial (LIKE/ILIKE/SIMILAR TO/REGEXP) sobre coluna sensível ({sens_cols[0]}) "
                                "em WHERE/HAVING bloqueado pela LGPD para evitar inferência de CPF."
                            ),
                        )
                if isinstance(pnode, range_pred_types):
                    # Permite LENGTH(num_doc_credor) > 11, mas bloqueia num_cpf > '123' / num_cpf BETWEEN '100' AND '200'
                    bare_range_cols = [
                        (c.name or "").lower().strip('"')
                        for c in pnode.find_all(exp.Column)
                        if _is_sensitive_col(c.name or "") and not isinstance(c.parent, exp.Length)
                    ]
                    if bare_range_cols:
                        return SQLValidationResult(
                            is_valid=False,
                            error_code="PII_MASKING_EVASION_BLOCKED",
                            error_message=(
                                f"Comparação de intervalo/desigualdade sobre coluna sensível ({bare_range_cols[0]}) "
                                "em WHERE/HAVING bloqueada pela LGPD para impedir inferência por busca binária."
                            ),
                        )

        # 4. Inspecionar todas as projeções SELECT contra renomeação não-auditável ou transformação de PII fora de COUNT
        passthrough_agg_types = tuple(
            t for t in (exp.Max, exp.Min, getattr(exp, "AnyValue", None)) if t is not None
        )
        for sel_node in select_nodes:
            for proj in sel_node.expressions:
                inner_expr = proj.this if isinstance(proj, exp.Alias) else proj
                if isinstance(inner_expr, exp.Column):
                    col_n = (inner_expr.name or "").lower().strip('"')
                    alias_n = (proj.alias if isinstance(proj, exp.Alias) and proj.alias else col_n).lower().strip('"')
                    if _is_sensitive_col(col_n) and not _is_auditable_pii_alias(alias_n):
                        return SQLValidationResult(
                            is_valid=False,
                            error_code="PII_MASKING_EVASION_BLOCKED",
                            error_message="Renomeação de coluna sensível (CPF/NIS/Documento) para alias não-auditável bloqueada (LGPD).",
                        )
                    continue

                sens_col_nodes = [
                    c for c in inner_expr.find_all(exp.Column) if _is_sensitive_col(c.name or "")
                ]
                if not sens_col_nodes:
                    continue

                # SEC-P2-04: Permite expressões matemáticas como ROUND(COUNT(DISTINCT num_cpf) * 100.0 / COUNT(*), 2)
                # quando TODAS as referências à coluna sensível estão estritamente envolvidas por exp.Count
                if all(self._is_col_strictly_inside_count(c, inner_expr) for c in sens_col_nodes):
                    continue

                # Permite MAX(num_doc_credor) / MIN(num_doc_credor) / COALESCE(num_doc_credor, 'N/D') APENAS se sem transformação
                # da coluna interna e com alias de saída explicitamente auditável para mascaramento pós-query
                if (
                    isinstance(inner_expr, passthrough_agg_types)
                    and isinstance(inner_expr.this, exp.Column)
                ):
                    col_n = (inner_expr.this.name or "").lower().strip('"')
                    alias_n = (proj.alias if isinstance(proj, exp.Alias) and proj.alias else "").lower().strip('"')
                    if _is_auditable_pii_alias(alias_n):
                        continue

                if isinstance(inner_expr, exp.Coalesce) and len(sens_col_nodes) == 1:
                    all_args = [inner_expr.this, *(inner_expr.expressions or [])]
                    if all(isinstance(a, (exp.Column, exp.Literal, exp.Null)) for a in all_args if a is not None):
                        alias_n = (proj.alias if isinstance(proj, exp.Alias) and proj.alias else "").lower().strip('"')
                        if _is_auditable_pii_alias(alias_n):
                            continue

                first_col = (sens_col_nodes[0].name or "").lower().strip('"')
                return SQLValidationResult(
                    is_valid=False,
                    error_code="PII_MASKING_EVASION_BLOCKED",
                    error_message=(
                        f"Transformação/fatiamento direto sobre coluna protegida pela LGPD ({first_col}) "
                        "bloqueado para perfil público. Selecione a coluna nominalmente para mascaramento padrão."
                    ),
                )

        return None

    def _extract_years_from_ast(self, root: exp.Expression) -> List[int]:
        """
        Extrai de forma determinística todos os anos (2018..2035) filtrados na AST,
        resolvendo igualdades (=), listas (IN), intervalos (BETWEEN) e desigualdades (>=, <=, >, <),
        exigindo identificador exato de coluna em `YEAR_COLUMNS` (sem falsos positivos como `plano` ou `dano`).
        """
        explicit_years: Set[int] = set()
        catalog_years = [y for yrs in self.table_years_map.values() for y in yrs]
        default_max_yr = max([2026, *catalog_years]) if catalog_years else 2026

        def _is_negated(node: exp.Expression) -> bool:
            p = node.parent
            while p is not None and not isinstance(p, (exp.Select, exp.Where, exp.Having)):
                if isinstance(p, exp.Not):
                    return True
                p = p.parent
            return False

        def _has_exact_year_col(node: Optional[exp.Expression]) -> bool:
            if node is None:
                return False
            for col in node.find_all(exp.Column):
                if (col.name or "").lower().strip('"') in YEAR_COLUMNS:
                    return True
            return False

        def _extract_int_year(node: Optional[exp.Expression]) -> Optional[int]:
            if node is None or isinstance(node, exp.Column):
                return None
            raw = node.name if isinstance(node, exp.Literal) and node.name else node.sql()
            raw = raw.strip("'\" ")
            if re.fullmatch(r"20(?:1[89]|2\d|3[0-5])", raw):
                return int(raw)
            return None

        # Intervalos BETWEEN: ex. ano_referencia BETWEEN 2019 AND 2026
        for bw in root.find_all(exp.Between):
            if _is_negated(bw) or not _has_exact_year_col(bw.this):
                continue
            low = _extract_int_year(bw.args.get("low"))
            high = _extract_int_year(bw.args.get("high"))
            if low and high and low <= high:
                for y in range(max(2018, low), min(2035, high) + 1):
                    explicit_years.add(y)

        # Comparações binárias (=, >=, >, <=, <) envolvendo colunas exatas de ano
        lower_bound: Optional[int] = None
        upper_bound: Optional[int] = None

        for cmp_node in root.find_all((exp.EQ, exp.GTE, exp.GT, exp.LTE, exp.LT)):
            if _is_negated(cmp_node):
                continue
            left_is_yr = _has_exact_year_col(cmp_node.left)
            right_is_yr = _has_exact_year_col(cmp_node.right)
            if not (left_is_yr or right_is_yr):
                continue

            yr = _extract_int_year(cmp_node.right if left_is_yr else cmp_node.left)
            if yr is None:
                continue

            if isinstance(cmp_node, exp.EQ):
                explicit_years.add(yr)
            elif isinstance(cmp_node, exp.GTE):
                if left_is_yr:
                    lower_bound = yr if lower_bound is None else max(lower_bound, yr)
                else:
                    upper_bound = yr if upper_bound is None else min(upper_bound, yr)
            elif isinstance(cmp_node, exp.GT):
                if left_is_yr:
                    lower_bound = (yr + 1) if lower_bound is None else max(lower_bound, yr + 1)
                else:
                    upper_bound = (yr - 1) if upper_bound is None else min(upper_bound, yr - 1)
            elif isinstance(cmp_node, exp.LTE):
                if left_is_yr:
                    upper_bound = yr if upper_bound is None else min(upper_bound, yr)
                else:
                    lower_bound = yr if lower_bound is None else max(lower_bound, yr)
            elif isinstance(cmp_node, exp.LT):
                if left_is_yr:
                    upper_bound = (yr - 1) if upper_bound is None else min(upper_bound, yr - 1)
                else:
                    lower_bound = (yr + 1) if lower_bound is None else max(lower_bound, yr + 1)

        if lower_bound is not None or upper_bound is not None:
            lb = lower_bound if lower_bound is not None else min(2020, upper_bound or 2020)
            ub = upper_bound if upper_bound is not None else max(default_max_yr, lower_bound or default_max_yr)
            for y in range(max(2018, lb), min(2035, ub) + 1):
                explicit_years.add(y)

        # Cláusulas IN: ex. ano_referencia IN (2022, 2023, 2026)
        for in_node in root.find_all(exp.In):
            if _is_negated(in_node) or not _has_exact_year_col(in_node.this):
                continue
            for expr in in_node.expressions:
                yr = _extract_int_year(expr)
                if yr:
                    explicit_years.add(yr)

        return sorted(explicit_years)

    def _resolve_source_files(self, base_tables: List[str], queried_years: List[int]) -> List[str]:
        """
        Mapeia de forma exata (sem substring ambígua nem I/O desnecessário) as tabelas
        e anos consultados para os arquivos CSV de origem.
        """
        sources: List[str] = []
        for tbl in base_tables:
            available_years = self.table_years_map.get(tbl, [2020, 2021, 2022, 2023, 2024, 2025])
            if queried_years:
                matched_years = [y for y in queried_years if y in available_years]
                if not matched_years:
                    for y in queried_years:
                        candidate = f"{tbl}_{y}.csv (ausente na base pública)"
                        if candidate not in sources:
                            sources.append(candidate)
                else:
                    for y in matched_years:
                        fname = f"{tbl}_{y}.csv"
                        if fname not in sources:
                            sources.append(fname)
            else:
                for y in available_years:
                    fname = f"{tbl}_{y}.csv"
                    if fname not in sources:
                        sources.append(fname)
        return sources

    def _detect_transparency_gaps(
        self,
        base_tables: List[str],
        queried_years: List[int],
    ) -> List[TransparencyWarning]:
        warnings: List[TransparencyWarning] = []
        is_caete = self.municipality_name.strip().lower() in ("caeté", "caete")

        if "remuneracao_municipio" in base_tables or "vw_gold_folha_servidores" in base_tables:
            avail_folha = sorted(
                self.table_years_map.get("remuneracao_municipio")
                or self.table_years_map.get("vw_gold_folha_servidores")
                or ([2023, 2024, 2025] if (not self.table_years_map and is_caete) else [])
            )
            if avail_folha and queried_years:
                min_folha = min(avail_folha)
                missing_folha_years = sorted(y for y in queried_years if y < min_folha)
                if missing_folha_years:
                    gap_code = (
                        "GAP-FOLHA-2020-2022"
                        if min_folha == 2023
                        else (f"GAP-FOLHA-2020-{min_folha - 1}" if min_folha > 2020 else f"GAP-FOLHA-PRE-{min_folha}")
                    )
                    period_label = (
                        f"2020–{min_folha - 1}" if min_folha > 2020 else f"anteriores a {min_folha}"
                    )
                    missing_str = ", ".join(str(y) for y in missing_folha_years)
                    avail_str = ", ".join(str(y) for y in avail_folha)
                    warnings.append(
                        TransparencyWarning(
                            gap_id=gap_code,
                            severity="HIGH",
                            title=f"Lacuna de Transparência Ativa — Folha de Pagamento ({period_label})",
                            message=(
                                f"Os pacotes de dados abertos do SICOM/TCE-MG para o município de {self.municipality_name} não contêm "
                                f"registros nominais de remuneração de servidores (remuneracao_municipio) nos exercícios consultados "
                                f"anteriores a {min_folha} ({missing_str}). Os dados deste município estão disponíveis a partir de "
                                f"{min_folha} (exercícios disponíveis: {avail_str}). "
                                f"Ausência de registros nesse período reflete lacuna de publicação na origem, e não gasto zero."
                            ),
                            affected_tables=["remuneracao_municipio", "pessoa_municipio", "vw_gold_folha_servidores"],
                            affected_years=missing_folha_years,
                            available_years=avail_folha,
                        )
                    )

        non_partial_tables = {
            "vw_gold_frota_consolidada",
            "frota_frota",
            "inep_ideb_educacao",
            "ibge_indicadores_municipais",
            "receita_qsa_fornecedores",
            "vw_gold_fornecedores_qsa_360",
            "orgao_municipio",
            "orgao_orgao",
            "orgao_orgaounidade",
        }
        for py in sorted(self.partial_years):
            flow_tables_py = [
                t
                for t in base_tables
                if t not in non_partial_tables
                and (not self.table_years_map or py in self.table_years_map.get(t, []))
            ]
            if flow_tables_py and (py in queried_years or not queried_years):
                avail_py = sorted(
                    {y for t in flow_tables_py for y in self.table_years_map.get(t, [2020, 2021, 2022, 2023, 2024, py])}
                )
                warnings.append(
                    TransparencyWarning(
                        gap_id=f"GAP-EXERCICIO-PARCIAL-{py}",
                        severity="INFO",
                        title=f"Nota Técnica — Exercício {py} em Execução Parcial",
                        message=(
                            f"A base pública do exercício de {py} em {self.municipality_name} reflete remessas parciais acumuladas "
                            f"até o fechamento do lote atual. Ao comparar totais de {py} com {py - 1} (12 meses fechados), "
                            f"considere a proporcionalidade mensal para evitar falsas conclusões de queda de arrecadação ou despesa."
                        ),
                        affected_tables=flow_tables_py[:4],
                        affected_years=[py],
                        available_years=avail_py or [py],
                    )
                )

        if is_caete and not self.repaired_2023:
            corrupted_used = sorted(TABLES_2023_CORRUPTED.intersection(base_tables))
            if corrupted_used and (not queried_years or 2023 in queried_years):
                warnings.append(
                    TransparencyWarning(
                        gap_id="GAP-EXPORT-2023",
                        severity="CRITICAL",
                        title="Alerta de Integridade — Exportação CSV 2023 sem Reparo de Delimitador",
                        message=(
                            "A consulta acessa tabelas de 2023 afetadas pela corrupção de delimitador pipe ('|') "
                            "nas descrições da EC 123/2022. Ative o banco reparado (repaired_2023=True) para evitar "
                            "subavaliação de R$ 11,75 milhões em pagamentos."
                        ),
                        affected_tables=corrupted_used,
                        affected_years=[2023],
                        available_years=[2020, 2021, 2022, 2023, 2024, 2025],
                    )
                )

        return warnings


# Regex sem \b para capturar CPFs colados em letras/underscores (ex.: 'JOAO12345678901SILVA') preservando CNPJs (14 dígitos ou '/')
_EMBEDDED_CPF_RE = re.compile(r"(?<![\d/])(\d{3})\.?(\d{3})\.?(\d{3})-?(\d{2})(?![\d/])")

# Padrões de CPF parcialmente mascarados na origem que expõem os 3 primeiros dígitos (impedindo reconstrução por cruzamento):
# - Padrão Betha: '046.244.***-**' ou '046244*****' -> '***.244.***-**'
_PARTIAL_BETHA_CPF_RE = re.compile(r"(?<![\d/])(\d{3})\.?(\d{3})\.?\*{3}-?(?:\*{2}|\d{2})(?![\d/])")
# - Padrão SICOM: '164***156**' ou '164.***.156-**' -> '***.***.156-**'
_PARTIAL_SICOM_CPF_RE = re.compile(r"(?<![\d/])(\d{3})\.?\*{3}\.?(\d{3})-?(?:\*{2}|\d{2})(?![\d/])")
# - Padrão 9 dígitos expostos: '046.244.156-**' -> '***.244.156-**'
_PARTIAL_9DIG_CPF_RE = re.compile(r"(?<![\d/])(\d{3})\.?(\d{3})\.?(\d{3})-?\*{2}(?![\d/])")
# - Padrão 3 primeiros dígitos expostos: '046.***.***-**' -> '***.***.***-**'
_PARTIAL_3DIG_CPF_RE = re.compile(r"(?<![\d/])(\d{3})\.?\*{3}\.?\*{3}-?(?:\*{2}|\d{2})(?![\d/])")


def _remask_partial_and_embedded_cpfs(text: str) -> str:
    """
    Re-mascara CPFs de 11 dígitos e qualquer CPF parcialmente mascarado que exponha os 3 primeiros dígitos
    (Betha `046.244.***-**` -> `***.244.***-**`; SICOM `164***156**` -> `***.***.156-**`).
    """
    out = text
    if "*" in out:
        out = _PARTIAL_9DIG_CPF_RE.sub(lambda m: f"***.{m.group(2)}.{m.group(3)}-**", out)
        out = _PARTIAL_BETHA_CPF_RE.sub(lambda m: f"***.{m.group(2)}.***-**", out)
        out = _PARTIAL_SICOM_CPF_RE.sub(lambda m: f"***.***.{m.group(2)}-**", out)
        out = _PARTIAL_3DIG_CPF_RE.sub("***.***.***-**", out)
    if _EMBEDDED_CPF_RE.search(out):
        out = _EMBEDDED_CPF_RE.sub(lambda m: f"***.{m.group(2)}.{m.group(3)}-**", out)
    return out


def mask_cpf_value(val: Any, is_sensitive_col: bool = False, strict_cpf_col: bool = False) -> Any:
    """
    Mascara CPFs (11 dígitos numéricos, formatados XXX.XXX.XXX-XX, inteiros/floats/Decimals resultantes de CAST,
    CPFs com zeros à esquerda, CPFs parcialmente mascarados que exponham os 3 primeiros dígitos — Betha `046.244.***-**`
    e SICOM `164***156**` — ou embutidos em strings/MEI), preservando CNPJs (14 dígitos) e raízes de CNPJ PJ (8 dígitos).
    """
    if val is None or isinstance(val, bool):
        return val

    min_int_cpf = 1 if strict_cpf_col else 100_000_000

    if isinstance(val, (int, float, Decimal)):
        if isinstance(val, float) and not math.isfinite(val):
            return val
        int_val: Optional[int] = None
        if isinstance(val, int):
            int_val = val
        elif isinstance(val, float) and val.is_integer():
            int_val = int(val)
        elif isinstance(val, Decimal) and val == val.to_integral_value():
            int_val = int(val)
        if int_val is not None and is_sensitive_col and min_int_cpf <= int_val <= 99_999_999_999:
            s = f"{int_val:011d}"
            return f"***.{s[3:6]}.{s[6:9]}-**"
        return val

    if isinstance(val, (list, tuple)):
        return [mask_cpf_value(v, is_sensitive_col=is_sensitive_col, strict_cpf_col=strict_cpf_col) for v in val]

    if isinstance(val, dict):
        return {k: mask_cpf_value(v, is_sensitive_col=is_sensitive_col, strict_cpf_col=strict_cpf_col) for k, v in val.items()}

    if not isinstance(val, str):
        return val

    stripped = val.strip()
    if re.fullmatch(r"\*{3}\.(?:\d{3}|\*{3})\.(?:\d{3}|\*{3})-\*{2}", stripped):
        return stripped

    # Detectar imediatamente padrões parciais exatos (Betha / SICOM) que expõem os 3 primeiros dígitos
    if "*" in stripped:
        d = re.sub(r"\D", "", stripped)
        if re.fullmatch(r"\d{3}\.?\d{3}\.?\*{3}-?\*{2}", stripped) and len(d) == 6:
            return "***." + d[3:6] + ".***-**"
        if re.fullmatch(r"\d{3}\.?\*{3}\.?\d{3}-?\*{2}", stripped) and len(d) == 6:
            return "***.***." + d[3:6] + "-**"
        remasked = _remask_partial_and_embedded_cpfs(val)
        if remasked != val:
            return remasked

    digits = re.sub(r"\D", "", stripped)
    # Se for exatamente um CNPJ de 14 dígitos, preserva integralmente (LAI)
    if len(digits) == 14 and len(stripped) in (14, 18):
        return val

    # Raiz de CNPJ (8 dígitos, ex.: 03103468 ou 03.103.468) em coluna genérica de credor/documento NÃO é CPF
    if not strict_cpf_col and len(digits) == 8 and re.fullmatch(r"\d{2}\.?\d{3}\.?\d{3}", stripped):
        return val

    # Tratar strings numéricas com sufixo decimal '.0' oriundas de serialização float/CAST
    min_digits = 1 if strict_cpf_col else 9
    if is_sensitive_col and re.fullmatch(rf"\d{{{min_digits},11}}(?:\.0+)?", stripped):
        int_s = int(stripped.split(".", 1)[0])
        if min_int_cpf <= int_s <= 99_999_999_999:
            s = f"{int_s:011d}"
            return f"***.{s[3:6]}.{s[6:9]}-**"

    if len(digits) == 11 and len(stripped) in (11, 14) and "*" not in stripped:
        return f"***.{digits[3:6]}.{digits[6:9]}-**"

    # Mascarar CPF (completo ou parcial com 3 primeiros dígitos expostos) embutido em strings compostas ou nomes de MEI
    remasked = _remask_partial_and_embedded_cpfs(val)
    if remasked != val:
        return remasked

    return val


def mask_sensitive_rows(
    rows: List[Dict[str, Any]],
    role: str = "citizen",
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Aplica mascaramento determinístico de PII (LGPD) em linhas retornadas pelo DuckDB
    quando o perfil do solicitante é público ('citizen'). Perfis 'auditor' e 'admin'
    mantêm visibilidade integral mediante trilha de auditoria.
    """
    if role in ("admin", "auditor", "controller"):
        return rows, 0

    sensitive_col_keywords = (
        "documento",
        "cpf",
        "cgc_habilitado",
        "doc_socio",
        "doc_credor",
        "doc_fornecedor",
        "doc_resp",
        "doc_liquidante",
        "doc_signatario",
        "doc_representante",
        "cnpj",
    )
    masked_count = 0
    out_rows: List[Dict[str, Any]] = []

    for row in rows:
        new_row = dict(row)
        nat_credor = str(row.get("natureza_credor") or "").upper().strip()
        is_pj_row = nat_credor in (
            "EMPRESA_PRIVADA",
            "SEM_FINS_LUCRATIVOS",
            "ESTATAL_CONCESSIONARIA_BANCO",
            "ENTE_PUBLICO_TRIBUTO_CONSORCIO",
        )
        for col, val in row.items():
            col_lower = col.lower()
            if col_lower == "doc_credor_raiz" and is_pj_row:
                continue
            has_nis = bool(_NIS_TOKEN_RE.search(col_lower))
            is_sens = has_nis or any(k in col_lower for k in sensitive_col_keywords)
            strict_cpf = ("cpf" in col_lower or has_nis) and not is_pj_row
            masked_val = mask_cpf_value(val, is_sensitive_col=is_sens, strict_cpf_col=strict_cpf)
            if masked_val != val:
                masked_count += 1
                new_row[col] = masked_val
        out_rows.append(new_row)

    return out_rows, masked_count


def mask_text_cpfs(text: str) -> str:
    """
    Mascara qualquer CPF de 11 dígitos ou CPF parcialmente mascarado com 3 primeiros dígitos expostos
    presente em texto corrido (`institutional_response` / `answer_text`) quando a requisição não possui
    token SSO/Admin autenticado, preservando CNPJs (14 dígitos) e anos.
    """
    if not text:
        return ""
    return _remask_partial_and_embedded_cpfs(text)


@dataclass
class PromptSafetyResult:
    is_safe: bool
    sanitized_question: str
    triggered_rules: List[str] = field(default_factory=list)


def check_prompt_safety(question: str) -> PromptSafetyResult:
    """
    Inspeciona a pergunta em linguagem natural contra ataques de Prompt Injection,
    tentativas de leitura de arquivos de sistema e SQL Injection contrabandeado.
    """
    q = (question or "").strip()
    triggered: List[str] = []
    for rule_name, pattern in PROMPT_INJECTION_PATTERNS:
        if pattern.search(q):
            triggered.append(rule_name)

    return PromptSafetyResult(
        is_safe=(len(triggered) == 0),
        sanitized_question=q,
        triggered_rules=triggered,
    )
