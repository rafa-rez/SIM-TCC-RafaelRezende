"""
Guardião determinístico de SQL, Linhagem de Dados, Lacunas de Transparência e Proteção PII (LGPD).

Resolve os achados:
- FIND-001: Execução arbitrária de SQL e leitura de sistema de arquivos (/etc/passwd, COPY, DROP, etc.)
- FIND-008: Extração defeituosa de fontes_utilizadas por regex ingênuo de anos
- FIND-011: Exposição de CPFs completos (11 dígitos) em consultas públicas (LGPD)
- RN-GOV-02: Detecção determinística da lacuna de transparência de folha (2020-2022)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp


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

PROMPT_INJECTION_PATTERNS: List[Tuple[str, re.Pattern[str]]] = [
    (
        "override_instructions",
        re.compile(
            r"(ignore\s+(all|todas|as)\s+(previous|instru[cç][oõ]es|regras)|"
            r"esque[cç]a\s+todas\s+as\s+regras|"
            r"disregard\s+previous\s+instructions|"
            r"novo\s+system\s+prompt|"
            r"reveal\s+your\s+system\s+prompt|"
            r"mostre\s+seu\s+prompt\s+de\s+sistema)",
            re.IGNORECASE,
        ),
    ),
    (
        "file_probe",
        re.compile(
            r"(/etc/passwd|/etc/shadow|\.env|id_rsa|read_csv_auto|read_text|COPY\s+.*\s+TO)",
            re.IGNORECASE,
        ),
    ),
    (
        "sql_smuggling",
        re.compile(
            r"(;\s*(DROP|DELETE|UPDATE|INSERT|ALTER|COPY|ATTACH|DETACH|INSTALL|LOAD)\b|"
            r"\bUNION\s+ALL\s+SELECT\b.*read_)",
            re.IGNORECASE,
        ),
    ),
]


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
    ) -> None:
        self.allowed_tables = {t.lower() for t in allowed_tables} if allowed_tables else None
        self.table_years_map = {
            k.lower(): sorted(v) for k, v in (table_years_map or {}).items()
        }
        self.max_limit = max_limit
        self.repaired_2023 = repaired_2023

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

        # 5. Identificar CTEs para diferenciá-las de tabelas físicas do banco
        cte_names: Set[str] = set()
        for cte in root.find_all(exp.CTE):
            alias = cte.alias_or_name
            if alias:
                cte_names.add(alias.lower())

        # 6. Inspecionar todas as funções na AST contra funções de leitura de arquivos/sistema
        for func_node in root.find_all(exp.Func):
            func_name = (func_node.sql_name() or "").lower()
            if isinstance(func_node, exp.Anonymous):
                func_name = (func_node.name or "").lower()
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
            ):
                return SQLValidationResult(
                    is_valid=False,
                    error_code="BLOCKED_FUNCTION",
                    error_message=f"Função de sistema/arquivo bloqueada por segurança: '{func_name}'.",
                )

        # 7. Extrair e validar todas as tabelas referenciadas
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
                # Ex: SELECT * FROM read_csv_auto(...) onde Table envolve Anonymous
                continue

            if tbl_name in cte_names:
                continue

            if tbl_name.startswith(BLOCKED_TABLE_PREFIXES) or "/" in tbl_name or "\\" in tbl_name or tbl_name.endswith((".csv", ".parquet", ".json", ".db", ".duckdb")):
                return SQLValidationResult(
                    is_valid=False,
                    error_code="BLOCKED_TABLE_OR_PATH",
                    error_message=f"Referência a tabela de sistema ou arquivo externo bloqueada: '{tbl_name}'.",
                )

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

        # 7B. Proteção Semântica Anti-Fan-Out: impedir JOIN redundante entre Gold View pré-unificada e sua tabela-fato bruta
        gold_view_raw_children = {
            "vw_gold_frota_consolidada": {"frota_gastofrota", "frota_frota"},
            "vw_gold_folha_servidores": {"remuneracao_municipio"},
            "vw_gold_contratos_vigencia_aditivos": {"contrato_termocontrato", "contrato_contcontrato"},
        }
        base_set = set(base_tables)
        for gv_name, raw_set in gold_view_raw_children.items():
            overlap = base_set.intersection(raw_set)
            if gv_name in base_set and overlap:
                bad_raw = ", ".join(sorted(overlap))
                return SQLValidationResult(
                    is_valid=False,
                    error_code="REDUNDANT_GOLD_VIEW_JOIN",
                    error_message=(
                        f"ERRO SEMÂNTICO DE FAN-OUT (PRODUTO CARTESIANO): A Gold View '{gv_name}' JÁ INCORPORA "
                        f"os registros e colunas de ({bad_raw})! Fazer JOIN entre '{gv_name}' e ({bad_raw}) "
                        f"multiplica as somas indevidamente. Reescreva a query consultando APENAS '{gv_name}' "
                        f"(em frota, use `vlr_gasto`, `dsc_tipo_gasto` e `status_cadastro_frota = 'CADASTRADO'` diretamente nela)."
                    ),
                )

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
                lim_expr = current_limit.expression
                lim_raw = (lim_expr.sql(dialect="duckdb") if lim_expr is not None else "").strip().upper()
                try:
                    limit_val = int(lim_raw)
                    if limit_val <= 0 or limit_val > self.max_limit:
                        root = root.limit(self.max_limit)
                        limit_applied = True
                except Exception:
                    # Bloqueia LIMIT ALL, LIMIT NULL ou expressões malformadas
                    root = root.limit(self.max_limit)
                    limit_applied = True
            elif not is_scalar_agg:
                root = root.limit(self.max_limit)
                limit_applied = True

        sanitized_sql = root.sql(dialect="duckdb")

        # 9. Extrair anos e linhagem determinística de fontes (fontes_utilizadas)
        years = self._extract_years_from_ast(root)
        fontes = self._resolve_source_files(base_tables, years)

        # 10. Detectar lacunas de transparência determinísticas (ex: folha 2020-2022 e exercício parcial 2025)
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

    def _extract_years_from_ast(self, root: exp.Expression) -> List[int]:
        """
        Extrai de forma determinística todos os anos (2020..2026) filtrados na AST,
        resolvendo igualdades (=), listas (IN), intervalos (BETWEEN) e desigualdades (>=, <=, >, <).
        """
        all_valid_years = [2020, 2021, 2022, 2023, 2024, 2025, 2026]
        explicit_years: Set[int] = set()

        def _extract_int_year(node: Optional[exp.Expression]) -> Optional[int]:
            if node is None:
                return None
            raw = node.name if hasattr(node, "name") and node.name else node.sql()
            raw = raw.strip("'\" ")
            m = re.match(r"^(202[0-6])", raw)
            if m:
                return int(m.group(1))
            return None

        # Intervalos BETWEEN: ex. ano_referencia BETWEEN 2021 AND 2024
        for bw in root.find_all(exp.Between):
            low = _extract_int_year(bw.args.get("low"))
            high = _extract_int_year(bw.args.get("high"))
            if low and high and low <= high:
                for y in range(max(2020, low), min(2026, high) + 1):
                    explicit_years.add(y)

        # Comparações binárias (=, >=, >, <=, <) envolvendo colunas de ano
        lower_bound: Optional[int] = None
        upper_bound: Optional[int] = None

        for cmp_node in root.find_all((exp.EQ, exp.GTE, exp.GT, exp.LTE, exp.LT)):
            left_sql = cmp_node.left.sql().lower()
            right_sql = cmp_node.right.sql().lower()
            is_year_col = any(yc in left_sql or yc in right_sql for yc in YEAR_COLUMNS)
            if not is_year_col:
                continue

            yr = _extract_int_year(cmp_node.right) or _extract_int_year(cmp_node.left)
            if yr is None:
                continue

            if isinstance(cmp_node, exp.EQ):
                explicit_years.add(yr)
            elif isinstance(cmp_node, exp.GTE):
                lower_bound = yr if lower_bound is None else max(lower_bound, yr)
            elif isinstance(cmp_node, exp.GT):
                lower_bound = (yr + 1) if lower_bound is None else max(lower_bound, yr + 1)
            elif isinstance(cmp_node, exp.LTE):
                upper_bound = yr if upper_bound is None else min(upper_bound, yr)
            elif isinstance(cmp_node, exp.LT):
                upper_bound = (yr - 1) if upper_bound is None else min(upper_bound, yr - 1)

        if lower_bound is not None or upper_bound is not None:
            lb = lower_bound if lower_bound is not None else 2020
            ub = upper_bound if upper_bound is not None else 2026
            for y in range(max(2020, lb), min(2026, ub) + 1):
                explicit_years.add(y)

        # Cláusulas IN: ex. ano_referencia IN (2022, 2023)
        for in_node in root.find_all(exp.In):
            for expr in in_node.expressions:
                yr = _extract_int_year(expr)
                if yr:
                    explicit_years.add(yr)

        # Fallback para literais de 4 dígitos 2020-2026 presentes em filtros de string/data
        if not explicit_years:
            for lit in root.find_all(exp.Literal):
                yr = _extract_int_year(lit)
                if yr in all_valid_years:
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
                # Se o usuário filtrou um ano em que a tabela não possui arquivo (ex: folha em 2021),
                # registramos a referência nominal da tabela para auditoria
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
        gap_payroll_years = {2020, 2021, 2022}

        if "remuneracao_municipio" in base_tables or "vw_gold_folha_servidores" in base_tables:
            overlap = sorted(gap_payroll_years.intersection(queried_years)) if queried_years else [2020, 2021, 2022]
            if overlap:
                warnings.append(
                    TransparencyWarning(
                        gap_id="GAP-FOLHA-2020-2022",
                        severity="HIGH",
                        title="Lacuna de Transparência Ativa — Folha de Pagamento (2020–2022)",
                        message=(
                            "Os pacotes de dados abertos do SICOM/TCE-MG para o município de Caeté não contêm "
                            "registros nominais de remuneração de servidores (remuneracao_municipio) nos exercícios "
                            "de 2020, 2021 e 2022. Os dados estão disponíveis exclusivamente a partir de 2023. "
                            "Ausência de registros nesse período reflete lacuna de publicação na origem, e não gasto zero."
                        ),
                        affected_tables=["remuneracao_municipio", "pessoa_municipio", "vw_gold_folha_servidores"],
                        affected_years=overlap,
                        available_years=[2023, 2024, 2025],
                    )
                )

        if 2025 in queried_years or (not queried_years and len(base_tables) > 0):
            warnings.append(
                TransparencyWarning(
                    gap_id="GAP-EXERCICIO-PARCIAL-2025",
                    severity="INFO",
                    title="Nota Técnica — Exercício 2025 em Execução Parcial (Jan–Ago/Set)",
                    message=(
                        "A base pública do exercício de 2025 reflete remessas parciais acumuladas até meados do 2º semestre "
                        "(ex.: ~7 a 9 meses de execução no SICOM/CGU). Ao comparar totais de 2025 com 2024 (12 meses fechados), "
                        "considere a proporcionalidade mensal para evitar falsas conclusões de queda de arrecadação ou despesa."
                    ),
                    affected_tables=base_tables[:4],
                    affected_years=[2025],
                    available_years=[2020, 2021, 2022, 2023, 2024, 2025],
                )
            )

        if not self.repaired_2023:
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


_EMBEDDED_CPF_RE = re.compile(r"(?<![\d/])\b(\d{3})\.?(\d{3})\.?(\d{3})-?(\d{2})\b(?![\d/])")


def mask_cpf_value(val: Any, is_sensitive_col: bool = False) -> Any:
    """
    Mascara CPFs (11 dígitos numéricos, formatados XXX.XXX.XXX-XX, inteiros resultantes de CAST ou
    embutidos em razão social de MEI) para '***.XXX.XXX-**', preservando CNPJs (14 dígitos) intactos.
    """
    if val is None or isinstance(val, bool):
        return val

    if isinstance(val, int):
        if is_sensitive_col and 100_000_000 <= val <= 99_999_999_999:
            s = f"{val:011d}"
            return f"***.{s[3:6]}.{s[6:9]}-**"
        return val

    if isinstance(val, (list, tuple)):
        return [mask_cpf_value(v, is_sensitive_col=is_sensitive_col) for v in val]

    if isinstance(val, dict):
        return {k: mask_cpf_value(v, is_sensitive_col=is_sensitive_col) for k, v in val.items()}

    if not isinstance(val, str):
        return val

    stripped = val.strip()
    if stripped.startswith("***"):
        return stripped

    digits = re.sub(r"\D", "", stripped)
    # Se for exatamente um CNPJ de 14 dígitos, preserva integralmente (LAI)
    if len(digits) == 14 and len(stripped) in (14, 18):
        return val

    if len(digits) == 11 and len(stripped) in (11, 14):
        return f"***.{digits[3:6]}.{digits[6:9]}-**"

    # Mascarar CPF embutido em strings compostas ou nomes de MEI (ex: "JOAO DA SILVA 01234567890")
    if _EMBEDDED_CPF_RE.search(stripped):
        return _EMBEDDED_CPF_RE.sub(r"***. \2.\3-**".replace(" ", ""), val)

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
        "documento", "cpf", "nis", "cgc_habilitado", "doc_socio",
        "doc_credor", "doc_resp", "doc_liquidante", "doc_signatario", "doc_representante",
    )
    masked_count = 0
    out_rows: List[Dict[str, Any]] = []

    for row in rows:
        new_row = dict(row)
        for col, val in row.items():
            col_lower = col.lower()
            is_sens = any(k in col_lower for k in sensitive_col_keywords)
            masked_val = mask_cpf_value(val, is_sensitive_col=is_sens)
            if masked_val != val:
                masked_count += 1
                new_row[col] = masked_val
        out_rows.append(new_row)

    return out_rows, masked_count


def mask_text_cpfs(text: str) -> str:
    """
    Mascara qualquer CPF de 11 dígitos presente em texto corrido (`institutional_response` / `answer_text`)
    quando a requisição não possui token SSO/Admin autenticado, preservando CNPJs (14 dígitos) e anos.
    """
    if not text:
        return ""
    return _EMBEDDED_CPF_RE.sub(r"***. \2.\3-**".replace(" ", ""), text)


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
