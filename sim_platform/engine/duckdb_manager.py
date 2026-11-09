"""
Motor Analítico DuckDB Endurecido, Concorrente e Multi-Tenant (DuckDBManager).

Resolve os achados:
- FIND-001: Execução SQL sem validação e acesso irrestrito ao sistema de arquivos
- FIND-002: Corrupção silenciosa de 4.723 linhas em 2023 (suporte nativo a overlay reparado `database_fix2023`)
- FIND-003: Inclusão das 65 tabelas SICOM (`database`) + 7 tabelas CGU (`staging_cgu`) = 72 tabelas completas com `ano_referencia` extraído de `filename=true`
- FIND-008: Unificação dos campos `fontes_utilizadas`, `fontes_csv` e `rastreabilidade_tcc` via AST
- FIND-011: Mascaramento determinístico de CPF (LGPD) por perfil RBAC
- FIND-012: Contagem correta de objetos (`BASE TABLE` + `VIEW`) no monitoramento
"""

from __future__ import annotations

import math
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb

from sim_platform.catalog.data_catalog import DataCatalog
from sim_platform.security.sql_guard import (
    SQLGuard,
    SQLValidationResult,
    mask_sensitive_rows,
)

_YEAR_FROM_SICOM = (
    "TRY_CAST(regexp_extract("
    "replace(filename, chr(92), '/'), "
    "'([0-9]{4})\\.\\d+\\.', 1) AS INTEGER)"
)
_YEAR_FROM_CGU = (
    "TRY_CAST(regexp_extract("
    "replace(filename, chr(92), '/'), "
    "'([0-9]{4})_[0-9]{2}_', 1) AS INTEGER)"
)


def _json_safe(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, Decimal):
        f = float(val)
        return None if (math.isnan(f) or math.isinf(f)) else round(f, 4)
    if isinstance(val, float):
        return None if (math.isnan(val) or math.isinf(val)) else round(val, 4)
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    return val


@dataclass
class TenantContext:
    tenant_id: str
    municipality_name: str
    state_uf: str
    ibge_code: str
    data_dir: Path
    cgu_dir: Optional[Path] = None
    fix2023_dir: Optional[Path] = None
    parquet_dir: Optional[Path] = None
    db_path: Optional[Path] = None
    repaired_2023: bool = True


class DuckDBManager:
    """
    Gerenciador multi-tenant de conexões DuckDB com isolamento por cursor,
    validação AST obrigatória (SQLGuard), timeout por consulta, reparo 2023 e leitura nativa em Parquet (Fase 1).
    """

    def __init__(
        self,
        tenant: Optional[TenantContext] = None,
        max_workers: int = 4,
        query_timeout_sec: float = 15.0,
        max_limit: int = 500,
    ) -> None:
        ws_root = Path(__file__).resolve().parents[2]
        default_pq = ws_root / "avaliacao" / "dados" / "parquet" / "caete_mg_3110004"
        if tenant is None:
            tenant = TenantContext(
                tenant_id="caete-mg",
                municipality_name="Caeté",
                state_uf="MG",
                ibge_code="3110004",
                data_dir=ws_root / "avaliacao" / "dados" / "database",
                cgu_dir=ws_root / "avaliacao" / "dados" / "staging_cgu",
                fix2023_dir=ws_root / "avaliacao" / "dados" / "database_fix2023",
                parquet_dir=default_pq if default_pq.exists() else None,
                repaired_2023=True,
            )
        self.tenant = tenant
        if self.tenant.cgu_dir is None:
            candidate_cgu = self.tenant.data_dir.parent / "staging_cgu"
            if candidate_cgu.exists():
                self.tenant.cgu_dir = candidate_cgu
        if (
            self.tenant.parquet_dir is None
            and self.tenant.repaired_2023
            and self.tenant.data_dir == (ws_root / "avaliacao" / "dados" / "database")
            and default_pq.exists()
        ):
            self.tenant.parquet_dir = default_pq

        self.query_timeout_sec = query_timeout_sec
        self.max_limit = max_limit
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=f"duckdb-{self.tenant.tenant_id}",
        )

        self.conn = self._init_connection()
        self.catalog = DataCatalog(
            data_dir=self.tenant.data_dir,
            conn=self.conn,
            tenant_id=self.tenant.tenant_id,
        )
        self.sql_guard = SQLGuard(
            allowed_tables=self.catalog.get_allowed_table_names(),
            table_years_map=self.catalog.get_table_years_map(),
            max_limit=max_limit,
            repaired_2023=self.tenant.repaired_2023,
        )

    def _init_connection(self) -> duckdb.DuckDBPyConnection:
        db_target = str(self.tenant.db_path) if self.tenant.db_path else ":memory:"
        con = duckdb.connect(db_target, read_only=False)
        self._create_views_from_csv(con)
        return con

    def _group_sicom_files(self) -> Dict[str, List[Path]]:
        groups: Dict[str, List[Path]] = {}
        fix_dir = (
            self.tenant.fix2023_dir
            if (self.tenant.repaired_2023 and self.tenant.fix2023_dir and self.tenant.fix2023_dir.exists())
            else None
        )

        if not self.tenant.data_dir.exists():
            return groups

        for arquivo in sorted(self.tenant.data_dir.glob("*.csv")):
            use_path = (fix_dir / arquivo.name) if (fix_dir and (fix_dir / arquivo.name).exists()) else arquivo
            partes_nome = arquivo.stem.split(".")
            if len(partes_nome) > 1 and partes_nome[1].isdigit():
                nome_base = "_".join(partes_nome[2:])
            else:
                nome_base = "_".join(partes_nome[1:])

            nome_tabela = (
                f"sicom_{nome_base.replace('-', '_')}"
                if nome_base and nome_base[0].isdigit()
                else nome_base.replace(".", "_").replace("-", "_")
            )
            groups.setdefault(nome_tabela, []).append(use_path)

        return groups

    def _create_views_from_csv(self, con: duckdb.DuckDBPyConnection) -> None:
        # 0. Fast-path Fase 1: se Parquet colunar já existe para o tenant, criar views direto sobre Parquet
        if self.tenant.parquet_dir and self.tenant.parquet_dir.exists():
            pq_files = sorted(self.tenant.parquet_dir.glob("*.parquet"))
            if len(pq_files) >= 60:
                for pq_path in pq_files:
                    tname = pq_path.stem
                    con.execute(
                        f"CREATE OR REPLACE VIEW {tname} AS SELECT * FROM read_parquet('{pq_path.as_posix()}');"
                    )
                return

        # 1. Views SICOM (65 views com ano_referencia extraído do nome do arquivo)
        sicom_groups = self._group_sicom_files()
        for nome_tabela, paths in sicom_groups.items():
            path_list = ", ".join(f"'{p.as_posix()}'" for p in paths)
            query_view = f"""
                CREATE OR REPLACE VIEW {nome_tabela} AS
                SELECT
                    *,
                    {_YEAR_FROM_SICOM} AS ano_referencia
                FROM read_csv_auto([{path_list}], delim='|', header=True, filename=true, union_by_name=true, ignore_errors=true);
            """
            con.execute(query_view)

        # 2. Views CGU (7 views em staging_cgu com ano_referencia extraído do nome do arquivo)
        if self.tenant.cgu_dir and self.tenant.cgu_dir.exists():
            categorias_cgu: set[str] = set()
            for arquivo in self.tenant.cgu_dir.glob("*.csv"):
                partes = arquivo.stem.split("_")
                if len(partes) >= 3:
                    categorias_cgu.add(partes[2])

            for categoria in sorted(categorias_cgu):
                nome_view = f"cgu_{categoria.replace('-', '_')}"
                padrao_busca = f"{self.tenant.cgu_dir.as_posix()}/*_{categoria}_cgu.csv"
                query_cgu = f"""
                    CREATE OR REPLACE VIEW {nome_view} AS
                    SELECT
                        *,
                        {_YEAR_FROM_CGU} AS ano_referencia
                    FROM read_csv_auto('{padrao_busca}', delim='|', header=True, filename=true, union_by_name=true, ignore_errors=true);
                """
                con.execute(query_cgu)

    def reload_catalog_and_views(self) -> None:
        """
        Recarrega a quente as VIEWs Parquet/CSV e o DataCatalog após execução do 'Atualizar Dados'.
        """
        self._create_views_from_csv(self.conn)
        self.catalog = DataCatalog(
            data_dir=self.tenant.data_dir,
            conn=self.conn,
            tenant_id=self.tenant.tenant_id,
        )
        self.sql_guard = SQLGuard(
            allowed_tables=self.catalog.get_allowed_table_names(),
            table_years_map=self.catalog.get_table_years_map(),
            max_limit=self.max_limit,
            repaired_2023=self.tenant.repaired_2023,
        )

    def get_mapped_objects_count(self) -> Dict[str, int]:
        """
        Corrige a regressão FIND-012 contando tanto VIEWs quanto BASE TABLEs no schema main.
        """
        cur = self.conn.cursor()
        rows = cur.execute(
            "SELECT table_type, COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name NOT LIKE 'duckdb_%' "
            "AND table_name NOT LIKE 'sqlite_%' AND table_name NOT LIKE 'pragma_%' "
            "GROUP BY table_type"
        ).fetchall()
        by_type = {r[0]: int(r[1]) for r in rows}
        return {
            "views": by_type.get("VIEW", 0),
            "base_tables": by_type.get("BASE TABLE", 0),
            "total": sum(by_type.values()),
        }

    def _run_cursor_query(self, sql: str) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        cur = self.conn.cursor()
        rel = cur.execute(sql)
        cols = [d[0] for d in (rel.description or [])]
        rows = rel.fetchall()
        return cols, rows

    def execute_safe_query(
        self,
        raw_sql: str,
        role: str = "citizen",
        enforce_limit: bool = True,
        allowed_tables_override: Optional[set[str]] = None,
    ) -> Dict[str, Any]:
        t0 = time.perf_counter()

        # 1. Validação determinística AST via SQLGuard (com escopo restrito se o usuário filtrou bases)
        active_guard = (
            SQLGuard(
                allowed_tables={t.lower() for t in allowed_tables_override},
                table_years_map=self.catalog.get_table_years_map(),
                max_limit=self.max_limit,
                repaired_2023=self.tenant.repaired_2023,
            )
            if allowed_tables_override
            else self.sql_guard
        )
        validation: SQLValidationResult = active_guard.validate_and_rewrite(
            raw_sql=raw_sql,
            enforce_limit=enforce_limit,
        )
        if not validation.is_valid:
            elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            return {
                "status": "bloqueado_seguranca",
                "tenant_id": self.tenant.tenant_id,
                "erro_codigo": validation.error_code,
                "detalhe": validation.error_message,
                "mensagem_llm": (
                    f"Consulta rejeitada pelo guardião de segurança ({validation.error_code}): "
                    f"{validation.error_message}"
                ),
                "dados": [],
                "colunas": [],
                "total_linhas": 0,
                "fontes_utilizadas": [],
                "fontes_csv": [],
                "rastreabilidade_tcc": [],
                "transparency_warnings": [],
                "exec_ms": elapsed_ms,
            }

        # 2. Execução isolada em thread pool com timeout estrito
        fut = self._executor.submit(self._run_cursor_query, validation.sanitized_sql)
        try:
            cols, raw_rows = fut.result(timeout=self.query_timeout_sec)
        except FuturesTimeoutError:
            try:
                self.conn.interrupt()
            except Exception:
                pass
            elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            return {
                "status": "timeout",
                "tenant_id": self.tenant.tenant_id,
                "erro_codigo": "QUERY_TIMEOUT",
                "detalhe": f"A consulta excedeu o tempo máximo de {self.query_timeout_sec}s e foi interrompida.",
                "dados": [],
                "colunas": [],
                "total_linhas": 0,
                "fontes_utilizadas": validation.fontes_utilizadas,
                "fontes_csv": validation.fontes_utilizadas,
                "rastreabilidade_tcc": validation.fontes_utilizadas,
                "transparency_warnings": [w.to_dict() for w in validation.transparency_warnings],
                "exec_ms": elapsed_ms,
            }
        except Exception as exc:
            elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            err_str = str(exc)
            hint = "Verifique os nomes exatos das colunas e tipos no DDL fornecido pelo Catálogo."
            if "sum(varchar)" in err_str.lower() or "avg(varchar)" in err_str.lower():
                hint = "A coluna de valor é VARCHAR: utilize SUM(TRY_CAST(coluna AS DECIMAL(18,2)))."
            return {
                "status": "erro_sql",
                "tenant_id": self.tenant.tenant_id,
                "erro_codigo": "DUCKDB_EXECUTION_ERROR",
                "detalhe": err_str,
                "dicas_correcao": hint,
                "mensagem_llm": f"Erro DuckDB: {err_str}. Dica: {hint}",
                "dados": [],
                "colunas": [],
                "total_linhas": 0,
                "fontes_utilizadas": validation.fontes_utilizadas,
                "fontes_csv": validation.fontes_utilizadas,
                "rastreabilidade_tcc": validation.fontes_utilizadas,
                "transparency_warnings": [w.to_dict() for w in validation.transparency_warnings],
                "exec_ms": elapsed_ms,
            }

        # 3. Serialização JSON-safe e Mascaramento Determinístico de PII (LGPD)
        dict_rows: List[Dict[str, Any]] = [
            {cols[i]: _json_safe(r[i]) for i in range(len(cols))}
            for r in raw_rows
        ]
        masked_rows, masked_count = mask_sensitive_rows(dict_rows, role=role)
        elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)

        return {
            "status": "sucesso",
            "tenant_id": self.tenant.tenant_id,
            "query_executada": validation.sanitized_sql,
            "limit_aplicado": validation.limit_applied,
            "colunas": cols,
            "total_linhas": len(masked_rows),
            "dados": masked_rows,
            "tabelas_referenciadas": validation.referenced_tables,
            "anos_referenciados": validation.referenced_years,
            "fontes_utilizadas": validation.fontes_utilizadas,
            "fontes_csv": validation.fontes_utilizadas,
            "rastreabilidade_tcc": validation.fontes_utilizadas,
            "transparency_warnings": [w.to_dict() for w in validation.transparency_warnings],
            "pii_masked_count": masked_count,
            "exec_ms": elapsed_ms,
        }
