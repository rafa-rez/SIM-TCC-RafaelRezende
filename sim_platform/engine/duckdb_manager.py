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

import json
import math
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb

from sim_platform.catalog.data_catalog import DataCatalog
from sim_platform.engine.gold_views import register_gold_views
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
    use_parquet: bool = True


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
                tenant_id="caete_mg_3110004",
                municipality_name="Caeté",
                state_uf="MG",
                ibge_code="3110004",
                data_dir=ws_root / "avaliacao" / "dados" / "database",
                cgu_dir=ws_root / "avaliacao" / "dados" / "staging_cgu",
                fix2023_dir=ws_root / "avaliacao" / "dados" / "database_fix2023",
                parquet_dir=default_pq if default_pq.exists() else None,
                repaired_2023=True,
                use_parquet=True,
            )
        self.tenant = tenant
        is_caete_tenant = (
            self.tenant.tenant_id in ("caete_mg_3110004", "caete-mg")
            or (
                self.tenant.ibge_code == "3110004"
                and self.tenant.data_dir.resolve() == (ws_root / "avaliacao" / "dados" / "database").resolve()
            )
        )
        # Isolamento Estrito Anti-Vazamento (P0-2): municípios diferentes de Caeté NUNCA podem herdar
        # o diretório CSV compartilhado `avaliacao/dados/database` (nem `staging_cgu` / `database_fix2023`) de Caeté!
        default_caete_csv = (ws_root / "avaliacao" / "dados" / "database").resolve()
        if not is_caete_tenant and self.tenant.data_dir.resolve() == default_caete_csv:
            self.tenant.data_dir = ws_root / "avaliacao" / "dados" / "raw_csv" / self.tenant.tenant_id
            self.tenant.fix2023_dir = None
            self.tenant.cgu_dir = None

        if self.tenant.cgu_dir is None and is_caete_tenant:
            candidate_cgu = self.tenant.data_dir.parent / "staging_cgu"
            if candidate_cgu.exists():
                self.tenant.cgu_dir = candidate_cgu
        if self.tenant.use_parquet and self.tenant.parquet_dir is None:
            tenant_pq = ws_root / "avaliacao" / "dados" / "parquet" / self.tenant.tenant_id
            expected_non_caete_raw = (ws_root / "avaliacao" / "dados" / "raw_csv" / self.tenant.tenant_id).resolve()
            if (
                not is_caete_tenant
                and tenant_pq.exists()
                and self.tenant.data_dir.resolve() == expected_non_caete_raw
            ):
                self.tenant.parquet_dir = tenant_pq
            elif (
                is_caete_tenant
                and self.tenant.repaired_2023
                and self.tenant.data_dir.resolve() == default_caete_csv
                and default_pq.exists()
            ):
                self.tenant.parquet_dir = default_pq

        self.query_timeout_sec = query_timeout_sec
        self.max_limit = max_limit
        self._reload_lock = threading.RLock()
        self._retired_conns: List[duckdb.DuckDBPyConnection] = []
        self._current_pointer_mtime: Optional[float] = None
        self.active_release_id: str = "base"
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=f"duckdb-{self.tenant.tenant_id}",
        )

        self.conn = self._init_connection()
        self.catalog = DataCatalog(
            data_dir=self.tenant.data_dir,
            conn=self.conn,
            tenant_id=self.tenant.tenant_id,
            parquet_dir=self.tenant.parquet_dir,
            resolved_parquet_files=list(self._resolve_layered_parquet_files().values()),
        )
        self.sql_guard = SQLGuard(
            allowed_tables=self.catalog.get_allowed_table_names(),
            table_years_map=self.catalog.get_table_years_map(),
            max_limit=max_limit,
            repaired_2023=self.tenant.repaired_2023,
            municipality_name=self.tenant.municipality_name,
        )

    def _get_pointer_path(self) -> Optional[Path]:
        if self.tenant.parquet_dir is None:
            return None
        return self.tenant.parquet_dir / "CURRENT.json"

    def _get_active_release_dir(self) -> Optional[Path]:
        if self.tenant.parquet_dir is None or not self.tenant.parquet_dir.exists():
            return None
        ptr = self._get_pointer_path()
        if ptr and ptr.exists():
            try:
                self._current_pointer_mtime = ptr.stat().st_mtime
                payload = json.loads(ptr.read_text(encoding="utf-8"))
                rel_sub = str(payload.get("release_dir") or "").strip()
                self.active_release_id = str(payload.get("release_id") or rel_sub or "base")
                if rel_sub:
                    cand = (self.tenant.parquet_dir / rel_sub).resolve()
                    base_res = self.tenant.parquet_dir.resolve()
                    if (cand == base_res or base_res in cand.parents) and cand.exists():
                        return cand
            except Exception:
                pass
        self.active_release_id = "base"
        return self.tenant.parquet_dir

    def _resolve_layered_parquet_files(self) -> Dict[str, Path]:
        """
        Resolve arquivos Parquet combinando a base do tenant com eventuais releases imutáveis
        (`releases/<ts>/` apontadas por `CURRENT.json`), evitando `PermissionError: [WinError 32]`
        no Windows sobre arquivos abertos pelo DuckDB.
        """
        by_low: Dict[str, Tuple[str, Path]] = {}
        if not (self.tenant.use_parquet and self.tenant.parquet_dir and self.tenant.parquet_dir.exists()):
            return {}
        base_dir = self.tenant.parquet_dir.resolve()
        for pq_path in sorted(base_dir.glob("*.parquet")):
            by_low[pq_path.stem.lower()] = (pq_path.stem, pq_path)

        ptr = self._get_pointer_path()
        if ptr and ptr.exists():
            try:
                self._current_pointer_mtime = ptr.stat().st_mtime
                payload = json.loads(ptr.read_text(encoding="utf-8"))
                self.active_release_id = str(payload.get("release_id") or payload.get("release_dir") or "base")
                chain = payload.get("release_chain")
                rel_dirs: List[str] = []
                if isinstance(chain, list):
                    rel_dirs.extend(str(x) for x in chain if x)
                rel_single = str(payload.get("release_dir") or "").strip()
                if rel_single and rel_single not in rel_dirs:
                    rel_dirs.append(rel_single)
                for rel_sub in rel_dirs:
                    cand = (base_dir / rel_sub).resolve()
                    if (cand == base_dir or base_dir in cand.parents) and cand.exists():
                        for pq_path in sorted(cand.glob("*.parquet")):
                            prev_stem = by_low.get(pq_path.stem.lower(), (pq_path.stem, pq_path))[0]
                            by_low[pq_path.stem.lower()] = (prev_stem, pq_path)
            except Exception:
                pass
        return {stem: path for stem, path in by_low.values()}

    @staticmethod
    def _read_pointer_signature(ptr: Path) -> Optional[Tuple[float, str]]:
        try:
            cur_mtime = ptr.stat().st_mtime
            payload = json.loads(ptr.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                rel_id = str(payload.get("release_id") or payload.get("release_dir") or "base")
                return cur_mtime, rel_id
        except Exception:
            return None
        return None

    def check_external_release_hotswap(self) -> bool:
        """
        Detecta se um processo externo (ex.: CLI via cronjob/Task Scheduler ou rollback)
        atualizou `CURRENT.json` comparando `(cur_mtime, release_id)` com double-checked locking,
        e dispara `reload_catalog_and_views()` automaticamente sem reiniciar o servidor.
        """
        ptr = self._get_pointer_path()
        if ptr is None or not ptr.exists():
            if self._current_pointer_mtime is not None:
                with self._reload_lock:
                    ptr_recheck = self._get_pointer_path()
                    if self._current_pointer_mtime is not None and (ptr_recheck is None or not ptr_recheck.exists()):
                        self._current_pointer_mtime = None
                        self.active_release_id = "base"
                        self.reload_catalog_and_views()
                        return True
            return False

        sig = self._read_pointer_signature(ptr)
        if sig is None:
            return False
        cur_mtime, cur_release_id = sig
        if self._current_pointer_mtime is not None and (
            cur_mtime == self._current_pointer_mtime and cur_release_id == self.active_release_id
        ):
            return False

        with self._reload_lock:
            sig2 = self._read_pointer_signature(ptr)
            if sig2 is None:
                return False
            cur_mtime2, cur_release_id2 = sig2
            if (
                self._current_pointer_mtime is None
                or cur_mtime2 != self._current_pointer_mtime
                or cur_release_id2 != self.active_release_id
            ):
                self.reload_catalog_and_views()
                return True
        return False

    def _init_connection(self) -> duckdb.DuckDBPyConnection:
        db_target = str(self.tenant.db_path) if self.tenant.db_path else ":memory:"
        con = duckdb.connect(db_target, read_only=False)
        self._create_views_from_csv(con)
        self._apply_kernel_sandbox(con)
        return con

    def _apply_kernel_sandbox(self, con: duckdb.DuckDBPyConnection) -> None:
        """
        Endurece o kernel DuckDB em nível de motor:
        - Limita memória, threads e diretório temporário por conexão (proteção anti-OOM / cross-join explosivo)
        - Restringe leitura de arquivos estritamente aos diretórios de dados do tenant
          ativando `enable_external_access = false` APÓS `allowed_directories` e criação das views
        - Desativa autoinstall/autoload de extensões remotas
        - Trava a configuração (`lock_configuration = true`) impedindo mutação em runtime.
        """
        con.execute("SET memory_limit = '512MB';")
        con.execute("SET threads = 4;")
        con.execute("SET max_temp_directory_size = '1GB';")
        con.execute("SET autoinstall_known_extensions = false;")
        con.execute("SET autoload_known_extensions = false;")
        tmp_base = self.tenant.parquet_dir or self.tenant.data_dir
        tmp_duck: Optional[Path] = None
        if tmp_base is not None:
            try:
                tmp_duck = tmp_base / "_duckdb_tmp"
                tmp_duck.mkdir(parents=True, exist_ok=True)
                con.execute(f"SET temp_directory = '{tmp_duck.resolve().as_posix()}';")
            except Exception:
                tmp_duck = None
        allowed_dirs: List[Path] = []
        candidates = [
            self.tenant.parquet_dir,
            self._get_active_release_dir(),
            self.tenant.data_dir,
            self.tenant.fix2023_dir,
            self.tenant.cgu_dir,
            self.tenant.db_path.parent if self.tenant.db_path else None,
            tmp_duck,
        ]
        for cand in candidates:
            if cand is not None and cand.exists():
                resolved = cand.resolve()
                if resolved not in allowed_dirs:
                    allowed_dirs.append(resolved)
        if allowed_dirs:
            dirs_sql = ", ".join(f"'{d.as_posix()}'" for d in allowed_dirs)
            con.execute(f"SET allowed_directories = [{dirs_sql}];")
        con.execute("SET enable_external_access = false;")
        con.execute("SET lock_configuration = true;")

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
        # 0. Fast-path Fase 1 + Releases Imutáveis: se Parquet colunar já existe para o tenant
        layered_pq = self._resolve_layered_parquet_files()
        if len(layered_pq) >= 60:
            for tname, pq_path in sorted(layered_pq.items()):
                con.execute(
                    f"CREATE OR REPLACE VIEW {tname} AS SELECT * FROM read_parquet('{pq_path.as_posix()}');"
                )
            register_gold_views(con)
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

        # 2B. Caso o tenant tenha arquivos Parquet avulsos/suplementares (< 60 tabelas), registrá-los por cima
        for tname, pq_path in sorted(layered_pq.items()):
            con.execute(
                f"CREATE OR REPLACE VIEW {tname} AS SELECT * FROM read_parquet('{pq_path.as_posix()}');"
            )

        # 3. Registrar Camada Gold Semântica (8 Views pré-desduplicadas)
        register_gold_views(con)

    def _retire_connection_safely(self, old_conn: Optional[duckdb.DuckDBPyConnection]) -> None:
        if old_conn is None:
            return
        with self._reload_lock:
            self._retired_conns.append(old_conn)

        def _deferred_close() -> None:
            try:
                old_conn.close()
            except Exception:
                pass
            with self._reload_lock:
                try:
                    if old_conn in self._retired_conns:
                        self._retired_conns.remove(old_conn)
                except Exception:
                    pass

        drain_delay = max(20.0, float(self.query_timeout_sec) + 5.0)
        timer = threading.Timer(drain_delay, _deferred_close)
        timer.daemon = True
        timer.start()

    def reload_catalog_and_views(self) -> None:
        """
        Recarrega a quente as VIEWs Parquet/CSV e o DataCatalog após execução do 'Atualizar Dados'
        ou atualização de `CURRENT.json` via CLI/cronjob, instanciando uma nova conexão endurecida
        com `lock_configuration = true` e drenagem suave de 5s para consultas em voo.
        """
        with self._reload_lock:
            old_conn = getattr(self, "conn", None)
            new_conn = self._init_connection()
            self.conn = new_conn
            self.catalog = DataCatalog(
                data_dir=self.tenant.data_dir,
                conn=self.conn,
                tenant_id=self.tenant.tenant_id,
                parquet_dir=self.tenant.parquet_dir,
                resolved_parquet_files=list(self._resolve_layered_parquet_files().values()),
            )
            self.sql_guard = SQLGuard(
                allowed_tables=self.catalog.get_allowed_table_names(),
                table_years_map=self.catalog.get_table_years_map(),
                max_limit=self.max_limit,
                repaired_2023=self.tenant.repaired_2023,
                municipality_name=self.tenant.municipality_name,
            )
            self._retire_connection_safely(old_conn)

    def close(self) -> None:
        """Libera o pool de threads e encerra a conexão DuckDB com segurança no shutdown."""
        try:
            self._executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            self._executor.shutdown(wait=False)
        except Exception:
            pass
        with self._reload_lock:
            retired_snapshot = list(getattr(self, "_retired_conns", []))
            if hasattr(self, "_retired_conns"):
                self._retired_conns.clear()
        for r_conn in retired_snapshot:
            try:
                r_conn.close()
            except Exception:
                pass
        if getattr(self, "conn", None) is not None:
            try:
                self.conn.close()
            except Exception:
                pass

    def get_mapped_objects_count(self) -> Dict[str, int]:
        """
        Corrige a regressão FIND-012 contando tanto VIEWs quanto BASE TABLEs no schema main.
        """
        self.check_external_release_hotswap()
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

    def _run_cursor_query(
        self,
        sql: str,
        enforce_limit: bool = True,
        cursor_holder: Optional[List[Any]] = None,
        probe_extra_row: bool = False,
    ) -> Tuple[List[str], List[Tuple[Any, ...]], bool]:
        cur = self.conn.cursor()
        if cursor_holder is not None:
            cursor_holder.append(cur)
        exec_sql = sql
        if probe_extra_row and enforce_limit and self.max_limit > 0:
            exec_sql = re.sub(
                rf"\bLIMIT\s+{self.max_limit}\s*;?\s*$",
                f"LIMIT {self.max_limit + 1}",
                sql,
                count=1,
                flags=re.IGNORECASE,
            )
        rel = cur.execute(exec_sql)
        cols = [d[0] for d in (rel.description or [])]
        truncated = False
        if enforce_limit and self.max_limit > 0:
            rows = rel.fetchmany(self.max_limit + 1)
            if len(rows) > self.max_limit:
                rows = rows[: self.max_limit]
                truncated = True
        else:
            rows = rel.fetchall()
        return cols, rows, truncated

    def execute_safe_query(
        self,
        raw_sql: str,
        role: str = "citizen",
        enforce_limit: bool = True,
        allowed_tables_override: Optional[set[str]] = None,
    ) -> Dict[str, Any]:
        t0 = perf_t0 = time.perf_counter()
        self.check_external_release_hotswap()

        # 1. Validação determinística AST via SQLGuard (com escopo restrito se o usuário filtrou bases, inclusive set() vazio)
        active_guard = (
            SQLGuard(
                allowed_tables={t.lower() for t in allowed_tables_override},
                table_years_map=self.catalog.get_table_years_map(),
                max_limit=self.max_limit,
                repaired_2023=self.tenant.repaired_2023,
                municipality_name=self.tenant.municipality_name,
            )
            if allowed_tables_override is not None
            else self.sql_guard
        )
        validation: SQLValidationResult = active_guard.validate_and_rewrite(
            raw_sql=raw_sql,
            enforce_limit=enforce_limit,
            role=role,
        )
        if not validation.is_valid:
            elapsed_ms = round((time.perf_counter() - perf_t0) * 1000.0, 2)
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

        # 2. Execução isolada em thread pool com timeout estrito e interrupção direta do cursor ativo
        cur_holder: List[Any] = []
        fut = self._executor.submit(
            self._run_cursor_query,
            validation.sanitized_sql,
            enforce_limit,
            cur_holder,
            bool(validation.limit_applied),
        )
        try:
            cols, raw_rows, truncated_by_fetch = fut.result(timeout=self.query_timeout_sec)
        except FuturesTimeoutError:
            if cur_holder:
                local_cur = cur_holder[0]
                if hasattr(local_cur, "interrupt"):
                    try:
                        local_cur.interrupt()
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
            "limit_aplicado": bool(validation.limit_applied or truncated_by_fetch),
            "truncado_pelo_limite": bool(truncated_by_fetch),
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
