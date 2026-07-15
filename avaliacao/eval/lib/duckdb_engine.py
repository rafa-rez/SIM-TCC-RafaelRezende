"""Execução local de SQL no DuckDB (sem container Docker)."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import duckdb

from eval.lib.paths import DADOS_ROOT

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

_CONNECTION: duckdb.DuckDBPyConnection | None = None


def reset_connection() -> None:
    global _CONNECTION
    _CONNECTION = None


def _posix(path: Path) -> str:
    return path.as_posix()


def get_connection(repo_root: Path | None = None) -> duckdb.DuckDBPyConnection:
    global _CONNECTION
    if _CONNECTION is not None:
        return _CONNECTION

    root = DADOS_ROOT if repo_root is None else repo_root
    pasta_database = root / "database"
    pasta_cgu = root / "staging_cgu"

    con = duckdb.connect(database=":memory:")

    if pasta_database.exists():
        for arquivo in pasta_database.glob("*.csv"):
            partes_nome = arquivo.stem.split(".")
            if len(partes_nome) > 1 and partes_nome[1].isdigit():
                sufixo = f"{partes_nome[-2]}.{partes_nome[-1]}"
                nome_base = "_".join(partes_nome[2:])
            else:
                sufixo = f"{partes_nome[-2]}.{partes_nome[-1]}"
                nome_base = "_".join(partes_nome[1:])

            nome_tabela = (
                f"sicom_{nome_base.replace('-', '_')}"
                if nome_base and nome_base[0].isdigit()
                else nome_base.replace(".", "_").replace("-", "_")
            )
            padrao_busca = f"{_posix(pasta_database)}/*{sufixo}.csv"
            query_view = f"""
                CREATE OR REPLACE VIEW {nome_tabela} AS
                SELECT
                    *,
                    {_YEAR_FROM_SICOM} AS ano_referencia
                FROM read_csv_auto('{padrao_busca}', delim='|', header=True, filename=true, union_by_name=true, ignore_errors=true);
            """
            try:
                con.execute(query_view)
            except Exception:
                pass

    if pasta_cgu.exists():
        categorias_cgu: set[str] = set()
        for arquivo in pasta_cgu.glob("*.csv"):
            partes = arquivo.stem.split("_")
            if len(partes) >= 3:
                categorias_cgu.add(partes[2])

        for categoria in categorias_cgu:
            nome_view = f"cgu_{categoria.replace('-', '_')}"
            padrao_busca = f"{_posix(pasta_cgu)}/*_{categoria}_cgu.csv"
            query_cgu = f"""
                CREATE OR REPLACE VIEW {nome_view} AS
                SELECT
                    *,
                    {_YEAR_FROM_CGU} AS ano_referencia
                FROM read_csv_auto('{padrao_busca}', delim='|', header=True, filename=true, union_by_name=true, ignore_errors=true);
            """
            try:
                con.execute(query_cgu)
            except Exception:
                pass

    _CONNECTION = con
    return con


def execute_query_local(
    sql: str,
    *,
    repo_root: Path | None = None,
    timeout: int = 60,
) -> dict[str, Any]:
    del timeout
    t0 = time.perf_counter()
    try:
        con = get_connection(repo_root)
        result_df = con.execute(sql).df()
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
        if result_df.empty:
            return {
                "ok": True,
                "data": [],
                "elapsed_ms": elapsed_ms,
                "error": None,
            }
        data = json.loads(result_df.to_json(orient="records", date_format="iso"))
        return {
            "ok": True,
            "data": data,
            "elapsed_ms": elapsed_ms,
            "error": None,
        }
    except Exception as exc:
        return {
            "ok": False,
            "data": [],
            "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
            "error": str(exc),
        }
