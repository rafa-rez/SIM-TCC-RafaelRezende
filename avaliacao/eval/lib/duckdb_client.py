"""Cliente HTTP para DuckDB API com fallback local."""

from __future__ import annotations

import time
from typing import Any
from urllib.parse import urlparse

import requests


def _use_local_fallback(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host in {"", "localhost", "127.0.0.1"}


def execute_query(
    url: str,
    sql: str,
    timeout: int = 60,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    try:
        resp = requests.post(
            url,
            json={"query": sql},
            timeout=timeout,
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000
        try:
            body = resp.json()
        except Exception:
            body = {
                "status": "error",
                "erro_tecnico": resp.text[:2000],
            }
        return {
            "http_status": resp.status_code,
            "elapsed_ms": round(elapsed_ms, 2),
            "body": body,
            "ok": body.get("status") == "success",
            "data": body.get("data") or [],
            "error": body.get("erro_tecnico") or body.get("mensagem_llm"),
        }
    except requests.RequestException as exc:
        if _use_local_fallback(url):
            from eval.lib.duckdb_engine import execute_query_local

            local = execute_query_local(sql, timeout=timeout)
            return {
                "http_status": 0,
                "elapsed_ms": local.get("elapsed_ms", 0),
                "body": {"status": "success" if local.get("ok") else "error"},
                "ok": bool(local.get("ok")),
                "data": local.get("data") or [],
                "error": local.get("error"),
                "local_fallback": True,
            }
        return {
            "http_status": 0,
            "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
            "body": {},
            "ok": False,
            "data": [],
            "error": str(exc),
        }
