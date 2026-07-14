"""Cliente HTTP para DuckDB API."""

from __future__ import annotations

import time
from typing import Any

import requests


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
        return {
            "http_status": 0,
            "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
            "body": {},
            "ok": False,
            "data": [],
            "error": str(exc),
        }
