#!/usr/bin/env python3
"""Valida query_referencia de cada linha do golden contra DuckDB API."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from eval.lib.config_loader import load_config, resolve_path
from eval.lib.duckdb_client import execute_query


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default=None)
    args = parser.parse_args()

    cfg = load_config()
    path = resolve_path(args.dataset or cfg["default_dataset"])
    url = cfg["duckdb_url"]

    ok, fail, empty = 0, 0, 0
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            tid = row["id_teste"]
            sql = (row.get("query_referencia") or "").strip()
            if not sql:
                print(f"[{tid}] SKIP — sem query_referencia")
                fail += 1
                continue
            res = execute_query(url, sql)
            if res["ok"]:
                rows = len(res["data"])
                if rows == 0 and row.get("espera_dados", "sim").lower() == "sim":
                    print(f"[{tid}] WARN — 0 linhas ({row['categoria']})")
                    empty += 1
                else:
                    print(f"[{tid}] OK — {rows} linhas")
                ok += 1
            else:
                print(f"[{tid}] FAIL — {str(res.get('error', ''))[:120]}")
                fail += 1

    print(f"\nResumo: OK={ok} FAIL/SKIP={fail} VAZIO_WARN={empty}")


if __name__ == "__main__":
    main()
