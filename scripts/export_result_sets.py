#!/usr/bin/env python3
"""Exporta conjuntos tabulares de referência e de experimentos para JSON."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from eval.lib.checkpoint import read_all_checkpoints
from eval.lib.duckdb_engine import execute_query_local as execute_query


def schema_hash(columns: list[str]) -> str:
    payload = "|".join(sorted(c.lower() for c in columns))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def result_to_json(
    id_teste: str,
    sql: str,
    data: list[dict],
    *,
    source: str,
    error: str | None = None,
) -> dict:
    columns = list(data[0].keys()) if data else []
    return {
        "id_teste": id_teste,
        "source": source,
        "sql": sql,
        "columns": columns,
        "rows": data,
        "row_count": len(data),
        "col_count": len(columns),
        "schema_hash": schema_hash(columns) if columns else None,
        "error": error,
        "exported_at": datetime.now(timezone.utc).isoformat(),
    }


def export_golden(args: argparse.Namespace) -> None:
    dataset = REPO / args.dataset
    out_dir = REPO / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    with dataset.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            tid = row["id_teste"]
            sql = (row.get("query_referencia") or "").strip()
            result = execute_query(sql, timeout=args.timeout)
            payload = result_to_json(
                tid,
                sql,
                result.get("data") or [],
                source="golden_reference",
                error=result.get("error"),
            )
            payload["input_usuario"] = row.get("input_usuario")
            payload["dificuldade"] = row.get("dificuldade")
            payload["categoria"] = row.get("categoria")
            (out_dir / f"{int(tid):03d}.json").write_text(
                json.dumps(payload, indent=2, ensure_ascii=False, default=str),
                encoding="utf-8",
            )
            status = "OK" if result.get("ok") else "ERRO"
            print(f"  [{tid}] referencia {status} ({payload['row_count']} linhas)")


def export_checkpoint(args: argparse.Namespace) -> None:
    ckpt_path = REPO / "experimentos" / args.experiment / "checkpoint.jsonl"
    if not ckpt_path.exists():
        ckpt_path = REPO / "eval" / "results" / args.experiment / "checkpoint.jsonl"
    if not ckpt_path.exists():
        print(f"Checkpoint não encontrado: {args.experiment}")
        sys.exit(1)

    out_dir = REPO / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    for rec in read_all_checkpoints(ckpt_path):
        if rec.get("status") != "completed":
            continue
        tid = rec["id_teste"]
        sql = (rec.get("query_gerada") or "").strip()
        result = execute_query(sql, timeout=args.timeout)
        payload = result_to_json(
            tid,
            sql,
            result.get("data") or [],
            source=f"generated:{args.experiment}",
            error=result.get("error"),
        )
        payload["metrics_checkpoint"] = rec.get("metrics")
        (out_dir / f"{int(tid):03d}.json").write_text(
            json.dumps(payload, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        status = "OK" if result.get("ok") else "ERRO"
        print(f"  [{tid}] gerada {status} ({payload['row_count']} linhas)")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        choices=["golden", "checkpoint"],
        required=True,
    )
    parser.add_argument("--experiment", help="Obrigatório se source=checkpoint")
    parser.add_argument(
        "--dataset",
        default="dados/golden/golden_dataset_v1.0.csv",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Ex.: dados/golden/reference_results",
    )
    parser.add_argument("--duckdb-url", default="http://localhost:8000/query")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    if args.source == "checkpoint" and not args.experiment:
        parser.error("--experiment é obrigatório com --source checkpoint")

    if args.source == "golden":
        export_golden(args)
    else:
        export_checkpoint(args)


if __name__ == "__main__":
    main()
