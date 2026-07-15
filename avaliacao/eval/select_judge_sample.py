#!/usr/bin/env python3
"""Seleciona IDs estratificados por dificuldade a partir do golden + checkpoint."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from eval.lib.checkpoint import read_all_checkpoints


def load_golden(path: Path) -> dict[str, dict]:
    rows = {}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter=";"):
            rows[str(row["id_teste"])] = row
    return rows


def pick_stratified(
    golden: dict[str, dict],
    completed_ids: set[str],
    per_level: int,
) -> list[str]:
    buckets: dict[str, list[str]] = {"facil": [], "medio": [], "dificil": []}
    for tid, row in sorted(golden.items(), key=lambda x: int(x[0])):
        if tid not in completed_ids:
            continue
        d = (row.get("dificuldade") or "").lower()
        if d in buckets and len(buckets[d]) < per_level:
            buckets[d].append(tid)
    out: list[str] = []
    for level in ("facil", "medio", "dificil"):
        out.extend(buckets[level])
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", default="baseline_16k")
    parser.add_argument("--per-level", type=int, default=10)
    parser.add_argument("--limit", type=int, default=0, help="Piloto: primeiros N do checkpoint")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    golden_path = REPO / "eval" / "datasets" / "golden_dataset_v1.0.csv"
    ckpt = REPO / "eval" / "results" / args.experiment / "checkpoint.jsonl"
    golden = load_golden(golden_path)
    completed = {str(r["id_teste"]) for r in read_all_checkpoints(ckpt) if r.get("status") == "completed"}

    if args.limit > 0:
        ids = sorted(completed, key=int)[: args.limit]
    else:
        ids = pick_stratified(golden, completed, args.per_level)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "experiment": args.experiment,
        "ids": ids,
        "count": len(ids),
        "meta": {tid: {"dificuldade": golden[tid].get("dificuldade"), "categoria": golden[tid].get("categoria")} for tid in ids if tid in golden},
    }
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"\nSalvo: {out_path}")


if __name__ == "__main__":
    main()
