#!/usr/bin/env python3
"""Agrega checkpoint.jsonl em métricas e tabelas para o TCC."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from eval.lib.checkpoint import read_all_checkpoints


def pct(num: float, den: float) -> float:
    return round(100.0 * num / den, 2) if den else 0.0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", required=True)
    args = parser.parse_args()

    ckpt = REPO / "eval" / "results" / args.experiment / "checkpoint.jsonl"
    if not ckpt.exists():
        print(f"Checkpoint não encontrado: {ckpt}")
        sys.exit(1)

    rows = [r for r in read_all_checkpoints(ckpt) if r.get("status") == "completed"]
    n = len(rows)
    if n == 0:
        print("Nenhum registro concluído.")
        sys.exit(0)

    ex = sum(1 for r in rows if r["metrics"]["ex"])
    vsr = sum(1 for r in rows if r["metrics"]["vsr"])

    by_diff: dict[str, list] = defaultdict(list)
    by_cat: dict[str, list] = defaultdict(list)
    for r in rows:
        by_diff[r.get("dificuldade") or "nao_definido"].append(r)
        by_cat[r.get("categoria") or "nao_definido"].append(r)

    summary = {
        "experiment": args.experiment,
        "total": n,
        "execution_accuracy_pct": pct(ex, n),
        "valid_sql_rate_pct": pct(vsr, n),
        "ex_count": ex,
        "vsr_count": vsr,
        "total_cost_usd": rows[-1].get("budget_total_usd"),
        "by_dificuldade": {
            k: {
                "n": len(v),
                "ex_pct": pct(sum(1 for x in v if x["metrics"]["ex"]), len(v)),
                "vsr_pct": pct(sum(1 for x in v if x["metrics"]["vsr"]), len(v)),
            }
            for k, v in sorted(by_diff.items())
        },
        "by_categoria": {
            k: {
                "n": len(v),
                "ex_pct": pct(sum(1 for x in v if x["metrics"]["ex"]), len(v)),
            }
            for k, v in sorted(by_cat.items())
        },
        "avg_gen_ms": round(
            sum(r["timing"]["gen_ms"] or 0 for r in rows) / n, 2
        ),
        "avg_sql_gen_ms": round(
            sum(r["timing"]["sql_gen_ms"] or 0 for r in rows) / n, 2
        ),
    }

    out_dir = ckpt.parent
    summary_path = out_dir / "metrics_summary.json"
    summary_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"\nSalvo em: {summary_path}")


if __name__ == "__main__":
    main()
