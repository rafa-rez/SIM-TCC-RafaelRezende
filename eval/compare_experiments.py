#!/usr/bin/env python3
"""Compara metrics_summary.json de todos os experimentos."""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
RESULTS = REPO / "eval" / "results"

EXPERIMENTS = [
    "baseline_16k",
    "ablation_precagada_30k",
    "ablation_gpt4o_mini",
    "ablation_gpt4o",
]


def main() -> None:
    rows = []
    for name in EXPERIMENTS:
        path = RESULTS / name / "metrics_summary.json"
        if not path.exists():
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        rows.append(data)

    if not rows:
        print("Nenhum metrics_summary.json encontrado.")
        sys.exit(1)

    print("| Experimento | EX (%) | VSR (%) | Custo (USD) | Latencia gen (ms) |")
    print("|---|---:|---:|---:|---:|")
    for r in rows:
        print(
            f"| {r['experiment']} | {r['execution_accuracy_pct']} | "
            f"{r['valid_sql_rate_pct']} | {r['total_cost_usd']:.4f} | {r['avg_gen_ms']} |"
        )

    out = RESULTS / "comparison_table.md"
    lines = [
        "# Comparacao de experimentos Text-to-SQL\n",
        "| Experimento | EX (%) | VSR (%) | Custo (USD) | Latencia gen (ms) |\n",
        "|---|---:|---:|---:|---:|\n",
    ]
    for r in rows:
        lines.append(
            f"| {r['experiment']} | {r['execution_accuracy_pct']} | "
            f"{r['valid_sql_rate_pct']} | {r['total_cost_usd']:.4f} | {r['avg_gen_ms']} |\n"
        )
    out.write_text("".join(lines), encoding="utf-8")
    print(f"\nTabela salva em: {out}")


if __name__ == "__main__":
    main()
