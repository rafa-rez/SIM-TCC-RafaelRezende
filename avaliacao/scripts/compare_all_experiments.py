#!/usr/bin/env python3
"""Gera tabela comparativa dos experimentos com métricas v3."""

from __future__ import annotations

import json
import sys
from pathlib import Path

AVALIACAO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AVALIACAO_ROOT))

from eval.lib.paths import EXPERIMENTOS_ROOT

EXPERIMENTS = [
    ("e1_baseline_compacto", "E1 baseline compacto"),
    ("e2_contexto_estendido", "E2 contexto estendido"),
    ("e3_gpt4o_mini", "E3 GPT-4o-mini"),
    ("e4_gpt4o", "E4 GPT-4o"),
]


def main() -> None:
    rows = []
    for exp_id, label in EXPERIMENTS:
        path = EXPERIMENTOS_ROOT / exp_id / "metrics_summary_v3.json"
        if not path.exists():
            print(f"Ausente: {path}", file=sys.stderr)
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        rows.append((label, data))

    header = (
        "| Experimento | EX resposta | EX proj | EX proj+tol | EX colmap | "
        "EX estrita | VSR |"
    )
    sep = "|---|---:|---:|---:|---:|---:|---:|"
    lines = [header, sep]
    for label, d in rows:
        lines.append(
            f"| {label} "
            f"| {d['execution_accuracy_principal_pct']} "
            f"| {d['execution_accuracy_proj_pct']} "
            f"| {d['execution_accuracy_proj_tol_pct']} "
            f"| {d['execution_accuracy_colmap_pct']} "
            f"| {d['execution_accuracy_strict_pct']} "
            f"| {d['valid_sql_rate_pct']} |"
        )

    table = "\n".join(lines)
    print(table)
    out = EXPERIMENTOS_ROOT / "comparison_table.md"
    out.write_text(table + "\n", encoding="utf-8")
    print(f"\nSalvo em {out}")


if __name__ == "__main__":
    main()
