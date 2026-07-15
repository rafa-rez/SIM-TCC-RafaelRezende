#!/usr/bin/env python3
"""Gera tabela comparativa dos experimentos com métricas v3."""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

EXPERIMENTS = [
    ("e1_baseline_compacto", "E1 baseline compacto"),
    ("e2_contexto_estendido", "E2 contexto estendido"),
    ("e3_gpt4o_mini", "E3 GPT-4o-mini"),
    ("e4_gpt4o", "E4 GPT-4o"),
]


def main() -> None:
    rows = []
    for exp_id, label in EXPERIMENTS:
        path = REPO / "experimentos" / exp_id / "metrics_summary_v3.json"
        if not path.exists():
            print(f"Ausente: {path}", file=sys.stderr)
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        rows.append((label, data))

    header = (
        "| Experimento | EX resposta | EX proj | EX proj+tol | EX colmap | "
        "EX estrita | VSR | Alias | Col+ | Tol num |"
    )
    sep = "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    lines = [header, sep]
    for label, d in rows:
        lines.append(
            f"| {label} "
            f"| {d['execution_accuracy_principal_pct']} "
            f"| {d['execution_accuracy_proj_pct']} "
            f"| {d['execution_accuracy_proj_tol_pct']} "
            f"| {d['execution_accuracy_colmap_pct']} "
            f"| {d['execution_accuracy_strict_pct']} "
            f"| {d['valid_sql_rate_pct']} "
            f"| {d['alias_apenas_pct']} "
            f"| {d['colunas_extras_aceitas_count']} "
            f"| {d['tolerancia_numerica_count']} |"
        )

    table = "\n".join(lines)
    print(table)
    out = REPO / "experimentos" / "comparison_table_v3.md"
    out.write_text(table + "\n", encoding="utf-8")
    print(f"\nSalvo em {out}")


if __name__ == "__main__":
    main()
