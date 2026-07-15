#!/usr/bin/env python3
"""Atualiza tabela de ablações em tcc-latex/secoes/resultados.tex."""

from __future__ import annotations

import json
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
RESULTS = REPO / "eval" / "results"
TEX = REPO / "tcc-latex" / "secoes" / "resultados.tex"

EXPERIMENTS = [
    ("baseline_16k", "baseline\\_16k (4.1-mini)"),
    ("ablation_precagada_30k", "ablation\\_precagada\\_30k (4.1-mini)"),
    ("ablation_gpt4o_mini", "ablation\\_gpt4o\\_mini"),
    ("ablation_gpt4o", "ablation\\_gpt4o"),
]


def fmt_row(label: str, data: dict) -> str:
    return (
        f"{label} & {data['execution_accuracy_pct']:.2f} & "
        f"{data['valid_sql_rate_pct']:.2f} & "
        f"{data['total_cost_usd']:.2f} & {data['avg_gen_ms']:.0f} \\\\"
    )


def main() -> None:
    rows = []
    for exp_id, label in EXPERIMENTS:
        path = RESULTS / exp_id / "metrics_summary.json"
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            rows.append(fmt_row(label, data))
        else:
            rows.append(f"{label} & --- & --- & --- & --- \\\\")

    body = "\n".join(rows)
    tex = TEX.read_text(encoding="utf-8")
    new_tex = re.sub(
        r"(\\begin\{tabular\}\{\|l\|r\|r\|r\|r\|\}\s*\\hline\s*"
        r"\\textbf\{Experimento\}.*?\\hline\s*)"
        r"(.*?)"
        r"(\s*\\hline\s*\\end\{tabular\})",
        lambda m: m.group(1) + body + "\n" + m.group(3),
        tex,
        count=1,
        flags=re.DOTALL,
    )
    TEX.write_text(new_tex, encoding="utf-8")
    print(f"Tabela atualizada em {TEX}")


if __name__ == "__main__":
    main()
