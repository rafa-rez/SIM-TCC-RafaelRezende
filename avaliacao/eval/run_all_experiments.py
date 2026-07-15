#!/usr/bin/env python3
"""Executa sequência de experimentos com controle de orçamento global."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

EXPERIMENTS = [
    ("baseline_16k", "gpt-4.1-mini", "poscagada_16k"),
    ("ablation_precagada_30k", "gpt-4.1-mini", "precagada_30k"),
    ("ablation_gpt4o_mini", "gpt-4o-mini", "poscagada_16k"),
    ("ablation_gpt4o", "gpt-4o", "poscagada_16k"),
]


def main() -> None:
    for name, model, prompt in EXPERIMENTS:
        ckpt = REPO / "eval" / "results" / name / "checkpoint.jsonl"
        resume = ckpt.exists()
        cmd = [
            sys.executable,
            str(REPO / "eval" / "run_experiment.py"),
            "--experiment", name,
            "--model", model,
            "--prompt", prompt,
        ]
        if resume:
            cmd.append("--resume")
        print(f"\n{'='*60}\n>>> {name} ({model} / {prompt}){' [resume]' if resume else ''}\n{'='*60}")
        result = subprocess.run(cmd, cwd=REPO)
        if result.returncode == 2:
            print("Orçamento esgotado — interrompendo sequência.")
            sys.exit(2)
        if result.returncode != 0:
            print(f"Experimento {name} falhou com código {result.returncode}")
            sys.exit(result.returncode)
        agg = subprocess.run(
            [sys.executable, str(REPO / "eval" / "aggregate_report.py"), "--experiment", name],
            cwd=REPO,
        )
        if agg.returncode != 0:
            sys.exit(agg.returncode)
    print("\n✅ Todos os experimentos concluídos.")


if __name__ == "__main__":
    main()
