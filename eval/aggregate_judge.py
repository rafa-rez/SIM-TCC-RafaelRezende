#!/usr/bin/env python3
"""Agrega pipeline_trace.jsonl em metricas JAR para o TCC."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def pct(n: float, d: float) -> float:
    return round(100.0 * n / d, 2) if d else 0.0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", default="baseline_16k")
    parser.add_argument("--run-name", default="default")
    args = parser.parse_args()

    trace_path = REPO / "eval" / "results" / args.experiment / "judge_runs" / args.run_name / "pipeline_trace.jsonl"
    if not trace_path.exists():
        print(f"Trace nao encontrado: {trace_path}")
        sys.exit(1)

    rows = [json.loads(l) for l in trace_path.read_text(encoding="utf-8").splitlines() if l.strip()]
    n = len(rows)
    ok_verdicts = {"correto", "parcialmente_correto"}
    jar = sum(1 for r in rows if r.get("judge_phase", {}).get("veredito") in ok_verdicts)
    ex = sum(1 for r in rows if r.get("sql_phase", {}).get("metrics", {}).get("ex"))

    by_diff: dict[str, list] = defaultdict(list)
    for r in rows:
        by_diff[r.get("dificuldade") or "nao_def"].append(r)

    score_dims = ["adequacao_semantica", "fidelidade_dados", "completude", "clareza_cidadao"]
    avg_scores = {}
    for dim in score_dims:
        vals = []
        for r in rows:
            s = r.get("judge_phase", {}).get("scores") or {}
            if dim in s and isinstance(s[dim], (int, float)):
                vals.append(float(s[dim]))
        avg_scores[dim] = round(sum(vals) / len(vals), 2) if vals else None

    summary = {
        "experiment": args.experiment,
        "run_name": args.run_name,
        "total": n,
        "jar_pct": pct(jar, n),
        "jar_count": jar,
        "ex_pct": pct(ex, n),
        "ex_count": ex,
        "avg_judge_scores": avg_scores,
        "by_dificuldade": {
            k: {
                "n": len(v),
                "jar_pct": pct(sum(1 for x in v if x.get("judge_phase", {}).get("veredito") in ok_verdicts), len(v)),
                "ex_pct": pct(sum(1 for x in v if x.get("sql_phase", {}).get("metrics", {}).get("ex")), len(v)),
            }
            for k, v in sorted(by_diff.items())
        },
        "total_cost_usd": round(sum(r.get("total_cost_usd", 0) for r in rows), 4),
    }

    out_path = trace_path.parent / "judge_summary.json"
    out_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"\nSalvo: {out_path}")


if __name__ == "__main__":
    main()
