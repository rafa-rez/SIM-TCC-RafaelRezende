#!/usr/bin/env python3
"""
LLM-as-Judge sobre respostas naturais geradas por run_response_eval.py.

Uso:
  python eval/run_judge.py --experiment baseline_16k --sample 30
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from openai import OpenAI

from eval.lib.budget import BudgetTracker
from eval.lib.config_loader import budget_limit_usd, get_openai_api_key, load_config

JUDGE_PROMPT = REPO / "eval" / "prompts" / "judge_rubric.txt"


def main() -> None:
    parser = argparse.ArgumentParser(description="LLM-as-Judge")
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--model", default="gpt-4.1-mini")
    parser.add_argument("--sample", type=int, default=0)
    parser.add_argument("--filter-ex-fail", action="store_true", help="So VSR=1 e EX=0")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    cfg = load_config()
    in_path = REPO / "eval" / "results" / args.experiment / "response_eval.jsonl"
    if not in_path.exists():
        print(f"Execute antes: python eval/run_response_eval.py --experiment {args.experiment}")
        sys.exit(1)

    out_path = REPO / "eval" / "results" / args.experiment / "judge_results.jsonl"
    budget_path = REPO / "eval" / "results" / args.experiment / "judge_budget.json"

    done: set[str] = set()
    if args.resume and out_path.exists():
        for line in out_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                done.add(str(json.loads(line)["id_teste"]))

    rows = [json.loads(l) for l in in_path.read_text(encoding="utf-8").splitlines() if l.strip()]
    if args.filter_ex_fail:
        rows = [r for r in rows if r.get("metrics_vsr") and not r.get("metrics_ex")]
    if args.sample > 0:
        rows = rows[: args.sample]

    judge_system = JUDGE_PROMPT.read_text(encoding="utf-8")
    client = OpenAI(api_key=get_openai_api_key())
    budget = BudgetTracker(budget_path, budget_limit_usd(cfg), 10.0, cfg["model_pricing"])

    print(f"Julgando {len(rows)} itens com {args.model}")

    for rec in rows:
        tid = str(rec["id_teste"])
        if tid in done:
            continue

        user_payload = json.dumps({
            "pergunta": rec.get("input_usuario"),
            "sql_executada": rec.get("query_gerada"),
            "total_linhas": rec.get("row_count"),
            "resposta_gerada": rec.get("resposta_natural"),
        }, ensure_ascii=False, indent=2)

        est = budget.estimate_call_cost(args.model, 1500, 400)
        if not budget.can_afford(est):
            print("Orcamento atingido.")
            sys.exit(2)

        t0 = time.perf_counter()
        resp = client.chat.completions.create(
            model=args.model,
            temperature=0.0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": judge_system},
                {"role": "user", "content": user_payload},
            ],
            timeout=cfg["openai"]["timeout_seconds"],
        )
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
        raw = resp.choices[0].message.content or "{}"
        usage = resp.usage
        cost = budget.register_usage(
            args.model,
            usage.prompt_tokens if usage else 0,
            usage.completion_tokens if usage else 0,
            {"id_teste": tid, "phase": "judge"},
        )

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = {"veredito": "erro_parse", "raw": raw}

        out_rec = {
            "id_teste": tid,
            "judge_model": args.model,
            "veredito": parsed.get("veredito"),
            "scores": parsed.get("scores"),
            "justificativa": parsed.get("justificativa"),
            "metrics_ex": rec.get("metrics_ex"),
            "timing_ms": elapsed_ms,
            "cost_usd": round(cost, 6),
            "finished_at": datetime.now().isoformat(),
        }

        with out_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(out_rec, ensure_ascii=False) + "\n")

        print(f"  [{tid}] {parsed.get('veredito')} | US$ {cost:.4f}")

    print(f"\n[OK] Julgamentos em: {out_path}")


if __name__ == "__main__":
    main()
