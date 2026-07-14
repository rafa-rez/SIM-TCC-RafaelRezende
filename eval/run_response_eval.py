#!/usr/bin/env python3
"""
Gera respostas em linguagem natural usando o prompt orquestrador (producao)
a partir dos resultados SQL salvos nos experimentos Text-to-SQL.

Uso:
  python eval/run_response_eval.py --experiment baseline_16k --sample 30
  python eval/run_response_eval.py --experiment baseline_16k --resume
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
from eval.lib.checkpoint import read_all_checkpoints
from eval.lib.config_loader import budget_limit_usd, get_openai_api_key, load_config
from eval.lib.duckdb_client import execute_query

ORCHESTRATOR_PROMPT = REPO / "eval" / "prompts" / "orchestrator_master.txt"
MAX_ROWS = 50


def load_orchestrator() -> str:
    if not ORCHESTRATOR_PROMPT.exists():
        raise FileNotFoundError(f"Prompt orquestrador nao encontrado: {ORCHESTRATOR_PROMPT}")
    return ORCHESTRATOR_PROMPT.read_text(encoding="utf-8")


def build_user_message(question: str, sql: str, data: list, fontes: list[str] | None = None) -> str:
    sample = data[:MAX_ROWS]
    payload = {
        "pergunta_usuario": question,
        "query_sql_executada": sql,
        "resultados": sample,
        "total_linhas": len(data),
        "fontes_utilizadas": fontes or ["SICOM/CGU via DuckDB"],
        "instrucao": (
            "Os dados abaixo ja foram obtidos pela ferramenta de banco de dados. "
            "Formule a resposta final ao cidadao seguindo seu protocolo. "
            "NAO invente dados alem do JSON."
        ),
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Avaliacao de respostas naturais (orquestrador)")
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--model", default="gpt-4o")
    parser.add_argument("--sample", type=int, default=0, help="Limitar N consultas (0=todas)")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--duckdb-url", default=None)
    args = parser.parse_args()

    cfg = load_config()
    ckpt_in = REPO / "eval" / "results" / args.experiment / "checkpoint.jsonl"
    if not ckpt_in.exists():
        print(f"Checkpoint nao encontrado: {ckpt_in}")
        sys.exit(1)

    out_dir = REPO / "eval" / "results" / args.experiment
    out_path = out_dir / "response_eval.jsonl"
    budget_path = out_dir / "response_eval_budget.json"

    done: set[str] = set()
    if args.resume and out_path.exists():
        for line in out_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                done.add(str(json.loads(line)["id_teste"]))

    records = [r for r in read_all_checkpoints(ckpt_in) if r.get("status") == "completed"]
    if args.sample > 0:
        records = records[: args.sample]

    system_prompt = load_orchestrator()
    client = OpenAI(api_key=get_openai_api_key())
    duck_url = args.duckdb_url or cfg["duckdb_url"]
    budget = BudgetTracker(budget_path, budget_limit_usd(cfg), 10.0, cfg["model_pricing"])

    print(f"Experimento: {args.experiment} | Modelo: {args.model} | Itens: {len(records)}")

    for rec in records:
        tid = str(rec["id_teste"])
        if tid in done:
            continue

        sql = rec.get("query_gerada") or ""
        gen_result = execute_query(duck_url, sql, cfg["execution"]["duckdb_timeout_seconds"]) if sql else {
            "ok": False, "data": [], "error": "sem sql"
        }
        data = gen_result.get("data") or []

        user_msg = build_user_message(
            rec.get("input_usuario", ""),
            sql,
            data,
        )

        est = budget.estimate_call_cost(args.model, 3000, 800)
        if not budget.can_afford(est):
            print("Orcamento atingido.")
            sys.exit(2)

        t0 = time.perf_counter()
        resp = client.chat.completions.create(
            model=args.model,
            temperature=0.3,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_msg},
            ],
            timeout=cfg["openai"]["timeout_seconds"],
        )
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
        text = resp.choices[0].message.content or ""
        usage = resp.usage
        cost = budget.register_usage(
            args.model,
            usage.prompt_tokens if usage else 0,
            usage.completion_tokens if usage else 0,
            {"id_teste": tid, "phase": "response_eval"},
        )

        out_rec = {
            "id_teste": tid,
            "input_usuario": rec.get("input_usuario"),
            "query_gerada": sql,
            "sql_ok": gen_result.get("ok"),
            "row_count": len(data),
            "metrics_ex": rec.get("metrics", {}).get("ex"),
            "metrics_vsr": rec.get("metrics", {}).get("vsr"),
            "resposta_natural": text,
            "model": args.model,
            "timing_ms": elapsed_ms,
            "cost_usd": round(cost, 6),
            "finished_at": datetime.now().isoformat(),
        }

        with out_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(out_rec, ensure_ascii=False) + "\n")

        print(f"  [{tid}] {len(text)} chars | US$ {cost:.4f} | {budget.status_line()}")

    print(f"\n[OK] Respostas salvas em: {out_path}")


if __name__ == "__main__":
    main()
