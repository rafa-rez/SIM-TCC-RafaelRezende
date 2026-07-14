#!/usr/bin/env python3
"""
Pipeline completo: resposta natural (orquestrador) + LLM-as-Judge.
Persiste input/output de cada etapa em pipeline_trace.jsonl.

Uso:
  python eval/run_judge_pipeline.py --experiment baseline_16k --ids-file eval/results/baseline_16k/judge_sample_pilot.json
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
JUDGE_PROMPT = REPO / "eval" / "prompts" / "judge_rubric.txt"
MAX_ROWS = 50


def load_ids(path: Path) -> list[str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return [str(x) for x in data["ids"]]


def build_orchestrator_user(question: str, sql: str, data: list) -> str:
    return json.dumps({
        "pergunta_usuario": question,
        "query_sql_executada": sql,
        "resultados": data[:MAX_ROWS],
        "total_linhas": len(data),
        "fontes_utilizadas": ["SICOM/CGU via DuckDB"],
        "instrucao": (
            "Os dados abaixo ja foram obtidos pela ferramenta de banco de dados. "
            "Formule a resposta final ao cidadao seguindo seu protocolo. "
            "NAO invente dados alem do JSON."
        ),
    }, ensure_ascii=False, indent=2)


def build_judge_user(question: str, sql: str, data: list, response: str) -> str:
    return json.dumps({
        "pergunta": question,
        "sql_executada": sql,
        "amostra_resultados": data[:20],
        "total_linhas": len(data),
        "resposta_gerada": response,
    }, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--ids-file", required=True)
    parser.add_argument("--orchestrator-model", default="gpt-4o")
    parser.add_argument("--judge-model", default="gpt-4.1-mini")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--run-name", default="default")
    args = parser.parse_args()

    cfg = load_config()
    ids = load_ids(Path(args.ids_file))
    ckpt_path = REPO / "eval" / "results" / args.experiment / "checkpoint.jsonl"
    ckpt_map = {str(r["id_teste"]): r for r in read_all_checkpoints(ckpt_path)}

    out_dir = REPO / "eval" / "results" / args.experiment / "judge_runs" / args.run_name
    out_dir.mkdir(parents=True, exist_ok=True)
    trace_path = out_dir / "pipeline_trace.jsonl"
    budget_path = out_dir / "budget_state.json"

    done: set[str] = set()
    if args.resume and trace_path.exists():
        for line in trace_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                done.add(str(json.loads(line)["id_teste"]))

    orch_sys = ORCHESTRATOR_PROMPT.read_text(encoding="utf-8")
    judge_sys = JUDGE_PROMPT.read_text(encoding="utf-8")
    client = OpenAI(api_key=get_openai_api_key())
    duck_url = cfg["duckdb_url"]
    budget = BudgetTracker(budget_path, budget_limit_usd(cfg), 10.0, cfg["model_pricing"])

    print(f"Pipeline judge | run={args.run_name} | n={len(ids)} | orch={args.orchestrator_model} | judge={args.judge_model}")

    for tid in ids:
        if tid in done:
            continue
        rec = ckpt_map.get(tid)
        if not rec:
            print(f"  [{tid}] SKIP: ausente no checkpoint")
            continue

        question = rec.get("input_usuario", "")
        sql = rec.get("query_gerada") or ""
        sql_result = execute_query(duck_url, sql, cfg["execution"]["duckdb_timeout_seconds"]) if sql else {
            "ok": False, "data": [], "error": "sem sql", "elapsed_ms": 0,
        }
        data = sql_result.get("data") or []

        orch_user = build_orchestrator_user(question, sql, data)
        est1 = budget.estimate_call_cost(args.orchestrator_model, 4000, 900)
        est2 = budget.estimate_call_cost(args.judge_model, 2000, 450)
        if not budget.can_afford(est1 + est2):
            print("Orcamento atingido.")
            sys.exit(2)

        t0 = time.perf_counter()
        orch_resp = client.chat.completions.create(
            model=args.orchestrator_model,
            temperature=0.3,
            messages=[
                {"role": "system", "content": orch_sys},
                {"role": "user", "content": orch_user},
            ],
            timeout=cfg["openai"]["timeout_seconds"],
        )
        orch_ms = round((time.perf_counter() - t0) * 1000, 2)
        natural = orch_resp.choices[0].message.content or ""
        u1 = orch_resp.usage
        cost1 = budget.register_usage(
            args.orchestrator_model,
            u1.prompt_tokens if u1 else 0,
            u1.completion_tokens if u1 else 0,
            {"id_teste": tid, "step": "orchestrator"},
        )

        judge_user = build_judge_user(question, sql, data, natural)
        t1 = time.perf_counter()
        judge_resp = client.chat.completions.create(
            model=args.judge_model,
            temperature=0.0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": judge_sys},
                {"role": "user", "content": judge_user},
            ],
            timeout=cfg["openai"]["timeout_seconds"],
        )
        judge_ms = round((time.perf_counter() - t1) * 1000, 2)
        judge_raw = judge_resp.choices[0].message.content or "{}"
        u2 = judge_resp.usage
        cost2 = budget.register_usage(
            args.judge_model,
            u2.prompt_tokens if u2 else 0,
            u2.completion_tokens if u2 else 0,
            {"id_teste": tid, "step": "judge"},
        )

        try:
            judge_parsed = json.loads(judge_raw)
        except json.JSONDecodeError:
            judge_parsed = {"veredito": "erro_parse", "raw": judge_raw}

        trace = {
            "id_teste": tid,
            "dificuldade": rec.get("dificuldade"),
            "categoria": rec.get("categoria"),
            "sql_phase": {
                "input_usuario": question,
                "query_referencia": rec.get("query_referencia"),
                "query_gerada": sql,
                "sql_exec_ok": sql_result.get("ok"),
                "sql_exec_error": sql_result.get("error"),
                "sql_exec_ms": sql_result.get("elapsed_ms"),
                "row_count": len(data),
                "resultado_amostra": data[:MAX_ROWS],
                "metrics": rec.get("metrics"),
            },
            "orchestrator_phase": {
                "model": args.orchestrator_model,
                "system_prompt_file": "eval/prompts/orchestrator_master.txt",
                "user_message": orch_user,
                "response": natural,
                "tokens_in": u1.prompt_tokens if u1 else 0,
                "tokens_out": u1.completion_tokens if u1 else 0,
                "timing_ms": orch_ms,
                "cost_usd": round(cost1, 6),
            },
            "judge_phase": {
                "model": args.judge_model,
                "system_prompt_file": "eval/prompts/judge_rubric.txt",
                "user_message": judge_user,
                "response_raw": judge_raw,
                "parsed": judge_parsed,
                "veredito": judge_parsed.get("veredito"),
                "scores": judge_parsed.get("scores"),
                "justificativa": judge_parsed.get("justificativa"),
                "tokens_in": u2.prompt_tokens if u2 else 0,
                "tokens_out": u2.completion_tokens if u2 else 0,
                "timing_ms": judge_ms,
                "cost_usd": round(cost2, 6),
            },
            "total_cost_usd": round(cost1 + cost2, 6),
            "finished_at": datetime.now().isoformat(),
        }

        with trace_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(trace, ensure_ascii=False) + "\n")

        print(
            f"  [{tid}] EX={rec.get('metrics', {}).get('ex')} "
            f"judge={judge_parsed.get('veredito')} | US$ {cost1+cost2:.4f}"
        )

    print(f"\n[OK] Trace completo: {trace_path}")


if __name__ == "__main__":
    main()
