#!/usr/bin/env python3
"""
Orquestrador de experimentos Text-to-SQL com checkpoint e controle de orçamento.

Uso:
  python eval/run_experiment.py --experiment piloto_10
  python eval/run_experiment.py --experiment baseline --model gpt-4.1-mini --prompt poscagada_16k
  python eval/run_experiment.py --experiment ablation_models --resume
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

# Garante import do pacote eval a partir da raiz do repo
REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from openai import OpenAI

from eval.lib.budget import BudgetTracker
from eval.lib.checkpoint import append_checkpoint, load_completed_ids
from eval.lib.compare import compute_metrics
from eval.lib.config_loader import budget_limit_usd, get_openai_api_key, load_config, resolve_path
from eval.lib.duckdb_client import execute_query
from eval.lib.prompt_loader import estimate_tokens, load_prompt


def clean_json_output(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def load_golden(path: Path) -> list[dict]:
    rows = []
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            rows.append(row)
    return rows


def run_single(
    client: OpenAI,
    system_prompt: str,
    model: str,
    question: str,
    temperature: float,
    timeout: int,
    max_retries: int,
) -> dict:
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            t0 = time.perf_counter()
            resp = client.chat.completions.create(
                model=model,
                temperature=temperature,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": question},
                ],
                timeout=timeout,
            )
            gen_ms = (time.perf_counter() - t0) * 1000
            raw = resp.choices[0].message.content or ""
            usage = resp.usage
            parsed = json.loads(clean_json_output(raw))
            return {
                "ok": True,
                "raw": raw,
                "parsed": parsed,
                "query_sql": parsed.get("query_sql", ""),
                "explicacao": parsed.get("explicacao_tecnica", ""),
                "tokens_in": usage.prompt_tokens if usage else 0,
                "tokens_out": usage.completion_tokens if usage else 0,
                "gen_ms": round(gen_ms, 2),
                "attempt": attempt,
            }
        except Exception as exc:
            last_err = str(exc)
            time.sleep(2)
    return {
        "ok": False,
        "error": last_err,
        "query_sql": "",
        "tokens_in": 0,
        "tokens_out": 0,
        "gen_ms": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Experimento Text-to-SQL TCC")
    parser.add_argument("--experiment", required=True, help="Nome da pasta em eval/results/")
    parser.add_argument("--model", default="gpt-4.1-mini")
    parser.add_argument("--prompt", default="poscagada_16k", dest="prompt_variant")
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="Limitar N consultas (0=todas)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Só estima tokens/custo sem chamar OpenAI",
    )
    args = parser.parse_args()

    cfg = load_config()
    dataset_path = resolve_path(args.dataset or cfg["default_dataset"])
    if not dataset_path.exists():
        print(f"Dataset não encontrado: {dataset_path}")
        sys.exit(1)

    system_prompt, prompt_path = load_prompt(args.prompt_variant, cfg)
    prompt_tokens_est = estimate_tokens(system_prompt)

    out_dir = cfg["_eval_root"] / "results" / args.experiment
    out_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = out_dir / "checkpoint.jsonl"
    budget_path = out_dir / "budget_state.json"
    meta_path = out_dir / "experiment_meta.json"

    limit_usd = budget_limit_usd(cfg)
    budget = BudgetTracker(
        budget_path,
        limit_usd,
        float(cfg.get("budget_warn_at_usd", 10)),
        cfg["model_pricing"],
    )

    meta = {
        "experiment": args.experiment,
        "started_at": datetime.now().isoformat(),
        "model": args.model,
        "prompt_variant": args.prompt_variant,
        "prompt_file": str(prompt_path),
        "prompt_tokens_est": prompt_tokens_est,
        "dataset": str(dataset_path),
        "budget_limit_usd": limit_usd,
    }
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")

    golden = load_golden(dataset_path)
    if args.limit > 0:
        golden = golden[: args.limit]

    completed = load_completed_ids(checkpoint_path) if args.resume else set()
    if completed:
        print(f"Retomando: {len(completed)} consultas já concluídas.")

    duck_url = cfg["duckdb_url"]
    client = None if args.dry_run else OpenAI(api_key=get_openai_api_key())

    print(f"Experimento: {args.experiment}")
    print(f"Modelo: {args.model} | Prompt: {args.prompt_variant} (~{prompt_tokens_est} tok)")
    print(f"Dataset: {len(golden)} consultas | {budget.status_line()}")

    if args.dry_run:
        est_cost = budget.estimate_call_cost(args.model, prompt_tokens_est + 80, 400)
        total_est = est_cost * len([g for g in golden if g["id_teste"] not in completed])
        print(f"[DRY-RUN] Custo estimado restante: US$ {total_est:.2f}")
        return

    for row in golden:
        tid = str(row["id_teste"])
        if tid in completed:
            continue

        # Pré-checagem de orçamento (estimativa conservadora)
        est = budget.estimate_call_cost(args.model, prompt_tokens_est + 100, 500)
        if not budget.can_afford(est):
            print(f"\n[LIMITE] Orcamento atingido. {budget.status_line()}")
            print("Resultados parciais salvos em:", checkpoint_path)
            sys.exit(2)

        print(f"\n[{tid}] {row.get('categoria', '')} — {row['input_usuario'][:70]}...")

        gen = run_single(
            client,
            system_prompt,
            args.model,
            row["input_usuario"],
            cfg["openai"]["temperature"],
            cfg["openai"]["timeout_seconds"],
            cfg["execution"]["max_retries_openai"],
        )

        cost = budget.register_usage(
            args.model,
            gen.get("tokens_in", 0),
            gen.get("tokens_out", 0),
            {"id_teste": tid},
        )

        ref_sql = (row.get("query_referencia") or "").strip()
        ref_result = execute_query(
            duck_url,
            ref_sql,
            cfg["execution"]["duckdb_timeout_seconds"],
        ) if ref_sql else {"ok": False, "data": [], "error": "sem query_referencia"}

        gen_result = execute_query(
            duck_url,
            gen.get("query_sql", ""),
            cfg["execution"]["duckdb_timeout_seconds"],
        ) if gen.get("query_sql") else {"ok": False, "data": [], "error": gen.get("error")}

        tables_exp = [t.strip() for t in row.get("tabelas_esperadas", "").split(",") if t.strip()]
        conds = [c.strip() for c in row.get("condicao_esperada", "").split(",") if c.strip()]

        bundle = compute_metrics(
            gen_result.get("data") or [],
            ref_result.get("data") or [],
            vsr=bool(gen_result["ok"]),
            sql_generated=gen.get("query_sql", ""),
            expected_tables=tables_exp,
            conditions=conds,
            espera_dados=row.get("espera_dados", "sim"),
            colunas_resposta=row.get("colunas_resposta", ""),
        ) if ref_sql else compute_metrics(
            [],
            [],
            vsr=bool(gen_result["ok"]),
            sql_generated=gen.get("query_sql", ""),
            expected_tables=tables_exp,
            conditions=conds,
            espera_dados=row.get("espera_dados", "sim"),
            colunas_resposta=row.get("colunas_resposta", ""),
        )

        record = {
            "status": "completed",
            "id_teste": tid,
            "dificuldade": row.get("dificuldade", ""),
            "categoria": row.get("categoria", ""),
            "input_usuario": row["input_usuario"],
            "model": args.model,
            "prompt_variant": args.prompt_variant,
            "metrics": bundle.to_dict(),
            "timing": {
                "gen_ms": gen.get("gen_ms"),
                "sql_ref_ms": ref_result.get("elapsed_ms"),
                "sql_gen_ms": gen_result.get("elapsed_ms"),
            },
            "tokens": {
                "in": gen.get("tokens_in"),
                "out": gen.get("tokens_out"),
                "cost_usd": round(cost, 6),
            },
            "query_referencia": ref_sql,
            "query_gerada": gen.get("query_sql", ""),
            "erro_geracao": gen.get("error"),
            "erro_sql_gerado": gen_result.get("error"),
            "erro_sql_referencia": ref_result.get("error"),
            "budget_total_usd": budget.state["total_usd"],
            "finished_at": datetime.now().isoformat(),
        }

        append_checkpoint(checkpoint_path, record)
        m = record["metrics"]
        print(
            f"  EXr={'SIM' if m.get('ex_resposta') else 'NAO'} | "
            f"EXp={'SIM' if m.get('ex_proj') else 'NAO'} | "
            f"VSR={'SIM' if m['vsr'] else 'NAO'} | US$ {cost:.4f} | {budget.status_line()}"
        )

    print(f"\n[OK] Experimento concluido: {checkpoint_path}")
    print(budget.status_line())


if __name__ == "__main__":
    main()
