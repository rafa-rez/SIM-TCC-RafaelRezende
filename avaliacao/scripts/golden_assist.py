#!/usr/bin/env python3
"""Assistente para construção do golden v2: propõe SQL com prompt de produção e valida no DuckDB."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

AVALIACAO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AVALIACAO_ROOT))

from openai import OpenAI

from eval.lib.config_loader import get_openai_api_key, load_config, resolve_path
from eval.lib.duckdb_engine import execute_query_local as execute_query
from eval.lib.paths import GOLDEN_DATASET_V2, GOLDEN_V2_DRAFT_DIR
from eval.lib.prompt_loader import load_prompt
from eval.run_experiment import clean_json_output, run_single

PREVIEW_ROWS = 20

KEY_HINTS = ("ano", "cod_", "seq_", "num_", "id_", "doc", "cnpj", "mes")
METRIC_HINTS = ("total", "valor", "vlr", "sal", "perc", "qtd", "media", "nome", "objeto")


def suggest_colunas(columns: list[str]) -> str:
    if not columns:
        return ""
    keys = [c for c in columns if any(h in c.lower() for h in KEY_HINTS)]
    metrics = [
        c
        for c in columns
        if c not in keys and any(h in c.lower() for h in METRIC_HINTS)
    ]
    chosen = metrics or [c for c in columns if c not in keys]
    if not chosen:
        chosen = columns
    return ",".join(chosen[:8])


def load_golden_rows(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter=";"))


def save_golden_rows(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def assist_one(
    row: dict,
    *,
    client: OpenAI,
    system_prompt: str,
    model: str,
    cfg: dict,
    apply_sql: bool,
) -> dict:
    question = (row.get("input_usuario") or "").strip()
    tid = row["id_teste"]
    if not question:
        raise ValueError(f"[{tid}] input_usuario vazio")

    gen = run_single(
        client,
        system_prompt,
        model,
        question,
        float(cfg["openai"]["temperature"]),
        int(cfg["openai"]["timeout_seconds"]),
        int(cfg["execution"]["max_retries_openai"]),
    )
    sql = (gen.get("query_sql") or "").strip()
    execution: dict = {"ok": False, "row_count": 0, "columns": [], "preview_rows": [], "error": None}
    colunas_sugeridas = ""

    if sql:
        timeout = int(cfg["execution"].get("duckdb_timeout_seconds", 60))
        res = execute_query(sql, timeout=timeout)
        execution["ok"] = bool(res.get("ok"))
        execution["error"] = res.get("error")
        if res.get("ok"):
            data = res.get("data") or []
            execution["row_count"] = len(data)
            execution["columns"] = list(data[0].keys()) if data else []
            execution["preview_rows"] = data[:PREVIEW_ROWS]
            colunas_sugeridas = suggest_colunas(execution["columns"])

    draft = {
        "id_teste": tid,
        "input_usuario": question,
        "assisted_at": datetime.now(timezone.utc).isoformat(),
        "model": model,
        "generation": gen,
        "query_sql": sql,
        "execution": execution,
        "colunas_resposta_sugeridas": colunas_sugeridas,
    }

    GOLDEN_V2_DRAFT_DIR.mkdir(parents=True, exist_ok=True)
    draft_path = GOLDEN_V2_DRAFT_DIR / f"{tid}.json"
    draft_path.write_text(json.dumps(draft, indent=2, ensure_ascii=False), encoding="utf-8")

    if apply_sql and sql and execution["ok"]:
        row["query_referencia"] = sql
        if colunas_sugeridas and not (row.get("colunas_resposta") or "").strip():
            row["colunas_resposta"] = colunas_sugeridas
        row["status"] = "sql_valida"

    return draft


def main() -> None:
    parser = argparse.ArgumentParser(description="Assistente golden v2 (SIM)")
    parser.add_argument("--dataset", type=Path, default=GOLDEN_DATASET_V2)
    parser.add_argument("--id", dest="ids", action="append", help="id_teste (ex.: 001). Repetível.")
    parser.add_argument("--all", action="store_true", help="Processar todos os itens do dataset")
    parser.add_argument("--status", default="rascunho", help="Processar todos com este status (se --all omitido)")
    parser.add_argument("--model", default=None, help="Modelo OpenAI (padrão: config golden_assist.model)")
    parser.add_argument("--prompt", default="poscagada_16k", dest="prompt_variant")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Grava SQL/colunas sugeridas no CSV quando execução OK",
    )
    parser.add_argument("--dry-run", action="store_true", help="Lista itens sem chamar API")
    args = parser.parse_args()

    dataset = args.dataset
    if not dataset.is_absolute():
        dataset = AVALIACAO_ROOT / dataset
    if not dataset.exists():
        print(f"Dataset não encontrado: {dataset}", file=sys.stderr)
        sys.exit(1)

    rows = load_golden_rows(dataset)
    if not rows:
        print("Dataset vazio.", file=sys.stderr)
        sys.exit(1)

    fieldnames = list(rows[0].keys())
    if args.all:
        targets = rows
    elif args.ids:
        wanted = {i.zfill(3) if i.isdigit() else i for i in args.ids}
        targets = [r for r in rows if r["id_teste"] in wanted or r["id_teste"].zfill(3) in wanted]
    else:
        targets = [r for r in rows if (r.get("status") or "").strip() == args.status]

    if not targets:
        print("Nenhum item correspondente.", file=sys.stderr)
        sys.exit(1)

    if args.dry_run:
        for r in targets:
            print(f"{r['id_teste']}: {r['input_usuario'][:80]}...")
        return

    cfg = load_config()
    ga = cfg.get("golden_assist") or {}
    model = args.model or ga.get("model") or "gpt-5"
    prompt_variant = ga.get("prompt_variant", args.prompt_variant)
    system_prompt, prompt_path = load_prompt(prompt_variant, cfg)
    client = OpenAI(api_key=get_openai_api_key())

    print(f"Prompt: {prompt_path}")
    print(f"Itens: {len(targets)} | model={model} | apply={args.apply}\n")

    by_id = {r["id_teste"]: r for r in rows}
    for row in targets:
        tid = row["id_teste"]
        print(f"--- {tid} ---")
        try:
            draft = assist_one(
                by_id[tid],
                client=client,
                system_prompt=system_prompt,
                model=model,
                cfg=cfg,
                apply_sql=args.apply,
            )
            ex = draft["execution"]
            if ex["ok"]:
                print(f"OK SQL — {ex['row_count']} linhas, colunas: {ex['columns']}")
                print(f"Sugestão colunas_resposta: {draft['colunas_resposta_sugeridas']}")
            else:
                print(f"SQL gerada mas execução falhou: {ex.get('error')}")
            print(f"Rascunho: {GOLDEN_V2_DRAFT_DIR / f'{tid}.json'}")
        except Exception as exc:
            print(f"ERRO: {exc}", file=sys.stderr)

    if args.apply:
        save_golden_rows(dataset, list(by_id.values()), fieldnames)
        print(f"\nCSV atualizado: {dataset}")


if __name__ == "__main__":
    main()
