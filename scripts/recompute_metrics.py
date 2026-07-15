#!/usr/bin/env python3
"""Recalcula métricas a partir de checkpoint.jsonl sem chamar a API OpenAI."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from eval.lib.checkpoint import read_all_checkpoints
from eval.lib.compare import DEFAULT_TAU, compute_metrics
from eval.lib.duckdb_engine import execute_query_local as execute_query


def pct(num: float, den: float) -> float:
    return round(100.0 * num / den, 2) if den else 0.0


def metric_rate(rows: list[dict], key: str) -> float:
    return pct(sum(1 for r in rows if r["metrics"].get(key)), len(rows))


def load_golden_map(path: Path) -> dict[str, dict]:
    import csv

    rows = {}
    with path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, delimiter=";"):
            rows[row["id_teste"]] = row
    return rows


def build_summary(
    experiment: str,
    updated: list[dict],
    tau: float,
) -> dict:
    n = len(updated)
    by_diff: dict[str, list] = defaultdict(list)
    by_cat: dict[str, list] = defaultdict(list)
    for r in updated:
        by_diff[r.get("dificuldade") or "nao_definido"].append(r)
        by_cat[r.get("categoria") or "nao_definido"].append(r)

    divergencia_keys = [
        "cardinalidade_linhas",
        "cardinalidade_colunas",
        "alias_colunas",
        "colunas_extras_aceitas",
        "tolerancia_numerica",
        "permutacao_valores_linha",
        "divergencia_valores",
    ]

    return {
        "experiment": experiment,
        "total": n,
        "metrics_version": "3.0",
        "tau_rel_tol": tau,
        "execution_accuracy_principal_pct": metric_rate(updated, "ex_resposta"),
        "execution_accuracy_proj_pct": metric_rate(updated, "ex_proj"),
        "execution_accuracy_proj_tol_pct": metric_rate(updated, "ex_proj_tol"),
        "execution_accuracy_strict_pct": metric_rate(updated, "ex_strict"),
        "execution_accuracy_colmap_pct": metric_rate(updated, "ex_colmap"),
        "execution_accuracy_content_pct": metric_rate(updated, "ex_content"),
        "execution_accuracy_rows_pct": metric_rate(updated, "ex_rows"),
        "execution_accuracy_cols_pct": metric_rate(updated, "ex_cols"),
        "valid_sql_rate_pct": metric_rate(updated, "vsr"),
        "nea_rate_pct": metric_rate(updated, "nea_ok"),
        "tsa_rate_pct": metric_rate(updated, "tsa"),
        "chs_mean": round(
            sum(r["metrics"].get("chs", 0) for r in updated) / n, 4
        ),
        "alias_apenas_count": sum(
            1 for r in updated if r["metrics"].get("alias_apenas")
        ),
        "alias_apenas_pct": metric_rate(updated, "alias_apenas"),
        "colunas_extras_aceitas_count": sum(
            1
            for r in updated
            if r["metrics"].get("divergencia_ex") == "colunas_extras_aceitas"
        ),
        "tolerancia_numerica_count": sum(
            1
            for r in updated
            if r["metrics"].get("divergencia_ex") == "tolerancia_numerica"
        ),
        "divergencia_ex": {
            k: sum(
                1
                for r in updated
                if r["metrics"].get("divergencia_ex") == k
            )
            for k in divergencia_keys
        },
        "by_dificuldade": {
            k: {
                "n": len(v),
                "ex_resposta_pct": metric_rate(v, "ex_resposta"),
                "ex_proj_pct": metric_rate(v, "ex_proj"),
                "ex_colmap_pct": metric_rate(v, "ex_colmap"),
                "ex_strict_pct": metric_rate(v, "ex_strict"),
                "vsr_pct": metric_rate(v, "vsr"),
            }
            for k, v in sorted(by_diff.items())
        },
        "by_categoria": {
            k: {
                "n": len(v),
                "ex_resposta_pct": metric_rate(v, "ex_resposta"),
                "ex_proj_pct": metric_rate(v, "ex_proj"),
            }
            for k, v in sorted(by_cat.items())
        },
        "total_cost_usd": updated[-1].get("budget_total_usd"),
        "avg_gen_ms": round(
            sum(r.get("timing", {}).get("gen_ms") or 0 for r in updated) / n,
            2,
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--checkpoint", help="Caminho alternativo para checkpoint.jsonl")
    parser.add_argument("--dataset", default="dados/golden/golden_dataset_v1.0.csv")
    parser.add_argument("--output", help="Saída JSON")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument(
        "--tau",
        type=float,
        default=DEFAULT_TAU,
        help="Tolerância relativa numérica (padrão 0,005 = 0,5%%)",
    )
    args = parser.parse_args()

    ckpt_path = (
        Path(args.checkpoint)
        if args.checkpoint
        else REPO / "experimentos" / args.experiment / "checkpoint.jsonl"
    )
    if not ckpt_path.exists():
        ckpt_path = REPO / "eval" / "results" / args.experiment / "checkpoint.jsonl"
    if not ckpt_path.exists():
        print(f"Checkpoint não encontrado: {args.experiment}")
        sys.exit(1)

    golden = load_golden_map(REPO / args.dataset)
    records = [r for r in read_all_checkpoints(ckpt_path) if r.get("status") == "completed"]
    if not records:
        print("Nenhum registro concluído.")
        sys.exit(0)

    per_case_dir = ckpt_path.parent / "metricas_por_caso"
    per_case_dir.mkdir(parents=True, exist_ok=True)

    updated: list[dict] = []
    for rec in records:
        tid = rec["id_teste"]
        g = golden.get(tid, {})
        ref_sql = (rec.get("query_referencia") or g.get("query_referencia") or "").strip()
        gen_sql = (rec.get("query_gerada") or "").strip()

        ref_result = (
            execute_query(ref_sql, timeout=args.timeout)
            if ref_sql
            else {"ok": False, "data": [], "error": "sem query_referencia"}
        )
        gen_result = (
            execute_query(gen_sql, timeout=args.timeout)
            if gen_sql
            else {"ok": False, "data": [], "error": "sem query_gerada"}
        )

        tables_exp = [
            t.strip()
            for t in (g.get("tabelas_esperadas") or "").split(",")
            if t.strip()
        ]
        conds = [
            c.strip()
            for c in (g.get("condicao_esperada") or "").split(",")
            if c.strip()
        ]

        bundle = compute_metrics(
            gen_result.get("data") or [],
            ref_result.get("data") or [],
            vsr=bool(gen_result.get("ok")),
            sql_generated=gen_sql,
            expected_tables=tables_exp,
            conditions=conds,
            espera_dados=g.get("espera_dados", "sim"),
            colunas_resposta=g.get("colunas_resposta", ""),
            tau=args.tau,
        )

        metrics = bundle.to_dict()
        case_detail = {
            "id_teste": tid,
            "input_usuario": rec.get("input_usuario") or g.get("input_usuario"),
            "colunas_resposta": g.get("colunas_resposta", ""),
            "metrics": metrics,
            "query_referencia": ref_sql,
            "query_gerada": gen_sql,
            "erro_sql_gerado": gen_result.get("error"),
            "erro_sql_referencia": ref_result.get("error"),
        }
        (per_case_dir / f"{int(tid):03d}.json").write_text(
            json.dumps(case_detail, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        new_rec = dict(rec)
        new_rec["metrics"] = metrics
        updated.append(new_rec)

    summary = build_summary(args.experiment, updated, args.tau)

    out_path = (
        Path(args.output)
        if args.output
        else ckpt_path.parent / "metrics_summary_v3.json"
    )
    out_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    # Mantém v2 como alias da principal para compatibilidade
    legacy = dict(summary)
    legacy["metrics_version"] = "2.0"
    legacy["execution_accuracy_content_pct"] = summary["execution_accuracy_content_pct"]
    (ckpt_path.parent / "metrics_summary_v2.json").write_text(
        json.dumps(legacy, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"\nSalvo em: {out_path}")
    print(f"Detalhes por caso: {per_case_dir}")


if __name__ == "__main__":
    main()
