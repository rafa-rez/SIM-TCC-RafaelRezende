#!/usr/bin/env python3
"""
Pipeline unificado de métricas — SIM v1.0 (instância Caeté).

Uso:
  python eval/run_pipeline.py                    # SQL offline (padrão)
  python eval/run_pipeline.py --with-rag         # SQL + RAG (Qdrant + OpenAI)
  python eval/run_pipeline.py --smoke-only       # só checagens de pré-requisito
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from eval.lib.config_loader import load_config, resolve_path
from eval.lib.paths import EXPERIMENTOS_ROOT, GOLDEN_DATASET

SQL_EXPERIMENTS = [
    "e1_baseline_compacto",
    "e2_contexto_estendido",
    "e3_gpt4o_mini",
    "e4_gpt4o",
]

# Gate de regressão (P0.1): mínimos do baseline E1 reportado no TCC.
BASELINE_EXPERIMENT = "e1_baseline_compacto"
BASELINE_THRESHOLDS = {
    "valid_sql_rate_pct": 97.5,                 # VSR
    "execution_accuracy_principal_pct": 62.5,   # EF (EX resposta)
}


def run(cmd: list[str], *, cwd: Path | None = None) -> None:
    print(f"\n>> {' '.join(cmd)}")
    subprocess.run(cmd, cwd=cwd or REPO, check=True)


def smoke_sql() -> list[str]:
    errors: list[str] = []
    if not GOLDEN_DATASET.exists():
        errors.append(f"Golden SQL ausente: {GOLDEN_DATASET}")
    db = REPO / "dados" / "database"
    if not db.is_dir() or not any(db.glob("*.csv")):
        errors.append(f"CSVs DuckDB ausentes em {db}")
    for exp in SQL_EXPERIMENTS:
        ck = EXPERIMENTOS_ROOT / exp / "checkpoint.jsonl"
        if not ck.exists():
            errors.append(f"Checkpoint ausente: {ck}")
    return errors


def smoke_rag() -> list[str]:
    errors: list[str] = []
    try:
        import requests
    except ImportError:
        return ["Pacote requests não instalado (necessário para RAG)."]

    cfg = load_config().get("rag") or {}
    url = cfg.get("qdrant_url", "http://localhost:6333")
    collection = cfg.get("collection", "jornais_caete")
    dataset = resolve_path(cfg.get("dataset", "dados/golden_rag/golden_rag_v1.1.csv"))
    if not dataset.exists():
        errors.append(f"Golden RAG ausente: {dataset}")
    try:
        r = requests.get(f"{url.rstrip('/')}/collections/{collection}", timeout=5)
        if r.status_code != 200:
            errors.append(f"Qdrant inacessível: {url} ({r.status_code})")
    except Exception as exc:
        errors.append(f"Qdrant inacessível: {exc}")
    return errors


def run_sql_pipeline() -> Path:
    run([sys.executable, "scripts/enrich_golden_colunas.py"])
    for exp in SQL_EXPERIMENTS:
        run([sys.executable, "scripts/recompute_metrics.py", "--experiment", exp])
    run([sys.executable, "scripts/compare_all_experiments.py"])
    return EXPERIMENTOS_ROOT / "comparison_table_v3.md"


def check_baseline() -> list[str]:
    """Compara métricas recalculadas do E1 com os mínimos do TCC (VSR/EF)."""
    summary_path = EXPERIMENTOS_ROOT / BASELINE_EXPERIMENT / "metrics_summary_v3.json"
    if not summary_path.exists():
        return [f"Resumo do baseline ausente: {summary_path}"]
    metrics = json.loads(summary_path.read_text(encoding="utf-8"))
    failures: list[str] = []
    for key, minimum in BASELINE_THRESHOLDS.items():
        value = metrics.get(key)
        if value is None:
            failures.append(f"{key} ausente em {summary_path.name}")
        elif float(value) < minimum - 1e-9:
            failures.append(f"{key} = {value} (mínimo: {minimum})")
    return failures


def run_rag_pipeline(experiment: str) -> Path:
    run(
        [
            sys.executable,
            "eval/run_rag_eval.py",
            "--experiment",
            experiment,
        ]
    )
    return EXPERIMENTOS_ROOT / experiment / "metrics_summary_rag.json"


def write_report(
    out: Path,
    *,
    sql_ok: bool,
    rag_ok: bool | None,
    sql_table: Path | None,
    rag_summary: Path | None,
    smoke_errors: list[str],
    baseline_gate: dict | None = None,
) -> None:
    report = {
        "generated_at": datetime.now().isoformat(),
        "version": "sim-metrics-v1",
        "sql": {
            "status": "ok" if sql_ok else "skipped",
            "comparison_table": str(sql_table) if sql_table else None,
            "experiments": SQL_EXPERIMENTS,
        },
        "rag": {
            "status": "ok" if rag_ok else ("skipped" if rag_ok is None else "failed"),
            "summary": str(rag_summary) if rag_summary else None,
        },
        "smoke_errors": smoke_errors,
    }
    if baseline_gate is not None:
        report["baseline_gate"] = baseline_gate
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nRelatório: {out}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline de métricas SIM v1.0")
    parser.add_argument(
        "--with-rag",
        action="store_true",
        help="Inclui avaliação RAG (requer Qdrant + OPENAI_API_KEY)",
    )
    parser.add_argument(
        "--skip-sql",
        action="store_true",
        help="Pula recálculo Text-to-SQL",
    )
    parser.add_argument(
        "--smoke-only",
        action="store_true",
        help="Apenas verifica pré-requisitos",
    )
    parser.add_argument(
        "--rag-experiment",
        default="rag_baseline_v1_1",
        help="Nome da pasta em experimentos/ para RAG",
    )
    parser.add_argument(
        "--check-baseline",
        action="store_true",
        help=(
            "Gate de regressão: falha (exit 1) se E1 tiver "
            "VSR < 97,5%% ou EF < 62,5%%. Com --skip-sql, valida o resumo existente."
        ),
    )
    args = parser.parse_args()

    smoke_errors: list[str] = []
    if not args.skip_sql:
        smoke_errors.extend(smoke_sql())
    if args.with_rag or args.smoke_only:
        smoke_errors.extend(smoke_rag())

    if smoke_errors:
        print("SMOKE — problemas encontrados:")
        for e in smoke_errors:
            print(f"  - {e}")
        if args.smoke_only:
            sys.exit(1)

    if args.smoke_only:
        print("SMOKE OK.")
        return

    if smoke_errors:
        sys.exit(1)

    sql_table = None
    rag_summary = None
    sql_ok = False
    rag_ok = None

    if not args.skip_sql:
        sql_table = run_sql_pipeline()
        sql_ok = True
        print(f"\nSQL OK -> {sql_table}")

    if args.with_rag:
        rag_errors = smoke_rag()
        if rag_errors:
            print("RAG ignorado:", "; ".join(rag_errors))
            rag_ok = False
        else:
            rag_summary = run_rag_pipeline(args.rag_experiment)
            rag_ok = True
            print(f"\nRAG OK -> {rag_summary}")

    baseline_gate: dict | None = None
    gate_failures: list[str] = []
    if args.check_baseline:
        gate_failures = check_baseline()
        baseline_gate = {
            "experiment": BASELINE_EXPERIMENT,
            "thresholds": BASELINE_THRESHOLDS,
            "status": "pass" if not gate_failures else "fail",
            "failures": gate_failures,
        }
        if gate_failures:
            print("\nGATE BASELINE — FALHOU:")
            for f in gate_failures:
                print(f"  - {f}")
        else:
            print("\nGATE BASELINE — OK (VSR >= 97,5% e EF >= 62,5%).")

    report_path = EXPERIMENTOS_ROOT / "pipeline_report.json"
    write_report(
        report_path,
        sql_ok=sql_ok,
        rag_ok=rag_ok,
        sql_table=sql_table,
        rag_summary=rag_summary,
        smoke_errors=smoke_errors,
        baseline_gate=baseline_gate,
    )

    if gate_failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
