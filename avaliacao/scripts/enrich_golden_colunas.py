#!/usr/bin/env python3
"""Preenche colunas_resposta no golden a partir dos resultados de referência."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

AVALIACAO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AVALIACAO_ROOT))

from eval.lib.paths import GOLDEN_DATASET, REFERENCE_RESULTS_DIR

KEY_HINTS = (
    "ano_referencia",
    "ano",
    "num_doc",
    "cnpj",
    "cod_",
    "seq_",
    "id_",
)

METRIC_HINTS = (
    "total",
    "valor",
    "vlr",
    "perc",
    "porcent",
    "isf",
    "media",
    "qtd",
    "quant",
    "soma",
    "saldo",
)


def is_key_column(name: str) -> bool:
    low = name.lower()
    return any(h in low for h in KEY_HINTS)


def is_metric_column(name: str) -> bool:
    low = name.lower()
    return any(h in low for h in METRIC_HINTS)


def infer_colunas(columns: list[str], row_count: int) -> str:
    if not columns:
        return ""
    if len(columns) == 1:
        return columns[0]

    keys = [c for c in columns if is_key_column(c)]
    metrics = [c for c in columns if is_metric_column(c) and c not in keys]
    others = [c for c in columns if c not in keys and c not in metrics]

    if row_count == 1 and metrics:
        return metrics[0]
    if keys and metrics:
        return ",".join([*keys, *metrics])
    if metrics:
        return ",".join(metrics)
    if keys and others:
        return ",".join([*keys, *others])
    return ",".join(columns)


def main() -> None:
    rows: list[dict[str, str]] = []
    with GOLDEN_DATASET.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        fieldnames = list(reader.fieldnames or [])
        if "colunas_resposta" not in fieldnames:
            fieldnames.append("colunas_resposta")
        for row in reader:
            tid = row["id_teste"]
            path = REFERENCE_RESULTS_DIR / f"{int(tid):03d}.json"
            if path.exists():
                payload = json.loads(path.read_text(encoding="utf-8"))
                cols = payload.get("columns") or []
                row["colunas_resposta"] = infer_colunas(cols, payload.get("row_count", 0))
            else:
                row["colunas_resposta"] = row.get("colunas_resposta", "")
            rows.append(row)

    with GOLDEN_DATASET.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)

    filled = sum(1 for r in rows if r.get("colunas_resposta"))
    print(f"Atualizado {GOLDEN_DATASET} ({filled}/{len(rows)} com colunas_resposta)")


if __name__ == "__main__":
    main()
