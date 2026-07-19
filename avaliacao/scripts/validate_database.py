#!/usr/bin/env python3
"""Relatório de qualidade dos CSVs SICOM: linhas com colunas numéricas corrompidas.

Não altera nenhum dado — apenas quantifica, por tabela e ano, quantas linhas têm
valores não-numéricos em colunas vlr_* (sintoma de colunas deslocadas no export,
que o read_csv_auto(ignore_errors=true) deixa passar silenciosamente).

Uso:
  python scripts/validate_database.py [--out experimentos/data_quality_report.json]
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

AVALIACAO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AVALIACAO_ROOT))

from eval.lib.duckdb_engine import get_connection  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        default=str(AVALIACAO_ROOT / "experimentos" / "data_quality_report.json"),
    )
    args = parser.parse_args()

    con = get_connection()
    tabelas = [
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.views WHERE table_schema = 'main' "
            "AND table_name NOT LIKE 'duckdb_%' AND table_name NOT LIKE 'sqlite_%' "
            "AND table_name NOT LIKE 'pragma_%' ORDER BY table_name"
        ).fetchall()
    ]

    report: dict = {
        "generated_at": datetime.now().isoformat(),
        "criterio": (
            "linhas com coluna vlr_* preenchida cujo TRY_CAST para DECIMAL(18,3) é NULL"
        ),
        "tabelas": {},
    }
    total_bad = 0

    for t in tabelas:
        cols = [
            r[0]
            for r in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'main' AND table_name = ? "
                "AND column_name LIKE 'vlr_%' AND data_type LIKE '%VARCHAR%'",
                [t],
            ).fetchall()
        ]
        if not cols:
            continue
        cond = " OR ".join(
            f'("{c}" IS NOT NULL AND TRY_CAST("{c}" AS DECIMAL(18,3)) IS NULL)'
            for c in cols
        )
        try:
            rows = con.execute(
                f'SELECT ano_referencia, COUNT(*) AS linhas_ruins FROM "{t}" '
                f"WHERE {cond} GROUP BY ano_referencia ORDER BY ano_referencia"
            ).fetchall()
        except Exception as exc:
            report["tabelas"][t] = {"erro": str(exc)[:200]}
            continue
        if rows:
            bad = {str(ano): int(n) for ano, n in rows}
            n_bad = sum(bad.values())
            total_bad += n_bad
            report["tabelas"][t] = {
                "colunas_varchar_vlr": cols,
                "linhas_corrompidas_por_ano": bad,
                "total": n_bad,
            }
            print(f"{t}: {n_bad} linhas corrompidas {bad}")

    report["total_linhas_corrompidas"] = total_bad
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nTotal geral: {total_bad} linhas | Relatório: {out}")


if __name__ == "__main__":
    main()
