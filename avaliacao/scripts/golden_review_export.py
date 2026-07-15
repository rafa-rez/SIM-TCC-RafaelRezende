#!/usr/bin/env python3
"""Exporta pacote de revisão humana do golden v2 (Markdown)."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import date
from pathlib import Path

AVALIACAO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AVALIACAO_ROOT))

from eval.lib.paths import GOLDEN_DATASET_V2, GOLDEN_V2_DRAFT_DIR, GOLDEN_V2_REVISAO_DIR


def load_rows(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter=";"))


def load_draft(tid: str) -> dict | None:
    p = GOLDEN_V2_DRAFT_DIR / f"{tid}.json"
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def fmt_table(columns: list[str], rows: list[dict], limit: int = 10) -> str:
    if not columns or not rows:
        return "_Sem dados na prévia._\n"
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for row in rows[:limit]:
        cells = [str(row.get(c, "")).replace("|", "\\|").replace("\n", " ")[:80] for c in columns]
        lines.append("| " + " | ".join(cells) + " |")
    if len(rows) > limit:
        lines.append(f"\n_… e mais {len(rows) - limit} linhas._")
    return "\n".join(lines) + "\n"


def render_item(row: dict) -> str:
    tid = row["id_teste"]
    draft = load_draft(tid)
    parts = [
        f"## {tid} — {row.get('categoria', '')}",
        "",
        f"**Status:** `{row.get('status', '')}` | **Origem:** {row.get('origem', '')} | **Dificuldade:** {row.get('dificuldade', '')}",
        "",
        "### Pergunta (tester)",
        "",
        row.get("input_usuario", ""),
        "",
    ]
    if row.get("notas"):
        parts.extend(["### Notas de construção", "", row["notas"], ""])

    sql = (row.get("query_referencia") or "").strip()
    if draft:
        sql = sql or (draft.get("query_sql") or "").strip()
    if sql:
        parts.extend(["### SQL de referência (rascunho)", "", "```sql", sql, "```", ""])
    else:
        parts.extend(["### SQL de referência", "", "_Pendente — rodar `golden_assist.py`._", ""])

    if draft and draft.get("execution", {}).get("ok"):
        ex = draft["execution"]
        parts.extend([
            f"### Prévia tabular ({ex['row_count']} linhas)",
            "",
            fmt_table(ex.get("columns", []), ex.get("preview_rows", [])),
        ])
        sug = draft.get("colunas_resposta_sugeridas") or row.get("colunas_resposta", "")
        if sug:
            parts.extend(["**Sugestão `colunas_resposta`:** `" + sug + "`", ""])

    col = (row.get("colunas_resposta") or "").strip()
    if col:
        parts.extend([f"**`colunas_resposta` no CSV:** `{col}`", ""])

    parts.extend([
        "### Checklist do revisor",
        "",
        "- [ ] A pergunta está clara e é realista para um cidadão?",
        "- [ ] A SQL responde à pergunta (não só executa)?",
        "- [ ] Os valores da prévia fazem sentido para Caeté?",
        "- [ ] `colunas_resposta` cobre o que o usuário pediu?",
        "- [ ] Concordo em incluir no golden v2 congelado",
        "",
        "---",
        "",
    ])
    return "\n".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, default=GOLDEN_DATASET_V2)
    parser.add_argument("--status", default=None, help="Filtrar por status (opcional)")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    dataset = args.dataset
    if not dataset.is_absolute():
        dataset = AVALIACAO_ROOT / dataset

    rows = load_rows(dataset)
    if args.status:
        rows = [r for r in rows if r.get("status") == args.status]

    GOLDEN_V2_REVISAO_DIR.mkdir(parents=True, exist_ok=True)
    out = args.output or GOLDEN_V2_REVISAO_DIR / f"REVISAO_{date.today().isoformat()}.md"

    header = [
        "# Revisão golden v2.0 — SIM Caeté",
        "",
        f"Gerado em {date.today().isoformat()}. Itens: **{len(rows)}**.",
        "",
        "Instruções: marque os checkboxes e registre ajustes no CSV ou com o autor.",
        "",
        "---",
        "",
    ]
    body = "".join(render_item(r) for r in rows)
    out.write_text("\n".join(header) + body, encoding="utf-8")
    print(f"Salvo: {out}")


if __name__ == "__main__":
    main()
