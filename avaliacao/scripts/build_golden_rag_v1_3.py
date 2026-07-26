#!/usr/bin/env python3
"""
Gera golden_rag_v1.3.csv (n=40): amostra honesta + itens temáticos curados.

Composição:
- 32 perguntas com número explícito de ato (sorteio uniforme do inventário, seed 42)
- 8 perguntas temáticas curadas (golden v1.1, estrato semântico)
"""

from __future__ import annotations

import csv
import json
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from eval.lib.config_loader import get_openai_api_key, load_config
from eval.lib.qdrant_validate import excerpt_from_chunk, find_chunks_for_act, parse_act_from_expected

OUT_DIR = ROOT / "dados" / "golden_rag"
INVENTARIO = OUT_DIR / "inventario_atos_v1.2.json"
CSV_V11 = OUT_DIR / "golden_rag_v1.1.csv"
CSV_OUT = OUT_DIR / "golden_rag_v1.3.csv"
REPORT_PATH = OUT_DIR / "validation_report_v1.3.json"

FIELDS = [
    "id",
    "status",
    "origem",
    "estrato",
    "pergunta",
    "doc_id_esperado",
    "titulo_documento",
    "trecho_referencia",
    "tipo",
    "dificuldade",
    "notas",
]

RANDOM_N = 32
THEMATIC_N = 8
SEED = 42


def format_numero(tipo: str, numero: int) -> str:
    if tipo in {"DECRETO", "PORTARIA"}:
        return f"{numero:03d}"
    return str(numero)


def canonical_doc_id(tipo: str, numero: int, ano: int) -> str:
    num = format_numero(tipo, numero)
    labels = {
        "DECRETO": f"Decreto Municipal nº {num}/{ano}",
        "PORTARIA": f"Portaria Municipal nº {num}/{ano}",
        "EDITAL": f"Edital nº {num}/{ano}",
        "LEI": f"Lei Municipal nº {num}/{ano}",
    }
    return labels.get(tipo, f"{tipo.title()} nº {num}/{ano}")


def question_template(tipo: str, numero: int, ano: int) -> str:
    doc_id = canonical_doc_id(tipo, numero, ano)
    if tipo == "PORTARIA":
        return f"O que dispõe a {doc_id} de Caeté?"
    return f"O que dispõe o {doc_id} de Caeté?"


def load_inventory() -> list[dict]:
    data = json.loads(INVENTARIO.read_text(encoding="utf-8"))
    return list(data["atos"])


def sample_random_acts(
    acts: list[dict],
    exclude_keys: set[str],
    n: int,
    *,
    qdrant_url: str,
    collection: str,
) -> list[dict]:
    pool = []
    for act in acts:
        if act["tipo"] != "DECRETO":
            continue
        key = f"{act['tipo']}|{act['numero']}|{act['ano']}"
        if key not in exclude_keys:
            pool.append(act)
    rng = random.Random(SEED)
    rng.shuffle(pool)
    selected: list[dict] = []
    for act in pool:
        if len(selected) >= n:
            break
        doc_id = canonical_doc_id(act["tipo"], int(act["numero"]), int(act["ano"]))
        act_ref = parse_act_from_expected(doc_id)
        if not act_ref:
            continue
        chunks = find_chunks_for_act(act_ref, url=qdrant_url, collection=collection)
        if not chunks:
            continue
        selected.append(act)
    if len(selected) < n:
        raise RuntimeError(f"Inventário insuficiente: {len(selected)} decretos validados, precisamos {n}")
    return selected


def load_v11_rows() -> list[dict]:
    with open(CSV_V11, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter=";"))


def validate_row(row: dict, *, qdrant_url: str, collection: str) -> dict:
    act = parse_act_from_expected(row["doc_id_esperado"])
    if not act:
        return {**row, "status": "invalido", "notas": row.get("notas", "") + " | doc_id não parseável"}
    chunks = find_chunks_for_act(act, url=qdrant_url, collection=collection)
    if not chunks:
        return {**row, "status": "ausente_indice", "notas": row.get("notas", "") + " | ato ausente no índice"}
    excerpt = excerpt_from_chunk(chunks[0].get("payload", {}).get("page_content", ""))
    return {
        **row,
        "status": "congelado",
        "trecho_referencia": excerpt[:400],
        "notas": (row.get("notas") or "") + f" | chunk confirmado ({len(chunks)} no índice)",
    }


def main() -> None:
    cfg = load_config()
    rag_cfg = cfg.get("rag") or {}
    qdrant_url = rag_cfg.get("qdrant_url", "http://localhost:6333")
    collection = rag_cfg.get("collection", "jornais_caete")
    _ = get_openai_api_key()  # fail fast if missing when validating

    v11_rows = load_v11_rows()[:THEMATIC_N]
    exclude = set()
    for row in v11_rows:
        act = parse_act_from_expected(row["doc_id_esperado"])
        if act:
            exclude.add(f"{act.tipo}|{int(act.numero)}|{act.ano}")

    random_acts = sample_random_acts(
        load_inventory(), exclude, RANDOM_N, qdrant_url=qdrant_url, collection=collection
    )
    rows: list[dict] = []

    for i, act in enumerate(random_acts, start=1):
        tipo = act["tipo"]
        numero = int(act["numero"])
        ano = int(act["ano"])
        doc_id = canonical_doc_id(tipo, numero, ano)
        rows.append(
            {
                "id": f"{i:03d}",
                "status": "candidato",
                "origem": f"amostra_aleatoria_indice_seed{SEED}",
                "estrato": "com_numero",
                "pergunta": question_template(tipo, numero, ano),
                "doc_id_esperado": doc_id,
                "titulo_documento": doc_id,
                "trecho_referencia": "",
                "tipo": tipo.lower(),
                "dificuldade": "fácil",
                "notas": (
                    f"Sorteio uniforme (seed {SEED}) do inventário de 747 atos; "
                    "inclusão independente de rank."
                ),
            }
        )

    offset = len(rows)
    for j, src in enumerate(v11_rows, start=1):
        rows.append(
            {
                "id": f"{offset + j:03d}",
                "status": src.get("status", "congelado"),
                "origem": src.get("origem", "v1.1_tematico"),
                "estrato": "tematico",
                "pergunta": src["pergunta"],
                "doc_id_esperado": src["doc_id_esperado"],
                "titulo_documento": src["titulo_documento"],
                "trecho_referencia": src.get("trecho_referencia", ""),
                "tipo": src.get("tipo", "decreto"),
                "dificuldade": src.get("dificuldade", "médio"),
                "notas": f"Reaproveitado do golden v1.1 (item {src['id']}).",
            }
        )

    validated: list[dict] = []
    report_items: list[dict] = []
    for row in rows:
        out = validate_row(row, qdrant_url=qdrant_url, collection=collection)
        validated.append(out)
        report_items.append(
            {
                "id": out["id"],
                "estrato": out["estrato"],
                "status": out["status"],
                "doc_id_esperado": out["doc_id_esperado"],
            }
        )

    missing = [r for r in validated if r["status"] != "congelado"]
    if missing:
        raise RuntimeError(
            f"{len(missing)} itens sem chunk confirmado após validação: "
            + ", ".join(r["doc_id_esperado"] for r in missing)
        )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(CSV_OUT, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, delimiter=";")
        writer.writeheader()
        writer.writerows({k: row.get(k, "") for k in FIELDS} for row in validated)

    report = {
        "dataset": str(CSV_OUT),
        "n_total": len(validated),
        "n_com_numero": sum(1 for r in validated if r["estrato"] == "com_numero"),
        "n_tematico": sum(1 for r in validated if r["estrato"] == "tematico"),
        "seed": SEED,
        "validated_ok": sum(1 for r in validated if r["status"] == "congelado"),
        "items": report_items,
    }
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
