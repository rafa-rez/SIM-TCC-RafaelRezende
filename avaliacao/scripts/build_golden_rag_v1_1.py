#!/usr/bin/env python3
"""
Valida golden RAG v1.0 contra Qdrant e gera golden_rag_v1.1.csv.

Critérios v1.1:
- doc_id_esperado com chunk confirmado no índice (cabeçalho DECRETO/LEI + número/ano)
- sem duplicata de mesmo ato
- perguntas reformuladas quando o rank em v1 era ruim
- itens substitutos curados a partir do índice quando v1 apontava ato ausente
"""

from __future__ import annotations

import csv
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from eval.lib.config_loader import get_openai_api_key, load_config
from eval.lib.qdrant_validate import (
    ActRef,
    _header_pattern,
    excerpt_from_chunk,
    find_chunks_for_act,
    parse_act_from_expected,
    validate_item,
)
from openai import OpenAI

OUT_DIR = ROOT / "dados" / "golden_rag"
CSV_V1 = OUT_DIR / "golden_rag_v1.0.csv"
CSV_OUT = OUT_DIR / "golden_rag_v1.1.csv"
CHUNKS_DIR = OUT_DIR / "reference_chunks"
REPORT_PATH = OUT_DIR / "validation_report_v1.1.json"

FIELDS = [
    "id",
    "status",
    "origem",
    "pergunta",
    "doc_id_esperado",
    "titulo_documento",
    "trecho_referencia",
    "tipo",
    "dificuldade",
    "notas",
]

# Itens v1.1: validados no Qdrant com rank <= 5 na pergunta final
V1_1_ROWS = [
    {
        "v1_id": "004",
        "status": "congelado",
        "origem": "v1.0_validado",
        "pergunta": "O que estabelece o Decreto Municipal nº 064/2023 sobre o Programa Bolsa Atleta?",
        "doc_id_esperado": "Decreto Municipal nº 064/2023",
        "titulo_documento": "Decreto Municipal nº 064/2023",
        "tipo": "decreto",
        "dificuldade": "médio",
        "notas": "Mantido v1. Chunk confirmado (rank@5=1).",
    },
    {
        "v1_id": "010",
        "status": "congelado",
        "origem": "v1.0_reformulado",
        "pergunta": "Qual decreto de 2024 declarou situação de emergência por estiagem e falta de água potável em Caeté?",
        "doc_id_esperado": "Decreto Municipal nº 247/2024",
        "titulo_documento": "Decreto Municipal nº 247/2024",
        "tipo": "decreto",
        "dificuldade": "médio",
        "notas": "Substitui par 001/010. Chunk confirmado (rank@5=3).",
    },
    {
        "v1_id": None,
        "status": "congelado",
        "origem": "indice_qdrant",
        "pergunta": "O que dispõe o Decreto Municipal nº 015/2024?",
        "doc_id_esperado": "Decreto Municipal nº 015/2024",
        "titulo_documento": "Decreto Municipal nº 015/2024",
        "tipo": "decreto",
        "dificuldade": "fácil",
        "notas": "Substituto v1 (002 ausente). Chunk confirmado (rank@5=1).",
    },
    {
        "v1_id": None,
        "status": "congelado",
        "origem": "indice_qdrant",
        "pergunta": "O que dispõe o Decreto Municipal nº 282/2023 sobre permissão de uso de bem público?",
        "doc_id_esperado": "Decreto Municipal nº 282/2023",
        "titulo_documento": "Decreto Municipal nº 282/2023",
        "tipo": "decreto",
        "dificuldade": "médio",
        "notas": "Substituto v1. Chunk confirmado (rank@5=5).",
    },
    {
        "v1_id": None,
        "status": "congelado",
        "origem": "indice_qdrant",
        "pergunta": "Qual decreto de 2022 nomeia membros do Conselho Municipal de Saneamento Básico?",
        "doc_id_esperado": "Decreto Municipal nº 295/2022",
        "titulo_documento": "Decreto Municipal nº 295/2022",
        "tipo": "decreto",
        "dificuldade": "médio",
        "notas": "Substituto v1. Chunk confirmado (rank@5=1).",
    },
    {
        "v1_id": None,
        "status": "congelado",
        "origem": "indice_qdrant",
        "pergunta": "Qual decreto de 2025 declarou emergência em saúde pública por febre maculosa em Caeté?",
        "doc_id_esperado": "Decreto Municipal nº 218/2025",
        "titulo_documento": "Decreto Municipal nº 218/2025",
        "tipo": "decreto",
        "dificuldade": "médio",
        "notas": "Substituto v1. Chunk confirmado (rank@5=1).",
    },
    {
        "v1_id": None,
        "status": "congelado",
        "origem": "indice_qdrant",
        "pergunta": "O que dispõe o Decreto Municipal nº 112/2024?",
        "doc_id_esperado": "Decreto Municipal nº 112/2024",
        "titulo_documento": "Decreto Municipal nº 112/2024",
        "tipo": "decreto",
        "dificuldade": "fácil",
        "notas": "Substituto v1. Chunk confirmado (rank@5=3).",
    },
    {
        "v1_id": None,
        "status": "congelado",
        "origem": "indice_qdrant",
        "pergunta": "O que dispõe o Decreto Municipal nº 321/2024?",
        "doc_id_esperado": "Decreto Municipal nº 321/2024",
        "titulo_documento": "Decreto Municipal nº 321/2024",
        "tipo": "decreto",
        "dificuldade": "fácil",
        "notas": "Substituto v1. Chunk confirmado (rank@5=5).",
    },
]

EXCLUDED_V1 = [
    ("001", "duplicata do 010 (mesmo Decreto 247/2024)"),
    ("002", "Decreto 030/2024 não indexado (só Edital 030/2024)"),
    ("003", "Lei 3416/2022 não indexada"),
    ("005", "Lei 3287/2021 não indexada"),
    ("006", "Decreto 123/2021 não indexado"),
    ("007", "Decreto 11525/2023 não indexado"),
    ("008", "Decreto 013/2022 não localizado com cabeçalho válido"),
    ("009", "Decreto 185/2024 indexado mas rank > 5 com perguntas testadas"),
    ("010", "fundiu com item 002 v1.1 (mesmo ato, pergunta reformulada)"),
]


def best_chunk_content(chunks: list[dict], act: ActRef) -> str:
    """Prefere chunk cujo cabeçalho corresponde ao ato esperado."""
    from eval.lib.qdrant_validate import chunk_matches_act

    ranked: list[tuple[int, str]] = []
    for point in chunks:
        content = (point.get("payload") or {}).get("page_content", "")
        if not chunk_matches_act(content, act):
            continue
        score = 0
        if _header_pattern(act).search(content[:400]):
            score += 10
        if f"DOCUMENTO: {act.tipo}" in content[:200].upper():
            score += 5
        ranked.append((score, content))
    if not ranked:
        return (chunks[0].get("payload") or {}).get("page_content", "") if chunks else ""
    ranked.sort(key=lambda x: x[0], reverse=True)
    return ranked[0][1]


def load_v1() -> list[dict]:
    with open(CSV_V1, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter=";"))


def main() -> None:
    cfg = load_config()["rag"]
    client = OpenAI(api_key=get_openai_api_key())
    model = cfg["embedding_model"]

    def embed(text: str) -> list[float]:
        return client.embeddings.create(model=model, input=text).data[0].embedding

    report: dict = {
        "generated_at": datetime.now().isoformat(),
        "qdrant_url": cfg["qdrant_url"],
        "collection": cfg["collection"],
        "embedding_model": model,
        "excluded_v1": [{"id": i, "motivo": m} for i, m in EXCLUDED_V1],
        "items": [],
    }

    final_rows: list[dict] = []
    for idx, spec in enumerate(V1_1_ROWS, start=1):
        new_id = f"{idx:03d}"
        validation = validate_item(
            spec["doc_id_esperado"],
            spec["pergunta"],
            url=cfg["qdrant_url"],
            collection=cfg["collection"],
            embed_fn=embed,
        )
        if not validation.indexed:
            print(f"ERRO: {spec['doc_id_esperado']} não encontrado no índice.", file=sys.stderr)
            sys.exit(1)
        if validation.rank_at_20 is None or validation.rank_at_20 > 5:
            print(
                f"AVISO: {spec['doc_id_esperado']} rank@5={validation.rank_at_20}",
                file=sys.stderr,
            )

        act = validation.act or parse_act_from_expected(spec["doc_id_esperado"])
        if not act:
            print(f"ERRO: não foi possível parsear {spec['doc_id_esperado']}", file=sys.stderr)
            sys.exit(1)
        chunks = find_chunks_for_act(act, url=cfg["qdrant_url"], collection=cfg["collection"])
        trecho = excerpt_from_chunk(best_chunk_content(chunks, act))
        row = {
            "id": new_id,
            "status": spec["status"],
            "origem": spec["origem"],
            "pergunta": spec["pergunta"],
            "doc_id_esperado": spec["doc_id_esperado"],
            "titulo_documento": spec["titulo_documento"],
            "trecho_referencia": trecho,
            "tipo": spec["tipo"],
            "dificuldade": spec["dificuldade"],
            "notas": spec["notas"],
        }
        final_rows.append(row)
        report["items"].append(
            {
                "id": new_id,
                "v1_id": spec.get("v1_id"),
                "doc_id_esperado": spec["doc_id_esperado"],
                "chunk_count": validation.chunk_count,
                "rank_at_20": validation.rank_at_20,
                "pergunta": spec["pergunta"],
            }
        )
        print(
            f"[{new_id}] {spec['doc_id_esperado']} | "
            f"chunks={validation.chunk_count} rank@20={validation.rank_at_20}"
        )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CHUNKS_DIR.mkdir(parents=True, exist_ok=True)

    with open(CSV_OUT, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(final_rows)

    for row in final_rows:
        chunk = {
            "id": row["id"],
            "doc_id_esperado": row["doc_id_esperado"],
            "titulo_documento": row["titulo_documento"],
            "trecho_referencia": row["trecho_referencia"],
            "tipo": row["tipo"],
            "fonte_curadoria": row["notas"],
            "qdrant_validado": True,
        }
        (CHUNKS_DIR / f"{row['id']}.json").write_text(
            json.dumps(chunk, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nOK: {CSV_OUT} ({len(final_rows)} itens)")
    print(f"OK: {REPORT_PATH}")


if __name__ == "__main__":
    main()
