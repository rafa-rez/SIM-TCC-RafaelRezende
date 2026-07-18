#!/usr/bin/env python3
"""Gera golden_rag_v1.0.csv e reference_chunks/ a partir da curadoria v1."""

from __future__ import annotations

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "dados" / "golden_rag"
CSV_PATH = OUT_DIR / "golden_rag_v1.0.csv"
CHUNKS_DIR = OUT_DIR / "reference_chunks"

ROWS = [
    {
        "id": "001",
        "status": "congelado",
        "origem": "manual",
        "pergunta": "Qual situação de emergência foi declarada pelo Decreto nº 247/2024 em Caeté?",
        "doc_id_esperado": "Decreto Municipal nº 247/2024",
        "titulo_documento": "Decreto Municipal nº 247/2024",
        "trecho_referencia": (
            "crise hídrica em razão da seca, estiagem prolongada e falta de abastecimento de água potável, "
            "declarada como situação de emergência no Município de Caeté/MG por meio do Decreto nº 247/2024"
        ),
        "tipo": "decreto",
        "dificuldade": "fácil",
        "notas": "Base: objeto de dispensa SICOM 2024 (caminhão-pipa). Validar chunk no índice Qdrant.",
    },
    {
        "id": "002",
        "status": "congelado",
        "origem": "manual",
        "pergunta": "O Decreto Municipal nº 030/2024 trata de qual tema de saúde pública?",
        "doc_id_esperado": "Decreto Municipal nº 030/2024",
        "titulo_documento": "Decreto Municipal nº 030/2024",
        "trecho_referencia": (
            "aquisição de material para diagnóstico de vírus da dengue conforme Decreto Municipal nº 030/2024 "
            "devido ao aumento dos números de casos de arbovirose, principalmente dengue e chikungunya"
        ),
        "tipo": "decreto",
        "dificuldade": "fácil",
        "notas": "Base: dispensa SICOM 2024. Validar chunk no índice Qdrant.",
    },
    {
        "id": "003",
        "status": "congelado",
        "origem": "manual",
        "pergunta": "Qual lei municipal institui o programa Bolsa Atleta em Caeté?",
        "doc_id_esperado": "Lei Municipal nº 3416/2022",
        "titulo_documento": "Lei Municipal nº 3416/2022",
        "trecho_referencia": (
            "Programa Bolsa Atleta 2024, auxílio financeiro a atletas em atendimento à Lei Municipal 3416/2022 "
            "e Decreto 064/2023"
        ),
        "tipo": "lei",
        "dificuldade": "médio",
        "notas": "Base: inexigibilidade SICOM 2024. Validar chunk no índice Qdrant.",
    },
    {
        "id": "004",
        "status": "congelado",
        "origem": "manual",
        "pergunta": "Qual decreto regulamenta o Programa Bolsa Atleta junto com a Lei 3416/2022?",
        "doc_id_esperado": "Decreto Municipal nº 064/2023",
        "titulo_documento": "Decreto Municipal nº 064/2023",
        "trecho_referencia": (
            "auxílio financeiro a atletas em atendimento à Lei Municipal 3416/2022 e Decreto 064/2023"
        ),
        "tipo": "decreto",
        "dificuldade": "médio",
        "notas": "Base: inexigibilidade SICOM 2024. Validar chunk no índice Qdrant.",
    },
    {
        "id": "005",
        "status": "congelado",
        "origem": "manual",
        "pergunta": "Qual lei municipal trata do auxílio de transporte universitário para estudantes?",
        "doc_id_esperado": "Lei Municipal nº 3287/2021",
        "titulo_documento": "Lei Municipal nº 3287/2021",
        "trecho_referencia": (
            "auxílio financeiro a estudantes em atendimento à Lei Municipal 3287/2021 e Decreto Municipal 123/2021 "
            "referente ao transporte universitário"
        ),
        "tipo": "lei",
        "dificuldade": "fácil",
        "notas": "Base: inexigibilidade SICOM 2024. Validar chunk no índice Qdrant.",
    },
    {
        "id": "006",
        "status": "congelado",
        "origem": "manual",
        "pergunta": "Qual decreto regulamenta o transporte universitário previsto na Lei 3287/2021?",
        "doc_id_esperado": "Decreto Municipal nº 123/2021",
        "titulo_documento": "Decreto Municipal nº 123/2021",
        "trecho_referencia": (
            "Decreto Municipal 123/2021 referente ao transporte universitário, Edital nº 01/2024"
        ),
        "tipo": "decreto",
        "dificuldade": "médio",
        "notas": "Base: inexigibilidade SICOM 2024. Validar chunk no índice Qdrant.",
    },
    {
        "id": "007",
        "status": "congelado",
        "origem": "manual",
        "pergunta": "O que dispõe o Decreto nº 11525/2023 no âmbito das ações culturais em Caeté?",
        "doc_id_esperado": "Decreto Municipal nº 11525/2023",
        "titulo_documento": "Decreto Municipal nº 11525/2023 (Decreto Paulo Gustavo)",
        "trecho_referencia": (
            "concessão de apoio financeiro a ações culturais contempladas pelo Edital nº 02/2023, nos termos da "
            "Lei Complementar nº 195/2022 (Lei Paulo Gustavo) e do Decreto nº 11525/2023 (Decreto Paulo Gustavo)"
        ),
        "tipo": "decreto",
        "dificuldade": "médio",
        "notas": "Base: inexigibilidade SICOM 2024. Validar chunk no índice Qdrant.",
    },
    {
        "id": "008",
        "status": "congelado",
        "origem": "manual",
        "pergunta": "Qual ato normativo municipal está associado à Defesa Civil conforme pagamentos de 2022?",
        "doc_id_esperado": "Decreto Municipal nº 013/2022",
        "titulo_documento": "Decreto Municipal nº 013/2022",
        "trecho_referencia": "Defesa Civil — Decreto nº 013/2022",
        "tipo": "decreto",
        "dificuldade": "difícil",
        "notas": "Base: histórico de pagamentos SICOM 2022. Validar texto integral no Diário/Qdrant.",
    },
    {
        "id": "009",
        "status": "congelado",
        "origem": "manual",
        "pergunta": "Qual decreto de 2024 abriu crédito suplementar com número 185?",
        "doc_id_esperado": "Decreto Municipal nº 185/2024",
        "titulo_documento": "Decreto Municipal nº 185/2024",
        "trecho_referencia": "Decreto de Crédito Suplementar nº 185/2024, assinado em 01/08/2024",
        "tipo": "decreto",
        "dificuldade": "médio",
        "notas": "Base: tabela decretos SICOM 2024. Validar publicação no Diário/Qdrant.",
    },
    {
        "id": "010",
        "status": "congelado",
        "origem": "manual",
        "pergunta": "Houve decreto por estiagem e falta de água potável — qual número e ano em Caeté?",
        "doc_id_esperado": "Decreto Municipal nº 247/2024",
        "titulo_documento": "Decreto Municipal nº 247/2024",
        "trecho_referencia": (
            "estiagem prolongada e falta de abastecimento de água potável declarada como situação de emergência "
            "no Município de Caeté/MG por meio do Decreto nº 247/2024"
        ),
        "tipo": "decreto",
        "dificuldade": "difícil",
        "notas": "Reformulação lexical distante do título do ato. Par com item 001.",
    },
]

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


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CHUNKS_DIR.mkdir(parents=True, exist_ok=True)

    with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(ROWS)

    for row in ROWS:
        chunk = {
            "id": row["id"],
            "doc_id_esperado": row["doc_id_esperado"],
            "titulo_documento": row["titulo_documento"],
            "trecho_referencia": row["trecho_referencia"],
            "tipo": row["tipo"],
            "fonte_curadoria": row["notas"],
        }
        path = CHUNKS_DIR / f"{row['id']}.json"
        path.write_text(json.dumps(chunk, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"OK: {CSV_PATH} ({len(ROWS)} itens)")
    print(f"OK: {CHUNKS_DIR}/")


if __name__ == "__main__":
    main()
