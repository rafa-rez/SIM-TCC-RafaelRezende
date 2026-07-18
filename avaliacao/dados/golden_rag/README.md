# Golden RAG — SIM Caeté

Conjuntos de perguntas sobre atos normativos do Diário Oficial municipal, com documento de referência validado no índice Qdrant (`jornais_caete`).

## Versões

| Versão | Itens | Status | Uso |
|--------|------:|--------|-----|
| **v1.1** | 8 | `congelado` | Pipeline RAG (pós-v1.0) |
| v1.0 | 10 | arquivado | Curadoria inicial (5 itens sem chunk no índice) |

## Arquivos v1.1

| Arquivo | Descrição |
|---------|-----------|
| `golden_rag_v1.1.csv` | Entrada principal (delimitador `;`) |
| `validation_report_v1.1.json` | Validação automática contra Qdrant |
| `reference_chunks/NNN.json` | Trecho de referência por item (001–008) |

## Critérios v1.1

1. `doc_id_esperado` com chunk confirmado no Qdrant (cabeçalho DECRETO/LEI + número/ano).
2. Pergunta com documento correto no **top-5** na validação (`text-embedding-3-small`).
3. Itens v1.0 sem índice foram **substituídos** por atos municipais recuperáveis.
4. Duplicata 001/010 (Decreto 247/2024) fundida em um item.

## Regenerar v1.1

```powershell
cd avaliacao
python scripts/build_golden_rag_v1_1.py
```

Requer Qdrant em `localhost:6333` e `OPENAI_API_KEY` no `.env`.

## Pipeline (desenvolvimento — fora do TCC v1.0)

```powershell
cd avaliacao
.\run_metrics_pipeline.ps1 -WithRag
```

Saída em `experimentos/rag_baseline_v1_1/` (gitignored).

## Exclusões v1.0 → v1.1

| id v1 | Motivo |
|-------|--------|
| 001 | Duplicata do 010 |
| 002 | Decreto 030/2024 não indexado |
| 003 | Lei 3416/2022 não indexada |
| 005 | Lei 3287/2021 não indexada |
| 006 | Decreto 123/2021 não indexado |
| 007 | Decreto 11525/2023 não indexado |
| 008 | Decreto 013/2022 não localizado |
| 009 | Indexado mas rank > 5 |
| 010 | Reformulado como item 002 v1.1 |

Plano completo: [`../../docs/PLANO_RAG.md`](../../docs/PLANO_RAG.md).
