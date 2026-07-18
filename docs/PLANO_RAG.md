# Plano de avaliação do subsistema RAG — SIM Caeté

Documento de **trabalho futuro** e infraestrutura no repositório. O TCC v1.0 reporta apenas a validação **Text-to-SQL** (80 consultas); o RAG opera em produção, mas **não entra nos PDFs** (artigo/monografia) nesta versão.

---

## Status v1.0 do TCC

| Item | Repositório | Texto acadêmico (PDF) |
|------|-------------|------------------------|
| Text-to-SQL E1–E4 | Sim | Sim |
| Golden RAG + runner | Sim (infra) | Não (trabalho futuro) |
| Métricas RAG | Pipeline opcional (`--with-rag`) | Não |

---

## Objetivo (pós-v1.0)

Comprovar que o subsistema RAG recupera os documentos corretos sobre o Diário Oficial, como evidência funcional complementar — sem tornar métricas o foco central do trabalho.

---

## Infraestrutura no repositório

| Artefato | Caminho |
|----------|---------|
| Golden RAG v1.1 (8 itens, validados no Qdrant) | `avaliacao/dados/golden_rag/golden_rag_v1.1.csv` |
| Runner | `avaliacao/eval/run_rag_eval.py` |
| Validação Qdrant | `avaliacao/eval/lib/qdrant_validate.py` |
| Regenerar golden | `avaliacao/scripts/build_golden_rag_v1_1.py` |
| Pipeline | `avaliacao/run_metrics_pipeline.ps1 -WithRag` |

Pré-requisitos RAG: Qdrant (`localhost:6333`, collection `jornais_caete`), `OPENAI_API_KEY`, embedding `text-embedding-3-small`.

---

## Fases planejadas

### Fase 1 — Golden RAG (concluída no repo)

`golden_rag_v1.1.csv` — perguntas cujo `doc_id_esperado` possui chunk confirmado no índice. Ver `avaliacao/dados/golden_rag/README.md`.

### Fase 2 — Runner (concluída no repo)

Recall@$k$, MRR via `run_rag_eval.py`. Resultados em `experimentos/rag_baseline_v1_1/` (gerados pela pipeline; **não citados no TCC v1.0**).

### Fase 3 — Avaliação de resposta (pendente)

Julgamento de adequação das respostas em linguagem natural (amostra).

### Fase 4 — Texto acadêmico (pendente)

Seção na monografia após baseline estável e revisão com orientador.

---

## Métricas de recuperação

| Métrica | Interpretação |
|---------|---------------|
| Recall@$k$ | Documento esperado aparece no top-$k$? |
| MRR | Quão cedo o documento correto aparece? |

---

## Lições da curadoria v1.0 → v1.1

- Gabarito derivado só de citações SICOM falha se o ato não estiver indexado no Diário.
- Embedding deve coincidir com o da indexação (`text-embedding-3-small`, não `ada-002`).
- Matching deve usar `page_content`, não apenas o nome do PDF.

---

## Ordem sugerida (pós-v1.0)

```
Pipeline SQL congelada (v1.0 TCC)
    ↓
Otimizar índice / chunking / prompts
    ↓
Expandir golden RAG (sem cherry-picking)
    ↓
Reportar no TCC v2 ou artigo estendido
```
