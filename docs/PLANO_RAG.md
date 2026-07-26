# Plano RAG — SIM Caeté

Protocolo de validação da recuperação documental (Lote III). Complementa a avaliação Text-to-SQL documentada em [`METRICAS.md`](METRICAS.md).

---

## Resultados — golden v1.1 (*n*=8, *k*=5)

Configuração base: Qdrant `jornais_caete`, embedding `text-embedding-3-small`.

| Configuração de recuperação | Recall@5 | MRR |
|-----------------------------|----------|----:|
| Busca densa | 100% | 63,33% |
| Deduplicação de `page_content` idêntico | 100% | 69,79% |
| Filtro `tipo_ato` + dedup | 100% | 72,92% |
| **Híbrido por nº de ato + dedup + filtro tipo** | 100% | **87,5%** |

A configuração final (híbrido + dedup + filtro tipo) foi adotada no subsistema RAG da instância Caeté.

---

## Golden v1.2 — amostra ampliada (*n*=20)

Amostra sorteada sem critério rank≤5, com perguntas uniformes por número de ato. Evidência complementar de robustez:

| Configuração | Recall@5 | MRR |
|--------------|----------|----:|
| Busca densa (*k*=5) | 0% | 0% |
| Híbrido + dedup (*k*=5) | 100% | 100% |

---

## Métricas

**Recall@*k*** — proporção de perguntas em que o documento esperado aparece no top-*k*:

$$\text{Recall@}k = \frac{1}{N_{\text{RAG}}} \sum_{i=1}^{N_{\text{RAG}}} \mathbb{1}[\text{doc esperado} \in \text{top-}k_i]$$

**MRR** (*Mean Reciprocal Rank*) — média do inverso da posição do primeiro acerto:

$$\text{MRR} = \frac{1}{N_{\text{RAG}}} \sum_{i=1}^{N_{\text{RAG}}} \frac{1}{\text{rank}_i}$$

Matching por `page_content` do payload (não pelo nome do PDF).

---

## Artefatos

| Item | Caminho |
|------|---------|
| Golden v1.1 | `avaliacao/dados/golden_rag/golden_rag_v1.1.csv` |
| Golden v1.2 | `avaliacao/dados/golden_rag/golden_rag_v1.2.csv` |
| Runner | `avaliacao/eval/run_rag_eval.py` |

---

## Execução

```powershell
cd avaliacao
.\run_metrics_pipeline.ps1 -WithRag
```

Pré-requisitos: Docker (`docker compose up -d qdrant`), `OPENAI_API_KEY` no `.env`.
