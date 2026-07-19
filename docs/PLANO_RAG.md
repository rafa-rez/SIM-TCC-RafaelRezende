# Plano RAG — SIM Caeté

Protocolo de validação da recuperação documental sobre o Diário Oficial. Complementa a avaliação Text-to-SQL documentada em [`METRICAS.md`](METRICAS.md).

---

## Resultados — golden v1.1 (*n*=8, *k*=5)

Configuração base: Qdrant `jornais_caete`, embedding `text-embedding-3-small`.

| Configuração de recuperação | Recall@5 | MRR |
|-----------------------------|----------|----:|
| Busca densa (baseline reportado no TCC) | 100% | 63,33% |
| Deduplicação de `page_content` idêntico (query-time) | 100% | 69,79% |
| Filtro `tipo_ato` | 100% | 65,62% |
| Filtro `tipo_ato` + dedup | 100% | 72,92% |
| **Híbrido por nº de ato + dedup + filtro tipo** | 100% | **87,5%** |

O ganho de MRR com deduplicação (+6,46 pp vs baseline) ocorre sem perda de Recall@5: *chunks* repetidos deixam de ocupar posições no top-5. A busca híbrida antecede *chunks* cujo cabeçalho cita o número/ano do ato extraído da pergunta.

---

## Golden v1.2 — amostra honesta (*n*=20)

Amostra sorteada sem critério rank≤5 (seed 42), com perguntas do tipo “O que dispõe o Decreto Municipal nº *N*/*AAAA*?”.

| Configuração | Recall@5 | MRR |
|--------------|----------|----:|
| Busca densa (*k*=5) | 0% | 0% |
| Híbrido + dedup (*k*=5) | 100% | 100% |

A busca densa isolada não codifica o número do ato; o stack híbrido+dedup garante lookup exato para perguntas com número explícito. Evidência complementar — não substitui o golden v1.1 reportado no TCC.

Artefatos: `avaliacao/dados/golden_rag/golden_rag_v1.2.csv`

---

## Artefatos

| Item | Caminho |
|------|---------|
| Golden v1.1 | `avaliacao/dados/golden_rag/golden_rag_v1.1.csv` |
| Golden v1.2 | `avaliacao/dados/golden_rag/golden_rag_v1.2.csv` |
| Runner | `avaliacao/eval/run_rag_eval.py` |
| Validação Qdrant | `avaliacao/eval/lib/qdrant_validate.py` |
| Regenerar golden | `avaliacao/scripts/build_golden_rag_v1_1.py` |

Flags do runner: `--dedup`, `--hybrid-act`, `--filtro-tipo-ato`

---

## Métricas

**Recall@*k*** — proporção de perguntas em que o documento esperado aparece no top-*k*:

$$\text{Recall@}k = \frac{1}{N_{\text{RAG}}} \sum_{i=1}^{N_{\text{RAG}}} \mathbb{1}[\text{doc esperado} \in \text{top-}k_i]$$

**MRR** (*Mean Reciprocal Rank*) — média do inverso da posição do primeiro acerto:

$$\text{MRR} = \frac{1}{N_{\text{RAG}}} \sum_{i=1}^{N_{\text{RAG}}} \frac{1}{\text{rank}_i}$$

Matching por `page_content` do payload (não pelo nome do PDF).

---

## Limitações

- Golden v1.1: amostra pequena (*n*=8), apenas decretos com *chunk* confirmado no índice.
- Golden v1.2: amostra maior, mas perguntas uniformes por número de ato.
- Não avalia resposta em linguagem natural nem roteamento SQL/RAG.

---

## Execução

```powershell
cd avaliacao
.\run_metrics_pipeline.ps1 -WithRag
```

Pré-requisitos: Docker (`docker compose up -d qdrant`), `OPENAI_API_KEY` no `.env`.

Log de experimentos: [`experimentos-v2/LOG.md`](experimentos-v2/LOG.md)
