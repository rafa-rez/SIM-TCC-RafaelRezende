# Plano RAG — SIM Caeté

Protocolo de validação da recuperação documental sobre o Diário Oficial. Complementa a avaliação Text-to-SQL documentada em [`METRICAS.md`](METRICAS.md).

---

## Resultados reportados (golden v1.1)

| Métrica | Valor | *n* |
|---------|------:|----:|
| Recall@5 | 100% | 8 |
| MRR | 63,33% | 8 |

Configuração: Qdrant `jornais_caete`, embedding `text-embedding-3-small`, *k*=5.

---

## Artefatos

| Item | Caminho |
|------|---------|
| Golden v1.1 | `avaliacao/dados/golden_rag/golden_rag_v1.1.csv` |
| Runner | `avaliacao/eval/run_rag_eval.py` |
| Validação Qdrant | `avaliacao/eval/lib/qdrant_validate.py` |
| Regenerar golden | `avaliacao/scripts/build_golden_rag_v1_1.py` |

---

## Métricas

**Recall@*k*** — proporção de perguntas em que o documento esperado aparece no top-*k*.

**MRR** (*Mean Reciprocal Rank*) — média do inverso da posição do primeiro acerto.

---

## Limitações (v1.1)

- Amostra pequena (*n*=8), apenas decretos municipais.
- Critério de inclusão: ato com *chunk* confirmado no índice.
- Não avalia resposta em linguagem natural nem roteamento SQL/RAG.

---

## Execução

```powershell
cd avaliacao
.\run_metrics_pipeline.ps1 -WithRag
```

Pré-requisitos: Docker (`docker compose up -d qdrant`), `OPENAI_API_KEY` no `.env`.

---

## Próximos passos

1. Expandir golden sem cherry-picking.
2. Julgamento de adequação das respostas (LLM-as-Judge ou humana).
3. Deduplicação de *chunks* e melhoria do índice.
