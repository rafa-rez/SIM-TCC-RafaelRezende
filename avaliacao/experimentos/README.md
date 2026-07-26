# Experimentos — SIM (instância Caeté)

Artefatos dos experimentos de validação empírica, organizados por lote conforme a monografia e o artigo.

## Lote I — Comparação de modelo e contexto (E1–E4)

Quatro configurações executadas sobre o golden v1.0 (80 consultas).

| Pasta | Modelo | Prompt |
|-------|--------|--------|
| `e1_baseline_compacto` | gpt-4.1-mini | compacto (~16k tokens) |
| `e2_contexto_estendido` | gpt-4.1-mini | estendido (~30k tokens) |
| `e3_gpt4o_mini` | gpt-4o-mini | compacto |
| `e4_gpt4o` | gpt-4o | compacto |

## Artefatos por experimento

| Arquivo | Conteúdo |
|---------|----------|
| `checkpoint.jsonl` | Registro por consulta (SQL, tokens, métricas) |
| `metrics_summary_v3.json` | Agregados recalculados |
| `metricas_por_caso/` | Detalhe e classificação de divergência |
| `generated_results/` | JSON tabular de cada SQL gerada |

## Tabela comparativa (Lote I)

- `comparison_table_v3.md`

## Lote II — Configuração final de prompt

Prompt v2.1 documentado em `eval/prompts/prompt_sql_v2_insights.txt`. Resultados: VSR 100%, EF 72,5% (mesmo golden v1, GPT-4.1-mini).

## Lote III — RAG

Golden: `dados/golden_rag/golden_rag_v1.1.csv` (8 perguntas). Configuração final: Recall@5 100%, MRR 87,5% (híbrido + dedup + filtro tipo).

```powershell
.\run_metrics_pipeline.ps1 -WithRag
```

Definições: [`../../docs/METRICAS.md`](../../docs/METRICAS.md) · [`../../docs/PLANO_RAG.md`](../../docs/PLANO_RAG.md)
