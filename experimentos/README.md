# Experimentos Text-to-SQL

| Pasta | ID legado | Modelo | Prompt |
|-------|-----------|--------|--------|
| `e1_baseline_compacto` | baseline_16k | gpt-4.1-mini | compacto (~16k tokens) |
| `e2_contexto_estendido` | ablation_precagada_30k | gpt-4.1-mini | estendido (~30k tokens) |
| `e3_gpt4o_mini` | ablation_gpt4o_mini | gpt-4o-mini | compacto |
| `e4_gpt4o` | ablation_gpt4o | gpt-4o | compacto |

## Por experimento

- `checkpoint.jsonl` — registro por consulta (SQL, tokens, métricas v1)
- `metrics_summary_v2.json` — agregados recalculados (métricas v2)
- `metricas_por_caso/` — detalhe e classificação de divergência EX
- `generated_results/` — JSON tabular de cada SQL gerada
- `experiment_meta.json` — metadados do experimento original

Tabela comparativa: `comparison_table_v2.md`

## Recálculo

```bash
python scripts/recompute_metrics.py --experiment e1_baseline_compacto
```
