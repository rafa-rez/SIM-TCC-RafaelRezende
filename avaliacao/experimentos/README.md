# Experimentos Text-to-SQL — SIM (instância Caeté)

Quatro configurações de modelo e prompt executadas sobre o conjunto golden v1.0 (80 consultas). Os resultados comprovam a viabilidade funcional do subsistema Text-to-SQL do SIM.

## Configurações

| Pasta | ID legado | Modelo | Prompt |
|-------|-----------|--------|--------|
| `e1_baseline_compacto` | baseline_16k | gpt-4.1-mini | compacto (~16k tokens) |
| `e2_contexto_estendido` | ablation_precagada_30k | gpt-4.1-mini | estendido (~30k tokens) |
| `e3_gpt4o_mini` | ablation_gpt4o_mini | gpt-4o-mini | compacto |
| `e4_gpt4o` | ablation_gpt4o | gpt-4o | compacto |

## Artefatos por experimento

| Arquivo | Conteúdo |
|---------|----------|
| `checkpoint.jsonl` | Registro por consulta (SQL, tokens, métricas) |
| `metrics_summary_v3.json` | Agregados recalculados |
| `metricas_por_caso/` | Detalhe e classificação de divergência |
| `generated_results/` | JSON tabular de cada SQL gerada |
| `experiment_meta.json` | Metadados do experimento original |

## Tabela comparativa

Gerada por `scripts/compare_all_experiments.py`:

- `comparison_table_v3.md` (formato principal)
- `comparison_table.md` (legado)

## Recálculo offline

```powershell
cd avaliacao
python scripts/recompute_metrics.py --experiment e1_baseline_compacto
python scripts/compare_all_experiments.py
```

Definições de métricas: [`../../docs/METRICAS.md`](../../docs/METRICAS.md).

## Avaliação RAG (fora do escopo TCC v1.0)

Infraestrutura em `dados/golden_rag/` e `eval/run_rag_eval.py`. Execução opcional:

```powershell
.\run_metrics_pipeline.ps1 -WithRag
```

Ver [`dados/golden_rag/README.md`](dados/golden_rag/README.md) e [`../../docs/PLANO_RAG.md`](../../docs/PLANO_RAG.md).
