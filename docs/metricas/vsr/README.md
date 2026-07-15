# Métrica VSR (Valid SQL Rate)

Proporção de consultas em que a SQL gerada executa sem erro no DuckDB.

```
VSR = (1/N) Σᵢ 𝟙[ exec(SQLᵢ) OK ]
```

Implementação: campo `vsr` em `avaliacao/eval/lib/compare.py` (`compute_metrics`).

Recálculo: `python scripts/recompute_metrics.py --experiment e1_baseline_compacto` (a partir de `avaliacao/`)

Definição completa: `docs/METRICAS.md`, seção 5.1.
