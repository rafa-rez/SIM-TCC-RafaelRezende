# Métrica EX (Execution Accuracy)

Família de métricas que compara o resultado tabular da SQL gerada com o gabarito de referência.

## Indicador de equivalência funcional

**EX resposta** (`ex_resposta`): equivalência nas colunas listadas em `colunas_resposta` do golden, com colunas extras permitidas no resultado gerado e tolerância numérica τ = 0,5%. Este é o indicador utilizado nos textos do trabalho para reportar acurácia de resultado de forma alinhada à pergunta do cidadão.

## Variantes no repositório

| Campo | Nome | Uso |
|-------|------|-----|
| `ex_resposta` | EX resposta | Equivalência funcional (reportada nos textos) |
| `ex_proj` | EX projeção | Colunas do gabarito encontradas no gerado (sem τ) |
| `ex_proj_tol` | EX projeção + τ | Com tolerância numérica |
| `ex_colmap` | EX colmap | Mesmo número de colunas + vetores |
| `ex_strict` | EX estrita | Referência da literatura (Spider) |
| `ex_rows` | EX linhas | Diagnóstico de cardinalidade |

## Implementação

- `avaliacao/eval/lib/compare.py`
- `avaliacao/scripts/recompute_metrics.py` → `metrics_summary_v3.json`
- `avaliacao/scripts/enrich_golden_colunas.py` → preenche `colunas_resposta`

## Documentação

- Formal: `docs/METRICAS.md`
- Relatório: `docs/RELATORIO_METRICAS_V3.md`
- Casos: `experimentos/*/metricas_por_caso/`

## Exemplos

| Caso | Situação | EX resposta |
|------|----------|-------------|
| 80 | Alias `total` vs `total_liquidado` | passa (via colmap/proj) |
| 1 | 4 colunas geradas, gabarito com 2 | passa (`colunas_extras_aceitas`) |
| 9 | Diferença ~0,36% no percentual | passa (`tolerancia_numerica`) |
| 3 | Empresas/valores diferentes | falha (erro real) |
