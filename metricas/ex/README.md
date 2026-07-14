# Métrica EX (Execution Accuracy)

Família de métricas que compara o resultado tabular da SQL gerada com o gabarito.

## Variantes

| Campo | Nome | Arquivo de saída |
|-------|------|------------------|
| `ex_strict` | EX estrita (literatura) | `metrics_summary_v2.json` |
| `ex_colmap` | EX com mapeamento de colunas | idem |
| `ex_content` | EX por conteúdo sem nomes | idem |
| `ex_rows` | Igualdade de linhas | idem |
| `ex_cols` | Igualdade de colunas | idem |
| `alias_apenas` | Falha estrita só por alias | `metricas_por_caso/` |

## Implementação

- `eval/lib/compare.py`
- Recálculo: `python scripts/recompute_metrics.py --experiment e1_baseline_compacto`

## Documentação formal

Ver `docs/METRICAS.md`, seções 4.1 a 4.6.

## Exemplo de caso (alias)

Consultas em que `ex_strict=false`, `ex_colmap=true` e `divergencia_ex=alias_colunas` estão listadas nos JSON em `experimentos/*/metricas_por_caso/`.
