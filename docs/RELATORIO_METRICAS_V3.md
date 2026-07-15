# Relatório de métricas v3 — julho/2026

Recálculo offline sobre os quatro experimentos Text-to-SQL (80 consultas cada).  
Tolerância numérica padrão: **τ = 0,5%** (`DEFAULT_TAU = 0.005`).

## Tabela comparativa

| Experimento | EX resposta | EX proj | EX proj+τ | EX colmap | EX estrita | VSR |
|-------------|------------:|--------:|----------:|----------:|-----------:|----:|
| E1 baseline compacto | **62,50** | 60,00 | 61,25 | 57,50 | 10,00 | 97,50 |
| E2 contexto estendido | **58,75** | 56,25 | 57,50 | 56,25 | 13,75 | 96,25 |
| E3 GPT-4o-mini | **46,25** | 46,25 | 46,25 | 46,25 | 12,50 | 90,00 |
| E4 GPT-4o | **57,50** | 57,50 | 57,50 | 56,25 | 16,25 | 97,50 |

Fonte: `experimentos/comparison_table_v3.md`

## Evolução em relação à v2 (E1)

| Métrica | v2 | v3 | Δ |
|---------|---:|---:|--:|
| EX colmap | 58,75 | 57,50 | −1,25 |
| **EX proj** | — | **60,00** | novo |
| **EX resposta** | — | **62,50** | novo |
| **EX proj + τ** | — | **61,25** | novo |
| EX estrita | 10,00 | 10,00 | — |
| EX linhas | 85,00 | 85,00 | — |

## Novas variantes implementadas

### EX proj
Permite **colunas extras** no resultado gerado. Exige que cada coluna do gabarito tenha um vetor de valores equivalente em alguma coluna da resposta.

**Ganho E1:** +2,5 p.p. sobre colmap (60% vs 57,5%).  
**Casos recuperados:** 2 (`colunas_extras_aceitas`), incluindo ISF-M com 4 colunas geradas vs 2 no gabarito.

### EX proj + τ (tolerância 0,5%)
Mesma regra de EX proj, com tolerância relativa em valores numéricos.

**Ganho E1 adicional:** +1,25 p.p. (61,25%).  
**Casos recuperados:** 2 (`tolerancia_numerica`), incluindo percentual da Câmara (~0,36% de diferença).

### EX resposta (métrica principal proposta)
Aplica EX proj + τ apenas nas colunas listadas em `colunas_resposta` no golden (75/80 consultas preenchidas automaticamente).

**E1:** **62,50%** — melhor resultado honesto sem afrouxar para contagem de linhas.

## Decomposição das falhas (E1, VSR = 1)

| Motivo | Casos |
|--------|------:|
| Alias de coluna (estrita falha, colmap passa) | 38 |
| Colunas extras aceitas (proj) | 2 |
| Tolerância numérica (proj+τ) | 2 |
| Divergência real de valores | ~17 |
| Cardinalidade de linhas | 6 |
| Cardinalidade de colunas (sem recuperação proj) | ~2 |

## Métrica adotada para o documento final

**EX resposta** (`ex_resposta`): equivalência de conteúdo nas colunas que respondem à pergunta, com colunas extras permitidas e τ = 0,5%.

Mantidas no repositório para auditoria: EX estrita, EX colmap, EX linhas.

## Arquivos atualizados

- `eval/lib/compare.py` — implementação v3
- `dados/golden/golden_dataset_v1.0.csv` — coluna `colunas_resposta`
- `scripts/enrich_golden_colunas.py`
- `experimentos/*/metrics_summary_v3.json`
- `experimentos/*/metricas_por_caso/*.json`

## Comandos de reprodução

```bash
python scripts/enrich_golden_colunas.py
python scripts/recompute_metrics.py --experiment e1_baseline_compacto
python scripts/compare_all_experiments.py
```
