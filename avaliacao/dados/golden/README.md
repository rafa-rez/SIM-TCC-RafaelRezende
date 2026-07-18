# Conjunto golden (v1.0)

Conjunto de referência com **80 consultas** em linguagem natural e SQL validada no DuckDB. Utilizado nos experimentos E1--E4 documentados neste repositório.

## Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `golden_dataset_v1.0.csv` | Entrada principal (delimitador `;`) |
| `reference_results/NNN.json` | Resultado tabular da SQL de referência por `id_teste` |

## Colunas principais

| Coluna | Descrição |
|--------|-----------|
| `id_teste` | Identificador numérico (001--080) |
| `pergunta` | Pergunta em linguagem natural (português) |
| `query_referencia` | SQL de gabarito validada |
| `colunas_resposta` | Colunas que respondem à pergunta (equivalência funcional) |
| `dificuldade` | Fácil, médio ou difícil |
| `dominio` | Área temática (RH, licitações, CGU, etc.) |

## Origem

Gerado por `eval/build_golden_80.py`: 10 consultas-base expandidas com variações paramétricas (anos, limites, agregações), totalizando 80 casos executáveis.

## Regeneração dos resultados tabulares

```powershell
cd avaliacao
python scripts/export_result_sets.py `
  --source golden `
  --output-dir dados/golden/reference_results
```
