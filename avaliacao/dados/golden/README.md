# Conjunto golden (v1.0)

80 consultas em linguagem natural com SQL de referência validada no DuckDB.

## Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `golden_dataset_v1.0.csv` | Entrada principal (delimitador `;`) |
| `reference_results/NNN.json` | Resultado tabular da SQL de referência por `id_teste` |

## Regeneração

```powershell
cd avaliacao
python scripts/export_result_sets.py `
  --source golden `
  --output-dir dados/golden/reference_results
```

## Origem

Gerado por `eval/build_golden_80.py` (10 consultas base + 70 derivações paramétricas).
