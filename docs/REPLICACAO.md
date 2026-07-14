# Replicação da avaliação

## 1. Ambiente

- Python 3.11 ou superior
- ~2 GB de espaço em disco (`dados/`)
- Opcional: Docker para API idêntica à produção (`docker compose up -d duckdb_api`)

```bash
pip install -r eval/requirements.txt
```

Variáveis (apenas para **novos** experimentos com API OpenAI):

```bash
cp .env.example .env
# OPENAI_API_KEY=...
```

## 2. Recálculo de métricas (sem custo de API)

Reexecuta as SQL de `checkpoint.jsonl` no DuckDB local e grava `metrics_summary_v2.json`:

```bash
python scripts/recompute_metrics.py --experiment e1_baseline_compacto
python scripts/recompute_metrics.py --experiment e2_contexto_estendido
python scripts/recompute_metrics.py --experiment e3_gpt4o_mini
python scripts/recompute_metrics.py --experiment e4_gpt4o
python scripts/compare_all_experiments.py
```

Saídas por experimento:

- `experimentos/<nome>/metrics_summary_v2.json`
- `experimentos/<nome>/metricas_por_caso/NNN.json`

## 3. Exportação de resultados tabulares

Referência (golden):

```bash
python scripts/export_result_sets.py \
  --source golden \
  --output-dir dados/golden/reference_results
```

SQL geradas (por experimento):

```bash
python scripts/export_result_sets.py \
  --source checkpoint \
  --experiment e1_baseline_compacto \
  --output-dir experimentos/e1_baseline_compacto/generated_results
```

## 4. Novo experimento (opcional, consome API)

```bash
python eval/run_experiment.py \
  --experiment meu_teste \
  --model gpt-4.1-mini \
  --prompt-variant poscagada_16k
```

## 5. Estrutura do golden dataset

Arquivo: `dados/golden/golden_dataset_v1.0.csv` (delimitador `;`)

| Coluna | Descrição |
|--------|-----------|
| `id_teste` | Identificador 1–80 |
| `input_usuario` | Pergunta em português |
| `query_referencia` | SQL validada |
| `tabelas_esperadas` | Para TSA |
| `condicao_esperada` | Para CHS |
| `espera_dados` | `sim` ou `indiferente` (NEA) |
| `dificuldade` | facil / medio / dificil |

Resultado tabular de cada referência: `dados/golden/reference_results/NNN.json`.

## 6. Docker (opcional)

```bash
docker compose up -d duckdb_api
```

A API escuta em `http://localhost:8000/query`. Os scripts deste repositório usam por padrão o motor DuckDB local (`eval/lib/duckdb_engine.py`), compatível com os mesmos CSV em `dados/`.
