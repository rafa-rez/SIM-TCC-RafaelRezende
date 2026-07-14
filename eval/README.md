# Avaliação Text-to-SQL — TCC CIC Caeté

Pipeline offline (sem WhatsApp/n8n) com checkpoint e controle de orçamento.

## Pré-requisitos

1. `docker compose up -d duckdb_api` (porta 8000)
2. `.env` na raiz com `OPENAI_API_KEY`
3. `pip install -r eval/requirements.txt`

## Comandos

```powershell
# Gerar golden dataset (80 consultas)
python eval/build_golden_80.py

# Validar queries de referência do golden
python eval/validate_golden.py

# Baseline
python eval/run_experiment.py --experiment baseline_16k --model gpt-4.1-mini --prompt poscagada_16k

# Ablations
python eval/run_experiment.py --experiment ablation_precagada_30k --model gpt-4.1-mini --prompt precagada_30k
python eval/run_experiment.py --experiment ablation_gpt4o_mini --model gpt-4o-mini --prompt poscagada_16k
python eval/run_experiment.py --experiment ablation_gpt4o --model gpt-4o --prompt poscagada_16k

# Sequência completa (com agregação)
python eval/run_all_experiments.py

# Retomar após interrupção
python eval/run_experiment.py --experiment baseline_16k --resume

# Agregar métricas
python eval/aggregate_report.py --experiment baseline_16k

# Comparar todos os experimentos
python eval/compare_experiments.py

# Atualizar tabela no LaTeX do TCC
python eval/update_tcc_results.py

# Estimar custo sem gastar API
python eval/run_experiment.py --experiment dry --dry-run --limit 10
```

## Artefatos

Cada experimento grava em `eval/results/<nome>/`:

- `checkpoint.jsonl` — uma linha por consulta (retomável)
- `budget_state.json` — gasto acumulado USD
- `experiment_meta.json` — configuração congelada
- `metrics_summary.json` — após `aggregate_report.py`

## Orçamento

Limite configurado em `eval/config.yaml` (`budget_usd` menos margem de segurança).
O runner **para antes** de estourar o limite.
