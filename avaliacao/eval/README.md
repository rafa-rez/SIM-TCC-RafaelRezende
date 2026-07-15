# Avaliação Text-to-SQL — SIM (instância Caeté)

Pipeline offline com checkpoint e controle de orçamento. Mede a qualidade das SQL geradas pelo assistente **SIM** sobre dados públicos de Caeté (MG).

## Pré-requisitos

1. `pip install -r ../requirements.txt` (a partir de `avaliacao/`)
2. `.env` na raiz do repositório com `OPENAI_API_KEY` (apenas para novos experimentos)
3. DuckDB local via `lib/duckdb_engine.py` (padrão) ou API HTTP em `config.yaml`

## Comandos

Execute a partir de `avaliacao/`:

```powershell
# Gerar golden dataset (80 consultas)
python eval/build_golden_80.py

# Baseline / ablations (gravam em experimentos/<nome>/)
python eval/run_experiment.py --experiment baseline_16k --model gpt-4.1-mini --prompt poscagada_16k

# Sequência completa
python eval/run_all_experiments.py

# Retomar após interrupção
python eval/run_experiment.py --experiment baseline_16k --resume

# Recálculo offline (sem API)
python scripts/recompute_metrics.py --experiment e1_baseline_compacto
```

## Artefatos

Cada experimento grava em `avaliacao/experimentos/<nome>/`:

- `checkpoint.jsonl` — uma linha por consulta (retomável)
- `budget_state.json` — gasto acumulado USD
- `experiment_meta.json` — configuração congelada
- `metrics_summary_v3.json` — após `scripts/recompute_metrics.py`

Experimentos legados de desenvolvimento permanecem em `eval/results/` (ablations internas).

## Orçamento

Limite em `eval/config.yaml` (`budget_usd` menos margem de segurança). O runner para antes de estourar o limite.
