# Avaliação Text-to-SQL — SIM (instância Caeté)

Pipeline offline para medir a qualidade das consultas SQL geradas pelo **SIM** sobre os dados públicos de Caeté (MG).

## Estrutura

```
avaliacao/
├── dados/
│   ├── golden/          # 80 consultas + resultados de referência
│   ├── database/        # CSVs consolidados para DuckDB local
│   ├── SICOM/           # fontes brutas SICOM
│   └── staging_cgu/     # complementos CGU
├── experimentos/        # E1–E4 (checkpoints, métricas, resultados gerados)
├── eval/                # runner, prompts, biblioteca compare.py
├── scripts/             # recálculo offline e exportação
├── requirements.txt
└── run_replicacao.ps1
```

## Pré-requisitos

- Python 3.11+
- `pip install -r requirements.txt`
- Para **novos** experimentos com API OpenAI: `.env` na raiz do repositório com `OPENAI_API_KEY`

## Recálculo de métricas (sem custo de API)

```powershell
python scripts/enrich_golden_colunas.py
python scripts/recompute_metrics.py --experiment e1_baseline_compacto
python scripts/compare_all_experiments.py
```

Saídas: `experimentos/<nome>/metrics_summary_v3.json` e `experimentos/comparison_table.md`.

## Novo experimento (opcional)

```powershell
python eval/run_experiment.py --experiment meu_teste --model gpt-4.1-mini --prompt poscagada_16k
```

Resultados em `experimentos/<nome>/`.

Documentação: [`../docs/REPLICACAO.md`](../docs/REPLICACAO.md) e [`../docs/METRICAS.md`](../docs/METRICAS.md).

## Golden v2.0 (perguntas reais de testers)

Em construção em `dados/golden/v2.0/`. Ver [`dados/golden/v2.0/README.md`](dados/golden/v2.0/README.md).

```powershell
# Propõe SQL para um item (requer OPENAI_API_KEY)
python scripts/golden_assist.py --id 001 --apply

# Pacote de revisão para o avaliador
python scripts/golden_review_export.py
```
