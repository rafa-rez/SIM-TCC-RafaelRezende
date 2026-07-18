# Replicação da avaliação

Procedimento para reproduzir a validação Text-to-SQL do **SIM** (*Sistema de Informações Municipais*), instância **Caeté (MG)**.

Visão geral do sistema: [`SIM.md`](SIM.md).

---

## 1. Ambiente

- Python 3.11 ou superior
- Aproximadamente 2 GB de espaço em disco (`avaliacao/dados/`)
- Opcional: Docker para API idêntica à produção (fora deste repositório)

```powershell
cd avaliacao
pip install -r requirements.txt
```

Variáveis (apenas para **novos** experimentos com API OpenAI):

```powershell
# Na raiz do repositório
cp .env.example .env
# OPENAI_API_KEY=...
```

---

## 2. Recálculo de métricas (sem custo de API)

Reexecuta as SQL de `checkpoint.jsonl` no DuckDB local e grava `metrics_summary_v3.json`:

```powershell
cd avaliacao
python scripts/enrich_golden_colunas.py
python scripts/recompute_metrics.py --experiment e1_baseline_compacto
python scripts/recompute_metrics.py --experiment e2_contexto_estendido
python scripts/recompute_metrics.py --experiment e3_gpt4o_mini
python scripts/recompute_metrics.py --experiment e4_gpt4o
python scripts/compare_all_experiments.py
```

Atalho: `.\run_replicacao.ps1` (a partir de `avaliacao/`).

### Saídas por experimento

- `avaliacao/experimentos/<nome>/metrics_summary_v3.json`
- `avaliacao/experimentos/<nome>/metricas_por_caso/NNN.json`
- `avaliacao/experimentos/comparison_table_v3.md`

---

## 3. Exportação de resultados tabulares

Referência (golden):

```powershell
cd avaliacao
python scripts/export_result_sets.py `
  --source golden `
  --output-dir dados/golden/reference_results
```

SQL geradas (por experimento):

```powershell
python scripts/export_result_sets.py `
  --source checkpoint `
  --experiment e1_baseline_compacto `
  --output-dir experimentos/e1_baseline_compacto/generated_results
```

---

## 4. Novo experimento (opcional, consome API)

```powershell
cd avaliacao
python eval/run_experiment.py `
  --experiment meu_teste `
  --model gpt-4.1-mini `
  --prompt poscagada_16k
```

---

## 5. Conjunto golden

Arquivo canônico: `avaliacao/dados/golden/golden_dataset_v1.0.csv` (delimitador `;`, 80 consultas).

Colunas principais: `id_teste`, `pergunta`, `query_referencia`, `colunas_resposta`.

Resultado tabular de cada referência: `avaliacao/dados/golden/reference_results/NNN.json`.

---

## 6. Motor DuckDB

Os scripts usam por padrão o motor DuckDB local (`avaliacao/eval/lib/duckdb_engine.py`), que carrega os CSV em `avaliacao/dados/database/`, `avaliacao/dados/SICOM/` e `avaliacao/dados/staging_cgu/`. Não é necessário Docker para recálculo offline.

---

## 7. Documentação relacionada

| Documento | Conteúdo |
|-----------|----------|
| [`SIM.md`](SIM.md) | Conceito, arquitetura e instância Caeté |
| [`METRICAS.md`](METRICAS.md) | Definições formais das métricas |
| [`RELATORIO_METRICAS_V3.md`](RELATORIO_METRICAS_V3.md) | Resultados dos quatro experimentos |
| [`../avaliacao/README.md`](../avaliacao/README.md) | Estrutura do pipeline de avaliação |
