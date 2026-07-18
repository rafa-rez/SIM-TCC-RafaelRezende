# Avaliação — SIM (Caeté)

Pipeline reprodutível para validar os subsistemas **Text-to-SQL** e **RAG** do SIM.

Documentação do sistema: [`../docs/SIM.md`](../docs/SIM.md)

---

## Resultados reportados no TCC

| Subsistema | Golden | Indicadores |
|------------|--------|-------------|
| Text-to-SQL | `dados/golden/golden_dataset_v1.0.csv` (80) | VSR, EF — experimentos E1–E4 |
| RAG | `dados/golden_rag/golden_rag_v1.1.csv` (8) | Recall@5, MRR |

| Experimento | VSR | EF |
|-------------|----:|---:|
| E1 baseline (GPT-4.1-mini) | 97,5% | **62,5%** |
| E2 contexto estendido | 96,3% | 58,8% |
| E3 GPT-4o-mini | 90,0% | 46,3% |
| E4 GPT-4o | 97,5% | 57,5% |

| RAG (v1.1, *k*=5) | Valor |
|-------------------|------:|
| Recall@5 | 100% |
| MRR | 63,33% |

Tabelas: `experimentos/comparison_table_v3.md`

---

## Pipeline de métricas

```powershell
cd avaliacao
pip install -r requirements.txt

.\run_metrics_pipeline.ps1              # recálculo SQL E1–E4 (sem API)
.\run_metrics_pipeline.ps1 -SmokeOnly   # checar pré-requisitos
.\run_metrics_pipeline.ps1 -WithRag     # SQL + avaliação RAG
```

Atalho legado (só SQL): `.\run_replicacao.ps1`

---

## Estrutura

```
avaliacao/
├── dados/
│   ├── golden/           # 80 consultas Text-to-SQL
│   ├── golden_rag/       # 8 perguntas RAG (v1.1)
│   ├── database/         # CSVs DuckDB
│   ├── SICOM/
│   └── staging_cgu/
├── experimentos/         # E1–E4 (checkpoints + métricas)
├── eval/
│   ├── run_experiment.py      # novos experimentos SQL (API)
│   ├── run_rag_eval.py        # avaliação RAG
│   └── run_pipeline.py        # orquestrador unificado
├── scripts/              # recompute, compare, build golden RAG
├── run_metrics_pipeline.ps1
└── requirements.txt
```

---

## Pré-requisitos

| Tarefa | Requisito |
|--------|-----------|
| Recálculo SQL | Python 3.11+, `pip install -r requirements.txt` |
| Novo experimento SQL | `OPENAI_API_KEY` no `.env` da raiz |
| Avaliação RAG | Qdrant em `localhost:6333` + `OPENAI_API_KEY` |

Stack Docker: `docker compose up -d` na raiz do repositório.

---

## Documentação

| Arquivo | Conteúdo |
|---------|----------|
| [`../docs/METRICAS.md`](../docs/METRICAS.md) | Definições Text-to-SQL |
| [`../docs/PLANO_RAG.md`](../docs/PLANO_RAG.md) | Protocolo e golden RAG |
| [`dados/golden/README.md`](dados/golden/README.md) | Golden SQL |
| [`dados/golden_rag/README.md`](dados/golden_rag/README.md) | Golden RAG |
| [`experimentos/README.md`](experimentos/README.md) | Artefatos E1–E4 |
