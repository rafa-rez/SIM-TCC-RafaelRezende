# Avaliação — SIM (instância Caeté)

Pipeline reprodutível para validar o subsistema **Text-to-SQL** do SIM sobre dados públicos de Caeté (MG). O TCC v1.0 reporta apenas esta validação (80 consultas, experimentos E1–E4).

Infraestrutura **RAG** (golden + runner) está no repositório para a pipeline de métricas futura; ver [`../docs/PLANO_RAG.md`](../docs/PLANO_RAG.md).

Documentação do sistema: [`../docs/SIM.md`](../docs/SIM.md).

---

## Estrutura

```
avaliacao/
├── dados/
│   ├── golden/              # Text-to-SQL — golden v1.0 (80 consultas) ★ TCC
│   ├── golden_rag/          # RAG — golden v1.1 (8 consultas) — pós-v1.0
│   ├── database/            # CSVs consolidados (DuckDB local)
│   ├── SICOM/               # Fontes brutas
│   └── staging_cgu/
├── experimentos/            # E1–E4 + tabelas comparativas ★ TCC
├── eval/                    # Runners, prompts, compare.py, run_pipeline.py
├── scripts/                 # Recálculo offline, golden RAG
├── run_replicacao.ps1       # Atalho SQL offline (legado)
├── run_metrics_pipeline.ps1 # Pipeline unificada ★
└── requirements.txt
```

---

## Pipeline de métricas (v1.0)

```powershell
cd avaliacao
pip install -r requirements.txt

# Text-to-SQL offline (sem API) — validação do TCC
.\run_metrics_pipeline.ps1

# Só checar pré-requisitos
.\run_metrics_pipeline.ps1 -SmokeOnly

# SQL + RAG (Qdrant + OpenAI) — desenvolvimento, não no PDF v1.0
.\run_metrics_pipeline.ps1 -WithRag
```

Saídas:
- `experimentos/comparison_table_v3.md` — E1–E4
- `experimentos/pipeline_report.json` — resumo da execução
- `experimentos/rag_baseline_v1_1/` — só com `-WithRag`

---

## Conjunto golden Text-to-SQL

Arquivo canônico: `dados/golden/golden_dataset_v1.0.csv` (80 consultas).

Detalhes: [`dados/golden/README.md`](dados/golden/README.md).

---

## Experimentos E1–E4

| Pasta | Modelo | Prompt |
|-------|--------|--------|
| `e1_baseline_compacto` | GPT-4.1-mini | Compacto |
| `e2_contexto_estendido` | GPT-4.1-mini | Estendido |
| `e3_gpt4o_mini` | GPT-4o-mini | Compacto |
| `e4_gpt4o` | GPT-4o | Compacto |

Tabela: `experimentos/comparison_table_v3.md`

---

## Pré-requisitos

- Python 3.11+
- Recálculo SQL: **sem API** (DuckDB em memória)
- Novos experimentos / RAG: `.env` na raiz com `OPENAI_API_KEY`
- RAG: Qdrant em `localhost:6333` (ver `docker-compose.yml` na raiz)

---

## Documentação

| Documento | Conteúdo |
|-----------|----------|
| [`../docs/REPLICACAO.md`](../docs/REPLICACAO.md) | Replicação passo a passo |
| [`../docs/METRICAS.md`](../docs/METRICAS.md) | Métricas Text-to-SQL |
| [`../docs/PLANO_RAG.md`](../docs/PLANO_RAG.md) | RAG — trabalho futuro |
