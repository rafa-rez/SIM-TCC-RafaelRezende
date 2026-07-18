# SIM — Sistema de Informações Municipais

Repositório público de documentação, dados e scripts de validação do **SIM** (*Sistema de Informações Municipais*), plataforma conversacional para consulta a dados abertos de prefeituras. Instância de referência: **Caeté (MG)** (IBGE 3110004).

**Autor:** Rafael Alves Silva Rezende (UFLA)  
**Orientador:** Prof. Dr. Denilson

---

## O que está na v1.0 do TCC

| Entregável | Onde |
|------------|------|
| Validação Text-to-SQL (80 consultas, E1–E4) | `avaliacao/` |
| Artigo e monografia (PDF + LaTeX) | `docs/artigo/`, `docs/monografia/` |
| Stack Docker (n8n, Qdrant, DuckDB API) | `docker-compose.yml`, `Dockerfile` |
| Pipeline de métricas reprodutível | `avaliacao/run_metrics_pipeline.ps1` |

O subsistema **RAG** está documentado e com infraestrutura no repo; métricas RAG **não** entram nos PDFs v1.0 (ver [`docs/PLANO_RAG.md`](docs/PLANO_RAG.md)).

---

## Validação Text-to-SQL (resultados do TCC)

Baseline **E1** (GPT-4.1-mini, *n* = 80):

| Indicador | Valor |
|-----------|------:|
| Taxa de SQL válida (VSR) | 97,5% |
| Equivalência funcional (EF) | 62,5% |

Detalhes: [`docs/METRICAS.md`](docs/METRICAS.md), [`avaliacao/experimentos/comparison_table_v3.md`](avaliacao/experimentos/comparison_table_v3.md).

---

## Replicação rápida

```powershell
cd avaliacao
.\run_metrics_pipeline.ps1
```

Requisitos: Python 3.11+. Sem custo de API para o recálculo SQL.

Guia completo: [`docs/REPLICACAO.md`](docs/REPLICACAO.md).

---

## Estrutura do repositório

```
tiokk-n8n/
├── avaliacao/           # Dados, golden, experimentos E1–E4, pipeline
├── docs/                # SIM, métricas, artigo, monografia
├── scripts_python/      # API DuckDB (container duckdb_api)
├── docker-compose.yml   # Qdrant, n8n, Redis, WAHA, Ollama, DuckDB
└── Dockerfile           # Imagem n8n customizada
```

---

## Infraestrutura local (opcional)

```powershell
docker compose up -d          # stack completa
docker compose up -d qdrant   # só vetorial (RAG)
```

Portas: Qdrant `6333`, DuckDB API `8000`, n8n `5679`.

---

## Documentação

| Documento | Descrição |
|-----------|-----------|
| [`docs/SIM.md`](docs/SIM.md) | Arquitetura e instância Caeté |
| [`docs/REPLICACAO.md`](docs/REPLICACAO.md) | Replicação da avaliação |
| [`docs/PLANO_RAG.md`](docs/PLANO_RAG.md) | RAG — plano pós-v1.0 |
| [`avaliacao/README.md`](avaliacao/README.md) | Pipeline e experimentos |

---

## Licença

Dados públicos (SICOM/CGU). Código e documentação: [`LICENSE`](LICENSE).
