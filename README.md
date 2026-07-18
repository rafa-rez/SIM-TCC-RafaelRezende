# SIM — Sistema de Informações Municipais

Plataforma conversacional para consulta a dados abertos municipais. **Instância de referência:** Caeté (MG), IBGE 3110004.

**Autor:** Rafael Alves Silva Rezende (UFLA) · **Orientador:** Prof. Dr. Denilson

---

## Resultados da validação (TCC v1.0)

### Text-to-SQL — 80 consultas (baseline E1)

| Indicador | Valor |
|-----------|------:|
| Taxa de SQL válida (VSR) | 97,5% |
| Equivalência funcional (EF) | 62,5% |

### RAG — 8 perguntas piloto (golden v1.1)

| Indicador | Valor |
|-----------|------:|
| Recall@5 | 100% |
| MRR | 63,33% |

Definições e experimentos completos: [`docs/METRICAS.md`](docs/METRICAS.md) · [`docs/PLANO_RAG.md`](docs/PLANO_RAG.md)

---

## Replicação

```powershell
cd avaliacao
pip install -r requirements.txt
.\run_metrics_pipeline.ps1              # Text-to-SQL (offline, sem API)
.\run_metrics_pipeline.ps1 -WithRag     # SQL + RAG (Qdrant + OpenAI)
```

Guia: [`docs/REPLICACAO.md`](docs/REPLICACAO.md)

---

## Estrutura

| Pasta | Conteúdo |
|-------|----------|
| [`avaliacao/`](avaliacao/) | Golden, experimentos E1–E4, pipeline de métricas |
| [`docs/`](docs/) | SIM, métricas, artigo SBC, monografia UFLA |
| [`docker-compose.yml`](docker-compose.yml) | Stack local: Qdrant, n8n, DuckDB API, Redis |
| [`scripts_python/`](scripts_python/) | API DuckDB (`main.py`) |

---

## Documentação

- [`docs/SIM.md`](docs/SIM.md) — arquitetura e instância Caeté
- [`avaliacao/README.md`](avaliacao/README.md) — pipeline e experimentos
- [`docs/artigo/`](docs/artigo/) · [`docs/monografia/`](docs/monografia/) — textos do TCC

---

## Licença

Dados públicos (SICOM/CGU). Código e documentação: [`LICENSE`](LICENSE).
