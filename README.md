# SIM — Sistema de Informações Municipais

Plataforma conversacional para consulta a dados abertos municipais. **Instância de referência:** Caeté (MG), IBGE 3110004.

**Autor:** Rafael Alves Silva Rezende (UFLA) · **Orientador:** Prof. Dr. Denilson

---

## Resultados da validação

A validação empírica organizou-se em **três lotes experimentais** sobre conjuntos de referência fixos. Detalhes completos na monografia e no artigo (`docs/monografia/`, `docs/artigo/`).

### Lote I — Text-to-SQL: comparação de modelo e contexto (E1–E4)

| Experimento | VSR | EF |
|-------------|----:|---:|
| E1 baseline (GPT-4.1-mini, prompt compacto) | 97,5% | 62,5% |
| E2 contexto estendido | 96,3% | 58,8% |
| E3 GPT-4o-mini | 90,0% | 46,3% |
| E4 GPT-4o | 97,5% | 57,5% |

### Lote II — Text-to-SQL: configuração final de prompt (v2.1)

| Variante | VSR | EF |
|----------|----:|---:|
| E1 — prompt v1 (referência Lote I) | 97,5% | 62,5% |
| **Prompt v2.1 (configuração final)** | **100%** | **72,5%** |

### Lote III — RAG: estratégias de recuperação (golden v1.1, *k*=5)

| Configuração | Recall@5 | MRR |
|--------------|----------|----:|
| Busca densa | 100% | 63,33% |
| Deduplicação de *chunks* | 100% | 69,79% |
| **Híbrido + dedup + filtro tipo** | 100% | **87,5%** |

Definições formais e fórmulas: [`docs/METRICAS.md`](docs/METRICAS.md) · [`docs/PLANO_RAG.md`](docs/PLANO_RAG.md)

---

## Replicação

```powershell
cd avaliacao
pip install -r requirements.txt
.\run_metrics_pipeline.ps1              # Text-to-SQL E1–E4 (offline, sem API)
.\run_metrics_pipeline.ps1 -WithRag     # SQL + RAG (Qdrant + OpenAI)
```

Guia: [`docs/REPLICACAO.md`](docs/REPLICACAO.md)

---

## Estrutura

| Pasta | Conteúdo |
|-------|----------|
| [`avaliacao/`](avaliacao/) | Golden, experimentos, pipeline de métricas |
| [`docs/`](docs/) | SIM, métricas, monografia UFLA, artigo SBC |
| [`docker-compose.yml`](docker-compose.yml) | Stack local: Qdrant, n8n, DuckDB API, Redis |
| [`scripts_python/`](scripts_python/) | API DuckDB |

---

## Documentação

- [`docs/SIM.md`](docs/SIM.md) — arquitetura e instância Caeté
- [`avaliacao/README.md`](avaliacao/README.md) — pipeline e experimentos
- [`docs/artigo/`](docs/artigo/) · [`docs/monografia/`](docs/monografia/) — textos do TCC (PDFs incluídos)

---

## Licença

Dados públicos (SICOM/CGU). Código e documentação: [`LICENSE`](LICENSE).
