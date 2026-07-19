# SIM — Sistema de Informações Municipais

Plataforma conversacional para consulta a dados abertos municipais. **Instância de referência:** Caeté (MG), IBGE 3110004.

**Autor:** Rafael Alves Silva Rezende (UFLA) · **Orientador:** Prof. Dr. Denilson

---

## Resultados da validação

### Text-to-SQL — 80 consultas (golden v1)

| Experimento | VSR | EF | Notas |
|-------------|----:|---:|-------|
| **E1 baseline** (GPT-4.1-mini, prompt compacto) | 97,5% | 62,5% | Configuração inicial reportada no TCC |
| **E6 prompt v2.1** (mesmo modelo, refinamento pós-validação) | **100%** | **72,5%** | +8,75 pp EF vs E1; ver [`docs/METRICAS.md`](docs/METRICAS.md) |

Comparativo E1–E4: `avaliacao/experimentos/comparison_table_v3.md`

### RAG — golden v1.1 (8 perguntas, *k*=5)

| Configuração de recuperação | Recall@5 | MRR |
|-----------------------------|----------|----:|
| Busca densa (baseline reportado) | 100% | 63,33% |
| Deduplicação de *chunks* idênticos | 100% | 69,79% |
| Híbrido + dedup + filtro por tipo de ato | 100% | **87,5%** |

Golden v1.2 honesto (*n*=20, amostra sem cherry-pick): busca densa 0% Recall@5; stack híbrido+dedup 100%/100%. Detalhes: [`docs/PLANO_RAG.md`](docs/PLANO_RAG.md)

Definições e protocolo: [`docs/METRICAS.md`](docs/METRICAS.md) · [`docs/experimentos-v2/LOG.md`](docs/experimentos-v2/LOG.md)

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

- [`AGENTS.md`](AGENTS.md) — contexto para **sessões v2** (abra chat novo e cite este arquivo)
- [`docs/V2_ROADMAP.md`](docs/V2_ROADMAP.md) — backlog de melhorias pós-v1.0
- [`docs/SIM.md`](docs/SIM.md) — arquitetura e instância Caeté
- [`avaliacao/README.md`](avaliacao/README.md) — pipeline e experimentos
- [`docs/artigo/`](docs/artigo/) · [`docs/monografia/`](docs/monografia/) — textos do TCC

---

## Licença

Dados públicos (SICOM/CGU). Código e documentação: [`LICENSE`](LICENSE).
