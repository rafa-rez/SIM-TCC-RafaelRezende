# AGENTS.md — Contexto para agentes (SIM / TCC)

Leia este arquivo no início de **qualquer sessão v2** (melhorias pós-v1.0). Substitui memória de chat anterior.

---

## Projeto em uma linha

**SIM** — plataforma conversacional multi-instância para dados abertos municipais. Instância piloto: **Caeté (MG)**, IBGE 3110004. Repo: `avaliacao/` (métricas) + `docs/` (TCC) + `docker-compose.yml` (stack local).

---

## v1.0 — CONGELADO (não alterar sem pedido explícito)

| Artefato | Caminho / valor |
|----------|-----------------|
| Golden SQL | `avaliacao/dados/golden/golden_dataset_v1.0.csv` (80) |
| Experimentos SQL | `e1_baseline_compacto` … `e4_gpt4o` |
| Baseline E1 | **VSR 97,5%** · **EF 62,5%** |
| Golden RAG reportado | `golden_rag_v1.1.csv` (8) · Recall@5 **100%** · MRR **63,33%** |
| Textos TCC | `docs/artigo/`, `docs/monografia/` + PDFs |
| Commit de referência | tag `v1.0` ou `main` até `aeee088` |

**Não tocar:** golden v1 SQL, E1–E4 checkpoints, PDFs do TCC, narrativa “métrica principal = transparência/SIM”.

---

## v2 — escopo livre (melhorias)

Prioridade e ordem: [`docs/V2_ROADMAP.md`](docs/V2_ROADMAP.md)

Áreas:

1. **RAG** — chunking, deduplicação, reindexação, `filtro_tipo_ato`, golden maior (sem cherry-pick), avaliação de resposta NL
2. **Text-to-SQL** — prompts (`eval/prompts/`), modelos, golden v2 (WIP em `dados/golden/v2.0/`, gitignored)
3. **Pipeline** — `run_metrics_pipeline.ps1`, regressão automática
4. **Produção** — n8n workflow, ETL diário, docker (não versionado antes do v1)

Regra de ouro: **uma variável por experimento**; comparar com baseline via pipeline.

---

## Comandos que você deve rodar

```powershell
# Regressão SQL (sem API)
cd avaliacao
.\run_metrics_pipeline.ps1

# SQL + RAG (Qdrant :6333 + OPENAI_API_KEY no .env)
.\run_metrics_pipeline.ps1 -WithRag

# Stack local
cd ..
docker compose up -d qdrant
```

---

## RAG — fatos técnicos (não esquecer)

| Item | Valor |
|------|-------|
| Collection Qdrant | `jornais_caete` |
| Embedding produção | `text-embedding-3-small` (1536 dims) — **não** `ada-002` |
| URL | `http://localhost:6333` |
| Matching métrica | `page_content` do payload, não `source` (nome PDF) |
| Runner | `avaliacao/eval/run_rag_eval.py` |
| Config | `avaliacao/eval/config.yaml` → seção `rag:` |

Lição v1.0: golden derivado só de citações SICOM falha se o ato não estiver no índice.

---

## Textos acadêmicos (se mexer)

- VSR / EF (nunca EX resposta nos PDFs)
- Sem custos USD nos textos
- RAG = evidência complementar, amostra piloto
- Regra Cursor: `.cursor/rules/sim-tcc-docs.mdc`
- Compilar: `cd docs; .\compile.ps1`

---

## Workflow de experimento v2

1. Branch: `exp/<area>-<nome-curto>` (ex. `exp/rag-dedup-chunks`)
2. Hipótese + uma mudança
3. Rodar pipeline; gravar em `avaliacao/experimentos/<nome>/`
4. Registrar 3 linhas em `docs/experimentos-v2/LOG.md`
5. Só atualizar TCC/PDFs quando o usuário pedir

---

## O que está gitignored (WIP local)

- `avaliacao/dados/golden/v2.0/`
- `avaliacao/experimentos/*_v2/`, `rag_baseline_v*/`
- `/dados/` na raiz (dados locais docker legado)
- `.env`

---

## Links rápidos

| Doc | Conteúdo |
|-----|----------|
| [`docs/V2_ROADMAP.md`](docs/V2_ROADMAP.md) | Backlog priorizado v2 |
| [`docs/PLANO_RAG.md`](docs/PLANO_RAG.md) | Protocolo RAG |
| [`docs/METRICAS.md`](docs/METRICAS.md) | Definições SQL |
| [`avaliacao/README.md`](avaliacao/README.md) | Pipeline |

---

## Instrução para nova sessão

Usuário pode começar com:

> Leia `AGENTS.md` e `docs/V2_ROADMAP.md`. Quero trabalhar em [área]. Baseline v1 não alterar.
