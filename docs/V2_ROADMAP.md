# Roadmap v2 — SIM Caeté

Plano de melhorias **pós-v1.0**. A v1 está congelada (TCC, golden SQL v1, E1–E4, golden RAG v1.1 reportado).

Contexto para agentes: [`../AGENTS.md`](../AGENTS.md)

---

## Princípios

1. **Baseline primeiro** — `run_metrics_pipeline.ps1` deve passar antes/depois de cada mudança SQL.
2. **Uma alavanca por vez** — não misturar novo embedding + novo chunking + novo prompt no mesmo commit.
3. **Golden honesto** — expandir RAG sem filtrar só perguntas que já passam no top-5.
4. **Regressão documentada** — cada experimento entra em `experimentos-v2/LOG.md`.

---

## Prioridade sugerida

### P0 — Infraestrutura de experimentação (1–2 dias)

| # | Tarefa | Saída |
|---|--------|-------|
| P0.1 | Gate na pipeline: falhar se VSR E1 < 97,5% ou EF < 62,5% | `run_pipeline.py --check-baseline` |
| P0.2 | Log de experimentos | `docs/experimentos-v2/LOG.md` |
| P0.3 | Tag git `v1.0` no commit dos PDFs | referência imutável |

### P1 — RAG (maior ganho percebido)

| # | Tarefa | Hipótese | Métrica alvo |
|---|--------|----------|--------------|
| P1.1 | Deduplicar chunks no Qdrant | top-5 deixa de repetir o mesmo trecho | MRR sobe com mesmo Recall |
| P1.2 | Script de reindexação versionado | reprodutibilidade do índice | pipeline sobe índice de teste |
| P1.3 | Golden RAG v1.2 (15–20 itens) | amostra menos enviesada | reportar Recall@5 + MRR **sem** critério rank≤5 na inclusão |
| P1.4 | `filtro_tipo_ato` no runner (como n8n) | menos confusão decreto/dec veto similar | MRR em perguntas temáticas |
| P1.5 | Enriquecer payload (`numero_ato`, `ano`) | match e filtro mais precisos | menos falsos positivos |
| P1.6 | Avaliação resposta NL (10 itens) | TA / rubrica `judge_rubric.txt` | complemento qualitativo |

**Não reindexar produção** sem backup do volume `qdrant_data`.

### P2 — Text-to-SQL (prompts e modelo)

| # | Tarefa | Notas |
|---|--------|-------|
| P2.1 | Ablations de prompt em `eval/prompts/` | comparar só no golden **v1** para não confundir com TCC |
| P2.2 | Revisão humana `colunas_resposta` (5 itens falhos EF) | pode subir EF sem mudar modelo |
| P2.3 | Golden v2 (testers) | WIP em `v2.0/` — **não** citar no TCC até curadoria fechada |
| P2.4 | Autocorreção SQL (retry com erro DuckDB) | métrica: VSR e EF pós-retry |

### P3 — Orquestrador e produção (depois)

- Roteamento SQL vs RAG (golden misto)
- Sincronizar workflow n8n exportado no repo
- ETL diário / jornais novos

---

## Matriz de experimentos RAG

| Variável | Valores a testar | Fixar |
|----------|------------------|-------|
| Embedding | `text-embedding-3-small`, `text-embedding-3-large` | chunking |
| Chunk size | 500, 1000, 1500 chars | embedding |
| top_k avaliação | 5, 10, 20 | embedding |
| Filtro tipo | nenhum, `DECRETO`, `LEI` | pergunta |

Nome de pasta: `experimentos/rag_<variavel>_<valor>_v2/`

---

## Matriz de experimentos SQL

| Variável | Valores | Fixar |
|----------|---------|-------|
| Prompt | `prompt_sql_compacto.txt`, variantes | modelo gpt-4.1-mini |
| Modelo | 4.1-mini, 4o-mini, 5-mini | prompt compacto |
| Temperatura | 0.0 (manter) | — |

Usar `eval/run_experiment.py --experiment <nome>`.

---

## Definição de pronto (v2 milestone)

- [ ] Pipeline com gate de regressão SQL
- [ ] Golden RAG ≥15 itens curados + relatório de validação Qdrant
- [ ] Pelo menos 1 melhoria RAG mensurável (ex. MRR +10 pp em golden honesto)
- [ ] Decisão documentada: reindexar produção ou não
- [ ] (Opcional) Seção TCC v2 ou artigo estendido — **só com OK do usuário**

---

## Como iniciar sessão limpa no Cursor

1. Abrir chat novo.
2. Primeira mensagem:

```
Leia AGENTS.md e docs/V2_ROADMAP.md.
Trabalhar em P1.1 (dedup chunks RAG). Não alterar v1.0.
```

3. O agente carrega contexto do repo, não do histórico.

---

## Registro de experimentos

Ver [`experimentos-v2/LOG.md`](experimentos-v2/LOG.md).
