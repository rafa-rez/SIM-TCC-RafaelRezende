# AGENTS.md — Contexto para agentes (SIM / TCC)

Leia este arquivo no início de sessões de manutenção do repositório.

---

## Projeto

**SIM** — plataforma conversacional multi-instância para dados abertos municipais. Instância piloto: **Caeté (MG)**, IBGE 3110004.

---

## Resultados reportados (TCC)

| Lote | Escopo | Configuração final |
|------|--------|-------------------|
| I | E1–E4 (modelo/contexto) | GPT-4.1-mini + prompt compacto: VSR 97,5%, EF 62,5% |
| II | Prompt v2.1 | VSR 100%, EF 72,5% |
| III | RAG v1.1 | Recall@5 100%, MRR 87,5% (híbrido+dedup) |

Textos acadêmicos: `docs/monografia/`, `docs/artigo/` + PDFs.

---

## Artefatos congelados

| Item | Caminho |
|------|---------|
| Golden SQL | `avaliacao/dados/golden/golden_dataset_v1.0.csv` (80) |
| Experimentos Lote I | `e1_baseline_compacto` … `e4_gpt4o` |
| Golden RAG | `golden_rag_v1.1.csv` (8) |
| Prompt final SQL | `eval/prompts/prompt_sql_v2_insights.txt` |

**Não alterar** golden v1, checkpoints E1–E4 nem PDFs do TCC sem pedido explícito do autor.

---

## Comandos

```powershell
cd avaliacao
.\run_metrics_pipeline.ps1              # SQL E1–E4 (sem API)
.\run_metrics_pipeline.ps1 -WithRag     # SQL + RAG
```

---

## RAG — fatos técnicos

| Item | Valor |
|------|-------|
| Collection | `jornais_caete` |
| Embedding | `text-embedding-3-small` (1536 dims) |
| Matching métrica | `page_content`, não `source` |
| Runner | `avaliacao/eval/run_rag_eval.py` |

---

## Textos acadêmicos

- Métricas nos PDFs: **VSR** e **EF** (nunca EX resposta)
- RAG = evidência complementar, amostra *n*=8
- Compilar: `cd docs; .\compile.ps1`

---

## Links

| Doc | Conteúdo |
|-----|----------|
| [`docs/METRICAS.md`](docs/METRICAS.md) | Definições e fórmulas |
| [`docs/PLANO_RAG.md`](docs/PLANO_RAG.md) | Protocolo RAG |
| [`docs/SIM.md`](docs/SIM.md) | Arquitetura |
