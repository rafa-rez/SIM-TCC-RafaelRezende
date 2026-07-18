# Golden RAG — SIM Caeté

Perguntas sobre decretos do Diário Oficial com documento de referência validado no índice Qdrant (`jornais_caete`).

---

## Versões

| Versão | Itens | Uso |
|--------|------:|-----|
| **v1.1** | 8 | Ativo — reportado no TCC |
| v1.0 | 10 | Arquivado (5 itens sem chunk no índice) |

Arquivo: `golden_rag_v1.1.csv` · Relatório: `validation_report_v1.1.json`

---

## Resultados (baseline v1.1, *k*=5)

| Métrica | Valor |
|---------|------:|
| Recall@5 | 100% (8/8) |
| MRR | 63,33% |

Embedding: `text-embedding-3-small` · Collection: `jornais_caete`

---

## Regenerar

```powershell
cd avaliacao
python scripts/build_golden_rag_v1_1.py   # requer Qdrant + OPENAI_API_KEY
```

---

## Avaliar

```powershell
.\run_metrics_pipeline.ps1 -WithRag
# ou
python eval/run_rag_eval.py --experiment rag_baseline_v1_1
```

Protocolo: [`../../docs/PLANO_RAG.md`](../../docs/PLANO_RAG.md)
