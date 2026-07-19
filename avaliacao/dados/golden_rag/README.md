# Golden RAG — SIM Caeté

Perguntas sobre decretos do Diário Oficial com documento de referência validado no índice Qdrant (`jornais_caete`).

---

## Versões

| Versão | Itens | Uso |
|--------|------:|-----|
| **v1.1** | 8 | Reportado no TCC (amostra curada) |
| **v1.2** | 20 | Amostra honesta (sem cherry-pick por rank) |
| v1.0 | 10 | Arquivado (5 itens sem chunk no índice) |

Arquivos: `golden_rag_v1.1.csv` · `golden_rag_v1.2.csv`

---

## Resultados — golden v1.1 (*k*=5)

| Configuração | Recall@5 | MRR |
|--------------|----------|----:|
| Busca densa (baseline TCC) | 100% (8/8) | 63,33% |
| Dedup query-time | 100% | 69,79% |
| Híbrido + dedup + filtro tipo | 100% | **87,5%** |

Embedding: `text-embedding-3-small` · Collection: `jornais_caete`

---

## Resultados — golden v1.2 honesto (*k*=5)

| Configuração | Recall@5 | MRR |
|--------------|----------|----:|
| Busca densa | 0% | 0% |
| Híbrido + dedup | 100% | 100% |

---

## Avaliar

```powershell
cd avaliacao
.\run_metrics_pipeline.ps1 -WithRag
# flags: --dedup --hybrid-act --filtro-tipo-ato
python eval/run_rag_eval.py --experiment rag_baseline_v1_1
```

Protocolo: [`../../docs/PLANO_RAG.md`](../../docs/PLANO_RAG.md)
