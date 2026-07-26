# Golden RAG — SIM Caeté

Perguntas sobre decretos do Diário Oficial com documento de referência validado no índice Qdrant (`jornais_caete`).

---

## Versões

| Versão | Itens | Uso |
|--------|------:|-----|
| **v1.1** | 8 | Amostra curada inicial (Lote III histórico) |
| **v1.2** | 20 | Amostra ampliada (evidência complementar) |
| **v1.3** | 40 | **Referência principal** — 32 com número explícito + 8 temáticos |

---

## Resultados — Lote III (golden v1.3, *k*=5)

Rodar avaliação:

```powershell
cd avaliacao
python eval/run_rag_eval.py --experiment rag_golden_v13_hibrido --dedup --hybrid-act --filtro-tipo-ato
```

| Configuração | Recall@5 | MRR | IC 95% (Recall) |
|--------------|----------|----:|-----------------|
| Busca densa | 22,5% (9/40) | 13,17% | 12,3–37,5% |
| Deduplicação | 22,5% (9/40) | 14,79% | 12,3–37,5% |
| Filtro tipo + dedup | 22,5% (9/40) | 15,42% | 12,3–37,5% |
| **Híbrido + dedup + filtro (final)** | **100%** (40/40) | **97,5%** | **91,2–100%** |

Estratificação (configuração final): `com_numero` (n=32) Recall 100% / MRR 100%; `tematico` (n=8) Recall 100% / MRR 87,5%.

Embedding: `text-embedding-3-small` · Collection: `jornais_caete`

---

## Resultados históricos — golden v1.1 (*k*=5)

| Configuração | Recall@5 | MRR |
|--------------|----------|----:|
| Busca densa | 100% (8/8) | 63,33% |
| **Híbrido + dedup + filtro tipo (final)** | 100% | **87,5%** |

Embedding: `text-embedding-3-small` · Collection: `jornais_caete`

---

## Avaliar

```powershell
cd avaliacao
.\run_metrics_pipeline.ps1 -WithRag
```

Protocolo: [`../../docs/PLANO_RAG.md`](../../docs/PLANO_RAG.md)
