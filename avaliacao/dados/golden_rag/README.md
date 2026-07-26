# Golden RAG — SIM Caeté

Perguntas sobre decretos do Diário Oficial com documento de referência validado no índice Qdrant (`jornais_caete`).

---

## Versões

| Versão | Itens | Uso |
|--------|------:|-----|
| **v1.1** | 8 | Referência principal (Lote III) |
| **v1.2** | 20 | Amostra ampliada (evidência complementar) |

---

## Resultados — Lote III (golden v1.1, *k*=5)

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
