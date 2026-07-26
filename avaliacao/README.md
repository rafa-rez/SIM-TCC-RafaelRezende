# Avaliação — SIM (Caeté)

Pipeline reprodutível para validar os subsistemas **Text-to-SQL** e **RAG** do SIM.

Documentação do sistema: [`../docs/SIM.md`](../docs/SIM.md)

---

## Resultados reportados (três lotes)

| Lote | Golden | Indicadores | Configuração final |
|------|--------|-------------|-------------------|
| I — modelo/contexto | SQL v1 (80) | VSR, EF — E1–E4 | GPT-4.1-mini + prompt compacto |
| II — engenharia de prompt | SQL v1 (80) | VSR, EF | **Prompt v2.1: VSR 100%, EF 72,5%** |
| III — recuperação RAG | RAG v1.1 (8) | Recall@5, MRR | **Híbrido+dedup: MRR 87,5%** |

### Lote I — E1–E4

| Experimento | VSR | EF |
|-------------|----:|---:|
| E1 baseline | 97,5% | 62,5% |
| E2 contexto estendido | 96,3% | 58,8% |
| E3 GPT-4o-mini | 90,0% | 46,3% |
| E4 GPT-4o | 97,5% | 57,5% |

### Lote II — configuração final

| Variante | VSR | EF |
|----------|----:|---:|
| E1 (referência) | 97,5% | 62,5% |
| **Prompt v2.1** | **100%** | **72,5%** |

### Lote III — RAG (*k*=5)

| Configuração | Recall@5 | MRR |
|--------------|----------|----:|
| Busca densa | 100% | 63,33% |
| Híbrido + dedup + filtro tipo | 100% | **87,5%** |

Tabela E1–E4: `experimentos/comparison_table_v3.md`

---

## Pipeline de métricas

```powershell
cd avaliacao
pip install -r requirements.txt

.\run_metrics_pipeline.ps1              # recálculo SQL E1–E4 (sem API)
.\run_metrics_pipeline.ps1 -SmokeOnly   # checar pré-requisitos
.\run_metrics_pipeline.ps1 -WithRag     # SQL + avaliação RAG
```

---

## Estrutura

```
avaliacao/
├── dados/
│   ├── golden/           # 80 consultas Text-to-SQL
│   ├── golden_rag/       # 8 perguntas RAG (v1.1)
│   ├── database/         # CSVs DuckDB
│   ├── SICOM/
│   └── staging_cgu/
├── experimentos/         # E1–E4 (checkpoints + métricas)
├── eval/                 # runners e biblioteca de comparação
├── scripts/              # recompute, compare
├── run_metrics_pipeline.ps1
└── requirements.txt
```

---

## Documentação

| Arquivo | Conteúdo |
|---------|----------|
| [`../docs/METRICAS.md`](../docs/METRICAS.md) | Definições formais (VSR, EF, fórmulas) |
| [`../docs/PLANO_RAG.md`](../docs/PLANO_RAG.md) | Protocolo e resultados RAG |
| [`dados/golden/README.md`](dados/golden/README.md) | Golden SQL |
| [`dados/golden_rag/README.md`](dados/golden_rag/README.md) | Golden RAG |
| [`experimentos/README.md`](experimentos/README.md) | Artefatos E1–E4 |
