# Documentação — SIM

Índice da documentação do **SIM** (*Sistema de Informações Municipais*) e da validação empírica na instância Caeté (MG).

---

## Sistema e replicação

| Documento | Descrição |
|-----------|-----------|
| [SIM.md](SIM.md) | Conceito, arquitetura, fontes de dados e instância Caeté |
| [REPLICACAO.md](REPLICACAO.md) | Procedimento para reproduzir a avaliação offline |
| [PLANO_RAG.md](PLANO_RAG.md) | Protocolo e resultados RAG (Lote III) |

## Validação empírica

| Documento | Descrição |
|-----------|-----------|
| [METRICAS.md](METRICAS.md) | Definições formais: VSR, EF, fórmulas e lotes I–II |
| [RELATORIO_METRICAS_V3.md](RELATORIO_METRICAS_V3.md) | Resultados do Lote I (E1–E4) |
| [metricas/](metricas/) | Notas por família de métrica |

## Textos do trabalho

| Pasta | Descrição |
|-------|-----------|
| [artigo/](artigo/) | Artigo SBC — `sim_artigo_sbc_v1.tex` + PDF |
| [monografia/](monografia/) | Monografia UFLA — `templufla_main.tex` + PDF |
| [referencias/](referencias/) | Bibliografia (`referencias_sim.bib`) |

---

## Compilar PDFs

```powershell
cd docs
.\compile.ps1
```

Requer TeX Live (ou MiKTeX) e `template-ufla/` na raiz do repositório para a monografia.

---

## Estrutura

```
docs/           → documentação, métricas e textos acadêmicos
avaliacao/      → dados, experimentos, pipeline e scripts reprodutíveis
```

O repositório contém validação Text-to-SQL (Lotes I–II), validação RAG (Lote III), textos do TCC e stack Docker para replicação local.
