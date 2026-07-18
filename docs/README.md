# Documentação — SIM

Índice da documentação do **SIM** (*Sistema de Informações Municipais*) e da validação Text-to-SQL na instância Caeté (MG).

---

## Sistema e replicação

| Documento | Descrição |
|-----------|-----------|
| [SIM.md](SIM.md) | Conceito, arquitetura, fontes de dados e instância Caeté |
| [REPLICACAO.md](REPLICACAO.md) | Procedimento para reproduzir a avaliação offline |
| [PLANO_RAG.md](PLANO_RAG.md) | Plano de avaliação do subsistema RAG |

## Validação Text-to-SQL

| Documento | Descrição |
|-----------|-----------|
| [METRICAS.md](METRICAS.md) | Definições formais das métricas (v3) |
| [RELATORIO_METRICAS_V3.md](RELATORIO_METRICAS_V3.md) | Resultados empíricos dos experimentos E1–E4 |
| [metricas/](metricas/) | Notas por família de métrica (EX, VSR, NEA, TSA, CHS, JAR) |

## Textos do trabalho

| Pasta | Descrição |
|-------|-----------|
| [artigo/](artigo/) | Artigo SBC — `sim_artigo_sbc_v1.tex` |
| [monografia/](monografia/) | Monografia UFLA — `templufla_main.tex` |
| [referencias/](referencias/) | Bibliografia (`referencias_sim.bib`), plano de leitura e fichamentos |

---

## Compilar PDFs

```powershell
cd docs
.\compile.ps1
```

Requer TeX Live (ou MiKTeX) e `template-ufla/` na raiz do repositório para a monografia.

---

## Relação entre pastas

```
docs/           → documentação conceitual, métricas e textos acadêmicos
avaliacao/      → dados, experimentos, pipeline e scripts reprodutíveis
```

O repositório público contém a avaliação Text-to-SQL (v1.0 do TCC) e a stack Docker (`docker-compose.yml`). Métricas RAG são executáveis pela pipeline, mas não compõem os PDFs nesta versão — ver [PLANO_RAG.md](PLANO_RAG.md).
