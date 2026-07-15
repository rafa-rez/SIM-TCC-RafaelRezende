# Documentação — SIM

| Documento | Descrição |
|-----------|-----------|
| [SIM.md](SIM.md) | Conceito do sistema e instância Caeté |
| [METRICAS.md](METRICAS.md) | Definições formais das métricas v3 |
| [RELATORIO_METRICAS_V3.md](RELATORIO_METRICAS_V3.md) | Resultados empíricos E1–E4 |
| [REPLICACAO.md](REPLICACAO.md) | Como reproduzir a avaliação |
| [PLANO_ETAPAS.md](PLANO_ETAPAS.md) | Planejamento geral do TCC |
| [referencias/](referencias/) | Bibliografia, plano de leitura e notas |
| [artigo/](artigo/) | Rascunho do artigo SBC v1 |
| [monografia/](monografia/) | Monografia UFLA v1 — PDF: `SIM_monografia_v1.pdf` |
| [artigo/](artigo/) | Artigo SBC v1 — PDF: `sim_artigo_sbc_v1.pdf` |

## Compilar PDFs

```powershell
cd docs
.\compile.ps1          # monografia + artigo
.\compile.ps1 -All     # idem
```

Requer TeX Live (ou MiKTeX) e `template-ufla/` na raiz do repositório para a monografia.

## Monografia LaTeX

A versão formatada UFLA fica em `template-ufla/` (local). O conteúdo textual é mantido alinhado com esta pasta `docs/`.

## Fluxo de trabalho documental

```
v1 sólida (SIM + métricas v3)
    ↓
baixar PDFs → fichamentos em referencias/notas/
    ↓
v2 com referências integradas na monografia e no artigo
```
