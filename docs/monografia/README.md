# Monografia UFLA — SIM (v1)

Cópia versionada do conteúdo LaTeX. A compilação com template UFLA usa `template-ufla/` (local).

## Arquivos

| Arquivo | Conteúdo |
|---------|----------|
| `templufla_main.tex` | Capa, resumo, metadados |
| `secoes/introducao.tex` | Contexto SIM + Caeté |
| `secoes/referencial.tex` | Spider, BIRD, RAG, SICOM |
| `secoes/metodologia.tex` | Arquitetura + métricas v3 |
| `secoes/resultados.tex` | Tabelas E1–E4 |
| `secoes/discussao.tex` | Transparência, viabilidade e limitações |
| `secoes/conclusao.tex` | Contribuições e futuro |
| `figuras/arquitetura_sim.tex` | Diagrama TikZ |

## Sincronização

Após editar aqui ou em `template-ufla/`, manter ambos alinhados até migrarmos compilação para o repositório.

## Compilar (local)

```powershell
cd docs
.\compile.ps1
```

Saída versionada: **`SIM_monografia_v1.pdf`** (nesta pasta).

Ou diretamente em `template-ufla/` com `.\compile.ps1` e copiar o PDF.

Bibliografia: `docs/referencias/referencias_sim.bib` (copiar para `template-ufla/refbib.bib` ou unificar).

## Próxima fase (v2)

Integrar citações dos PDFs em `docs/referencias/pdfs/` conforme `LEITURAS.md`.
