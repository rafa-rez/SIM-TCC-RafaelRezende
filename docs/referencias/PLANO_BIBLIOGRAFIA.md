# Plano bibliográfico — SIM (monografia e artigo SBC)

## Objetivo

Construir um referencial teórico sólido em duas fases:

1. **v1 (agora):** monografia e artigo com estrutura completa, métricas v3 e citações estruturais já conhecidas (`referencias_sim.bib`).
2. **v2 (após leitura):** incorporar análise crítica dos PDFs baixados, ajustar argumentos e expandir a discussão com citações diretas.

## Pastas

```
docs/referencias/
  referencias_sim.bib      # BibTeX versionado (fonte canônica)
  PLANO_BIBLIOGRAFIA.md    # este arquivo
  LEITURAS.md              # checklist de leitura por prioridade
  pdfs/                    # PDFs baixados (gitignored ou LFS)
  notas/                   # fichamentos por artigo (markdown)
```

A monografia em LaTeX (`template-ufla/`) importa `refbib.bib` local; manter sincronizado com `referencias_sim.bib` a cada rodada.

## Eixos temáticos e buscas sugeridas

### 1. Text-to-SQL e avaliação (núcleo)

| Prioridade | Obra | Onde buscar |
|:---:|---|---|
| P0 | Yu et al. — Spider (EMNLP 2018) | [ACL Anthology D18-1425](https://aclanthology.org/D18-1425/) |
| P0 | Zhong et al. — Test Suite Accuracy (EMNLP 2020) | ACL Anthology |
| P0 | Li et al. — BIRD (NeurIPS 2023/2024) | [arXiv:2305.03111](https://arxiv.org/abs/2305.03111) |
| P1 | Pourreza & Rafiei — DIN-SQL (NeurIPS 2023) | arXiv / NeurIPS |
| P1 | Gao et al. — DAIL-SQL (EMNLP 2023) | ACL Anthology |
| P1 | Wang et al. — RAT-SQL (ACL 2020) | ACL Anthology |
| P2 | Surveys recentes Text-to-SQL com LLM (2024–2025) | arXiv, ACM DL |

**Termos de busca:** `text-to-sql execution accuracy`, `schema linking LLM`, `in-context learning SQL`, `decomposed text-to-sql`.

### 2. RAG e orquestração

| Prioridade | Obra | Onde buscar |
|:---:|---|---|
| P0 | Lewis et al. — RAG (NeurIPS 2020) | arXiv:2005.11401 |
| P1 | Surveys RAG (2024) | arXiv |
| P2 | Tool use / agents (ReAct, function calling) | OpenAI, LangChain papers |

### 3. LLM-as-Judge

| Prioridade | Obra | Onde buscar |
|:---:|---|---|
| P0 | Zheng et al. — MT-Bench / LLM-as-Judge (2023) | arXiv:2306.05685 |
| P1 | Trabalhos sobre viés de juiz automático em QA | ACL 2024 |

### 4. Dados públicos e GovTech no Brasil

| Prioridade | Obra | Onde buscar |
|:---:|---|---|
| P0 | Lei 12.527/2011 (LAI) | Planalto |
| P0 | Portal SICOM — TCE-MG | dadosabertos.tce.mg.gov.br |
| P1 | Lei et al. — Ciência de Dados e Transparência: SICOM (SBBD 2024) | [SOL SBC](https://doi.org/10.5753/sbbd_estendido.2024.243805) |
| P1 | API Siconfi — STN | Tesouro Transparente |
| P2 | CGU — ranking de transparência | cgu.gov.br |
| P2 | Trabalhos sobre portais de transparência municipal | SciELO, Google Scholar |

**Termos:** `dados abertos municípios`, `SICOM TCE-MG`, `transparência fiscal municipal Brasil`.

### 5. Engenharia de software e reprodutibilidade

| Prioridade | Tema |
|:---:|---|
| P1 | Benchmarks reprodutíveis em NLP |
| P2 | Custos operacionais de LLM em produção |

## Protocolo de download

Para cada artigo:

1. Salvar PDF em `docs/referencias/pdfs/<autorAno>-<slug>.pdf`
2. Criar ficha em `docs/referencias/notas/<autorAno>.md` com:
   - Problema e contribuição (3 linhas)
   - Métricas usadas
   - Limitações citáveis
   - Onde encaixa no SIM (intro / referencial / metodologia / discussão)
3. Atualizar `LEITURAS.md` (status: baixado / lido / citado)
4. Propagar citações para `referencias_sim.bib` e seções LaTeX

## Script de apoio (manual por enquanto)

```powershell
mkdir docs\referencias\pdfs, docs\referencias\notas -Force
# Exemplo: baixar Spider
# Invoke-WebRequest -Uri "https://aclanthology.org/D18-1425.pdf" -OutFile "docs\referencias\pdfs\Yu2018-spider.pdf"
```

## Meta de volume

| Fase | Artigos lidos | Citações ativas no texto |
|------|:---:|:---:|
| v1 | 0 (só estruturais) | ~15 |
| v2 | 20–30 | 35–50 |

## Integração com os documentos

| Documento | Caminho | Status v1 |
|-----------|---------|-----------|
| Monografia UFLA | `template-ufla/` | Reescrita SIM + métricas v3 |
| Artigo SBC | `docs/artigo/sim_artigo_sbc_v1.tex` | Rascunho completo |
| Métricas técnicas | `docs/METRICAS.md` | Atualizado |
| Relatório empírico | `docs/RELATORIO_METRICAS_V3.md` | Atualizado |
