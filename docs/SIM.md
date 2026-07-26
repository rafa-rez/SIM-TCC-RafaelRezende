# SIM — Sistema de Informações Municipais

## Definição

O **SIM** (*Sistema de Informações Municipais*) é uma plataforma conversacional que permite ao cidadão consultar dados públicos de uma prefeitura em linguagem natural. O sistema integra:

- **Dados estruturados** — receitas, despesas, empenhos, folha de pagamento, frota, licitações e contratos, originados do SICOM e complementos da CGU;
- **Dados textuais** — atos normativos e publicações do Diário Oficial municipal, indexados para recuperação semântica;
- **Orquestração cognitiva** — roteamento automático entre consulta analítica (Text-to-SQL) e recuperação documental (RAG), com resposta em linguagem acessível ao cidadão.

O SIM é **multi-instância**: cada município pode operar sua própria base de dados, esquema relacional e persona do assistente. A arquitetura é replicável; variam o código IBGE, os CSVs locais e eventuais lacunas de transparência.

## Instância de referência: Caeté (MG)

Neste repositório, a validação documentada refere-se à **instância Caeté**:

| Atributo | Valor |
|----------|-------|
| Município | Caeté, Minas Gerais |
| Código IBGE / SICOM | 3110004 |
| Período dos dados | 2020–2025 |
| Fontes | SICOM (TCE-MG), staging CGU, Diário Oficial local |
| Canal operacional | WhatsApp (via automação n8n) |

### Lacunas documentadas (Caeté)

A prefeitura não publicou dados de folha de pagamento e cadastro de servidores para **2020, 2021 e 2022**. Essa lacuna constitui limitação documentada da base local, incorporada ao escopo de resposta do sistema.

## Arquitetura (visão geral)

```
Usuário (WhatsApp)
       │
       ▼
  Automação (n8n)
       │
       ▼
 Agente orquestrador (LLM)
    ┌──┴──┐
    ▼     ▼
  RAG    Text-to-SQL
(Diário)  (DuckDB)
    └──┬──┘
       ▼
 Resposta em linguagem natural
```

## Escopo deste repositório

| Componente | Versionado |
|------------|------------|
| Golden SQL (80 consultas) + experimentos E1–E4 | Sim |
| Golden RAG v1.1 (8 perguntas) + runner | Sim |
| Pipeline de métricas (`run_metrics_pipeline.ps1`) | Sim |
| Stack Docker (n8n, Qdrant, DuckDB API) | Sim |
| Dados brutos SICOM/CGU | Sim (`avaliacao/dados/`) |
| Monografia e artigo (LaTeX + PDF) | Sim (`docs/`) |

## Validação empírica (três lotes)

### Lote I — Text-to-SQL: modelo e contexto (E1–E4)

Quatro configurações comparadas sobre golden v1 (80 consultas). Referência: **VSR 97,5%**, **EF 62,5%** (E1).

### Lote II — Text-to-SQL: engenharia de prompt

Configuração final v2.1 sobre o mesmo golden: **VSR 100%**, **EF 72,5%**.

### Lote III — RAG: estratégias de recuperação

Golden v1.1 (8 perguntas, *k*=5). Configuração final (híbrido + dedup + filtro tipo): **Recall@5 100%**, **MRR 87,5%**.

Documentação:

- [`METRICAS.md`](METRICAS.md) — definições formais, fórmulas VSR/EF
- [`PLANO_RAG.md`](PLANO_RAG.md) — protocolo e resultados RAG
- [`REPLICACAO.md`](REPLICACAO.md) — replicação offline
- [`../avaliacao/`](../avaliacao/) — dados, experimentos e scripts

## Nomenclatura

| Termo anterior | Termo atual |
|----------------|-------------|
| CIC (Centro de Informações Caetense) | **SIM** — persona da instância Caeté |
| EX resposta (repositório) | **EF** — equivalência funcional (textos do TCC) |
