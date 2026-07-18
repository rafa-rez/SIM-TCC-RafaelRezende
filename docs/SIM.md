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

### Lacunas conhecidas (Caeté)

A prefeitura não publicou dados de folha de pagamento e cadastro de servidores para **2020, 2021 e 2022**. O assistente deve informar essa lacuna quando o usuário solicitar salários ou vínculos nesses anos.

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

1. O usuário envia a pergunta via mensageria instantânea.
2. A plataforma de automação aciona o agente orquestrador.
3. O agente seleciona recuperação documental (atos normativos) ou tradução Text-to-SQL (dados tabulares).
4. O subsistema SQL consulta um motor analítico (DuckDB) sobre bases consolidadas.
5. O orquestrador interpreta o resultado e responde em linguagem natural.

## Escopo deste repositório

Este repositório contém os artefatos de **validação Text-to-SQL** do SIM:

- conjunto golden de 80 consultas com SQL de referência;
- quatro experimentos reprodutíveis (E1–E4);
- scripts de recálculo offline e documentação de métricas.

Componentes de produção (n8n, WhatsApp, API Docker, índice Qdrant do Diário Oficial) são descritos nos textos do TCC, mas não estão versionados aqui.

```
docs/           → documentação conceitual e metodológica
avaliacao/      → dados, eval, experimentos e scripts
```

## Validação

A avaliação quantitativa comprova a viabilidade funcional do subsistema Text-to-SQL: geração de SQL executável e correspondência dos resultados ao gabarito curado. O protocolo, as definições de métricas e os resultados dos experimentos estão em:

- [`METRICAS.md`](METRICAS.md)
- [`RELATORIO_METRICAS_V3.md`](RELATORIO_METRICAS_V3.md)
- [`REPLICACAO.md`](REPLICACAO.md)
- [`../avaliacao/dados/golden/`](../avaliacao/dados/golden/)

A avaliação quantitativa reportada no TCC v1.0 concentra-se no Text-to-SQL. O subsistema RAG (Diário Oficial) opera em produção; protocolo e pipeline estão em [`PLANO_RAG.md`](PLANO_RAG.md) para trabalho futuro.

## Nomenclatura

| Termo anterior | Termo atual |
|----------------|-------------|
| CIC (Centro de Informações Caetense) | **SIM** — persona da instância Caeté |
| Agente / protótipo | **SIM Caeté** (instância municipal) |
