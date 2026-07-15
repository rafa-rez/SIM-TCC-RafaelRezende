# SIM — Sistema de Informações Municipais

## Definição

O **SIM** (*Sistema de Informações Municipais*) é um assistente conversacional que permite ao cidadão consultar dados públicos de uma prefeitura em linguagem natural. O sistema combina:

- **Dados estruturados** — finanças, empenhos, contratos, licitações, folha de pagamento, frota etc., originados do SICOM e complementos da CGU;
- **Dados textuais** — atos normativos e publicações do Diário Oficial (quando disponíveis na instância);
- **Orquestração por LLM** — roteamento entre consulta SQL (Text-to-SQL) e busca semântica (RAG), com resposta em linguagem acessível.

O SIM é **multi-instância**: cada município pode ter sua própria base de dados, esquema e persona do assistente. A arquitetura é replicável; o que muda por instância são os CSVs, o código IBGE e eventuais lacunas de transparência local.

## Instância de referência: Caeté (MG)

Neste repositório, toda a avaliação usa a **instância Caeté**:

| Atributo | Valor |
|----------|-------|
| Município | Caeté, Minas Gerais |
| Código IBGE | 3110004 |
| Período dos dados | 2020–2025 (consolidados até 2025) |
| Fontes | SICOM (Tesouro Nacional), staging CGU, Diário Oficial local |

### Lacunas conhecidas (Caeté)

A prefeitura não publicou dados de folha de pagamento e cadastro de servidores para **2020, 2021 e 2022**. O assistente deve informar esse “apagão de transparência” quando o usuário solicitar salários ou vínculos nesses anos.

## Escopo deste repositório

Este repositório contém **apenas a avaliação Text-to-SQL** do SIM — golden dataset, experimentos, métricas e scripts de replicação. Componentes de produção (n8n, WhatsApp, API Docker, RAG) ficam fora do escopo do versionamento aqui.

```
docs/           → documentação conceitual e metodológica
avaliacao/      → dados, eval, experimentos e scripts reprodutíveis
```

## Nomenclatura

| Termo antigo | Termo atual |
|--------------|-------------|
| CIC (Centro de Informações Caetense) | **SIM** — persona da instância Caeté |
| Agente / protótipo | **SIM Caeté** (instância municipal) |

Daqui em diante, toda documentação e prompts de avaliação referem-se ao **SIM**, não ao CIC.

## Referências no TCC

- Trabalho: *Sistema de Informações Municipais* — validação de consultas em linguagem natural sobre dados abertos municipais.
- Métrica principal adotada: **EX resposta** (ver [`METRICAS.md`](METRICAS.md)).
- Conjunto de teste: 80 consultas em [`avaliacao/dados/golden/`](../avaliacao/dados/golden/).
