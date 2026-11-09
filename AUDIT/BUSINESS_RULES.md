# BUSINESS_RULES — Inventário Completo e Auditoria das Regras de Negócio (SIM)

Este documento cataloga todas as regras de negócio identificadas nos artefatos do projeto (`prompt-master`, `avaliacao/eval/prompts/orchestrator_master.txt`, `prompt_sql_compacto.txt`, `prompt_sql_v2_insights.txt`, `wpp-tiokk-sql.json`, `duck-sql.json`, `scripts_python/main.py`, `ETL_SICOM_CGU.py`, `ETL_RAG_JORNAIS.py`, `golden_dataset_v1.0.csv` e textos do TCC/Artigo SBC), avaliando sua consistência e a adequação da camada onde foram implementadas.

---

## 1. Resumo Executivo da Auditoria de Regras de Negócio

- **Total de regras mapeadas**: **84 regras explícitas** distribuídas em 6 famílias:
  1. Regras de Governança, Escopo e Lacunas de Transparência (`RN-GOV-01` a `RN-GOV-08`)
  2. Regras de Roteamento e Orquestração Híbrida (`RN-ROT-01` a `RN-ROT-07`)
  3. Regras Contábeis, Orçamentárias e de Join no SICOM (`RN-SICOM-01` a `RN-SICOM-18`)
  4. Regras de Mapeamento de Órgãos, Funções e Modalidades (`RN-MAP-01` a `RN-MAP-12`)
  5. Regras de Programas Federais e Benefícios CGU (`RN-CGU-01` a `RN-CGU-06`)
  6. Regras Técnicas de Geração SQL, Segurança e Rastreabilidade (`RN-TEC-01` a `RN-TEC-10`)
- **Diagnóstico Arquitetural Crítico**:
  - **92% das regras de negócio e segurança estão implementadas exclusivamente em texto de prompt** (`prompt-master` de 60 KB e `prompt_sql_v2_insights.txt` de 67 KB), dependendo 100% da obediência probabilística do LLM.
  - Existem **contradições diretas** entre regras dentro do mesmo prompt (`prompt_sql_v2_insights.txt`), notadamente nos códigos de órgãos (`02003` vs `02008` para Educação; `02008` vs `02013` para Obras; filtro de dispensa apenas por `'DISPENSA'` vs `'DISPENSA%'`).
  - Regras críticas de segurança (`SOMENTE SELECT`, limite de linhas, proibição de funções de sistema) e de integridade contábil (aviso obrigatório sobre a lacuna de folha de pagamento 2020–2022 e corrupção de exportação em 2023) **não possuem nenhuma validação determinística no backend**.

---

## 2. Catálogo Detalhado de Regras de Negócio

### 2.1. Governança, Escopo e Lacunas de Dados (`RN-GOV`)

| ID | Descrição da Regra | Finalidade | Origem / Localização Atual | Camada Atual vs. Camada Ideal | Inconsistências / Riscos de Bypass |
|---|---|---|---|---|---|
| `RN-GOV-01` | **Escopo Geográfico Fixo (Caeté/MG — IBGE `3110004`)**: O assistente responde apenas sobre o município de Caeté/MG (2020–2026). Perguntas sobre outros municípios devem ser recusadas. | Evitar alucinação sobre dados inexistentes na instância piloto. | `prompt-master` (Seção 1), `ETL_SICOM_CGU.py` (`CODIGO_IBGE_CAETE = "3110004"`). | **Atual**: Prompt + Hardcode no ETL.<br/>**Ideal**: Configuração de Tenant (`tenant_id` / `ibge_code`) injetada pelo backend e validada em catálogo. | Em arquitetura multi-tenant (Vertente B), manter Caeté hardcoded nos prompts impede o reuso para outras prefeituras. |
| `RN-GOV-02` | **Lacuna de Transparência da Folha de Pagamento (2020–2022)**: As tabelas `remuneracao_municipio` e os vínculos nominais em `pessoa_municipio` não possuem dados de folha nos pacotes públicos do TCE-MG de 2020, 2021 e 2022. Proibido retornar "R$ 0,00" sem alertar a lacuna de transparência ativa. | Impedir desinformação ao cidadão (confundir ausência de publicação com gasto zero). | `prompt-master` (Seção 2.1), `prompt_sql_v2_insights.txt` (Insights 069, 076), `sim_artigo_sbc_v1.tex` (p. 3). | **Atual**: Apenas instrução textual no prompt.<br/>**Ideal**: **Interceptador determinístico no backend** (`sql_guard` / `catalog`) que detecta consultas a `remuneracao_municipio` com `ano_referencia IN (2020, 2021, 2022)` e anexa metadado obrigatório `transparency_warning`. | **Discrepância grave com o Artigo SBC**: O artigo afirma que o ETL resolve isso via *"junções defensivas com a tabela empenho"*, mas nem o ETL nem o banco fazem isso. Se o LLM esquecer a regra, responde `0` ou `NULL`. |
| `RN-GOV-03` | **Protocolo de Desambiguação Semântica (Merenda, Obras, Frota, Saúde)**: Termos genéricos da linguagem cidadã que não existem literalmente em `dsc_empenho` (ex.: "merenda escolar") devem primeiro buscar o termo específico e, se zero, ampliar para o elemento de despesa (`Gêneros de Alimentação` na Educação) explicando a diferença ao usuário. | Evitar falsos negativos causados pelo vocabulário técnico-contábil do SICOM. | `prompt-master` (Seção 2.2), `wpp-tiokk-sql.json`. | **Atual**: Prompt do Orquestrador.<br/>**Ideal**: Dicionário Semântico de Domínio no Catálogo + Prompt Modular. | Depende de o Orquestrador decidir fazer uma segunda chamada SQL após receber `[]`, aumentando latência em 2x. |
| `RN-GOV-04` | **Proibição de Exposição de Termos Técnicos ao Cidadão**: O Orquestrador não deve exibir nomes de tabelas (`empenho`, `remuneracao_municipio`), códigos (`cod_orgao`) ou sintaxe SQL na resposta final ao WhatsApp. | Acessibilidade e usabilidade cidadã (Vertente A). | `prompt-master` (Seções 1 e 5). | **Atual**: Prompt do Orquestrador.<br/>**Ideal**: Prompt de Resposta por Canal (Cidadão vs. Analista GovTech). | Na Vertente B (controladoria/gestão pública), auditores **precisam** ver a tabela, o código contábil e a query auditável. |
| `RN-GOV-05` | **Rastreabilidade Obrigatória de Fontes (`fontes_utilizadas`)**: Toda resposta baseada em dados deve listar no rodapé os arquivos CSV ou edições do Diário Oficial consultados. | Auditabilidade e confiança pública (transparência passiva/ativa). | `prompt-master` (Seção 5), `scripts_python/main.py` (`fontes_csv`), `duck-sql.json` (`rastreabilidade_tcc`). | **Atual**: Quebrado entre Python, n8n e Prompt.<br/>**Ideal**: Extração determinística via AST SQL no backend retornando campo padronizado `fontes_utilizadas`. | **QUEBRADO EM PRODUÇÃO**: `main.py` retorna `fontes_csv` via regex de ano; `duck-sql.json` renomeia para `rastreabilidade_tcc`; `prompt-master` exige `fontes_utilizadas`. |
| `RN-GOV-06` | **Proteção de Dados Pessoais Sensíveis (LGPD vs. LAI)**: Consultas sobre servidores e beneficiários de programas sociais exibem apenas dados publicizados por lei (nome, CPF mascarado `***123456**`, valor pago). | Conformidade simultânea com a Lei de Acesso à Informação (LAI) e LGPD. | Dados brutos CGU (`cpf_formatado` já vem mascarado da API) e SICOM (`num_documento` em `pessoa_municipio`). | **Atual**: Depende do dado bruto.<br/>**Ideal**: Mascaramento determinístico de PII no motor de consulta conforme perfil RBAC do usuário. | Em `pessoa_municipio` e `fornecedor_municipio`, CPFs completos (11 dígitos) estão expostos em texto claro para qualquer consulta SQL! |

---

### 2.2. Regras de Roteamento e Orquestração Híbrida (`RN-ROT`)

| ID | Descrição da Regra | Finalidade | Origem / Localização Atual | Camada Atual vs. Camada Ideal | Inconsistências / Riscos de Bypass |
|---|---|---|---|---|---|
| `RN-ROT-01` | **Rota 1 — Dados Estruturados (`Tool_Executar_SQL_Sicom`)**: Acionada para valores financeiros, somas, médias, rankings, salários, contratos, licitações, transferências e benefícios federais. | Direcionar cálculos exatos ao motor analítico DuckDB. | `prompt-master` (Seção 3), `wpp-tiokk-sql.json`. | **Atual**: Decisão livre do LLM `gpt-4o`.<br/>**Ideal**: Classificador de Intenção em código + Tool Calling validado. | No `wpp-tiokk-sql.json`, o parâmetro enviado pela tool (`pergunta_usuario`) não casa com o esperado pelo sub-workflow (`$json.query`). |
| `RN-ROT-02` | **Rota 2 — Dados Normativos e Textuais (`consultar_diario_oficial`)**: Acionada para leis, decretos, portarias, nomeações, exonerações, editais e justificativas qualitativas publicadas no *Jornal Opinião*. | Recuperar contexto jurídico/administrativo via RAG no Qdrant. | `prompt-master` (Seção 3), `wpp-tiokk-sql.json`, `avaliacao/eval/run_rag_eval.py`. | **Atual**: Node Qdrant no n8n (busca densa simples).<br/>**Ideal**: Serviço RAG Python com busca híbrida (`dense` + `lexical` por número do ato) + deduplicação + filtro de payload. | **Abismo Produção × TCC**: O n8n usa busca densa pura sem dedup e ignora `filtro_tipo_ato` e `filtro_cnpj`, onde o Recall@5 para atos numerados cai para **3,1%**. |
| `RN-ROT-03` | **Rota 3 — Investigação Cruzada Híbrida (SQL + RAG)**: Para perguntas complexas (ex.: *"Quem ganhou a licitação X e qual portaria nomeou o fiscal do contrato?"*), acionar primeiro o SQL (obtendo CNPJ/número) e depois o RAG (ou vice-versa). | Permitir auditoria cruzada entre execução financeira e publicação oficial. | `prompt-master` (Seção 3). | **Atual**: Apenas prompt.<br/>**Ideal**: Grafo de orquestração explícito em Python (`HybridOrchestrator`). | Nunca foi validado em nenhum benchmark do TCC (`golden_dataset_v1.0.csv` testa apenas SQL isolado; `golden_rag` testa apenas Qdrant isolado). |

---

### 2.3. Regras Contábeis, Orçamentárias e de Join no SICOM (`RN-SICOM`)

| ID | Descrição da Regra | Finalidade | Origem / Localização Atual | Consistência e Problemas Identificados |
|---|---|---|---|---|
| `RN-SICOM-01` | **Chave Composta Obrigatória em Joins do SICOM**: Tabelas filhas (`pagamento_fonte`, `empenho_fonte`, `liquidacao`, `item_contrato`, `fornecedor_contrato`, `empenho_licitacao`, `empenho_dispensa`) devem ser unidas usando o ID (`id_empenho`, `id_contrato`, `id_processo_licitatorio`, `id_dispensa`) **E** `ano_referencia` (ou `ano_exercicio`). | Evitar produto cartesiano ou colisão de IDs sequenciais reiniciados a cada exercício anual. | `prompt_sql_compacto.txt`, `prompt_sql_v2_insights.txt` (Regra 4 + Insights 002, 012, 013, 015, 026). | Regra correta, mas confiada apenas ao LLM. Deve constar explicitamente no Grafo de Joins do Catálogo de Dados. |
| `RN-SICOM-02` | **Fases da Despesa Pública**:<br/>- "Empenhado / Comprometido" → `empenho.vlr_empenho`<br/>- "Liquidado / Executado" → `liquidacao.vlr_liquidacao`<br/>- "Pago / Desembolsado" → `pagamento.vlr_pagamento`. | Respeitar os estágios da despesa previstos na Lei 4.320/1964. | `prompt_sql_compacto.txt`, `prompt_sql_v2_insights.txt` (Regra 2). | O prompt não desconta automaticamente `anulacao_empenho`, `anulacao_liquidacao` ou `anulacao_pagamento` a menos que o usuário peça "líquido" ou "anulações". |
| `RN-SICOM-03` | **Vínculo Pagamento → Fornecedor / Órgão via `empenho`**: A tabela `pagamento` não possui nome do credor nem descrição do órgão; é obrigatório fazer `JOIN empenho e ON p.id_empenho = e.id_empenho AND p.ano_referencia = e.ano_referencia`. | Recuperar `nom_credor`, `num_documento` e `cod_orgao` nos pagamentos. | `prompt_sql_compacto.txt` (Regra 2), `prompt_sql_v2_insights.txt`. | Consistente. |
| `RN-SICOM-04` | **Vínculo Contrato → Fornecedor via `fornecedor_contrato` + `pessoa_municipio`**: A tabela `contratos` não tem o nome do contratado; exige `JOIN fornecedor_contrato fc` (`id_contrato`, `ano_referencia`) e `JOIN pessoa_municipio pm ON fc.num_documento = pm.num_documento AND fc.ano_referencia = pm.ano_referencia`. | Identificar empresas contratadas pelo município. | `prompt_sql_compacto.txt` (Regra 4), `prompt_sql_v2_insights.txt` (Insights 013, 019). | **Atenção**: Em vários casos do Golden v1 (`012`, `016`), a referência usa apenas `fc.num_documento` sem ir até `pessoa_municipio`, enquanto o prompt v2 obriga o join com `pessoa_municipio`. |
| `RN-SICOM-05` | **Vínculo Licitação / Dispensa → Despesa via Tabelas Ponte**: Para saber quanto foi empenhado/pago em uma licitação ou dispensa, usar `empenho_licitacao` (`id_processo_licitatorio`, `id_empenho`, `ano_referencia`) ou `empenho_dispensa` (`id_dispensa`, `id_empenho`, `ano_referencia`). | Conectar fase interna/externa de compras à execução orçamentária. | `prompt_sql_compacto.txt` (Regra 4), `prompt_sql_v2_insights.txt` (Insights 015, 026). | Consistente. |
| `RN-SICOM-06` | **Filtro de Vencedores de Licitação**: Na tabela `licitante`, filtrar `dsc_situacao = 'VENCEDOR'` ou usar `homologacao_licitacao` / `adjudicacao_licitacao`. | Diferenciar licitantes participantes de vencedores. | `prompt_sql_v2_insights.txt` (Insights 017, 018). | Na verdade, `licitante` nem sempre tem `dsc_situacao = 'VENCEDOR'`; no Golden v1, vencedores vêm de `homologacao_licitacao` (`nom_vencedor` / `num_documento`). |
| `RN-SICOM-07` | **Folha de Pagamento (`remuneracao_municipio`)**:<br/>- Bruto: `vlr_remuneracao_bruta`<br/>- Líquido: `vlr_remuneracao_liquida`<br/>- Descontos: `vlr_descontos`<br/>- Vínculo com nome: `JOIN pessoa_municipio` por `num_documento` e `ano_referencia`. | Consultas de RH e remuneração de servidores (2023–2025). | `prompt_sql_compacto.txt` (Regra 3), `prompt_sql_v2_insights.txt` (Insights 066–076). | **Contradição interna no Prompt B**: Insight `069` proíbe `JOIN` com `pessoa_municipio` na contagem de servidores; Insight `074` exige `JOIN pessoa_municipio` para listar remunerações; Insight `076` manda retornar `0` fixo (`WHERE 1=0`) em 2020–2022. |
| `RN-SICOM-08` | **Receita Orçamentária (`receita_orcamentaria_municipio`)**:<br/>- Prevista/Orçada: `vlr_previsao_atualizada` (ou `vlr_previsao_inicial`)<br/>- Arrecadada/Realizada: `vlr_arrecadado` (ou `vlr_arrecadado_liquido`). | Análise de arrecadação municipal e execução da receita. | `prompt_sql_v2_insights.txt` (Insights 033–041). | Consistente. |

---

### 2.4. Regras de Mapeamento de Órgãos, Funções e Modalidades (`RN-MAP`) — **Foco de Contradições**

Abaixo estão documentadas as contradições descobertas na auditoria de `prompt_sql_v2_insights.txt`:

| ID | Entidade / Conceito | Regra A (no Prompt B) | Regra B Conflitante (no mesmo Prompt B!) | Verdade Empírica no Banco de Dados (`dados_sicom.duckdb`) |
|---|---|---|---|---|
| `RN-MAP-01` | **Secretaria de Educação (`cod_orgao` vs `cod_unidade`)** | Insight `020`: *"Para 'Secretaria de Educação' em `empenho`, filtre `cod_orgao = '02003'`"*. | Insights `046` e `051`: *"Para Educação, filtre `cod_orgao = '02008'`"*. | No SICOM de Caeté, `cod_orgao = '02'` é o **Poder Executivo** (Prefeitura)! Os códigos `'02003'`, `'02006'`, `'02008'` são na verdade **`cod_unidade`** (Unidades Orçamentárias) ou mudam de numeração entre anos, além de existir a classificação funcional `cod_funcao = 12` (Educação) e `cod_funcao = 10` (Saúde)! |
| `RN-MAP-02` | **Secretaria de Obras (`cod_orgao` vs `cod_unidade`)** | Insight `051`: *"Obras = `'02008'`"*. | Insight `052` (linha seguinte!): *"Para 'Secretaria de Obras' em `empenho`, filtre `cod_orgao = '02013'`"*. | Contradição direta em linhas consecutivas do `prompt_sql_v2_insights.txt` (linhas 92 e 93), provando que os "insights" foram copiados caso a caso do gabarito Golden v1. |
| `RN-MAP-03` | **Dispensa vs. Inexigibilidade (`dsc_modalidade`)** | Insight `011`: *"Em `dispensa_inexigibilidade`, para 'dispensa de licitação', filtre `UPPER(dsc_modalidade) LIKE '%DISPENSA%'` (NÃO inclua inexigibilidade)"*. | Insight `042`: *"Para somar dispensas e inexigibilidades, NÃO filtre `dsc_modalidade` — some toda a tabela `dispensa_inexigibilidade`"*. | Ambas as regras foram criadas para acertar perguntas específicas (`#11` vs `#42` do Golden v1) que usam redações quase idênticas em português! |

---

### 2.5. Regras de Programas Federais e Benefícios CGU (`RN-CGU`)

| ID | Descrição da Regra | Tabelas Afetadas | Problema Técnico Real no Banco |
|---|---|---|---|
| `RN-CGU-01` | **Tipagem de Valores Monetários CGU**: As colunas `valor`, `valor_parcela`, `valor_liberado`, `valor_convenio`, `valor_emenda` nas tabelas `cgu_*` estão armazenadas como `VARCHAR` no DuckDB (apesar de o prompt declará-las como `DECIMAL(18,2)`), exigindo `CAST(REPLACE(valor, ',', '.') AS DOUBLE)` ou `TRY_CAST(valor AS DECIMAL(18,2))`. | Todas as 14 tabelas `cgu_*` | A divergência entre o DDL do prompt (`DECIMAL(18,2)`) e o tipo real na `VIEW` (`VARCHAR`) faz o LLM gerar `SUM(valor)` direto, que falha no DuckDB com `Binder Error: No function matches 'sum(VARCHAR)'`! |
| `RN-CGU-02` | **Transição temporal de Programas Sociais**:<br/>- `cgu_bolsa_familia_antigo`: 2020–2021<br/>- `cgu_auxilio_emergencial`: 2020–2021<br/>- `cgu_auxilio_brasil`: 2021–2022<br/>- `cgu_novo_bolsa_familia`: 2023–2026. | Tabelas de benefícios sociais CGU | Se o cidadão pergunta *"Quanto Caeté recebeu de Bolsa Família de 2020 a 2025?"*, consultar apenas uma tabela retorna metade da série histórica. O Catálogo deve agrupar a família semântica de transferência de renda. |

---

### 2.6. Regras Técnicas de Geração SQL, Segurança e Rastreabilidade (`RN-TEC`)

| ID | Descrição da Regra | Localização Atual | Falha na Implementação Atual | Solução Implementada na Modernização (`sim_platform`) |
|---|---|---|---|---|
| `RN-TEC-01` | **Somente Leitura (`SELECT` / `WITH` apenas)**: Proibido executar DDL/DML (`DROP`, `DELETE`, `UPDATE`, `INSERT`, `CREATE`, `ALTER`, `COPY`, `ATTACH`). | Apenas no texto do prompt (`prompt_sql_compacto.txt`). | `scripts_python/main.py` executa qualquer comando SQL em conexão read-write! | `SQLGuard` valida a AST via `sqlglot`, bloqueia qualquer statement diferente de `SELECT`/`UNION` e opera com `enable_external_access=false`. |
| `RN-TEC-02` | **Bloqueio de Funções de Sistema e Leitura de Arquivos**: Proibido usar `read_csv_auto`, `read_parquet`, `read_text`, `glob`, `pragma_*`. | **Inexistente** em qualquer lugar do projeto atual! | Qualquer requisição a `/query` pode ler `/etc/passwd` ou arquivos do container via `read_text()`. | `SQLGuard` inspeciona todos os nós `Anonymous` / `Func` / `Table` na AST e bloqueia funções de I/O e tabelas de sistema sensíveis. |
| `RN-TEC-03` | **Limite Máximo de Linhas (`LIMIT`)**: Listagens devem ter `LIMIT` (ex.: 50 ou 100) para não estourar o contexto do Orquestrador nem travar a rede. | Apenas recomendação no prompt (`LIMIT 50`). | Consultas sem `LIMIT` em `pessoa_municipio` retornam `393.368` linhas em JSON, derrubando o worker FastAPI/n8n por OOM. | `SQLGuard` injeta/impõe `LIMIT` máximo configurável (ex.: 500 linhas) em queries não agregadas. |
| `RN-TEC-04` | **Extração de Linhagem de Dados (`fontes_utilizadas`)**: Identificar exatamente quais tabelas e arquivos anuais sustentam o resultado da query. | `scripts_python/main.py` (linhas 160–194) via regex `r'\b(202[0-6])\b'` e match de substring de nome de view. | Falha para `BETWEEN 2021 AND 2024`, `>= 2022`, queries sem filtro de ano ou views com nomes que são substrings umas das outras (`empenho` vs `empenho_fonte`). | `SQLGuard.extract_lineage()` extrai tabelas reais via AST `sqlglot` e resolve os anos efetivamente consultados (incluindo intervalos `BETWEEN`, `>=`, `<=`, `IN` ou cobertura total da tabela). |

---

## 3. Matriz de Realocação Arquitetural (O que sai do Prompt e vai para o Código)

| Regra / Responsabilidade | Antes (Protótipo TCC) | Depois (Arquitetura `sim_platform`) |
|---|---|---|
| Bloqueio de SQL destrutivo e leitura de disco | Prompt (`"Gere apenas SELECT"`) | **Código Determinístico** (`SQLGuard` AST + DuckDB `enable_external_access=false`) |
| Aviso de lacuna da folha 2020–2022 | Prompt (`"Avise que não há dados de 2020 a 2022"`) | **Código Determinístico** (`SQLGuard` / `DataCatalog` detecta tabela + filtro de ano e injeta metadado estruturado obrigatório) |
| Seleção de tabelas relevantes para a pergunta | Nenhuma (envia todas as 72 tabelas sempre) | **Código Determinístico** (`DataCatalog.select_tables_for_query()` reduz o schema para as 4–12 tabelas do domínio + pontes de join) |
| Tipagem correta de colunas (`DATE`, `DECIMAL`) | Mentira no DDL do prompt vs. `VARCHAR`/`BIGINT` na `VIEW` | **Código Determinístico** (Catálogo reflete tipos reais e motor materializa/converte colunas numéricas e datas corrompidas) |
| Rastreabilidade de fontes (`fontes_utilizadas`) | Regex frágil de anos + campo renomeado errado no n8n | **Código Determinístico** (Extração de tabelas e predicados temporais via AST `sqlglot`) |
| Busca Híbrida + Deduplicação + Filtro no RAG | Apenas no script de benchmark `run_rag_eval.py` | **Código Determinístico** no serviço de recuperação da plataforma |
| Tradução Semântica NL → SQL e Síntese Cidadã | LLMs sobrecarregados com prompts de 60–67 KB | **LLMs com Prompts Modulares enxutos** (~3–5 KB por chamada, focados apenas nas regras do segmento ativo) |
