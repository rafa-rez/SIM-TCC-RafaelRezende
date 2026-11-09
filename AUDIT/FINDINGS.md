# FINDINGS — Catálogo Classificado e Priorizado de Achados da Auditoria (SIM)

Este documento consolida todos os achados técnicos, funcionais, de dados, de segurança, de IA/prompts e de arquitetura identificados durante a investigação profunda do projeto SIM.

## Legenda de Classificação e Prioridade
- **Natureza**: `Problema Confirmado` | `Problema Provável` | `Risco Potencial` | `Oportunidade de Melhoria` | `Limitação Não Validada`
- **Severidade**:
  - `P0 (Crítico)`: Compromete segurança, integridade financeira/contábil ou impede funcionamento ponta a ponta.
  - `P1 (Alto)`: Degrada seriamente acurácia, confiabilidade sob concorrência ou rastreabilidade.
  - `P2 (Médio)`: Afeta manutenibilidade, custo operacional, latência ou escalabilidade multi-tenant.
  - `P3 (Baixo)` / `P4 (Informativo)`: Dívida técnica cosmética, documentação ou melhoria incremental.

---

## 1. Achados Críticos (`P0`)

### `FIND-001` — Execução Arbitrária de SQL e Acesso ao Sistema de Arquivos em `/query`
- **Severidade**: `P0` | **Categoria**: Segurança / Banco de Dados | **Natureza**: `Problema Confirmado` | **Confiança**: 100%
- **Arquivos e linhas afetadas**:
  - `scripts_python/main.py` (linhas 60, 148–157)
  - `experimental/backend-v2/main.py` (linhas 121–133, 250–254)
- **Evidência**:
  - A API `duckdb_api` abre `duckdb.connect(DB_FILE, read_only=False)` com `enable_external_access=true` (padrão no `scripts_python/main.py` e quando `DUCKDB_EXTERNAL_ACCESS` não é `"0"` no `backend-v2`) e executa `conn.execute(request.query)` diretamente, sem qualquer validação sintática (AST), sem bloqueio de DDL/DML e sem autenticação no endpoint `POST /query`.
- **Cenário de reprodução**:
  - `POST /query` com `{"query": "SELECT * FROM read_csv_auto('/etc/passwd', delim=':', header=false)"}` lê arquivos do sistema operacional.
  - `POST /query` com `{"query": "DROP VIEW empenho"}` ou `{"query": "COPY (SELECT 'pwned') TO '/app/pwn.txt'"}` destrói objetos do catálogo ou grava no disco se houver permissão de escrita.
- **Impacto técnico e de negócio**: Comprometimento total do container analítico, exfiltração de arquivos locais, negação de serviço (DoS) e adulteração das respostas entregues aos cidadãos ou gestores públicos.
- **Recomendação**: Implementar validador AST determinístico (`SQLGuard` via `sqlglot`) permitindo exclusivamente `SELECT`/`WITH`, bloqueando funções de leitura de arquivo (`read_csv*`, `read_parquet*`, `read_text*`, `glob*`), comandos `COPY`/`ATTACH`/`INSTALL`/`LOAD`/`PRAGMA`/`SET`, impondo `enable_external_access=false`, timeout por consulta e autenticação via API Key/RBAC.
- **Status**: **Corrigido na arquitetura `sim_platform`** (`sim_platform/security/sql_guard.py` + `sim_platform/engine/duckdb_manager.py`).

---

### `FIND-002` — Corrupção Silenciosa de 4.723 Linhas nos Dados de 2023 (Subavaliação de R$ 11,75 Milhões em Pagamentos)
- **Severidade**: `P0` | **Categoria**: Engenharia de Dados / Integridade Contábil | **Natureza**: `Problema Confirmado` | **Confiança**: 100%
- **Arquivos e linhas afetadas**:
  - `AUDIT/historical_artifacts/ETL_SICOM_CGU.py` (linhas 216–240)
  - `scripts_python/main.py` (linhas 92–97: `read_csv_auto(..., delim='|', ignore_errors=true)`)
  - `avaliacao/dados/database/*_2023.csv` (8 tabelas: `pagamento`, `pagamento_fonte`, `liquidacao_fonte`, `empenho_fonte`, `anulacao_empenho_fonte`, `anulacao_liquidacao_fonte`, `anulacao_pagamento_fonte`, `restos_pagar`)
- **Evidência**:
  - O ETL converte CSVs do SICOM de `;` para `|` sem `quoting`/`escapechar`. Em 2023, as descrições de fonte de recurso da Emenda Constitucional nº 123/2022 contêm dois pipes literais (`"... ART. 5º | INCISO IV | EC Nº 123/2022"`). Ao carregar os CSVs com `delim='|'` e `ignore_errors=true`, o DuckDB descarta silenciosamente ou desloca colunas em **4.723 registros**.
- **Cenário de reprodução**:
  - Comparar `SUM(vlr_pagamento)` em `pagamento_2023.csv` via `read_csv_auto(..., ignore_errors=true)` (**R$ 134.084.996,81**, `33.249` linhas) contra o arquivo original `SICOM/2023/3110004_2023.zip` (**R$ 145.839.461,60**, `34.623` linhas) — perda de **R$ 11.754.464,79 (-8,06%)** apenas em `pagamento`!
- **Impacto técnico e de negócio**: Qualquer consulta cidadã ou de controladoria sobre execução orçamentária, gastos por secretaria ou indicadores fiscais (ISF-M, IES-C, IEQ-C) em 2023 retorna valores materialmente incorretos, gerando risco jurídico e perda de credibilidade institucional.
- **Recomendação**: Reprocessar/reparar os CSVs de 2023 substituindo pipes internos aos campos por `-` ou exportando diretamente para Parquet/DuckDB tipado, e proibir `ignore_errors=true` silencioso sem auditoria de rejeições.
- **Status**: **Corrigido e verificado em `sim_platform`** (integração do reparo determinístico de 2023 e validação contábil).

---

### `FIND-003` — Quebra Total do Fluxo WhatsApp → n8n → Sub-Workflow SQL → DuckDB em Produção
- **Severidade**: `P0` | **Categoria**: Integração / Orquestração | **Natureza**: `Problema Confirmado` | **Confiança**: 100%
- **Arquivos e linhas afetadas**:
  - `docker-compose.yml` (linha 58: `WHATSAPP_HOOK_URL` vs `database` volume mount na linha 78)
  - `AUDIT/historical_artifacts/wpp-tiokk-sql.json` (linhas 164–179, 250–310, 412–435)
  - `AUDIT/historical_artifacts/duck-sql.json` (linhas 18–45, 95–130)
- **Evidência**: Quatro falhas encadeadas impedem ou corrompem a execução em produção:
  1. **Volume `./database` inexistente em `HEAD`**: O commit `55a2880` moveu `./database` para `avaliacao/dados/database`, mas `docker-compose.yml` continua montando `./database:/data/export:ro`.
  2. **Divergência de URL de Webhook**: `docker-compose.yml` configura o WAHA para enviar eventos a `http://n8n:5678/webhook/whatsapp-entrada`, enquanto o nó `Webhook` em `wpp-tiokk-sql.json` escuta em `/webhook/webhook`.
  3. **Debouncer Desativado e Lock Deletado Antes do Agente**: Em `wpp-tiokk-sql.json`, o nó `Wait1` (10s) está com `"disabled": true` e o nó `limpar estado de lock` (`DEL lock:<chat>`) é executado **antes** de `Start Typing` e `AI Agent`. Mensagens enviadas em sequência disparam execuções paralelas do `gpt-4o` sobre a mesma sessão Redis.
  4. **Incompatibilidade de Parâmetro no Sub-Workflow SQL**: `Tool_Executar_SQL_Sicom` instrui o Orquestrador a enviar `{"pergunta_usuario": "..."}`, mas `duck-sql.json` lê `={{ $json.query }}` no ` Prompt_ Padrao_TCC`.
- **Impacto técnico e de negócio**: O ambiente Docker descrito no repositório não funciona ponta a ponta sem intervenção manual, e quando acionado sofre condições de corrida e perda do parâmetro da pergunta SQL.
- **Recomendação**: Substituir a orquestração frágil em JSON do n8n por um orquestrador controlado em código Python (`sim_platform/orchestrator/`) com contratos tipados Pydantic, lock atômico por sessão e corrigir o mapeamento de volumes.
- **Status**: **Corrigido na arquitetura `sim_platform`**.

---

### `FIND-004` — Abismo entre o RAG Avaliado no TCC/Artigo (Recall@5 100%) e o RAG Real em Produção (Recall@5 3,1% para Atos Numerados)
- **Severidade**: `P0` | **Categoria**: IA / RAG / Qualidade de Recuperação | **Natureza**: `Problema Confirmado` | **Confiança**: 100%
- **Arquivos e linhas afetadas**:
  - `AUDIT/historical_artifacts/wpp-tiokk-sql.json` (nó `consultar_diario_oficial`, linhas 365–405)
  - `AUDIT/historical_artifacts/ETL_RAG_JORNAIS.py` (linha 264: `id=str(uuid.uuid4())`)
  - `avaliacao/eval/run_rag_eval.py` (linhas 140–235)
- **Evidência**:
  - O TCC e o artigo SBC reportam `Recall@5 = 100%` e `MRR = 97,5%` ($N=40$) usando busca híbrida denso+lexical por número de ato (`--hybrid-act`), deduplicação de conteúdo (`--dedup`) e filtro por tipo de ato (`--filtro-tipo-ato`).
  - Contudo, essas três técnicas **existem apenas no script de avaliação `run_rag_eval.py`**.
  - Na produção (`wpp-tiokk-sql.json`), o nó `@n8n/n8n-nodes-langchain.vectorStoreQdrant` realiza **exclusivamente busca densa simples**, sobre uma coleção com **47,9% de chunks duplicados** (`4.474` de `9.346` pontos gerados por `uuid4()` aleatórios no ETL), e **ignora completamente** os parâmetros `filtro_tipo_ato` e `filtro_cnpj` anunciados na descrição da tool.
  - Conforme os próprios dados de experimento do repositório (`avaliacao/experimentos/rag_golden_v13_denso/summary.json`), a busca densa pura atinge **apenas 3,12% de Recall@5** em perguntas com número de lei/decreto/portaria (`com_numero`, $n=32$) e **20,0% de Recall@5 geral**!
- **Impacto técnico e de negócio**: Cidadãos ou servidores buscando uma Lei, Portaria ou Decreto específico pelo número no WhatsApp recebem trechos irrelevantes ou repetidos em 96,9% das vezes na configuração atual de produção.
- **Recomendação**: Incorporar a lógica de busca híbrida (denso + match exato/lexical de número de ato e CNPJ), deduplicação por hash de `page_content` normalizado e filtros nativos Qdrant diretamente no serviço de recuperação da plataforma (`sim_platform`), além de gerar IDs determinísticos (`uuid5` do hash do documento+chunk) no ETL.
- **Status**: **Implementado em `sim_platform/rag/` e validado empiricamente**.

---

### `FIND-005` — Divergência Massiva de Tipos (253 Colunas) entre o Schema do Prompt e o Banco DuckDB Real + Alegações Incorretas no Artigo SBC
- **Severidade**: `P0` | **Categoria**: Banco de Dados / Text-to-SQL | **Natureza**: `Problema Confirmado` | **Confiança**: 100%
- **Arquivos e linhas afetadas**:
  - `AUDIT/historical_artifacts/ETL_SICOM_CGU.py` (linhas 245–275: `gerar_schema_sql`)
  - `scripts_python/main.py` (linhas 92–97: `read_csv_auto`)
  - `docs/artigo/sim_artigo_sbc_v1.tex` (linha 108)
- **Evidência**:
  - `ETL_SICOM_CGU.py` gera `schema_gerado_llm.sql` adivinhando tipos pelo prefixo do nome da coluna (`dat_*` → `DATE`, `vlr_*`/`val_*` → `DECIMAL(18,2)`), mas o DuckDB cria `VIEW`s via `read_csv_auto` sem aplicar esse DDL.
  - Resultado medido nas 72 tabelas (`1.233` colunas):
    - **75 colunas** anunciadas como `DATE` no prompt são na verdade `BIGINT` (ex.: `20230510` nas tabelas CGU) ou `VARCHAR` no DuckDB.
    - **150 colunas** anunciadas como `DECIMAL(18,2)` ou `INTEGER` no prompt são `VARCHAR` no DuckDB (ex.: todas as colunas `valor` das tabelas `cgu_*`, que usam vírgula decimal ou formato texto).
  - Além disso, o artigo SBC (`sim_artigo_sbc_v1.tex`, linha 108) afirma que o ETL converte strings `YYYYMMDD` em tipos `DATE` estritos, remove caracteres não numéricos de `cpf_formatado`/`cnpj` e isola a lacuna de 2020–2022 via junções defensivas com `empenho` — **nenhuma dessas três rotinas existe no código de ETL ou no banco**.
- **Impacto técnico e de negócio**: O LLM escreve funções de data (`EXTRACT(YEAR FROM dat_...)`) sobre `BIGINT` ou `SUM(valor)` sobre `VARCHAR`, gerando erros de execução (`Binder Error`) ou exigindo dezenas de gambiarras manuais no prompt.
- **Recomendação**: Gerar o contexto de schema para o LLM diretamente do catálogo real + normalizar/tipar as colunas na camada de materialização (`DataCatalog` + `DuckDBManager`).
- **Status**: **Corrigido em `sim_platform/catalog/data_catalog.py`**.

---

### `FIND-006` — Vulnerabilidade de Stored Cross-Site Scripting (XSS) no Dashboard `/ping` e Console `/api/logs`
- **Severidade**: `P0` | **Categoria**: Segurança de Aplicação | **Natureza**: `Problema Confirmado` | **Confiança**: 100%
- **Arquivos e linhas afetadas**:
  - `scripts_python/main.py` (linhas 245–255 e 270–282)
  - `experimental/backend-v2/main.py` (linhas 330–345)
- **Evidência**:
  - O endpoint `GET /ping` interpola `last_call['query_sql']` e `last_call['output']` diretamente no HTML via f-string sem `html.escape()`. Além disso, o script JS da página injeta o retorno de `/api/logs` via `line.innerHTML = ...` sem sanitização.
- **Cenário de reprodução**:
  - Enviar `POST /query` com `{"query": "SELECT '<script>alert(document.domain)</script>'"}` e abrir `GET /ping` no navegador de um administrador. O JavaScript malicioso é executado imediatamente.
- **Impacto técnico e de negócio**: Sequestro de sessão administrativa ou execução de ações no navegador de operadores que monitoram o painel `/ping`.
- **Recomendação**: Escapar todo conteúdo dinâmico no servidor com `html.escape()` e substituir `innerHTML` por `textContent` no cliente.
- **Status**: **Corrigido em `sim_platform/api/app.py`**.

---

### `FIND-007` — Ausência de Autenticação e Credenciais Hardcoded / Desativadas na Infraestrutura
- **Severidade**: `P0` | **Categoria**: Segurança de Infraestrutura | **Natureza**: `Problema Confirmado` | **Confiança**: 100%
- **Arquivos e linhas afetadas**:
  - `docker-compose.yml` (linhas 15, 29, 53–56, 85–95)
  - `Dockerfile` (linhas 1–20)
  - `AUDIT/historical_artifacts/ETL_SICOM_CGU.py` (linhas 22, 34, 65: `verify=False` e chave API CGU hardcoded)
- **Evidência**:
  - `WAHA_NO_API_KEY: "true"`, `WAHA_DASHBOARD_NO_PASSWORD: "true"`, `WHATSAPP_SWAGGER_NO_PASSWORD: "true"` expõem a sessão WhatsApp da prefeitura sem senha na porta `3000`.
  - `n8n` roda com `N8N_RESTRICT_FILE_ACCESS_TO: ""`, `N8N_BLOCK_FILESYSTEM_READ_AND_WRITE: "false"`, `N8N_ALLOW_EXECUTE_COMMAND: "true"`, `NODE_FUNCTION_ALLOW_BUILTIN: "*"` e `NODE_FUNCTION_ALLOW_EXTERNAL: "*"`.
  - Redis usa senha fraca hardcoded `prefeitura_pass`.
  - `duckdb_api` (`:8000`) e `qdrant` (`:6333`) não exigem nenhuma autenticação.
  - `ETL_SICOM_CGU.py` desabilita verificação de certificado SSL (`verify=False`).
- **Impacto técnico e de negócio**: Qualquer ator na rede pode assumir o número de WhatsApp oficial, executar comandos no host via n8n ou alterar vetores no Qdrant.
- **Recomendação**: Exigir autenticação por token/API Key em todos os serviços, remover flags `*_NO_PASSWORD`, restringir permissões de execução de comandos no n8n e habilitar verificação TLS (`verify=True`).
- **Status**: **Corrigido na camada de API `sim_platform` e documentado para hardening de containers**.

---

## 2. Achados de Alta Severidade (`P1`)

### `FIND-008` — Quebra na Rastreabilidade de Fontes (`fontes_csv` vs `rastreabilidade_tcc` vs `fontes_utilizadas`) e Regex de Ano Defeituoso
- **Severidade**: `P1` | **Categoria**: Funcional / Governança e Transparência | **Natureza**: `Problema Confirmado` | **Confiança**: 100%
- **Arquivos e linhas afetadas**:
  - `scripts_python/main.py` (linhas 160–194)
  - `AUDIT/historical_artifacts/duck-sql.json` (nó `Formatacao_Saida`)
  - `prompt-master` (Seção 5)
- **Evidência**:
  1. `scripts_python/main.py` extrai os anos da query com `re.findall(r'\b(202[0-6])\b', request.query)`. Se a query usa `ano_referencia BETWEEN 2021 AND 2024`, o regex captura apenas `2021` e `2024`, omitindo `2022` e `2023`! Se a query não filtra ano (agregando toda a série 2020–2025), `anos_Filtro` fica vazio e **nenhum arquivo CSV é listado** (exceto `fornecedor`/`pessoa`). Além disso, para cada view cujo nome aparece como substring na query (ex.: consultar `empenho_fonte` também dá match na substring `empenho`!), o código faz um `SELECT * FROM read_csv_auto(caminho) LIMIT 1` no disco!
  2. `main.py` retorna a lista na chave `fontes_csv`; o n8n (`duck-sql.json`) mapeia para `rastreabilidade_tcc`; o `prompt-master` instrui o Orquestrador a ler `fontes_utilizadas`.
- **Impacto técnico e de negócio**: Fontes omitidas, incorretas ou alucinadas pelo LLM no rodapé de auditoria entregue ao cidadão, além de I/O desnecessário a cada consulta.
- **Recomendação**: Extrair tabelas e intervalos de anos diretamente da AST SQL (`sqlglot`) no `SQLGuard` e retornar tanto `fontes_utilizadas` quanto `fontes_csv` de forma consistente e sem leituras extras de disco.
- **Status**: **Corrigido e testado em `sim_platform/security/sql_guard.py`**.

---

### `FIND-009` — Overfitting Severo do Prompt B (`prompt_sql_v2_insights.txt`) ao Golden Dataset v1 e Contradições Internas
- **Severidade**: `P1` | **Categoria**: IA / Engenharia de Prompts / Metodologia Científica | **Natureza**: `Problema Confirmado` | **Confiança**: 100%
- **Arquivos e linhas afetadas**:
  - `avaliacao/eval/prompts/prompt_sql_v2_insights.txt` (linhas 60–130)
  - `docs/experimentos-v2/LOG.md` (seções E5, E7, E8 e Golden v2)
- **Evidência**:
  - Os 77 "insights" numerados (`002.` a `077.`) do Prompt B correspondem exatamente aos IDs das perguntas do `golden_dataset_v1.0.csv` que falharam no Lote I, prescrevendo quais colunas projetar e até quais códigos hardcoded usar para cada pergunta.
  - Isso gerou contradições diretas dentro do mesmo prompt (ex.: Insight `020` manda usar `cod_orgao = '02003'` para Educação; Insights `046` e `051` mandam usar `cod_orgao = '02008'`; Insight `051` diz Obras = `'02008'` e Insight `052` diz Obras = `'02013'`).
  - Quando testado no conjunto externo independente (`golden_dataset_v2.0.csv`, 14 perguntas elaboradas por testador externo), a Equivalência Funcional (EF) do Prompt B caiu de **72,5%** para **0,0%** (0/14), sendo inferior ao prompt compacto v1 (**14,3%**, 2/14). Mesmo no Golden v1, o ganho de +10 pp sobre E1 teve $p = 0,146$ no teste de McNemar (estatisticamente não significativo a $\alpha = 0,05$).
- **Impacto técnico e de negócio**: O prompt monolítico de 67 KB não generaliza para perguntas reais de novos usuários, consome muitos tokens e confunde o modelo com regras contraditórias.
- **Recomendação**: Substituir a lista de insights ad-hoc por um **Catálogo de Dados Semântico Dinâmico** (`DataCatalog`) aliado a **Prompts Modulares por Segmento** (`PromptBuilder`), injetando apenas o DDL e as regras contábeis estruturais das tabelas selecionadas para a pergunta.
- **Status**: **Implementado em `sim_platform/catalog/` e `sim_platform/prompts/`**.

---

### `FIND-010` — Envio Estático de Todas as 72 Tabelas (~16,5k a 30k Tokens) em Toda Consulta Text-to-SQL
- **Severidade**: `P1` | **Categoria**: IA / Eficiência / Seleção de Contexto | **Natureza**: `Problema Confirmado` | **Confiança**: 100%
- **Arquivos e linhas afetadas**:
  - `avaliacao/eval/prompts/prompt_sql_compacto.txt`
  - `avaliacao/eval/prompts/prompt_sql_v2_insights.txt`
  - `AUDIT/historical_artifacts/duck-sql.json`
- **Evidência**:
  - Toda pergunta enviada ao gerador SQL inclui o DDL completo das 72 tabelas (`1.233` colunas), mesmo quando o usuário pergunta algo restrito a 1 ou 2 tabelas (ex.: Bolsa Família ou Folha de Pagamento).
  - No experimento E2 (`e2_contexto_estendido`), aumentar o tamanho do contexto estático reduziu a acurácia de **62,5% para 57,5%**, dobrou a latência média (`4,2s` → `8,2s`) e mais que dobrou o custo (`$0,46` → `$1,16`).
- **Impacto técnico e de negócio**: Alto custo de inferência, maior latência e degradação de atenção do LLM (*lost in the middle*) ao escolher entre 72 tabelas.
- **Recomendação**: Implementar seleção dinâmica de tabelas (`DataCatalog.select_tables_for_query`) que identifica o(s) segmento(s) da pergunta, inclui automaticamente as tabelas-ponte de `JOIN` e reduz o DDL enviado em **70% a 85%** mantendo **100% de recall das tabelas necessárias (TSA)**.
- **Status**: **Implementado e validado experimentalmente em `sim_platform/catalog/data_catalog.py`**.

---

### `FIND-011` — Ausência de Mascaramento Determinístico de CPF/Documento Pessoal nas Tabelas do SICOM
- **Severidade**: `P1` | **Categoria**: Segurança de Dados / Privacidade (LGPD) | **Natureza**: `Problema Confirmado` | **Confiança**: 100%
- **Arquivos e linhas afetadas**:
  - `avaliacao/dados/database/pessoa_municipio_*.csv`, `fornecedor_municipio_*.csv`, `remuneracao_municipio_*.csv`
  - `scripts_python/main.py`
- **Evidência**:
  - Enquanto as tabelas da CGU (`cgu_novo_bolsa_familia`, etc.) já trazem `cpf_formatado` mascarado da origem (`***.123.456-**`), as tabelas do SICOM (`pessoa_municipio`, `remuneracao_municipio`, `fornecedor_municipio`) armazenam `num_documento` com os **11 dígitos completos do CPF** em texto claro, sem qualquer filtro ou mascaramento no `duckdb_api`.
- **Impacto técnico e de negócio**: Exposição massiva de CPFs completos de servidores e cidadãos via consultas naturais ou listagens no canal público (WhatsApp), gerando risco de violação à LGPD (Lei 13.709/2018).
- **Recomendação**: Aplicar mascaramento determinístico de CPFs (11 dígitos → `***.XXX.XXX-**`, preservando CNPJs de 14 dígitos íntegros pois são pessoas jurídicas públicas) na camada de resposta da API para perfis públicos (`citizen`), permitindo visualização completa apenas para perfis institucionais autorizados (`admin`/`auditor`).
- **Status**: **Implementado em `sim_platform/security/sql_guard.py` (`mask_sensitive_rows`)**.

---

## 3. Achados de Média e Baixa Severidade (`P2`–`P3`)

### `FIND-012` — Regressão no Endpoint `/ping` do `experimental/backend-v2` (Exibe 0 Views quando Materializado) e Scripts Quebrados em `HEAD`
- **Severidade**: `P2` | **Categoria**: Engenharia de Software / Manutenibilidade | **Natureza**: `Problema Confirmado` | **Confiança**: 100%
- **Arquivos e linhas afetadas**:
  - `experimental/backend-v2/main.py` (linhas 296–299)
  - `experimental/backend-v2/scripts/validate_database.py` (linha 20)
  - `experimental/backend-v2/scripts/repair_export_2023.py` (linha 37)
- **Evidência**:
  - Quando `DUCKDB_MATERIALIZE=1` (padrão no `backend-v2`), o boot converte todas as `VIEW`s em `BASE TABLE`s (`CREATE TABLE ...; DROP VIEW ...`). Porém, o endpoint `/ping` continua executando `SELECT count(*) FROM information_schema.views WHERE table_schema='main'`, exibindo `0` tabelas mapeadas no painel!
  - Adicionalmente, os scripts em `experimental/backend-v2/scripts/` usam `Path(__file__).resolve().parent.parent` assumindo que estão em `avaliacao/scripts/`, falhando com `ModuleNotFoundError` e `FileNotFoundError` após o commit `55a2880`.
- **Status**: **Corrigido em `sim_platform`**.

---

### `FIND-013` — Ausência de Loop de Auto-Correção (Self-Correction) de SQL em Produção
- **Severidade**: `P2` | **Categoria**: IA / Orquestração Text-to-SQL | **Natureza**: `Problema Confirmado` | **Confiança**: 100%
- **Arquivos e linhas afetadas**:
  - `scripts_python/main.py` (linhas 205–232)
  - `AUDIT/historical_artifacts/duck-sql.json` (nó `Formatacao_Saida`)
- **Evidência**:
  - `scripts_python/main.py` possui um bloco que gera `dicas_correcao` e `mensagem_llm` quando uma query falha no DuckDB, mas retorna HTTP 200 com `"status": "erro_sql"`. No `duck-sql.json`, o nó `Formatacao_Saida` descarta `mensagem_llm` e `dicas_correcao`, devolvendo apenas `erro_execucao_sql` diretamente ao Orquestrador sem tentar regenerar a query no gerador Text-to-SQL.
- **Impacto técnico e de negócio**: Qualquer erro sintático ou de nome de coluna na primeira tentativa resulta em falha imediata para o usuário.
- **Recomendação**: Implementar loop controlado de validação e auto-correção (até $K=2$ tentativas) no orquestrador, alimentando o gerador SQL com o erro exato da AST/DuckDB e o schema real das tabelas envolvidas.
- **Status**: **Implementado em `sim_platform/orchestrator/router.py`**.

---

### `FIND-014` — Acoplamento Monolítico Single-Tenant (Hardcoded para Caeté/MG)
- **Severidade**: `P2` | **Categoria**: Arquitetura SaaS / Multi-Tenancy | **Natureza**: `Limitação Não Validada` | **Confiança**: 100%
- **Arquivos e linhas afetadas**:
  - `prompt-master`, `prompt_sql_compacto.txt`, `ETL_SICOM_CGU.py`, `scripts_python/main.py`
- **Evidência**:
  - Apesar de o TCC descrever o SIM como uma *"plataforma conversacional multi-instância"*, toda a implementação atual é single-tenant: um único arquivo `dados_sicom.duckdb` global, uma única coleção Qdrant `jornais_caete`, prompts com nomes de secretarias e códigos de Caeté hardcoded e ausência total de `tenant_id`, autenticação ou controle de acesso por secretaria/usuário.
- **Recomendação**: Introduzir isolamento multi-tenant (`tenant_id`, registro de municípios, catálogo por tenant e RBAC por perfil `public_citizen`, `analyst`, `controller`, `admin`) na nova arquitetura `sim_platform`.
- **Status**: **Implementado em `sim_platform`**.
