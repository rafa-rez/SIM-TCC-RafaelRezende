# DECISIONS — Registro de Decisões Técnicas e Arquiteturais (ADR)

Este documento registra as decisões de arquitetura, engenharia de dados, segurança, IA e produto tomadas durante a auditoria e modernização da plataforma SIM (`sim_platform`).

---

## ADR-001: Isolamento Estrito do Repositório Original e Evolução Paralela em `sim_platform`
- **Contexto**: O repositório original contém artefatos congelados do TCC e artigo SBC (`golden_dataset_v1.0.csv`, `e1`..`e4`, `golden_rag_v1.1.csv`, `golden_rag_v1.3.csv`, textos LaTeX). Qualquer alteração no repositório original ou quebra de reprodutibilidade dos experimentos do TCC violaria as regras do projeto (`AGENTS.md` e Regra 16).
- **Decisão**:
  1. Manter `/home/antigravity/rafa-rez/SIM-TCC-RafaelRezende` 100% intocado, validado por manifesto criptográfico SHA-256 (`AUDIT/ORIGINAL_MANIFEST.sha256` e `AUDIT/verify_original_integrity.sh`).
  2. Construir a evolução técnica como um pacote Python nativo e modular (`sim_platform/`) dentro de `/home/antigravity/sim-workspace`, preservando a compatibilidade retroativa com `avaliacao/eval/run_pipeline.py --check-baseline` e com o contrato `POST /query` esperado por integrações legadas.

---

## ADR-002: Migração do Núcleo de Inteligência do n8n para Orquestração Nativa em Código Python
- **Contexto**: A auditoria dos workflows visuais do n8n (`wpp-tiokk-sql.json` e `duck-sql.json`) revelou falhas graves de integração que são invisíveis sem inspeção de JSON: parâmetros desconectados (`pergunta_usuario` vs `$json.query`), perda do campo de rastreabilidade (`rastreabilidade_tcc` vs `fontes_utilizadas`), debouncer desativado (`Wait1` disabled) e deleção prematura de lock Redis causando race conditions sob concorrência, além de prompts monolíticos de 60 KB colados em nós visuais.
- **Decisão**:
  - Substituir a orquestração em JSON visual do n8n por um **Orquestrador em Código Python (`sim_platform/orchestrator/router.py`)** exposto via FastAPI (`sim_platform/api/app.py`).
  - Manter adaptadores de entrada/saída compatíveis com webhooks (WhatsApp Cloud API / WAHA) e sub-workflows, permitindo que o n8n seja opcional ou usado apenas para integrações periféricas.
- **Justificativa**: Contratos tipados (Pydantic), controle de concorrência atômico em código (`asyncio.Lock` / Redis lock com `finally`), testes automatizados (`pytest`) em CI/CD, versionamento Git limpo e loop determinístico de auto-correção de SQL.

---

## ADR-003: Validação Determinística de SQL por AST (`sqlglot`) em Substituição à Confiança no Prompt
- **Contexto**: No protótipo original (`scripts_python/main.py`), a segurança do banco de dados dependia exclusivamente da frase *"Gere apenas SELECT"* no prompt do LLM, enquanto a conexão DuckDB rodava com `read_only=False` e `enable_external_access=true`, permitindo leitura de `/etc/passwd` via `read_csv_auto` e destruição de views.
- **Decisão**:
  - Introduzir o módulo `SQLGuard` (`sim_platform/security/sql_guard.py`) baseado no parser AST `sqlglot` (dialeto `duckdb`) + configuração `enable_external_access=false` no DuckDB.
  - Nenhuma query chega ao motor DuckDB sem passar por: (1) verificação de instrução única `SELECT`/`WITH`, (2) bloqueio de todas as funções de I/O (`read_*`, `parquet_*`, `glob`, `pragma_*`), (3) validação contra a allowlist de tabelas do catálogo do município (`tenant_id`), e (4) injeção automática de `LIMIT` máximo em consultas não agregadas.

---

## ADR-004: Extração de Linhagem (`fontes_utilizadas`) e Detecção de Lacunas via AST em vez de Regex
- **Contexto**: `scripts_python/main.py` inferia os arquivos CSV utilizados aplicando `re.findall(r'\b(202[0-6])\b', query)` e verificando se o nome da view era substring da query (fazendo `SELECT * FROM read_csv_auto(...) LIMIT 1` para cada arquivo!). Isso falhava em cláusulas `BETWEEN 2020 AND 2025`, `>= 2023`, queries sem filtro de ano e gerava falsos positivos entre `empenho` e `empenho_fonte`. Além disso, a lacuna da folha de pagamento (`2020`–`2022`) dependia de o LLM lembrar de avisar o usuário.
- **Decisão**:
  - Extrair tabelas base e predicados temporais (`=`, `IN`, `BETWEEN`, `>=`, `<=`) diretamente da AST SQL no `SQLGuard`.
  - Detectar de forma 100% determinística no `SQLGuard` qualquer acesso a `remuneracao_municipio` nos anos `2020`–`2022` e anexar o objeto estruturado `TransparencyWarning` (`GAP-FOLHA-2020-2022`) na resposta da API e no contexto do sintetizador.

---

## ADR-005: Catálogo Dinâmico de Dados e Seleção Semântica de Tabelas (`DataCatalog`) em Substituição ao Prompt Monolítico de 72 Tabelas
- **Contexto**: Enviar todas as 72 tabelas (~16,5k tokens no prompt compacto, ~30k no estendido) em toda chamada SQL elevou custos, dobrou a latência no experimento E2 e levou à criação do Prompt B (`prompt_sql_v2_insights.txt`), que decorou 77 regras ad-hoc do Golden v1 (com contradições internas como `cod_orgao = '02003'` vs `'02008'`) e teve **0% de EF no Golden v2 externo**.
- **Decisão**:
  - Implementar o `DataCatalog` (`sim_platform/catalog/data_catalog.py`) contendo metadados reais das 72 tabelas organizados em **11 segmentos de domínio**, com grafo explícito de chaves estrangeiras e tabelas-ponte (`empenho_empenho`, `credor_credor`, `pessoa_pessoa`, `orgao_orgao`).
  - Implementar o algoritmo determinístico `select_tables_for_query()` que seleciona apenas os segmentos pertinentes à pergunta (ou indicados manualmente pelo usuário/canal) + tabelas-ponte obrigatórias, reduzindo o DDL injetado no prompt em **~75–85%** enquanto preserva **100% de cobertura das tabelas necessárias (TSA = 100%)**.

---

## ADR-006: Reparo Determinístico da Corrupção de Delimitador de 2023 e Materialização Tipada no DuckDB
- **Contexto**: A conversão de `;` para `|` sem `quoting` em `ETL_SICOM_CGU.py` corrompeu **4.723 linhas** em 8 tabelas de 2023 devido aos pipes literais na descrição da EC 123/2022 (`"... ART. 5º | INCISO IV | EC Nº 123/2022"`), subavaliando os pagamentos de 2023 em **R$ 11,75 milhões (-8,06%)**.
- **Decisão**:
  - Integrar o reparo de delimitadores de 2023 e a materialização de tabelas (`DuckDBManager`) como padrão da plataforma evoluída, mantendo o banco original (`avaliacao/dados/database/dados_sicom.duckdb`) intacto para não alterar os hashes do baseline congelado E1–E4, e disponibilizando o banco reparado para validação empírica e produção.

---

## ADR-007: Unificação da Busca Híbrida RAG (Denso + Lexical por Ato/CNPJ + Deduplicação + Filtro) no Backend
- **Contexto**: Os resultados de `Recall@5 = 100%` e `MRR = 97,5%` do artigo SBC dependiam de busca híbrida por número de ato (`--hybrid-act`), deduplicação de chunks (`--dedup`) e filtro por tipo de ato (`--filtro-tipo-ato`), que existiam apenas em `avaliacao/eval/run_rag_eval.py`. No n8n de produção, a busca densa pura tinha apenas **3,1% de Recall@5** em perguntas com número de ato.
- **Decisão**:
  - Promover a lógica de recuperação híbrida + deduplicação de conteúdo + filtragem de metadados (`tipo_ato`, `cnpjs_Mencionados`, `ano_edicao`) para um módulo de produção de primeira classe (`sim_platform/rag/hybrid_retriever.py`), usado diretamente pelo orquestrador e pela API.
