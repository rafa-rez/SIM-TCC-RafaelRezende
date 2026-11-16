# ATTENTION_POINTS.md — Manual de Invariantes, Armadilhas e Anti-Regressão (Para Agentes de IA)

> **PROPÓSITO:** Memória institucional destilada de mais de 85 falhas reais corrigidas entre `v3.0` e `v4.5` (Caeté/MG + Lagoa da Prata/MG). Leia as seções correspondentes **ANTES** de editar módulos de `sim_platform/` ou homologar um novo município.
> **REGRA CONGELADA (`AGENTS.md`):** NUNCA altere `avaliacao/dados/golden/golden_dataset_v1.0.csv`, `golden_rag_v1.1.csv`, `golden_rag_v1.3.csv`, checkpoints `e1`–`e4`, `eval/prompts/prompt_sql_v2_insights.txt` nem PDFs em `docs/`.

---

## Índice Rápido por Arquivo / Tarefa (Leia Antes de Editar)

| O que você vai editar / executar | Famílias obrigatórias a consultar abaixo |
| :--- | :--- |
| `sim_platform/engine/gold_views.py` ou `audit/proactive_insights.py` | **F1**, **F2**, **F10** |
| `sim_platform/catalog/data_catalog.py` ou `entity_linker.py` | **F2**, **F3**, **F4**, **F7** |
| `sim_platform/orchestrator/router.py` ou `prompts/builder.py` | **F2**, **F3**, **F4**, **F8**, **F9** |
| `sim_platform/visualization/chart_generator.py` ou `table_renderer.py` | **F3**, **F5**, **F10** |
| `sim_platform/ingestion/*` (`sync_manager`, `city_onboarding`, `fleet_verifier`, `connectors/`) | **F1**, **F2**, **F3**, **F6**, **F10** |
| `sim_platform/security/*` (`sql_guard`, `auth_rbac`, `quota_manager`, `semantic_guardrail`) | **F6**, **F7** |
| `sim_platform/messaging/*` (`waha_manager`, `whatsapp_router`) ou `api/app.py` | **F6**, **F7**, **F8**, **F9** |
| `sim_platform/api/auditor_console_v0.html` | **F2**, **F5**, **F7**, **F9** |
| **Onboarding / Atualização de Novo Município em MG** | Rodar `python -m sim_platform.ingestion.fleet_verifier --check-readiness` + checar **F1**, **F2**, **F3**, **F6** |

---

## F1. Contabilidade Pública Municipal (Domínio GovTech / LRF / TCE-MG)

1. **Valores Líquidos vs. Brutos:** Em empenhos, liquidações, pagamentos e receitas, SEMPRE deduza estornos, anulações e deduções (`FUNDEB`/retificações). Somar empenho bruto inflou `2024` em `+R$ 16,74M` no passado.
2. **Folha Mensal vs. 13º Salário vs. Retificações:**
   - **PROIBIDO:** Misturar competência de `13º Salário` / `Rescisão` com média salarial mensal de dezembro, ou ingerir JSON de folha sem deduplicar retificações por matrícula/competência.
   - **OBRIGATÓRIO:** Filtrar `COALESCE(dsc_tipo_pag, 'Mensal') = 'Mensal'` para quadros mensais e deduplicar lançamentos retificadores na ingestão.
3. **Classificação de `Agente Político`:**
   - **PROIBIDO:** Inferir `Agente Político` usando `Lotacao` (`GABINETE DO PREFEITO`, `SECRETARIA MUNICIPAL DE...`) ou aceitar `SECRETÁRIO ESCOLAR` / `AUXILIAR DE SECRETARIA` como secretário municipal. Isso inflou `17` agentes políticos para `926`.
   - **OBRIGATÓRIO:** Classificar exclusivamente por `FormaContratacaoInvestidura` + `dsc_cargo` validado contra cargos políticos reais (`PREFEITO`, `VICE-PREFEITO`, `VEREADOR`, `SECRETÁRIO MUNICIPAL`, `SEC. MUN.`, `PROCURADOR GERAL`, `CONTROLADOR GERAL`, `CHEFE DE GABINETE`).
4. **Rankings de "Maiores Fornecedores / Empresas Privadas" (`natureza_credor`):**
   - **PROIBIDO:** Exibir `INSS`, `FGTS`, `PASEP`, `PREFEITURA` (folha própria), `CEMIG`, `COPASA`, `TRT/TRF`, consórcios intermunicipais (`ICISMEP`), bancos públicos (`BDMG`, `BB`, `CEF`) ou hospitais filantrópicos conveniados como `EMPRESA_PRIVADA`.
   - **OBRIGATÓRIO:** Segregar via `_natureza_credor_sql` em `vw_gold_execucao_despesa_credor` e usar `natureza_credor = 'EMPRESA_PRIVADA'` por padrão quando o usuário pede "empresas/fornecedores".
5. **Função Orçamentária (`10 - SAÚDE`, `12 - EDUCAÇÃO`) em Portais Abertos:**
   - Portais como Betha Cloud muitas vezes exportam despesas sem `dsc_funcao` preenchida (fazendo perguntas de Saúde/Educação retornarem `R$ 0,00`).
   - **OBRIGATÓRIO:** Reconciliar `despesa_despesa` com o `RREO-Anexo 01/02` oficial da STN/SICONFI por função/subfunção (Portaria MOG nº 42/1999).
6. **Contratos PNCP (`Lei 14.133/2021`):**
   - A rota resumida de compras do PNCP frequentemente omite `niFornecedor` e `nomeRazaoSocialFornecedor` (`NULL` -> `"FORNECEDOR NÃO INFORMADO"`).
   - **OBRIGATÓRIO:** Buscar o vencedor homologado em `/contratos` ou `/itens/{n}/resultados` do PNCP antes de gravar `contrato_contratos`.

---

## F2. Assimetria Multi-Cidade e Multi-ERP (Betha vs. Cidade360 vs. SICOM)

1. **Tabelas Stub Vazias (`0` linhas) NUNCA entram no Prompt SQL:**
   - Cidades sem pacote SICOM completo possuem stubs Parquet de `0` linhas para manter o schema de 79 tabelas.
   - **PROIBIDO:** Incluir tabelas com `is_populated == False` (`row_count == 0`) em `select_tables_for_query()`, `build_dynamic_ddl()` ou trilhas de auditoria (`ProactiveAuditEngine._has()`). Se o LLM vir a tabela no DDL, ele gera SQL válido que retorna `0` linhas e mente dizendo que "não houve contratos/gastos".
2. **Contagem de Servidores (`seq_dim_pessoa`, NUNCA `num_cpf`):**
   - Portais municipais mascaram CPFs (`***.244.890-**` ou `046.244.***-**`), gerando milhares de colisões se contados por `num_cpf`.
   - **PROIBIDO:** `COUNT(DISTINCT num_cpf)` ou `COUNT(DISTINCT num_documento)` em folha de pagamento.
   - **OBRIGATÓRIO:** `COUNT(DISTINCT seq_dim_pessoa)`.
3. **Cruzamento Societário com CPF Mascarado (`vw_gold_conflito_interesses_licitacao`):**
   - Receita Federal mascara QSA (`***235516**`), Betha mascara fim (`046.244.***-**`), SICOM tem 11 dígitos.
   - **PROIBIDO:** `JOIN` ingênuo `s.cpf = p.cpf` (sempre dá `0` quando um lado está mascarado) ou agrupar desconhecidos sob string constante `'SEM_DOC'`.
   - **OBRIGATÓRIO:** Comparar miolo de 6 dígitos (`SUBSTR(..., 4, 6)`) + nome normalizado sem acento quando o CPF estiver mascarado, e restringir `doc_credor_raiz` (`LEFT(..., 8)`) estritamente a CNPJs de `14` dígitos (`NULL` para CPF).
4. **Acentuação Variável entre ERPs no DuckDB (`ILIKE` não remove acento):**
   - SICOM grava `'SECRETARIO MUNICIPAL'`, enquanto Cidade360/Betha gravam `'Secretário Municipal DS-01'`.
   - **PROIBIDO:** `dsc_cargo ILIKE '%SECRETARI%'`
   - **OBRIGATÓRIO:** `strip_accents(UPPER(COALESCE(dsc_cargo, ''))) ILIKE '%SECRETARI%'`
5. **Zero Vazamento de Caeté para Outras Cidades:**
   - **PROIBIDO:** Fallbacks `tenant_id or "caete_mg_3110004"` em onboarding (`ibge_code` inválido deve retornar `HTTP 400`), seeds estáticos de CNES/IDEB/CAPAG/Convênios de Caeté copiados para outro IBGE, ou textos `"Caeté"`, `"Jornal Opinião"`, `"F.C.C.C."`, `"SEDESMA"` exibidos quando outra cidade estiver ativa.

---

## F3. Consciência Temporal Dupla e Virada de Exercício (`2025` -> `2026` -> `2027`)

1. **Zero Ano Corrente Hardcoded:**
   - **PROIBIDO:** Chumbar `2025` ou `2026` como teto fixo em `router.py`, `chart_generator.py`, `semantic_guardrail.py` ou `builder.py`.
   - **OBRIGATÓRIO:** Ler `catalog.max_data_year` dinamicamente e aceitar anos `2018..2035` em validadores temporais.
2. **Defasagem Diferente por Tabela na Mesma Cidade:**
   - Em Caeté, `receita_receita` vai até `11/2026`, `remuneracao_municipio` até `10/2026`, `despesa_despesa` até `08/2026` e `frota_gastoFrota` / `empenho_empenho` até `08/2025`.
   - **PROIBIDO:** Estampar no texto que *"os dados de frota estão atualizados até novembro de 2026"* só porque `max_data_year` global da cidade é `2026`.
   - **OBRIGATÓRIO:** Usar `catalog.get_effective_freshness_human(referenced_tables)` para informar ao usuário exatamente o mês/ano real das tabelas usadas na query SQL executada.
3. **Comparação Ano Fechado vs. Ano Parcial:**
   - Nunca narrar `2026 < 2025` (ou `2025` parcial `< 2024`) como "queda de arrecadação/gasto" sem explicitar que o último exercício está em **execução parcial acumulada**.
4. **Datas Futuras de Vigência Capturadas como Data de Publicação no RAG:**
   - Contratos publicados em `11/2026` contêm frases *"vigência até 31/12/2027"*. Regex solto no corpo captura `2027-12-31` como `data_publicacao`.
   - **OBRIGATÓRIO:** Extrair `data_publicacao` apenas do cabeçalho/rodapé oficial da edição (`Publicada em DD/MM/YYYY` na AMM-MG ou nome da edição) e aplicar clamp estrito `<= date.today().isoformat()`.

---

## F4. Orquestração Text-to-SQL, RAG Normativo e Anti-Alucinação

1. **Decomposição em `hybrid_sql_rag`:**
   - Quando a pergunta mistura finanças + ato normativo (*"Quanto a empresa X recebeu e qual portaria nomeou a comissão?"*), o SQL deve responder **somente** à parte tabular/financeira e o `HybridRAGRetriever` responde às portarias/decretos. Tentar fazer `JOIN` com tabela vazia de portaria zera o valor financeiro (`R$ 0,00`).
2. **Qualidade do Chunking de Diários Oficiais (`ActBoundaryChunker`):**
   - **PROIBIDO:** Fatiar PDFs de jornal por `split("\n\n")` fixo ou truncar atos em `550`/`2500` caracteres (corta valores e assinaturas ao meio ou cola duas portarias distintas).
   - **OBRIGATÓRIO:** Segmentar por fronteira semântica de ato (`PORTARIA Nº`, `DECRETO Nº`, `EXTRATO DE CONTRATO`), deduplicar republicações idênticas por hash normalizado do corpo e nunca duplicar `[Município: ...]` dentro de `page_content` e `contextual_header`.
3. **Proibição de Pedido de Desculpas quando Anexo Visual Foi Gerado:**
   - Se `chart_generated=True` ou `table_generated=True`, avisar explicitamente no prompt de naturalização e varrer via regex qualquer frase do LLM dizendo *"não consigo gerar gráficos/tabelas aqui"*.

---

## F5. Apresentação Visual Executiva (Gráficos PNG e Tabelas no WhatsApp/Web)

1. **Proteção Estrita de Colunas de Ano e Código (`2026` != `2.026`):**
   - **PROIBIDO:** Formatar colunas `ano*`, `*exercicio*`, `competencia`, `cod_*`, `seq_*`, `num_contrato` com separador de milhar (`2.026`), plotá-las como barra monetária ao lado de milhões ou somá-las no rodapé `TOTAL GERAL` (`2024+2025+2026 = 6.075`).
   - **OBRIGATÓRIO:** Classificar via `_YEAR_OR_CODE_COL_RE` como dimensão/rótulo (`year_or_code`), nunca métrica aditiva.
2. **Recusa de Gráfico Escalar (`1x1`) e Abreviação Inteligente de Secretarias:**
   - Não gerar imagem de barra para resultado escalar de `1 linha x 1 número`.
   - Antes de truncar rótulos horizontais em `28` chars, aplicar `_smart_shorten_gov_label()` (`SECRETARIA MUNICIPAL DE OBRAS` -> `SEC. MUN. DE OBRAS`), senão todos os rótulos viram `"SECRETARIA MUNICIPAL DE..."`.
3. **Rodapé `TOTAL GERAL` vs. `TOTAL PARCIAL (EXIBIDO)`:**
   - Se a query sofreu corte por `LIMIT` (`truncated_by_security_limit=True`), o rodapé da tabela DEVE exibir `TOTAL PARCIAL (EXIBIDO)`, nunca `TOTAL GERAL`.

---

## F6. Ingestão, Releases Imutáveis, Concorrência e Cloud Data Lake

1. **Merge Incremental Seguro por Fatia `(ano, mes, orgao)`:**
   - **PROIBIDO:** Cair silenciosamente em `full_replace` ao subir pacote de 1 ano/mês sobre tabela histórica (apaga 2020–2025!), ou deixar fatias antigas sujas ao re-homologar o mesmo exercício com granularidade diferente de órgãos.
   - **OBRIGATÓRIO:** Validar schema (`overlap >= 50%`), mesclar por `(ano_referencia, mes, cod_orgao)` e bloquear upload cujo conteúdo contenha código IBGE de outro município.
2. **Preservação Anti-`NULL` em APIs Federais Oscilantes (SICONFI / IBGE / QSA):**
   - **PROIBIDO:** Sobrescrever `vlr_despesa_pessoal_lrf`, `populacao_residente` (ex.: buraco 2023 do SIDRA pós-Censo 2022) ou `nomes_socios_qsa` com `NULL` quando uma API federal falha/omite um campo na rodada incremental.
   - **OBRIGATÓRIO:** `LEFT JOIN` com o Parquet da release anterior + `COALESCE(novo.col, antigo.col)`.
3. **Two-Phase Delete com `size + MD5` no Google Drive (`fleet_verifier.py`):**
   - **PROIBIDO:** `unlink()` de arquivo em `inbox/` ou `cloud_outbox/` apenas porque `gdrive mutate upload` retornou `exit code 0`.
   - **OBRIGATÓRIO:** Confirmar via `gdrive readonly info <file_id> --json` que `size == local_size` e `md5Checksum == local_md5`, persistir `_cloud_datalake_manifest.json` em disco **antes** do `unlink()`, e manter `KEEP_RECENT_RELEASES = 2` + `PREVIOUS.json` para rollback instantâneo.
4. **Hot-Swap DuckDB e Invalidação de Caches:**
   - Ao trocar `CURRENT.json`, aguardar drenagem das queries em voo (`20s` grace, **sem** `conn.interrupt()` global que mata requests paralelos legítimos) e invalidar caches semânticos SQLite/L1 e cache de auditoria (`(tenant_id, active_release_id)`).

---

## F7. Segurança Cibernética B2G, Sandbox `SQLGuard` e Conformidade LGPD

1. **Zero Confiança em IP Privado RFC1918 (`172.18.x.x` Bridge Docker):**
   - **PROIBIDO:** Conceder perfil `admin` automático quando `ip_obj.is_private` for `True`. No Docker, tráfego externo encaminhado pela bridge chega com IP `172.18.0.1`! Apenas loopback direto sem headers de proxy e em modo de teste explícito pode usar fallback dev.
2. **Credenciais RBAC em SQLite (`PBKDF2-HMAC-SHA256`) + Escopo de Token por Cidade:**
   - **PROIBIDO:** Dicionários de senhas em texto claro no código Python ou aceitar JWT de `vereador01` de Caeté para ler dados internos de Lagoa da Prata.
   - **OBRIGATÓRIO:** Validar `expected_tenant_id` em rotas normais e checar `allowed_tenants` (`["*"]` ou lista explícita) em `/api/tenants/select`.
3. **`SQLGuard` AST + Hardening DuckDB 2.0:**
   - Manter `enable_external_access = false` no DuckDB e bloquear no `SQLGuard`: `read_*`, `parquet_scan`, `enable_profiling`, `pg_sleep`/`sleep_ms`, `generate_series`/`range` gigantes, `json_each`/`to_json`/`row_to_json` (bypass de projeção de colunas PII) e `FORMAT` largo. Cuidado com falsos positivos lexicais (`"; set"` casava com `"setembro"` -> exigir fim de comando `;\s*set\s+[a-z_]`).
4. **Anti-Reconstrução Matemática de CPF e Privacidade de Logs:**
   - Re-mascarar CPFs parciais heterogêneos para o formato canônico único `***.XXX.***-**` para `citizen` (impede cruzar `046.244.***-**` com `***244890**` e reconstruir 9–11 dígitos).
   - Nunca gravar nomes de tabelas PII (`pessoa_municipio`, `remuneracao_municipio`, `cgu_bpc`, `cgu_novo_bolsa_familia`) em `_catalog_profile_v1.json`, nem expor telefones em `/ping` ou `/api/interactions` sem RBAC do gabinete.

---

## F8. Protocolo WhatsApp (`WAHA`), Áudio/ASR e Sessão Multi-Turno

1. **Higiene de Webhook WAHA:**
   - Deduplicar `msg_id` **antes** de baixar áudio/mídia (evita custo duplo no Whisper e bloqueia DoS), impor teto de `10 MB` em mídia/Base64, validar URL de mídia contra SSRF/path traversal (`unquote` + `posixpath.normpath`), e ignorar eventos `fromMe=True` exceto auto-teste explícito `/sim`.
2. **Roteamento Multi-Cidade por Conversa (`WhatsAppTenantRouter`):**
   - Preservar o seletor do Console como padrão global, mas sempre passar o texto bruto primeiro por `whatsapp_router.route_message(...)` para respeitar `/lagoa <pergunta>`, `/caete <pergunta>`, `/lagoa` (vínculo fixo por `chat_id`), `/padrao` e `/cidades`, chaveando a sessão de memória para `f"{tenant_id}::{chat_id}"`.
3. **Multi-Turno Sem Sequestro de Tópico:**
   - Perguntas curtas autossuficientes com entidade nova não podem herdar filtros `WHERE` da pergunta anterior; se o turno anterior retornou erro ou `0` linhas, não salvá-lo no cache semântico.

---

## F9. Contrato Backend <-> Frontend e Salvamento de JSON Truncado

1. **Consistência da Console UI (`auditor_console_v0.html`):**
   - Ao trocar de cidade (`switchActiveMunicipality`), limpar modais abertos, histórico de chat da aba e recarregar inventário/pulso/insights da nova cidade.
   - Enviar `getSimAuthHeaders()` (`Authorization: Bearer ...`) em **todas** as chamadas autenticadas (`/api/tenants/*`, `/api/interactions*`, `/api/waha/*`, `/api/dossier/*`) e manter timeout `>= 20000ms` para consultas analíticas com geração de imagem.
   - Nunca fazer fallback silencioso para `SIM_BUNDLE` estático de Caeté quando a página estiver servida via `http://` ou `https://`.
2. **Salvamento de JSON Truncado do LLM (`_salvage_truncated_json_intro`):**
   - Respostas longas em `response_format={"type": "json_object"}` podem cortar no `max_tokens`. Sempre passar pelo extrator tolerante para nunca exibir `{"citizen_intro": "...` cru ao cidadão.

---

## F10. Física de Dados, *Fan-Out* em `JOIN`s e Portabilidade Windows/Docker

1. **Prevenção de *Fan-Out* em `JOIN`s Dimensionais:**
   - Tabelas como `orgao_orgao`, `orgao_orgaoUnidade` e `pessoa_municipio` repetem chaves ao longo dos exercícios/remessas.
   - **PROIBIDO:** `FROM despesa_despesa d LEFT JOIN orgao_orgaoUnidade u ON d.seq_orgao = u.seq_orgao AND d.cod_unidade = u.cod_unidade` direto (multiplica valores por `6x`!).
   - **OBRIGATÓRIO:** Deduplicar a dimensão em uma CTE `ROW_NUMBER() OVER (PARTITION BY seq_orgao, cod_unidade ORDER BY ano_referencia DESC) = 1` **antes** do `JOIN` (e usar `LEFT JOIN` para não zerar anos novos como `2026` onde `orgao_orgaoUnidade` parou em `2025`).
2. **Sanitização CSV e Monetária PT-BR:**
   - Converter strings `"1.234.567,89"` removendo pontos de milhar antes de trocar `,` por `.` e fazer cast para `DECIMAL(18,2)` (nunca `FLOAT` binário impreciso em somas contábeis).
3. **Paridade Windows Host <-> Container Linux (`Dockerfile.sim`):**
   - Manter `fonts-dejavu-core` instalado em `Dockerfile.sim` (imagens `python:3.11-slim` não trazem fontes TrueType por padrão, quebrando acentos e alinhamento em PNGs de tabelas/gráficos).

---

## Gate Automático Obrigatório Pós-Mudança

Sempre que alterar ingestão, catálogo, Gold Views ou cadastrar/atualizar uma cidade, execute:

```powershell
& "D:\tiokk-n8n\.venv\Scripts\python.exe" -m sim_platform.ingestion.fleet_verifier --check-readiness
& "D:\tiokk-n8n\.venv\Scripts\python.exe" -m pytest -q
```
