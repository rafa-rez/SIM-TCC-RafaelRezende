# Auditoria Adversarial Independente da SIM Platform v2.3 (Relatório Consolidado dos 5 Subagentes)

> [!IMPORTANT]
> **Por que esta auditoria sem viés mudou o jogo:** Ao isolar 5 subagentes em contextos limpos e sem saberem quem escreveu a `v2.3`, eles vasculharam os 79 arquivos Parquet, testaram ataques reais contra o `SQLGuard` e interrogaram as tabelas do SICOM/CGU por conta própria. O resultado revelou **corrupções milionárias nos dados brutos que ninguém tinha visto antes**, **brechas críticas de segurança/concorrência** e **8 achados reais explosivos em Caeté** que multiplicam o valor comercial da plataforma.

---

## 1. As 7 "Bombas Ocultas" (Erros Reais no Código e nos Dados da v2.3)

### 🔴 Bomba 1: Corrupção Silenciosa de R$ 60,70 Milhões na Folha de Pagamento (`remuneracao_municipio` 2023–2025)
- **Quem descobriu:** [Subagente 2 (Dados)](conversation://0cd612ea0ab9a8ad4a1a6b222a27db1c), [Subagente 3 (IA)](conversation://6397be866e3fa16cfe7e3e5c601ebb14) e [Subagente 4 (TCE-MG)](conversation://319315a34d04b72fb179c4087385ecc4).
- **O que aconteceu:** Em **16.062 de 58.134 contracheques (27,63% de toda a folha de 2023 a 2025)** — especificamente nos cargos de **Médicos PSF, Enfermeiros, Dentistas, Engenheiros e Técnicos** —, a coluna `dsc_req_crg` continha `;` no meio do texto (*"Cargo privativo de profissionais de saúde; ..."*).
- Como os valores deslocados que caíram nas colunas de salário (`20.0`, `30.0`, `40.0` horas semanais) também eram números, o script `validate_database.py` não percebeu!
- **Consequência na tela hoje:**
  - Para **12.808 contracheques da Saúde**, o banco diz que o **Salário Bruto (`vlr_rmn_bruta`) é R$ 20,00, R$ 30,00 ou R$ 40,00** (a carga horária semanal!), enquanto o Salário Bruto real foi parar dentro de `vlr_rmn_liquida`!
  - Por isso, quando alguém pergunta a folha de 2024, o sistema responde que a **Folha Bruta foi R$ 47,28M e a Folha Líquida foi R$ 61,53M** (salário líquido R$ 14,25 milhões maior que o bruto!), escondendo **R$ 60,70 milhões da folha bruta no triênio** e sumindo com todos os médicos e engenheiros do ranking de maiores salários.
- **Como consertar (100% recuperável):** A view/reparo determinístico `vw_gold_folha_servidores` realinha as colunas quando `cod_municipio IN ('NÃO INFORMADO', 'A')`, recuperando 100% dos centavos sem perda de dados.

---

### 🔴 Bomba 2: `receita_qsa_fornecedores` estava 96,8% vazia de sócios por erro na chave do SICOM (e misturava INSS/FGTS como empresa privada)
- **Quem descobriu:** [Subagente 2 (Dados)](conversation://0cd612ea0ab9a8ad4a1a6b222a27db1c) e [Subagente 4 (TCE-MG)](conversation://319315a34d04b72fb179c4087385ecc4).
- **O que aconteceu:** Em [`sync_manager.py`](file:///home/antigravity/sim-workspace/sim_platform/ingestion/sync_manager.py#L311-L319), tentamos cruzar `despesa_pagamento.num_doc_credor` direto com `licitacao_quadroSocLicitacao.num_cgc_habilitado`.
- Só que no SICOM, `num_cgc_habilitado` guarda o **CPF de 11 dígitos do sócio pessoa física**, enquanto o CNPJ da empresa fica na tabela-ponte **`licitacao_habLicitacao.num_documento`** (ligada por `seq_hab_licitacao`)!
- **Consequência:** 242 dos 250 maiores fornecedores apareciam sem sócios. Ao fazer o JOIN correto via `licitacao_habLicitacao` + `contrato_contContrato`, saltamos de 8 para **149 grandes fornecedores com Quadro Societário e Representantes completos** (e separamos recolhimentos como `INSS`, `FGTS`, `TJMG` e `SAAE` para não aparecerem como "fornecedores privados").

---

### 🔴 Bomba 3: 4 Bypasses Críticos de Segurança no `SQLGuard`, LGPD e Concorrência ([Subagente 1 — Red Team](conversation://943c61266bcb69b5fe48f208d97339c3))
1. **Leitura de `/etc/passwd` via `json_execute_serialized_sql`:** O DuckDB estava abrindo sem `SET enable_external_access = false;`, e funções nativas como `json_execute_serialized_sql(json_serialize_sql('SELECT * FROM read_csv_auto(\'/etc/passwd\')'))` e `sniff_csv` não estavam na blocklist do [`sql_guard.py`](file:///home/antigravity/sim-workspace/sim_platform/security/sql_guard.py).
2. **Bypass de `LIMIT 500` via `UNION ALL`, `LIMIT ALL` e `GROUP BY`:** Queries com `UNION ALL` na raiz da AST não recebiam `LIMIT`, materializando 200.000+ linhas em RAM. Solução: envolver a query validada ou garantir `LIMIT` no topo de qualquer AST (`Select` ou `Union`) e `SET enable_external_access = false`.
3. **Bypass de LGPD (CPF exposto para Cidadão):**
   - `POST /chat` aceitava `{"role": "auditor"}` enviado no próprio JSON pelo cliente (`effective_role = req.role if req.role != "citizen" else role`);
   - Se a query fizesse `CAST(num_documento AS BIGINT)`, `mask_sensitive_rows` ignorava porque só mascarava `isinstance(val, str)`;
   - E 55 fornecedores MEI têm o CPF de 11 dígitos colado no próprio nome (`nom_credor = 'NOME COMPLETO 12345678901'`).
4. **Race Condition no Singleton `SIMOrchestrator` (`router.py:768-771`):** `self._active_allowed_sources` e `self._active_llm_api_key` eram salvos como atributos do objeto singleton `self`. Se 2 usuários fizessem perguntas ao mesmo tempo, a API key e os filtros de um vazavam para a requisição do outro.

---

### 🔴 Bomba 4: Falhas de Comunicação Frontend ↔ Backend e Docker ([Subagente 1](conversation://943c61266bcb69b5fe48f208d97339c3) e [Subagente 5](conversation://b57cfacbe10e058d926352fc36c577fb))
1. **URL chumbada em `localhost:8090` + Timeout de `1500ms`:** Em [`generate_interactive_app.py`](file:///home/antigravity/sim-workspace/scripts_python/generate_interactive_app.py#L1338), o `fetch` apontava para `http://localhost:8090/chat` (enquanto o Docker sobe na `8000`) e tinha `AbortSignal.timeout(1500)`, abortando chamadas reais ao Gemini/OpenAI (que levam ~3s) e caindo silenciosamente no fallback estático!
2. **Falta de `requests` no `docker-compose.yml`:** A lista do `pip install` no container `duckdb_api` não incluía `requests`, fazendo com que chamadas à OpenAI/Gemini dentro do Docker falhassem silenciosamente.
3. **Impressão de PDF (`window.print()`) sem `@media print`:** Imprimia o fundo escuro, o menu lateral inteiro e cortava a tabela na 6ª linha por causa do `max-height: 310px`.

---

### 🔴 Bomba 5: Armadilhas de JOIN (Multiplicação de Valores e Perda de 99,1% da Frota em 2025)
- **Frota (-99,1% em 2025):** `frota_frota` só registra o veículo no ano em que foi cadastrado. Fazer `JOIN frota_frota USING (seq_veiculo, ano_referencia)` apaga **99,1% dos gastos de frota em 2025** (`R$ 1,06M` cai para `R$ 9,3 mil`)! O JOIN correto é apenas por `seq_veiculo` (sem `ano_referencia`).
- **Folha (Inflação de 2,90x):** `pessoa_municipio` tem 1 linha por ano para a mesma pessoa. Fazer `JOIN pessoa_municipio ON r.seq_dim_pessoa = p.seq_dim_pessoa` sem `AND r.ano_referencia = p.ano_referencia` multiplica os salários por **2,90x**.
- **Educação/Saúde (Inflação de 4x e 2x):** `orgao_orgaoUnidade` tem 4 subunidades para Educação e 2 para Saúde no mesmo `cod_unidade`.

---

### 🔴 Bomba 6: Gafes nos Dados Estáticos de `sync_manager.py` e nas Fórmulas de Índices Constitucionais
- **IDEB em anos pares (2020, 2022, 2024):** O MEC/INEP só calcula IDEB em anos ímpares (`2019, 2021, 2023, 2025`). Qualquer secretário de educação veria na hora que notas de IDEB para 2020, 2022 e 2024 não existem.
- **IDHM anualizado (2020–2025):** O IDHM do PNUD/IBGE só é medido no Censo Demográfico (decenal).
- **Cálculo de Saúde (`IES-C = 11,94%`) e Educação (`IEQ-C = 80,58%`) em `router.py`:** O denominador de Educação esquecia as transferências constitucionais (FPM/ICMS), dando 80,58%, e o de Saúde incluía transferências do SUS/FUNDEB no denominador, acusando falsamente Caeté de aplicar apenas 11,94% em Saúde (abaixo dos 15% constitucionais!).

---

### 🔴 Bomba 7: Contaminação e Colisão de Chaves nos 104 Atos do RAG (`newspaper_pipeline.py`)
- **[Subagente 3 (IA/RAG)](conversation://6397be866e3fa16cfe7e3e5c601ebb14)** descobriu que:
  1. `build_clean_individual_collection()` não estava chamando `ActBoundaryChunker` antes de indexar os chunks de 3.000 caracteres, deixando **24 chunks com atos secundários engolidos** após a assinatura do prefeito (ex.: Decreto 207/2025 engolido dentro do Decreto 198/2025; Dispensa 050/2025 engolida dentro do Decreto 220/2025).
  2. Como `RE_ACT_HEADER` não continha `EXTRATO DE DISPENSA` nem `EXTRATO DE INEXIGIBILIDADE`, **4 extratos reais de dispensa/inexigibilidade de 2024** (`015/2024`, `017/2024`, `038/2024`, `024/2024`) que citavam no texto *"Comissão nomeada pelo Decreto nº 015/2024"* receberam todos a chave `DECRETO::15/2024` e foram apagados pelo deduplicador!
  3. 17 entradas vinham de trechos truncados/sintéticos do CSV de benchmark `golden_rag`.

---

## 2. Os Desalinhamentos entre o Playbook Comercial e o Código Atual

| Item Prometido no Playbook Comercial | Status Real no Código Hoje | O que fazer |
| :--- | :--- | :--- |
| **Cache Semântico/SQL (Hit de 2ms a custo R$ 0,00)** | Não implementado (e `_call_external_llm_for_sql` rodava antes do `_golden_lookup`). | Implementar `QueryCache` em memória/SQLite normalizado + checar cache antes do LLM. |
| **Rate Limit de 20 perguntas/dia por telefone no WhatsApp** | Não implementado em `/webhook/waha`. | Implementar contador diário por `chat_id` no `QuotaManager`. |
| **Ledger de Franquia Mensal (15k/30k/60k) + Alerta em 85%** | Não implementado (tokens da resposta OpenAI/Gemini não eram contabilizados). | Implementar `QuotaManager` lendo `usage` real + endpoint `GET /api/governance/quota`. |
| **Memória Conversacional Multi-Turn (`session_id`)** | `self.conversation_history` era *write-only* (gravava, mas nunca lia). | Ler os últimos turnos no `router.py` e no `ModularPromptBuilder` para perguntas como *"E em 2023?"* ou *"Quem são os sócios da segunda empresa?"*. |

---

## 3. Os 6 "Tesouros Reais de Caeté" (Descobertas Inéditas no Banco que Vendem o Produto Sozinhas)

Ao consertar o JOIN de sócios (`licitacao_quadroSocLicitacao` + `licitacao_habLicitacao`) e o *column shift* da folha (`remuneracao_municipio`), os Subagentes 2 e 4 descobriram **6 achados 100% reais nos dados de Caeté** que nenhum portal de transparência mostra:

1. **Conflito de Interesses & "Porta Giratória" (`QSA × Folha × Pagamentos`):**
   - **Caso 1:** `BRENO ORNELLAS SILVA MAGALHAES` (CPF `10223551635`) era sócio-administrador da `ALCATEIA ENGENHARIA E CONSTRUCAO LTDA` (`30935086000105`), que recebeu **R$ 4.183.159,47** da Prefeitura de Caeté (com 6 dispensas em 2020) até **`31/05/2021`**. **Exatamente 48 horas depois (`02/06/2021`)**, ele tomou posse como **Secretário Municipal de Obras** de Caeté!
   - **Caso 2:** `ALINE CRISTINA DA SILVA` (CPF `08894352609`), servidora ativa na folha de 2023 (`PSICOLOGO SOCIAL ANS-01` no CREAS), consta no QSA da `RCA COMERCIAL LTDA` (`11263197000178`), que recebeu **R$ 779.735,50** do município (sendo **R$ 162.095,40 em 12 pagamentos durante 2023 enquanto ela estava ativa na folha** — vedação do Art. 14, IV da Lei 14.133/2021).
2. **Empresas de CNPJs Diferentes com os Mesmos Sócios Disputando a Mesma Licitação:**
   - Na Licitação `seq_licitacao = 1309976` (2023), as empresas `3PH MEDICAMENTOS ESPECIAIS LTDA` (`44387711000136`) e `TRES PHARMA DISTRIBUIDORA E SERVICOS LTDA` (`26401571000121`) habilitaram-se **juntas no mesmo certame** compartilhando **exatamente os mesmos 2 sócios** (`FABIO AUGUSTO CARVALHO PEREIRA` e `LEANDRO CALIXTO SILVEIRA DE SOUZA`).
   - E no SAAE, `BAUMINAS QUIMICA SA FILIAL` e `BAUMINAS QUIMICA NNE LTDA` disputaram juntas 5 licitações seguidas (2020–2024) com os mesmos 5 sócios.
3. **O "Império das Caronas" (R$ 85,94 Milhões via Adesão a Atas de Registro de Preços):**
   - Caeté empenhou **R$ 85,94 milhões** em 1.752 empenhos de "Carona", incluindo **R$ 24,62 milhões em pavimentação asfáltica** com a `UNICAPA`, **R$ 7,62 milhões em festas/eventos** e adesões a atas de municípios a milhares de km (**Fortaleza/CE, Araguaçu/TO e Embu das Artes/SP**).
4. **Frota Fantasma (R$ 7,97 Milhões / 61,7% do gasto em veículos sem cadastro/placa):**
   - Dos R$ 12,91 milhões gastos com combustível e peças em `frota_gastoFrota`, **R$ 7,97 milhões (61,7%)** foram lançados em códigos `seq_veiculo` que não existem em `frota_frota` (ex.: `seq_veiculo = 195856` consumiu R$ 661,7 mil sem placa cadastrada).
5. **Baixa Competitividade Licitatória (Candidato Único em 65,6% das licitações de 2024 e 81% em 2025):**
   - Em 2024, 42 das 64 licitações com habilitação tiveram **apenas 1 único concorrente**, incluindo obras milionárias (`Concorrência 378/2024 — R$ 4,43M` e `Concorrência 135/2024 — R$ 3,75M`).
6. **Pressão de Fim de Mandato (Art. 42 LRF) e "Orçamento Paralelo" por Decreto:**
   - Em 2024, foram abertos **R$ 85,08 milhões (38,4% da LOA)** via Decretos de Suplementação, os Restos a Pagar saltaram **+229% (para R$ 28,08 milhões)** e **R$ 16,74 milhões em empenhos foram anulados** (sendo 52,8% nos últimos 60 dias do mandato).

---

## 4. Plano de Implementação Recomendado (O que fazemos agora?)

Podemos dividir a execução em **3 pacotes cirúrgicos** (ou rodar todos em sequência):

1. **Pacote A — Blindagem Técnica, Correção dos Dados (R$ 60,7M da Folha + QSA Real + RAG Atômico) e Travas do Playbook:**
   - Reparar os 16.062 contracheques deslocados em `remuneracao_municipio.parquet` (recuperando os R$ 60,7M da folha bruta e os salários de médicos/engenheiros);
   - Reconstruir `receita_qsa_fornecedores.parquet` com o JOIN correto via `licitacao_habLicitacao` + `contrato_contContrato` (149 fornecedores com sócios reais, separando INSS/FGTS/entes públicos);
   - Corrigir os anos pares inexistentes do IDEB e os cálculos constitucionais de Saúde/Educação;
   - Passar `ActBoundaryChunker` de verdade nos jornais oficiais, corrigir `RE_ACT_HEADER` para preservar todas as Dispensas/Inexigibilidades e remover os 17 mocks do `golden_rag`;
   - Blindar `SQLGuard` (`enable_external_access = false`, bloqueio de `json_execute_serialized_sql`/`sniff_csv`, `LIMIT` obrigatório em `UNION ALL`/`GROUP BY`), LGPD em números/MEIs, remover a *race condition* do `SIMOrchestrator` e implementar o **Cache SQL + Rate Limit 20/dia WhatsApp + Ledger de Franquia**.
2. **Pacote B — Motor de IA Investigativa (Memória Multi-Turn + Resolução de Entidades + Camada Gold):**
   - Criar as **6 Gold Views (`vw_gold_*`)** no DuckDB para eliminar *fan-out traps*;
   - Ativar **Memória Multi-Turn real** (`session_id`) e **Extração de Entidades (`WHERE` dinâmico para qualquer empresa, secretaria, cargo ou ano)** + **Few-Shot Dinâmico** com os exemplos do Golden Dataset.
3. **Pacote C — Revolução na Interface (`v3.0`): Dossiê 360º Clicável + Novas Trilhas TCE-MG + Nota Técnica PDF Timbrada:**
   - Transformar cada Fornecedor, CNPJ, Contrato, Sócio e Ato na tela em um **link clicável que abre o Dossiê 360º Lateral (estilo Palantir Foundry)**;
   - Incluir no painel de **🚨 Auditoria Proativa** os novos achados reais de Caeté (*Conflito QSA × Folha*, *Sócios em Comum na Mesma Licitação*, *Caronas em Atas de Registro de Preços*, *Frota sem Placa* e *Licitações de Candidato Único*);
   - Adicionar **Modal de Leitura Integral dos Atos do Jornal Oficial**, **KPIs Financeiros Executivos** no topo, **`@media print` timbrado em papel branco** para o PDF não cortar tabelas e URL relativa `/chat` com timeout de 15s para LLMs.
