# EXPERIMENTS — Reprodução Científica do Baseline (TCC/Artigo) e Novos Experimentos Comparativos (`sim_platform`)

Este documento consolida:
1. A **reprodução empírica integral dos resultados do TCC e Artigo SBC** no ambiente local (`/home/antigravity/sim-workspace`).
2. Os **5 novos experimentos comparativos (`E-A` a `E-E`)** executados por [`scripts_python/run_comparative_experiments.py`](file:///home/antigravity/sim-workspace/scripts_python/run_comparative_experiments.py) (resultados brutos salvos em [`AUDIT/comparative_experiments_results.json`](file:///home/antigravity/sim-workspace/AUDIT/comparative_experiments_results.json)).

---

## 1. Reprodução do Baseline Congelado do TCC (Lote I: E1–E4, $n=80$)

Executado via `avaliacao/eval/run_pipeline.py --check-baseline` sobre o `golden_dataset_v1.0.csv` ($n=80$ consultas SQL em 12 categorias):

| Experimento | Modelo | Prompt | VSR (%) | EF (`EX proj+tol` %) | `EX resposta` (%) | `EX estrita` (%) | TSA (%) | Latência Média (ms) | Custo Total (USD) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| **E1 (Baseline Compacto)** | `gpt-4.1-mini` | `prompt_sql_compacto.txt` (~13,0k tok) | **97,50%** | **62,50%** | 63,75% | 10,00% | 97,50% | 4.198 ms | $0,461 |
| **E2 (Contexto Estendido)** | `gpt-4.1-mini` | `prompt_sql_estendido` (~28k tok) | 96,25% | 57,50% | 60,00% | 13,75% | 96,25% | 8.188 ms | $1,160 |
| **E3 (GPT-4o-mini)** | `gpt-4o-mini` | `prompt_sql_compacto.txt` | 90,00% | 46,25% | 46,25% | 12,50% | 95,00% | 3.981 ms | $0,206 |
| **E4 (GPT-4o)** | `gpt-4o` | `prompt_sql_compacto.txt` | 97,50% | 57,50% | 58,75% | 16,25% | 96,25% | 29.522 ms | $3,450 |

- **Veredito da Reprodução**: **100% reprodutível (`GATE BASELINE — OK`)**.
- **Análise Crítica**:
  - O experimento **E2** comprova empiricamente que **inflar o prompt estático degrada a Equivalência Funcional (-5,0 pp)**, quase dobra a latência (`4,2s` → `8,2s`) e aumenta o custo em 2,5x.
  - No **Lote II** (documentado em `docs/experimentos-v2/LOG.md`), o `prompt_sql_v2_insights.txt` (Prompt B) atingiu `VSR = 100%` e `EF = 72,5%` no Golden v1 ($p = 0,146$ no teste de McNemar, estatisticamente não significativo a $\alpha=0,05$), mas caiu para **0,0% de EF (0/14)** no conjunto de teste externo independente (`golden_dataset_v2.0.csv`), confirmando **overfitting severo** às 80 perguntas do Golden v1.

---

## 2. Experimento E-A — Impacto Financeiro e Fiscal da Corrupção de Exportação em 2023 (Antes vs. Depois do Reparo)

### 2.1. Metodologia
Comparamos a execução das somas financeiras e do indicador de Saúde Financeira Municipal (`ISF-M`) para o exercício de `2023` entre:
- **Base Bruta v1 (`repaired_2023=False`)**: CSVs originais de `avaliacao/dados/database` onde descrições da EC 123/2022 contêm dois pipes (`|`) literais não escapados.
- **Base Reparada (`repaired_2023=True`)**: Overlay determinístico `database_fix2023` carregado pelo `DuckDBManager`.

### 2.2. Resultados Medidos (Exercício 2023)

| Tabela Afetada (2023) | Coluna de Valor Auditada | Linhas c/ `vlr_*` Corrompido (Bruto) | Linhas Corrompidas (Reparado) | Soma no Banco Bruto (R$) | Soma no Banco Reparado (R$) | Diferença / Valor Recuperado (R$) |
|---|---|---:|---:|---:|---:|---:|
| `despesa_pagamento` | `vlr_pag_fonte` | **1.000** | **0** | R$ 145.796.501,40 | R$ 157.549.769,37 | **+R$ 11.753.267,97** |
| `despesa_liquidacaoFonte` | `vlr_liq_fonte` | **1.093** | **0** | R$ 165.547.429,72 | R$ 178.839.511,24 | **+R$ 13.292.081,52** |
| `empenho_empenhoFonte` | `vlr_empenho` | **1.542** | **0** | R$ 186.289.945,01 | R$ 203.377.751,62 | **+R$ 17.087.806,61** |
| `despesa_despesa` | `vlr_liquidado` *(e demais `vlr_*`)* | **787** *(em todas `vlr_*`)* | **0** | R$ 175.071.294,35 | R$ 173.810.018,75 | **-R$ 1.261.275,60** *(elimina inflação por coluna deslocada)* |
| `receita_receita` | `vlr_realizadoateperiodo` | **235** *(em todas `vlr_*`)* | **0** | R$ 180.329.675,76 | R$ 180.329.675,76 | R$ 0,00 *(deslocamento afetava colunas adjacentes)* |
| `contrato_creditoContrato` | `vlr_recurso` | **34** | **0** | R$ 0,00 | R$ 0,00 | R$ 0,00 *(corrigiu deslocamento textual)* |
| `licitacao_recLicitacao` | `vlr_recurso` | **30** | **0** | R$ 0,00 | R$ 0,00 | R$ 0,00 *(corrigiu deslocamento textual)* |
| `empenho_movFonteRsp` | `vlr_mov_fonte` | **2** | **0** | R$ 17.216.956,55 | R$ 17.216.956,55 | R$ 0,00 |
| **TOTAL GERAL (2023)** | — | **4.723 linhas** | **0 linhas** | — | — | **+R$ 11,75M em pagamentos; +R$ 17,09M em empenhos** |

- **Impacto no Índice de Saúde Financeira Municipal (`ISF-M 2023`)**:
  - Antes do reparo: **`1,0562`** (distorcido pelo deslocamento de colunas em `despesa_despesa`).
  - Após o reparo: **`1,0615`** (+0,53 pp de correção contábil real).

---

## 3. Experimento E-B — Schema Estático (72 Tabelas) vs. Seleção Dinâmica de Catálogo (`DataCatalog` + `ModularPromptBuilder`)

### 3.1. Metodologia
Avaliamos o algoritmo `DataCatalog.select_tables_for_query(question)` e o `ModularPromptBuilder` sobre todas as perguntas do **Golden Dataset v1 ($n=80$)** e do **Golden Dataset v2 ($n=14$)**, comparando contra o envio estático das 72 tabelas nos prompts v1 (`prompt_sql_compacto.txt`) e v2.1 (`prompt_sql_v2_insights.txt`).

### 3.2. Resultados Medidos

| Métrica | Prompt v1 Compacto (Estático) | Prompt B v2.1 Insights (Estático) | `sim_platform` no Golden v1 ($n=80$) | `sim_platform` no Golden v2 ($n=14$) |
|---|---:|---:|---:|---:|
| **Tabelas Injetadas no Prompt** | 72 (100%) | 72 (100%) | **18,25 em média** (mín: 9, máx: 39) | **20,14 em média** (mín: 12, máx: 39) |
| **Redução Média de Tabelas (Ruído)** | 0,0% | 0,0% | **-74,65%** | **-72,02%** |
| **Acurácia de Seleção de Tabelas (TSA)** | 100,0% (força bruta) | 100,0% (força bruta) | **100,0% (80/80)** | **100,0% (14/14)** |
| **Tamanho do Prompt (Tokens Estimados)** | 12.951 tok (51,8 KB) | 14.961 tok (59,8 KB) | **3.111 tok** | **3.446 tok** |
| **Economia de Tokens por Chamada** | Baseline | +15,5% mais caro | **-75,98% vs v1 / -79,21% vs v2.1** | **-73,39% vs v1 / -76,97% vs v2.1** |

- **Conclusão**: O `DataCatalog` seleciona com **100% de recall (94/94 consultas nos dois benchmarks)** todas as tabelas necessárias para responder às perguntas, cortando **mais de 75% dos tokens de entrada** e eliminando regras conflitantes de outros domínios.

---

## 4. Experimento E-C — Extração de Linhagem (`fontes_utilizadas`): Regex Legado vs. AST `SQLGuard`

### 4.1. Metodologia
Comparamos a lógica de rastreabilidade de `scripts_python/main.py` (`re.findall(r'\b(202[0-6])\b', query)` + match de substring de nome de view) contra o extrator baseado em AST (`SQLGuard.validate_and_rewrite`) nas queries do Golden v1 e em consultas de intervalo temporal.

### 4.2. Resultados Medidos
1. **Intervalos `BETWEEN` e Desigualdades (`>=`, `<=`)**:
   - Na query #1 do Golden v1 (`WHERE r.ano_referencia BETWEEN 2020 AND 2025`), o regex legado captura apenas `['2020', '2025']`, **omitindo 4 anos inteiros de arquivos CSV (`2021`, `2022`, `2023`, `2024`)** da lista de fontes!
   - O `SQLGuard` resolve a árvore sintática do nó `exp.Between` e retorna exatamente `[2020, 2021, 2022, 2023, 2024, 2025]`.
2. **Colisão de Substrings de Nomes de Tabelas**:
   - Quando uma query consulta `empenho_empenhoFonte` ou `despesa_liquidacaoFonte`, o código legado faz `if view_name in query_lower` e aciona falsos positivos se houver qualquer view cujo nome seja prefixo de outra, além de executar um `SELECT * FROM read_csv_auto(...) LIMIT 1` no disco para cada arquivo.
   - O `SQLGuard` extrai os nós `exp.Table` exatos da AST em **< 1 ms**, com **100% de precisão (80/80 no Golden v1)** e zero I/O de disco.

---

## 5. Experimento E-D — Desempenho Analítico: `VIEW`s sobre CSV vs. Tabelas Materializadas no DuckDB

### 5.1. Metodologia
Medimos o tempo de execução de 4 agregações pesadas (`despesa_pagamento`, `receita_receita`, `despesa_despesa`, `remuneracao_municipio`) operando sobre `VIEW`s que varrem os CSVs via `read_csv_auto` a cada consulta vs. tabelas materializadas no DuckDB.

### 5.2. Resultados Medidos
- **Latência Média sobre `VIEW`s (Scan de CSVs)**: **`279,36 ms`** por consulta.
- **Latência Média sobre Tabelas Materializadas**: **`2,42 ms`** por consulta.
- **Ganho de Desempenho (Speedup)**: **`115,44x` mais rápido**!

---

## 6. Experimento E-E — Ablação de Recuperação RAG no Diário Oficial (`golden_rag_v1.3.csv`, $N=40$)

Abaixo está a comparação empírica entre os 4 modos de recuperação avaliados sobre as 40 perguntas do benchmark ampliado (`32` perguntas de busca exata `com_numero` de Lei/Decreto/Portaria e `8` perguntas conceituais `tematico`):

| Configuração RAG | Correspondência Arquitetural | Recall@5 Geral ($N=40$) | MRR Geral ($N=40$) | Recall@5 `com_numero` ($n=32$) | MRR `com_numero` ($n=32$) | Recall@5 `tematico` ($n=8$) | MRR `tematico` ($n=8$) |
|---|---|---:|---:|---:|---:|---:|---:|
| **1. Denso Puro (`rag_golden_v13_denso`)** | **Produção legada no n8n (`wpp-tiokk-sql.json`)** | 22,50% | 13,17% | **3,12% (1/32)** | 0,62% | 100,0% | 63,33% |
| **2. Denso + Dedup (`rag_golden_v13_dedup`)** | Remoção dos 47,9% de chunks duplicados | 22,50% | 14,79% | 3,12% (1/32) | 1,04% | 100,0% | 69,79% |
| **3. Denso + Filtro + Dedup (`rag_golden_v13_filtro_dedup`)** | Adição do filtro `tipo_ato` no payload | 22,50% | 15,42% | 3,12% (1/32) | 1,04% | 100,0% | 72,92% |
| **4. Híbrido + Filtro + Dedup (`rag_golden_v13_hibrido`)** | **Produção `sim_platform` (`HybridRAGRetriever`)** | **100,00%** | **97,50%** | **100,00% (32/32)** | **100,00%** | **100,0%** | **87,50%** |

- **Conclusão**: A promoção do recuperador híbrido (denso + lexical por número de ato/CNPJ + deduplicação + filtro de tipo de ato) para o código de produção em `sim_platform/rag/hybrid_retriever.py` elimina o abismo entre o artigo científico e a plataforma real, elevando o Recall@5 em atos numerados de **3,12% para 100,0%**.
