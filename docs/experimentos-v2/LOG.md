# Log de experimentos v2

Formato por entrada:

```
## YYYY-MM-DD — exp/nome-branch
- Hipótese:
- Mudança:
- SQL: VSR __ / EF __ (E1 recalc)
- RAG: Recall@5 __ / MRR __ (se aplicável)
- Conclusão:
```

---

<!-- Adicione entradas abaixo -->

## 2026-07-18 — exp/pipeline-baseline-gate

- Hipótese: gate automático evita regressão silenciosa do baseline E1 em experimentos v2 (P0.1).
- Mudança: `run_pipeline.py --check-baseline` (e `run_metrics_pipeline.ps1 -CheckBaseline`) falha com exit 1 se E1 tiver VSR < 97,5% ou EF < 62,5% no `metrics_summary_v3.json`; resultado gravado em `pipeline_report.json` (`baseline_gate`). Corrigido também o encoding do `.ps1` (UTF-8 com BOM) que quebrava o parse no PowerShell 5.1.
- SQL: VSR 97,5 / EF 63,75 (E1 recalc — gate PASS; nota: recompute atual dá EF 63,75 vs 62,5 da tabela congelada `comparison_table_v3.md`; limiar mantém o valor reportado no TCC).
- RAG: n/a.
- Conclusão: gate ativo e testado (caminhos pass, fail e resumo ausente). Achado colateral: caso 052 (E1/E2) oscila entre `alias_colunas` e `tolerancia_numerica` por não-determinismo de ponto flutuante no DuckDB — afeta só EX proj/colmap, não EF/VSR; gate usa métricas estáveis.

## 2026-07-18 — exp/rag-dedup-chunks

- Hipótese: deduplicar `page_content` idêntico no retrieval melhora MRR sem perder Recall@5 (P1.1). Diagnóstico: 47,9% da collection `jornais_caete` é conteúdo duplicado (4.474 de 9.346 chunks) e as 8 perguntas do golden v1.1 tinham 1–2 pares duplicados no top-5.
- Mudança: `run_rag_eval.py --dedup [--fetch-k N]` — busca `4×top_k` candidatos, colapsa `page_content` idêntico (hash MD5 do texto) e mantém os `top_k` únicos. Opt-in: sem a flag, comportamento v1.1 é byte a byte idêntico (baseline re-verificado: 63,33%). Sem reindexação — dedup em query-time, índice de produção intocado.
- SQL: n/a.
- RAG: Recall@5 100 / MRR **69,79** (baseline v1.1: 100 / 63,33 → **+6,46 pp**). Ranks por pergunta: 002 (3→2), 004 (5→4), 007 (3→2), 008 (5→3); demais mantiveram rank 1. Saída em `experimentos/rag_dedup_qtime_v2/`.
- Conclusão: ganho mensurável sem custo de Recall. Próximo passo natural: P1.3 (golden RAG v1.2 com 15–20 itens sem cherry-pick) para validar o ganho em amostra honesta antes de portar o dedup para o workflow n8n / decidir reindexação limpa (P1.2).

## 2026-07-18 — golden RAG v1.2 honesto (P1.3) + busca híbrida por nº de ato

- Hipótese: o golden v1.1 (critério de inclusão rank≤5) superestima o retrieval; um golden sorteado sem olhar rank revela a taxa real.
- Mudança: inventário completo do índice via scroll + regex de cabeçalho (747 atos distintos: 592 decretos, 80 portarias, 69 editais, 6 leis); amostra de 20 atos com seed 42 e perguntas-template uniformes ("O que dispõe o Decreto Municipal nº N/AAAA de Caeté?"). Inclusão independente de rank. Artefatos: `dados/golden_rag/golden_rag_v1.2.csv` + `inventario_atos_v1.2.json`.
- RAG v1.2 baseline (k=5): Recall@5 **0%** / MRR 0 — a busca densa não codifica o número do ato; retorna decretos "parecidos" com números errados. Em k=50: recall 5%. Com dedup em k=50: 25%.
- Mudança 2: `run_rag_eval.py --hybrid-act` — extrai nº/ano da pergunta e antepõe chunks cujo CABEÇALHO cita o ato (índice client-side via scroll; produção intocada).
- RAG v1.2 híbrido+dedup (k=5): Recall@5 **100%** / MRR **100** (por construção para perguntas com número — o ganho real é a garantia de lookup exato). v1.1 híbrido+dedup: Recall 100% / MRR **87,5** (vs 63,33 baseline; as 2 perguntas temáticas caem no caminho denso+dedup).
- Conclusão: (1) o número v1.1 reportado no TCC continua válido como está, mas o v1.2 mostra que lookup por número exige busca híbrida — gap crítico de produção (usuários perguntam "o que diz o decreto X/AAAA?"); (2) recomendação: portar hybrid-act + dedup para o workflow n8n e/ou criar índice de payload `numero_ato`/`ano_ato` no Qdrant (P1.5), com backup do volume antes.

## 2026-07-18 — incidente infra: container DuckDB sem dados (mount obsoleto)

- Sintoma: 100% das queries (geradas E de referência) com Catalog Error; VSR 0/80 no primeiro run do e5.
- Causa: `duckdb_prefeitura` criado com compose antigo montava `./dados` (legado) em vez de `./avaliacao/dados`; após reinício do Docker Desktop o mount ficou órfão. `docker compose up -d duckdb_api` recriou com o mount correto.
- Custo evitado: o checkpoint preserva o SQL gerado; métricas recalculadas offline com `recompute_metrics.py` (sem repetir chamadas OpenAI).
- Follow-up sugerido (P0): healthcheck no serviço + query-canário no início de `run_experiment.py` (falhar rápido se o catálogo estiver vazio); imagem com dependências pré-instaladas (boot atual ~3 min por `pip install`).

## 2026-07-18 — exp/sql-prompt-insights-v2 (P2.1): e5 (v2.0), e6 (v2.1), e7 (v2.1.1)

- Hipótese: incorporar (a) correções derivadas das 29 falhas reais do E1 e (b) regras do `INSIGHTS_GOLDEN_V2.md` (SAAE, RSP saldo, datas YYYYMMDD, ILIKE âncora) melhora EF sem perder VSR.
- Mudança: `eval/prompts/prompt_sql_v2_insights.txt` = prompt compacto v1 + escopo de filtros de higiene por formato de resposta + regras de precisão (folha anti-duplicata, rankings por credor, cruzamentos por chave única, tabelas setoriais, licitações valor×contagem, Sim/Não em texto, séries ano a ano) + seção de perguntas reais (testers). Variante `v2_insights` no config. Modelo fixo gpt-4.1-mini, temp 0, golden v1 (80).
- Resultados (EF = EX resposta, recompute local):
  | Exp | Prompt | EF | VSR | TSA |
  |---|---|---:|---:|---:|
  | E1 | v1 compacto | 63,75 | 97,5 | 95,0 |
  | e5 | v2.0 | 71,25 | 95,0 | 100 |
  | e6 | v2.1 | **72,50** | **100** | 100 |
  | e7 | v2.1.1 | 72,50 | 98,75 | 100 |
  | e8 | v2.1 + DDL real | 70,00 | 98,75 | 98,75 |
- Iterações: v2.0→v2.1 corrigiu 7 regressões (licitações contagem×valor, contratos ativos, LIMIT/colunas, cruzamento por documento, ano a ano com GROUP BY); v2.1→v2.1.1 corrigiu 5 vazamentos de escopo (filtro 2,4,5 vazando para pagamentos/receita; natureza 3.1 vazando para IEQ-C; frota cadastro×gasto), mas trocou acertos de lugar (EF igual, VSR −1,25) — platô de ajuste fino.
- **Decisão: v2.1 é o vencedor** (EF 72,50 = +8,75 pp vs E1; VSR 100%). `eval/prompts/prompt_sql_v2_insights.txt` restaurado para v2.1; v2.1.1 arquivado em `e7_prompt_v2_1_1_insights/`.
- Smoke no golden v2 dos testers (14 casos avaliáveis, validação externa — NÃO citar no TCC): v1 EF 14,3% (2/14) / VSR 78,6%; v2.1 EF 0% / VSR 85,7%. Leitura: ambos os prompts estão longe das perguntas reais abertas; v2.1 melhora a validade estrutural (SAAE e assistência social passaram a gerar SQL válido; RSP saldo usa a tabela certa) mas o EF estrito pune formato de resposta em perguntas abertas (ex.: caso 14 exige exatamente `isf_m,ies_c`). Falha ilustrativa: caso 3 quebrou por `strptime '%Y-%m-%d'` em `dat_assinatura` BIGINT — exatamente o que a proposta D1 (DDL real no prompt) endereça.
- Achado colateral: inconsistência interna no golden v1 — caso 011 (SUM pagamentos 2024) exige filtro `nom_credor NOT ILIKE`, caso 042 (SUM pagamentos 2024/2025) exige SEM filtro. Candidato à revisão humana P2.2.
- Prompts arquivados em `experimentos/e{5,6}_*/prompt_usado_*.txt`.

## 2026-07-18 — diagnóstico: drift de tipos DDL do prompt vs schema real (DuckDB)

- Método: `information_schema.columns` da API (:8000) comparado ao DDL embutido no prompt v1.
- Resultado: **253 divergências em 1233 colunas**. Destaques: 75 colunas declaradas `DATE` que são BIGINT/VARCHAR (YYYYMMDD) — causa direta de Binder Errors (casos 8 e 46 do E1); 15 colunas `DECIMAL` que são VARCHAR (origem da regra "CAST obrigatório"); 135 `INTEGER`→VARCHAR.
- Causa raiz: views sobre `read_csv_auto(..., union_by_name, ignore_errors=true)` — tipos inferidos por arquivo, lixo de cadastro força VARCHAR, e `ignore_errors` descarta linhas silenciosamente.
- Propostas (aguardando aprovação, mexem em produção/repo):
  1. **D1** — gerar o DDL do prompt automaticamente do `information_schema` real (script pronto; `ddl_real` com anotações YYYYMMDD/CAST). Zero risco de runtime. **Testado (e8)**: no golden v1 deu EF 70,0 (−2,5 vs v2.1 com DDL antigo) — Cálculo Fiscal +8, Despesas Pagamentos −20; o golden v1 foi construído sobre as convenções do DDL antigo, então parte do "ganho" do DDL real não aparece nele (smoke testers em separado). Manter DDL antigo no golden v1; reavaliar D1 quando o golden v2 fechar curadoria. No smoke dos testers, o DDL real não mudou o EF (0%) e derrubou 1 caso de VSR (85,7→78,6). Conclusão: corrigir tipos via prompt não paga; a correção certa é na CAMADA DE DADOS (D2/ETL — tipar na carga, aí DDL e realidade coincidem por construção).
  2. **D2** — materializar tabelas tipadas no boot da API (`CREATE TABLE AS` + `TRY_CAST` para `vlr_*`/`dat_*`), substituindo views re-lidas a cada query: tipos estáveis, queries mais rápidas, linhas rejeitadas logadas.
  3. **D3** — healthcheck do serviço no compose + canário no `run_experiment.py` + imagem Docker com dependências pré-instaladas.
