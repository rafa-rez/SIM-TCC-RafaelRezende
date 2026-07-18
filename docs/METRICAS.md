# Métricas de avaliação Text-to-SQL

Documento de referência para o repositório de validação do TCC *Sistema de Informações Municipais* (**SIM**, instância Caeté/MG).

Versão das métricas: **3.0** (julho/2026). Relatório: `docs/RELATORIO_METRICAS_V3.md`.

---

## 1. Visão geral

Este documento descreve as métricas utilizadas para **comprovar a funcionalidade** do subsistema Text-to-SQL do SIM na instância Caeté. As métricas não constituem o objeto central do trabalho — que é o acesso conversacional a dados públicos municipais —, mas fornecem evidência reprodutível de que o pipeline gera SQL válida e resultados alinhados ao gabarito em grande parte dos casos.

A avaliação combina indicadores **sintáticos** (a SQL executa?), **estruturais** (tabelas e filtros esperados?) e **semânticos sobre o resultado** (o conjunto de linhas retornado corresponde ao gabarito?). Para perguntas abertas em linguagem natural, a literatura de Text-to-SQL adota comparação estrita de resultados (Execution Accuracy, EX), que penaliza aliases de coluna e formulações SQL alternativas semanticamente equivalentes. Por isso, reportamos **várias variantes de EX**, alinhadas à literatura e estendidas para o contexto municipal.

Implementação: `avaliacao/eval/lib/compare.py`. Recálculo offline: `avaliacao/scripts/recompute_metrics.py`.

---

## 2. Pré-condições

Para cada consulta *i* no conjunto de referência (*N* = 80):

1. Existe `query_referencia` validada no DuckDB.
2. O experimento produziu `query_gerada`.
3. Define-se *Rᵢ* = resultado tabular da referência e *Gᵢ* = resultado da SQL gerada (lista de registros como dicionários coluna → valor).

Métricas de EX só são avaliadas quando **ambas** as SQL executam sem erro. Caso contrário, EXᵢ = 0 e VSRᵢ = 0.

---

## 3. Normalização de células

Função *ν* aplicada a cada valor antes de qualquer comparação:

| Tipo | Regra |
|------|--------|
| `NULL` | valor unificado nulo |
| Numérico | `Decimal` com arredondamento em 4 casas, convertido para `float` |
| String numérica | vírgula substituída por ponto, depois mesma regra numérica |
| Demais strings | `strip()` e maiúsculas |
| Booleano | preservado |

Esta normalização segue a prática de benchmarks que comparam resultados numéricos com tolerância de arredondamento (cf. Spider, BIRD).

---

## 4. Família Execution Accuracy (EX)

### 4.1 EX estrita (EX, `ex_strict`)

**Definição.** Para cada linha, constrói-se a assinatura ordenada de pares (nome da coluna em minúsculas, valor normalizado). O resultado é o **multiconjunto** dessas assinaturas (ordem das linhas irrelevante).

```
EX_strict = (1/N) Σᵢ 𝟙[ multiset(σ(Rᵢ)) = multiset(σ(Gᵢ)) ]
```

onde σ(r) = sort({ (k.lower(), ν(r[k])) : k ∈ colunas(r) }).

**Embasamento.** Métrica padrão em Spider (Yu et al., 2018) e derivados: compara o resultado executado com o gabarito sem admitir diferença de nomenclatura de colunas.

**Interpretação.** Métrica conservadora, adequada quando o esquema de saída é parte da especificação da consulta.

**Resultado E1 (baseline):** 10,0%.

---

### 4.2 EX por cardinalidade de linhas (EX-L, `ex_rows`)

```
EX_rows = (1/N) Σᵢ 𝟙[ |Rᵢ| = |Gᵢ| ]
```

**Uso.** Diagnóstico intermediário: o modelo acertou o volume de resposta, mas não necessariamente o conteúdo.

**Resultado E1:** 85,0%.

---

### 4.3 EX por cardinalidade de colunas (EX-COL, `ex_cols`)

```
EX_cols = (1/N) Σᵢ 𝟙[ |cols(Rᵢ)| = |cols(Gᵢ)| ]
```

**Resultado E1:** 83,75%.

---

### 4.4 EX por conteúdo sem nomes de coluna (EX-C, `ex_content`)

**Definição.** Exige |Rᵢ| = |Gᵢ|, |cols(Rᵢ)| = |cols(Gᵢ)| e igualdade dos multiconjuntos de **tuplas de valores normalizados por linha** (valores ordenados por chave de tipo, nomes de coluna ignorados).

**Embasamento.** Extensão motivada por consultas abertas em que o cidadão não especifica aliases SQL. Trabalhos sobre equivalência de resultados (e.g. análise de queries em BIRD, Li et al., 2024) reconhecem que comparação estrita superestima erros quando múltiplas projeções são válidas.

**Limitação declarada.** Não distingue permutação semântica entre colunas de mesmo tipo na mesma linha (caso raro nos dados municipais tabulares).

**Resultado E1:** 58,75%.

---

### 4.5 EX por mapeamento de colunas (EX-M, `ex_colmap`)

**Definição.** Exige mesma cardinalidade de linhas e colunas. Para cada coluna, constrói-se o vetor de valores ao longo das linhas (linhas ordenadas por assinatura de valores). Compara-se o multiconjunto desses vetores entre *Rᵢ* e *Gᵢ*.

**Interpretação.** Duas colunas com nomes distintos mas mesmos valores em todas as linhas são consideradas equivalentes. Esta variante isola divergências por **alias de coluna**, frequentemente introduzidas na elaboração manual do golden dataset e não na interpretação do cidadão.

**Flag `alias_apenas`.** Verdadeiro quando EX estrita falha, EX-M passa e a classificação de divergência é `alias_colunas`.

**Resultado E1:** 58,75% (idêntico a EX-C neste corpus); **48,75%** das consultas falham na EX estrita **somente** por alias/colunas equivalentes.

---

### 4.6 Taxonomia de divergência (`divergencia_ex`)

Quando EX estrita falha e VSR = 1:

| Código | Significado | Atribuição típica |
|--------|-------------|-------------------|
| `cardinalidade_linhas` | \|R\| ≠ \|G\| | Modelo ou referência |
| `cardinalidade_colunas` | número de colunas distinto | Modelo (projeção incorreta) |
| `alias_colunas` | EX-M passa, EX estrita falha | **Gabarito** (nomenclatura) ou modelo |
| `permutacao_valores_linha` | EX-C passa, EX-M falha | Caso limítrofe |
| `divergencia_valores` | conteúdo efetivamente distinto | Modelo |

---

## 5. Demais métricas

### 5.1 VSR (Valid SQL Rate)

```
VSR = (1/N) Σᵢ 𝟙[ SQL gerada executa sem erro no DuckDB ]
```

**Embasamento.** Indicador de correção sintática e aderência ao esquema (Zhong et al., 2017; Yu et al., 2018).

**Resultado E1:** 97,5%.

---

### 5.2 NEA (Non-Empty Answer)

```
NEA_ok(i) = 𝟙[ |Gᵢ| > 0 ]  se espera_dados = sim
           = 1              caso contrário
```

**Resultado E1:** conforme agregação em `metrics_summary_v2.json`.

---

### 5.3 TSA (Table Selection Accuracy)

```
TSA(i) = 𝟙[ tabelas_esperadas ⊆ tabelas(parse(SQL gerada)) ]
```

`parse` extrai identificadores após `FROM` e `JOIN`.

**Resultado E1:** 95,0% (média de acertos por consulta no recálculo v2).

---

### 5.4 CHS (Condition Heuristic Score)

```
CHS(i) = (1/|Cᵢ|) Σ_{c ∈ Cᵢ} 𝟙[ c aparece como substring na SQL gerada ]
```

onde *Cᵢ* = lista em `condicao_esperada` do golden (filtros de ano, órgão, etc.).

**Interpretação.** Heurística de cobertura de filtros, não equivalência lógica.

**Resultado E1:** média 0,9313.

---

### 5.5 JAR (Judge Agreement Rate)

Avaliação em amostra estratificada (*M* = 30) com LLM-as-Judge sobre resposta em linguagem natural.

```
JAR = (1/M) Σⱼ 𝟙[ vereditoⱼ ∈ {correto, parcialmente_correto} ]
```

**Embasamento.** Complemento qualitativo quando EX estrita subestima utilidade percebida (Zheng et al., 2023, LLM-as-judge; rubrica em `avaliacao/eval/prompts/judge_rubric.txt`).

**Resultado publicado (E1, n=30):** JAR = 100%, EX estrita = 20% na mesma amostra.

---

---

## 4.7 EX por projeção (EX proj, `ex_proj`)

**Definição.** Exige |Rᵢ| = |Gᵢ| e |cols(Gᵢ)| ≥ |cols(Rᵢ)|. Para cada coluna do gabarito, deve existir alguma coluna no resultado gerado com o **mesmo vetor de valores** (linhas ordenadas por assinatura de conteúdo).

**Uso.** Perguntas abertas em que o modelo pode incluir colunas de contextualização (`receita_corrente`, `nom_credor`) além do pedido.

**Resultado E1:** 60,0%.

---

## 4.8 EX com tolerância numérica (EX proj + τ, `ex_proj_tol`)

**Definição.** Igual a EX proj, porém células numéricas aceitas se:

```
|ν(a) − ν(b)| / max(|ν(b)|, ε) ≤ τ,   τ = 0,005 (0,5%)
```

**Resultado E1:** 61,25%.

---

## 4.9 Equivalência funcional (`ex_resposta` / EF)

**Nome no texto acadêmico:** Equivalência funcional (EF).

**Definição.** EX proj + τ restrito às colunas em `colunas_resposta` no golden (chaves + grandezas de resposta). Campo preenchido por `avaliacao/scripts/enrich_golden_colunas.py` (75/80 consultas).

**Interpretação.** Mede se o cidadão receberia as grandezas corretas, independentemente de aliases ou colunas auxiliares. É o indicador de **equivalência funcional** adotado nos textos do trabalho para reportar acurácia de resultado.

**Resultado E1:** **62,50%**.

---

## 6. Tabela comparativa dos experimentos (v3)

| Experimento | EX resposta | EX proj | EX proj+τ | EX colmap | EX estrita | VSR |
|-------------|------------:|--------:|----------:|----------:|-----------:|----:|
| E1 baseline | **62,50** | 60,00 | 61,25 | 57,50 | 10,00 | 97,50 |
| E2 contexto estendido | 58,75 | 56,25 | 57,50 | 56,25 | 13,75 | 96,25 |
| E3 GPT-4o-mini | 46,25 | 46,25 | 46,25 | 46,25 | 12,50 | 90,00 |
| E4 GPT-4o | 57,50 | 57,50 | 57,50 | 56,25 | 16,25 | 97,50 |

Fonte: `avaliacao/experimentos/comparison_table.md`.

---

## 7. Referências bibliográficas

- LI, J. et al. **BIRD**: A Big Bench for Large-Scale Database Grounded Text-to-SQL. NeurIPS Datasets and Benchmarks, 2024.
- YU, T. et al. **Spider**: A Large-Scale Human-Labeled Dataset for Complex and Cross-Domain Semantic Parsing and Text-to-SQL Task. EMNLP, 2018.
- ZHONG, V. et al. **Seq2SQL**: Generating Structured Queries from Natural Language using Reinforcement Learning. arXiv:1709.00103, 2017.
- DENG, C. et al. Structure-Grounded Pretraining for Text-to-SQL. NAACL, 2021.
- ZHENG, L. et al. Judging LLM-as-a-Judge with MT-Bench and Chatbot Arena. NeurIPS, 2023.

---

## 8. Arquivos relacionados

| Caminho | Conteúdo |
|---------|----------|
| `avaliacao/eval/lib/compare.py` | Implementação |
| `avaliacao/scripts/recompute_metrics.py` | Recálculo por experimento |
| `avaliacao/experimentos/*/metrics_summary_v3.json` | Agregados v3 |
| `avaliacao/experimentos/*/metricas_por_caso/` | Detalhe por `id_teste` |
| `avaliacao/dados/golden/reference_results/` | Resultado tabular de cada SQL de referência |
| `avaliacao/experimentos/*/generated_results/` | Resultado tabular de cada SQL gerada |
