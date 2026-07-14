# Guia das Métricas — Avaliação Text-to-SQL CIC

Documento de apoio à escrita do TCC. Versão em LaTeX: `tcc-latex/secoes/metricas.tex`.

---

## Para quem não é da área (resumo rápido)

| Métrica | Em uma frase | Analogia |
|---------|--------------|----------|
| **EX** | A resposta numérica bate com a gabarito? | Você pediu o total de gastos de 2024 e o sistema trouxe **os mesmos números** que o especialista esperava. |
| **VSR** | O banco aceitou a consulta sem erro? | A “receita” roda na cozinha sem queimar — mas o prato pode não ser o pedido. |
| **NEA** | Veio algum dado quando devia vir? | Não adianta a consulta funcionar se a lista veio **vazia** sem motivo. |
| **TSA** | Usou as tabelas certas? | Para falar de frota, foi na tabela de frota — não na de licitação. |
| **CHS** | Colocou os filtros importantes? | Ano 2025, órgão, tipo de despesa aparecem na consulta? |
| **Latência** | Quanto tempo o usuário espera? | Importante no WhatsApp. |
| **Custo (USD)** | Quanto custa cada pergunta na API? | Define qual modelo dá para manter em produção. |

**Padrão dos nossos experimentos:** VSR ~90–97%, EX ~11–19%.  
Tradução: o modelo **quase sempre fala SQL válido**, mas **raramente reproduz exatamente** a mesma resposta que anotamos como referência.

---

## Definições técnicas

### EX — Execution Accuracy

- **Fórmula:** fração de consultas em que `multiset(R_gen) == multiset(R_ref)` após normalização.
- **Implementação:** `eval/lib/compare.py` → `execution_match()`.
- **Por que:** padrão Spider/BIRD; mede utilidade prática do resultado.
- **Inferência:** EX baixo + VSR alto = erro de **intenção**, não de sintaxe. Não reconhece SQLs alternativas corretas.

### VSR — Valid SQL Rate

- **Fórmula:** fração de `S_gen` que executam sem exceção no DuckDB.
- **Por que:** separa “não compila” de “compila mas erra o sentido”.
- **Inferência:** valida eficácia do prompt/DDL; quedas em modelo menor indicam limite em joins complexos.

### NEA — Non-Empty Answer

- **Fórmula:** `|R_gen| > 0` quando `espera_dados=sim`.
- **Por que:** detecta filtros errados que zeram resultado.
- **Inferência:** complementa EX em diagnóstico por categoria.

### TSA — Table Selection Accuracy

- **Fórmula:** todas as tabelas em `tabelas_esperadas` aparecem em FROM/JOIN de `S_gen`.
- **Por que:** erro de tabela é grave em esquema grande.
- **Inferência:** TSA alto + EX baixo = lógica errada nas tabelas certas.

### CHS — Condition Heuristic Score

- **Fórmula:** proporção de substrings em `condicao_esperada` presentes em `S_gen`.
- **Por que:** proxy barato de “regra de negócio”.
- **Inferência:** útil para RH, órgãos, categorias fiscais.

### Operacionais

- **gen_ms:** tempo OpenAI; **sql_gen_ms:** tempo DuckDB.
- **tokens/custo:** `eval/lib/budget.py` com preços em `config.yaml`.

---

## O que os números dos 4 experimentos nos dizem

| Experimento | EX | VSR | Leitura |
|-------------|-----|-----|---------|
| baseline_16k | 11,25% | 97,5% | Produção atual: SQL quase sempre válida, poucas coincidências exatas. |
| precagada_30k | 15,0% | 96,25% | Mais contexto ajuda um pouco o EX, custa ~2×. |
| gpt4o_mini | 16,25% | 90,0% | Melhor EX barato; perde robustez (VSR). |
| gpt4o | 18,75% | 97,5% | Melhor EX geral; caro e lento para WhatsApp. |

---

## LLM-as-Judge

**Status:** não implementado. Ver `docs/PLANO_LLM_JUDGE.md`.
