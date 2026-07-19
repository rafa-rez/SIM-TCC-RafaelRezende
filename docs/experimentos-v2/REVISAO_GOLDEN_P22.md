# Revisão do golden v1 (P2.2) — casos suspeitos

Gerado em 2026-07-18 a partir da comparação caso a caso de 5 variantes de prompt
(E1, e5, e6, e7, e8). **Nenhuma alteração foi aplicada ao golden v1 (congelado)** —
este documento é insumo para a curadoria do golden v2.

## Método

Um caso que falha em EX resposta em **todas** as 5 variantes de prompt tem duas
explicações possíveis: é genuinamente difícil, ou o gabarito/critério pune uma
interpretação razoável. Os 13 casos abaixo estão nessa condição.

## Inconsistências de convenção comprovadas (pares contraditórios)

| Par | Contradição |
|-----|-------------|
| **011 × 042** | Ambos somam `vlr_pag_fonte` de `despesa_pagamento`. A ref do 011 exige `nom_credor NOT ILIKE '%NAO INFORMADO%'`; a do 042 exige **sem** filtro. Impossível satisfazer os dois com uma regra geral. |
| **020 × 046/051** | A ref do 020 ("valor total de contratos assinados em 2025") exige `cod_orgao IN (2,4,5)` sem a pergunta citar prefeitura; as refs do 046 ("contratos ativos") e 051 ("contratos acima de 1 milhão") **não** filtram órgão. |
| **069 × 074 × 076** | Cruzamentos SICOM×CGU análogos com convenções distintas de LIMIT (10 / 20 / 5) e de filtros de higiene (com/sem `num_doc NOT IN`). |

## Os 13 casos que falham em todos os prompts

| id | Pergunta (resumo) | Hipótese da causa |
|----|-------------------|-------------------|
| 002 | Servidores ativos 2024 × Novo Bolsa Família | Ref agrupa por NOME; qualquer variação (seq_dim_pessoa, colunas extras) falha por cardinalidade. Interpretação única não sinalizada no enunciado. |
| 008 | Contratos 2023 com aditivo 2024 | Ref exige `num_ano_contrato` + ano do termo dentro do ON (LEFT JOIN com filtro no ON — padrão não óbvio). |
| 020 | Total contratos assinados 2025 | Convenção implícita de prefeitura (ver inconsistência acima). |
| 034 | Total pago por função Saúde | Ref usa `despesa_pagamento JOIN empenho_empenho`; a tabela setorial `saude_saude` é uma leitura igualmente razoável. |
| 058 | Servidores ativos × BPC | Mesma família do 002; ref sem LIMIT, saída de 3 colunas exatas. |
| 065 | Total gasto com frota | Ref é SUM direto; qualquer filtro de higiene (que o prompt v1 mandava aplicar!) diverge. |
| 069 | Pagamentos × CGU despesas | LIMIT 10 + colunas exatas; convenção divergente dos irmãos 074/076. |
| 070 | Servidores × Auxílio Brasil | Mesma família do 002. |
| 074 | Credores × Bolsa Família por nome | LIMIT 20 arbitrário. |
| 075 | FPM e ICMS | Ref exige GROUP BY por programa (2 linhas); total único é leitura defensável do "quanto recebeu". |
| 076 | Top 5 fornecedores × CGU | Colunas exatas (nome + 2 totais). |
| 078 | Top 5 licitações por ano (ROW_NUMBER) | Ref agrupa por `seq_licitacao + nom_vencedor` e exibe vencedor; enunciado não diz quais colunas. |
| 079 | Contratos 2022 com aditivo 2023 | Igual ao 008. |

## Recomendações para o golden v2

1. **Fixar convenções globais por escrito** (e aplicá-las uniformemente): quando
   filtrar higiene, quando filtrar órgão, LIMIT padrão por tipo de pergunta,
   colunas mínimas aceitas.
2. Usar `colunas_resposta` de forma consistente e aceitar superconjunto de
   colunas quando o enunciado é aberto (EF principal já tolera alias; falta
   tolerar coluna extra informativa).
3. Nos cruzamentos SICOM×CGU, padronizar: chave única de agregação, totais dos
   dois lados, sem LIMIT (ou LIMIT único documentado).
4. Reevaluar os pares 011×042 e 020×046/051 — um lado de cada par precisa mudar.
