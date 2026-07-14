#!/usr/bin/env python3
"""Gera golden_dataset_v1.0.csv com 80 consultas validadas."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from eval.lib.config_loader import load_config
from eval.lib.duckdb_client import execute_query

OUT = REPO / "eval" / "datasets" / "golden_dataset_v1.0.csv"

# 10 originais — mantidos via leitura ou embutidos nas extensões
BASE_ROWS: list[dict] = []

EXTRA: list[dict] = [
    # --- Consulta Simples (11-22) ---
    {"id": 11, "dif": "facil", "cat": "Consulta Simples", "q": "Qual foi o total de pagamentos realizados pela prefeitura em 2024?",
     "sql": "SELECT SUM(CAST(vlr_pag_fonte AS DECIMAL)) AS total_pagamentos FROM despesa_pagamento WHERE ano_referencia = 2024 AND COALESCE(nom_credor, '') NOT ILIKE '%NAO INFORMADO%';",
     "tab": "despesa_pagamento", "cond": "SUM, CAST", "esp": "sim"},
    {"id": 12, "dif": "facil", "cat": "Consulta Simples", "q": "Quanto o município recebeu em transferências federais em 2023?",
     "sql": 'SELECT SUM(CAST("VALOR TRANSFERIDO" AS DECIMAL)) AS total FROM cgu_transferencias WHERE ano_referencia = 2023;',
     "tab": "cgu_transferencias", "cond": "VALOR TRANSFERIDO", "esp": "sim"},
    {"id": 13, "dif": "facil", "cat": "Consulta Simples", "q": "Quantos empenhos foram registrados em 2025?",
     "sql": "SELECT COUNT(*) AS qtd_empenhos FROM empenho_empenho WHERE ano_referencia = 2025;",
     "tab": "empenho_empenho", "cond": "COUNT", "esp": "sim"},
    {"id": 14, "dif": "facil", "cat": "Consulta Simples", "q": "Qual o total liquidado em despesas em 2024?",
     "sql": "SELECT SUM(CAST(vlr_liquidado AS DECIMAL)) AS total_liquidado FROM despesa_despesa WHERE ano_referencia = 2024;",
     "tab": "despesa_despesa", "cond": "SUM", "esp": "sim"},
    {"id": 15, "dif": "facil", "cat": "Consulta Simples", "q": "Qual a receita corrente realizada em 2025?",
     "sql": "SELECT SUM(CAST(vlr_realizadoateperiodo AS DECIMAL)) AS receita_corrente FROM receita_receita WHERE ano_referencia = 2025 AND dsc_cateconomica ILIKE '%Corrente%';",
     "tab": "receita_receita", "cond": "Corrente", "esp": "sim"},
    {"id": 16, "dif": "facil", "cat": "Consulta Simples", "q": "Liste os 5 maiores credores por valor pago em 2025.",
     "sql": "SELECT nom_credor, SUM(CAST(vlr_pag_fonte AS DECIMAL)) AS total FROM despesa_pagamento WHERE ano_referencia = 2025 AND COALESCE(nom_credor, '') NOT ILIKE '%NAO INFORMADO%' AND num_doc_credor NOT IN ('-1', '0') GROUP BY nom_credor ORDER BY total DESC LIMIT 5;",
     "tab": "despesa_pagamento", "cond": "LIMIT, NOT ILIKE", "esp": "sim"},
    {"id": 17, "dif": "facil", "cat": "Consulta Simples", "q": "Qual o total pago em saúde em 2024?",
     "sql": "SELECT SUM(CAST(vlr_pagamento AS DECIMAL)) AS total_saude FROM saude_saude WHERE ano_referencia = 2024;",
     "tab": "saude_saude", "cond": "SUM", "esp": "sim"},
    {"id": 18, "dif": "facil", "cat": "Consulta Simples", "q": "Quantos beneficiários do BPC constam em 2024?",
     "sql": 'SELECT COUNT(DISTINCT "NIS BENEFICIÁRIO") AS qtd FROM cgu_bpc WHERE ano_referencia = 2024;',
     "tab": "cgu_bpc", "cond": "COUNT", "esp": "sim"},
    {"id": 19, "dif": "facil", "cat": "Consulta Simples", "q": "Qual o total de despesas empenhadas em educação em 2023?",
     "sql": "SELECT SUM(CAST(vlr_empenho AS DECIMAL)) AS total FROM educacao_educacao WHERE ano_referencia = 2023;",
     "tab": "educacao_educacao", "cond": "SUM", "esp": "sim"},
    {"id": 20, "dif": "facil", "cat": "Consulta Simples", "q": "Qual o valor total de contratos assinados em 2025?",
     "sql": "SELECT SUM(CAST(vlr_contrato AS DECIMAL)) AS total FROM contrato_contratos WHERE ano_referencia = 2025 AND CAST(cod_orgao AS INTEGER) IN (2, 4, 5);",
     "tab": "contrato_contratos", "cond": "IN (2, 4, 5)", "esp": "sim"},
    {"id": 21, "dif": "facil", "cat": "Consulta Simples", "q": "Quantas licitações foram abertas em 2024?",
     "sql": "SELECT COUNT(DISTINCT seq_licitacao) AS qtd FROM licitacao_licitacao WHERE ano_referencia = 2024;",
     "tab": "licitacao_licitacao", "cond": "COUNT", "esp": "sim"},
    {"id": 22, "dif": "facil", "cat": "Consulta Simples", "q": "Qual o total de Auxílio Brasil pago em 2022?",
     "sql": 'SELECT SUM(CAST("VALOR PARCELA" AS DECIMAL)) AS total FROM cgu_auxilio_brasil WHERE ano_referencia = 2022;',
     "tab": "cgu_auxilio_brasil", "cond": "SUM", "esp": "sim"},
    # --- Cálculo Fiscal (23-32) ---
    {"id": 23, "dif": "medio", "cat": "Calculo Fiscal", "q": "Calcule o Índice de Saúde (IES-C) do município em 2024.",
     "sql": "WITH desp AS (SELECT SUM(CAST(vlr_liquidado AS DECIMAL)) AS d FROM saude_saude WHERE ano_referencia = 2024), rec AS (SELECT SUM(CAST(vlr_realizadoateperiodo AS DECIMAL)) AS r FROM receita_receita WHERE ano_referencia = 2024 AND (dsc_especie ILIKE '%Imposto%' OR dsc_origem ILIKE '%Transfer%')) SELECT (desp.d / NULLIF(rec.r, 0)) * 100 AS ies_c FROM desp, rec;",
     "tab": "saude_saude, receita_receita", "cond": "NULLIF, * 100", "esp": "sim"},
    {"id": 24, "dif": "medio", "cat": "Calculo Fiscal", "q": "Qual o ISF-M da prefeitura no ano de 2024?",
     "sql": "WITH receita_corrente AS (SELECT SUM(CAST(vlr_realizadoateperiodo AS DECIMAL)) AS r FROM receita_receita WHERE ano_referencia = 2024 AND dsc_cateconomica ILIKE '%Corrente%'), despesa_corrente AS (SELECT SUM(CAST(vlr_liquidado AS DECIMAL)) AS d FROM despesa_despesa WHERE ano_referencia = 2024 AND dsc_naturezadespesa LIKE '3%') SELECT r / NULLIF(d, 0) AS isf_m FROM receita_corrente, despesa_corrente;",
     "tab": "receita_receita, despesa_despesa", "cond": "NULLIF, Corrente", "esp": "sim"},
    {"id": 25, "dif": "medio", "cat": "Calculo Fiscal", "q": "Qual o IEQ-C em 2025 e se atingiu 25%?",
     "sql": "WITH despesa_mde AS (SELECT SUM(CAST(vlr_liquidado AS DECIMAL)) AS total_despesa FROM educacao_educacao WHERE ano_referencia = 2025), receita_impostos AS (SELECT SUM(CAST(vlr_realizadoateperiodo AS DECIMAL)) AS total_receita FROM receita_receita WHERE ano_referencia = 2025 AND dsc_especie ILIKE '%Imposto%') SELECT (d.total_despesa / NULLIF(r.total_receita, 0)) * 100 AS ieq_c, CASE WHEN (d.total_despesa / NULLIF(r.total_receita, 0)) * 100 >= 25 THEN 'Sim' ELSE 'Não' END AS atingiu FROM despesa_mde d, receita_impostos r;",
     "tab": "educacao_educacao, receita_receita", "cond": "Imposto, * 100", "esp": "sim"},
    {"id": 26, "dif": "medio", "cat": "Calculo Fiscal", "q": "Qual a porcentagem do gasto da Câmara sobre a receita corrente em 2024?",
     "sql": "WITH receita_corrente AS (SELECT SUM(CAST(vlr_realizadoateperiodo AS DECIMAL)) AS r FROM receita_receita WHERE dsc_cateconomica ILIKE '%Corrente%' AND ano_referencia = 2024), despesas_camara AS (SELECT SUM(CAST(vlr_liquidado AS DECIMAL)) AS d FROM despesa_despesa WHERE cod_orgao = 1 AND ano_referencia = 2024) SELECT (d / NULLIF(r, 0)) * 100 AS iev_c FROM despesas_camara, receita_corrente;",
     "tab": "despesa_despesa, receita_receita", "cond": "cod_orgao, Corrente", "esp": "sim"},
    {"id": 27, "dif": "medio", "cat": "Calculo Fiscal", "q": "Calcule o ISF-M para cada ano entre 2022 e 2024.",
     "sql": "WITH receita_corrente AS (SELECT ano_referencia, SUM(CAST(vlr_realizadoateperiodo AS DECIMAL)) AS r FROM receita_receita WHERE dsc_cateconomica ILIKE '%Corrente%' AND ano_referencia BETWEEN 2022 AND 2024 GROUP BY ano_referencia), despesa_corrente AS (SELECT ano_referencia, SUM(CAST(vlr_liquidado AS DECIMAL)) AS d FROM despesa_despesa WHERE dsc_naturezadespesa LIKE '3%' AND ano_referencia BETWEEN 2022 AND 2024 GROUP BY ano_referencia) SELECT rc.ano_referencia, rc.r / NULLIF(dc.d, 0) AS isf_m FROM receita_corrente rc JOIN despesa_corrente dc ON rc.ano_referencia = dc.ano_referencia ORDER BY rc.ano_referencia;",
     "tab": "receita_receita, despesa_despesa", "cond": "BETWEEN, NULLIF", "esp": "sim"},
    {"id": 28, "dif": "medio", "cat": "Calculo Fiscal", "q": "Qual o total de receita de impostos em 2023?",
     "sql": "SELECT SUM(CAST(vlr_realizadoateperiodo AS DECIMAL)) AS total_impostos FROM receita_receita WHERE ano_referencia = 2023 AND dsc_especie ILIKE '%Imposto%';",
     "tab": "receita_receita", "cond": "Imposto", "esp": "sim"},
    {"id": 29, "dif": "medio", "cat": "Calculo Fiscal", "q": "Qual o total de despesa corrente liquidada em 2025?",
     "sql": "SELECT SUM(CAST(vlr_liquidado AS DECIMAL)) AS total FROM despesa_despesa WHERE ano_referencia = 2025 AND dsc_naturezadespesa LIKE '3%';",
     "tab": "despesa_despesa", "cond": "3%", "esp": "sim"},
    {"id": 30, "dif": "medio", "cat": "Calculo Fiscal", "q": "Qual o gasto total em educação liquidado em 2024?",
     "sql": "SELECT SUM(CAST(vlr_liquidado AS DECIMAL)) AS total FROM educacao_educacao WHERE ano_referencia = 2024;",
     "tab": "educacao_educacao", "cond": "SUM", "esp": "sim"},
    {"id": 31, "dif": "medio", "cat": "Calculo Fiscal", "q": "Qual o gasto em saúde liquidado em 2025?",
     "sql": "SELECT SUM(CAST(vlr_liquidado AS DECIMAL)) AS total FROM saude_saude WHERE ano_referencia = 2025;",
     "tab": "saude_saude", "cond": "SUM", "esp": "sim"},
    {"id": 32, "dif": "dificil", "cat": "Calculo Fiscal", "q": "Calcule o ISF-M ano a ano de 2020 a 2023.",
     "sql": "WITH receita_corrente AS (SELECT ano_referencia, SUM(CAST(vlr_realizadoateperiodo AS DECIMAL)) AS r FROM receita_receita WHERE dsc_cateconomica ILIKE '%Corrente%' AND ano_referencia BETWEEN 2020 AND 2023 GROUP BY ano_referencia), despesa_corrente AS (SELECT ano_referencia, SUM(CAST(vlr_liquidado AS DECIMAL)) AS d FROM despesa_despesa WHERE dsc_naturezadespesa LIKE '3%' AND ano_referencia BETWEEN 2020 AND 2023 GROUP BY ano_referencia) SELECT rc.ano_referencia, rc.r / NULLIF(dc.d, 0) AS isf_m FROM receita_corrente rc JOIN despesa_corrente dc ON rc.ano_referencia = dc.ano_referencia ORDER BY rc.ano_referencia;",
     "tab": "receita_receita, despesa_despesa", "cond": "BETWEEN, NULLIF", "esp": "sim"},
    # --- Despesas/Pagamentos (33-42) ---
    {"id": 33, "dif": "medio", "cat": "Despesas Pagamentos", "q": "Quais os 10 maiores pagamentos em 2025 com nome do credor?",
     "sql": "SELECT nom_credor, SUM(CAST(vlr_pag_fonte AS DECIMAL)) AS total FROM despesa_pagamento WHERE ano_referencia = 2025 AND COALESCE(nom_credor, '') NOT ILIKE '%NAO INFORMADO%' GROUP BY nom_credor ORDER BY total DESC LIMIT 10;",
     "tab": "despesa_pagamento", "cond": "NOT ILIKE, LIMIT", "esp": "sim"},
    {"id": 34, "dif": "medio", "cat": "Despesas Pagamentos", "q": "Qual o total pago por função de Saúde em 2024?",
     "sql": "SELECT SUM(CAST(vlr_pag_fonte AS DECIMAL)) AS total FROM despesa_pagamento dp JOIN empenho_empenho e ON dp.seq_empenho = e.seq_empenho AND dp.ano_referencia = e.ano_referencia WHERE dp.ano_referencia = 2024 AND e.dsc_funcao ILIKE '%SAÚDE%';",
     "tab": "despesa_pagamento, empenho_empenho", "cond": "JOIN, ano_referencia", "esp": "sim"},
    {"id": 35, "dif": "medio", "cat": "Despesas Pagamentos", "q": "Liste os 5 empenhos de maior valor em 2025.",
     "sql": "SELECT num_empenho, nom_credor, CAST(vlr_empenhado AS DECIMAL) AS valor FROM empenho_empenho e JOIN empenho_credorEmpenho c ON e.seq_empenho = c.seq_empenho AND e.ano_referencia = c.ano_referencia WHERE e.ano_referencia = 2025 ORDER BY valor DESC LIMIT 5;",
     "tab": "empenho_empenho, empenho_credorEmpenho", "cond": "JOIN, LIMIT", "esp": "sim"},
    {"id": 36, "dif": "facil", "cat": "Despesas Pagamentos", "q": "Qual o total empenhado em 2024?",
     "sql": "SELECT SUM(CAST(vlr_empenhado AS DECIMAL)) AS total FROM empenho_empenho WHERE ano_referencia = 2024;",
     "tab": "empenho_empenho", "cond": "SUM", "esp": "sim"},
    {"id": 37, "dif": "medio", "cat": "Despesas Pagamentos", "q": "Quanto foi liquidado em 2025 por órgão da prefeitura (cod_orgao 2, 4, 5)?",
     "sql": "SELECT cod_orgao, SUM(CAST(vlr_liquidado AS DECIMAL)) AS total FROM despesa_despesa WHERE ano_referencia = 2025 AND CAST(cod_orgao AS INTEGER) IN (2, 4, 5) GROUP BY cod_orgao ORDER BY total DESC;",
     "tab": "despesa_despesa", "cond": "IN (2, 4, 5)", "esp": "sim"},
    {"id": 38, "dif": "facil", "cat": "Despesas Pagamentos", "q": "Quantos pagamentos foram feitos em 2023?",
     "sql": "SELECT COUNT(*) AS qtd FROM despesa_pagamento WHERE ano_referencia = 2023;",
     "tab": "despesa_pagamento", "cond": "COUNT", "esp": "sim"},
    {"id": 39, "dif": "medio", "cat": "Despesas Pagamentos", "q": "Qual o total pago a pessoal via empenhos em 2024?",
     "sql": "SELECT SUM(CAST(vlr_empenhado AS DECIMAL)) AS total FROM empenho_empenho WHERE ano_referencia = 2024 AND dsc_naturezadespesa LIKE '3.1%';",
     "tab": "empenho_empenho", "cond": "3.1", "esp": "sim"},
    {"id": 40, "dif": "medio", "cat": "Despesas Pagamentos", "q": "Liste os 5 fornecedores com mais empenhos em 2025.",
     "sql": "SELECT nom_credor, SUM(CAST(vlr_empenhado AS DECIMAL)) AS total FROM empenho_empenho e JOIN empenho_credorEmpenho c ON e.seq_empenho = c.seq_empenho AND e.ano_referencia = c.ano_referencia WHERE e.ano_referencia = 2025 AND COALESCE(c.nom_credor, '') NOT ILIKE '%NAO INFORMADO%' GROUP BY nom_credor ORDER BY total DESC LIMIT 5;",
     "tab": "empenho_empenho, empenho_credorEmpenho", "cond": "GROUP BY, LIMIT", "esp": "sim"},
    {"id": 41, "dif": "facil", "cat": "Despesas Pagamentos", "q": "Qual o valor total recebido via CGU despesas favorecidos em 2024?",
     "sql": 'SELECT SUM(CAST("Valor Recebido" AS DECIMAL)) AS total FROM cgu_despesas_favorecidos WHERE ano_referencia = 2024;',
     "tab": "cgu_despesas_favorecidos", "cond": "SUM", "esp": "sim"},
    {"id": 42, "dif": "medio", "cat": "Despesas Pagamentos", "q": "Compare total pago em 2024 e 2025.",
     "sql": "SELECT ano_referencia, SUM(CAST(vlr_pag_fonte AS DECIMAL)) AS total FROM despesa_pagamento WHERE ano_referencia IN (2024, 2025) GROUP BY ano_referencia ORDER BY ano_referencia;",
     "tab": "despesa_pagamento", "cond": "GROUP BY", "esp": "sim"},
    # --- Licitações/Contratos (43-52) ---
    {"id": 43, "dif": "medio", "cat": "Licitacoes Contratos", "q": "Quais os 5 maiores contratos da prefeitura em 2025?",
     "sql": "SELECT cc.dsc_nome_credor, c.vlr_contrato FROM contrato_contratos c LEFT JOIN contrato_contContrato cc ON c.seq_contrato = cc.seq_contrato AND c.ano_referencia = cc.ano_referencia WHERE c.ano_referencia = 2025 AND CAST(c.cod_orgao AS INTEGER) IN (2, 4, 5) AND c.vlr_contrato > 0 ORDER BY c.vlr_contrato DESC LIMIT 5;",
     "tab": "contrato_contratos, contrato_contContrato", "cond": "IN (2, 4, 5), LIMIT", "esp": "sim"},
    {"id": 44, "dif": "medio", "cat": "Licitacoes Contratos", "q": "Quantas dispensas de licitação em 2024?",
     "sql": "SELECT COUNT(DISTINCT seq_dispensa) AS qtd FROM licitacao_dispensa WHERE ano_referencia = 2024;",
     "tab": "licitacao_dispensa", "cond": "COUNT", "esp": "sim"},
    {"id": 45, "dif": "medio", "cat": "Licitacoes Contratos", "q": "Liste os 5 vencedores com maior valor homologado em 2024.",
     "sql": "WITH lic AS (SELECT h.nom_vencedor, SUM(CAST(h.vlr_global AS DECIMAL)) AS total FROM licitacao_homologLicitacao h WHERE h.ano_referencia = 2024 AND h.vlr_global > 0 GROUP BY h.seq_licitacao, h.nom_vencedor) SELECT nom_vencedor, total FROM lic ORDER BY total DESC LIMIT 5;",
     "tab": "licitacao_homologLicitacao", "cond": "GROUP BY, SUM", "esp": "sim"},
    {"id": 46, "dif": "facil", "cat": "Licitacoes Contratos", "q": "Quantos contratos ativos em 2025?",
     "sql": "SELECT COUNT(*) AS qtd FROM contrato_contratos WHERE ano_referencia = 2025;",
     "tab": "contrato_contratos", "cond": "COUNT", "esp": "sim"},
    {"id": 47, "dif": "medio", "cat": "Licitacoes Contratos", "q": "Qual o valor total homologado em licitações em 2025?",
     "sql": "SELECT SUM(CAST(vlr_global AS DECIMAL)) AS total FROM licitacao_homologLicitacao WHERE ano_referencia = 2025;",
     "tab": "licitacao_homologLicitacao", "cond": "SUM", "esp": "sim"},
    {"id": 48, "dif": "dificil", "cat": "Licitacoes Contratos", "q": "Top 3 licitações por valor em 2024 com nome do vencedor.",
     "sql": "WITH licitacoes AS (SELECT h.nom_vencedor, SUM(CAST(h.vlr_global AS DECIMAL)) AS total_valor FROM licitacao_homologLicitacao h JOIN licitacao_licitacao l ON h.seq_licitacao = l.seq_licitacao AND h.ano_referencia = l.ano_referencia WHERE h.ano_referencia = 2024 GROUP BY h.seq_licitacao, h.nom_vencedor) SELECT nom_vencedor, total_valor FROM licitacoes ORDER BY total_valor DESC LIMIT 3;",
     "tab": "licitacao_homologLicitacao, licitacao_licitacao", "cond": "GROUP BY, JOIN", "esp": "sim"},
    {"id": 49, "dif": "medio", "cat": "Licitacoes Contratos", "q": "Qual o total de aditivos contratuais em 2024?",
     "sql": "SELECT SUM(CAST(vlr_termo_aditivo AS DECIMAL)) AS total FROM contrato_termoContrato WHERE ano_referencia = 2024;",
     "tab": "contrato_termoContrato", "cond": "SUM", "esp": "sim"},
    {"id": 50, "dif": "facil", "cat": "Licitacoes Contratos", "q": "Quantas licitações em modalidade pregão em 2025?",
     "sql": "SELECT COUNT(*) AS qtd FROM licitacao_licitacao WHERE ano_referencia = 2025 AND dsc_modalidade ILIKE '%PREG%';",
     "tab": "licitacao_licitacao", "cond": "ILIKE", "esp": "sim"},
    {"id": 51, "dif": "medio", "cat": "Licitacoes Contratos", "q": "Liste contratos com valor acima de 1 milhão em 2025.",
     "sql": "SELECT num_contrato, vlr_contrato FROM contrato_contratos WHERE ano_referencia = 2025 AND CAST(vlr_contrato AS DECIMAL) > 1000000 ORDER BY vlr_contrato DESC LIMIT 20;",
     "tab": "contrato_contratos", "cond": "LIMIT", "esp": "sim"},
    {"id": 52, "dif": "medio", "cat": "Licitacoes Contratos", "q": "Qual empresa venceu mais licitações em 2025?",
     "sql": "SELECT nom_vencedor, COUNT(DISTINCT seq_licitacao) AS qtd FROM licitacao_homologLicitacao WHERE ano_referencia = 2025 AND COALESCE(nom_vencedor, '') NOT ILIKE '%NAO INFORMADO%' GROUP BY nom_vencedor ORDER BY qtd DESC LIMIT 1;",
     "tab": "licitacao_homologLicitacao", "cond": "COUNT, GROUP BY", "esp": "sim"},
    # --- RH/Folha (53-62) ---
    {"id": 53, "dif": "medio", "cat": "RH Folha", "q": "Qual o maior salário líquido pago em 2024?",
     "sql": "WITH salarios AS (SELECT seq_dim_pessoa, MAX(CAST(vlr_rmn_liquida AS DECIMAL)) AS salario FROM remuneracao_municipio WHERE ano_referencia = 2024 GROUP BY seq_dim_pessoa) SELECT MAX(salario) AS maior_salario FROM salarios;",
     "tab": "remuneracao_municipio", "cond": "MAX, GROUP BY", "esp": "sim"},
    {"id": 54, "dif": "medio", "cat": "RH Folha", "q": "Quantos servidores ativos em 2025?",
     "sql": "SELECT COUNT(DISTINCT seq_dim_pessoa) AS qtd FROM remuneracao_municipio WHERE ano_referencia = 2025 AND dsc_sit_serv_pens ILIKE '%Ativo%';",
     "tab": "remuneracao_municipio", "cond": "Ativo, ILIKE", "esp": "sim"},
    {"id": 55, "dif": "medio", "cat": "RH Folha", "q": "Qual o total da folha de pagamento em 2024?",
     "sql": "WITH mensal AS (SELECT seq_dim_pessoa, CAST(SUBSTR(CAST(seq_dat_pagamento AS VARCHAR), 5, 2) AS INTEGER) AS mes, MAX(CAST(vlr_rmn_liquida AS DECIMAL)) AS sal FROM remuneracao_municipio WHERE ano_referencia = 2024 GROUP BY seq_dim_pessoa, mes) SELECT SUM(sal) AS total_folha FROM mensal;",
     "tab": "remuneracao_municipio", "cond": "SUBSTR, GROUP BY", "esp": "sim"},
    {"id": 56, "dif": "facil", "cat": "RH Folha", "q": "Liste os 5 cargos com maior remuneração média em 2024.",
     "sql": "WITH sal AS (SELECT dsc_cargo, seq_dim_pessoa, MAX(CAST(vlr_rmn_liquida AS DECIMAL)) AS salario FROM remuneracao_municipio WHERE ano_referencia = 2024 GROUP BY dsc_cargo, seq_dim_pessoa) SELECT dsc_cargo, AVG(salario) AS media FROM sal GROUP BY dsc_cargo ORDER BY media DESC LIMIT 5;",
     "tab": "remuneracao_municipio", "cond": "AVG, LIMIT", "esp": "sim"},
    {"id": 57, "dif": "medio", "cat": "RH Folha", "q": "Evolução mensal da folha em 2025.",
     "sql": "SELECT CAST(SUBSTR(CAST(seq_dat_pagamento AS VARCHAR), 5, 2) AS INTEGER) AS mes, SUM(CAST(vlr_rmn_liquida AS DECIMAL)) AS total FROM remuneracao_municipio WHERE ano_referencia = 2025 GROUP BY mes ORDER BY mes;",
     "tab": "remuneracao_municipio", "cond": "SUBSTR, CAST", "esp": "sim"},
    {"id": 58, "dif": "dificil", "cat": "RH Folha", "q": "Servidores ativos em 2025 que também recebem BPC em 2025.",
     "sql": 'WITH servidores AS (SELECT TRIM(UPPER(p.nom_pessoa)) AS nome, MAX(CAST(r.vlr_rmn_liquida AS DECIMAL)) AS salario FROM remuneracao_municipio r JOIN pessoa_municipio p ON r.seq_dim_pessoa = p.seq_dim_pessoa WHERE r.ano_referencia = 2025 AND r.dsc_sit_serv_pens ILIKE \'%Ativo%\' GROUP BY nome), bpc AS (SELECT TRIM(UPPER("NOME BENEFICIÁRIO")) AS nome, SUM(CAST("VALOR PARCELA" AS DECIMAL)) AS total FROM cgu_bpc WHERE ano_referencia = 2025 GROUP BY nome) SELECT s.nome, s.salario, b.total FROM servidores s JOIN bpc b ON s.nome = b.nome;',
     "tab": "remuneracao_municipio, cgu_bpc", "cond": "TRIM, UPPER, Ativo", "esp": "indiferente"},
    {"id": 59, "dif": "facil", "cat": "RH Folha", "q": "Quantos registros de remuneração em 2023?",
     "sql": "SELECT COUNT(*) AS qtd FROM remuneracao_municipio WHERE ano_referencia = 2023;",
     "tab": "remuneracao_municipio", "cond": "COUNT", "esp": "sim"},
    {"id": 60, "dif": "medio", "cat": "RH Folha", "q": "Média salarial dos servidores ativos em 2024.",
     "sql": "WITH sal AS (SELECT seq_dim_pessoa, MAX(CAST(vlr_rmn_liquida AS DECIMAL)) AS salario FROM remuneracao_municipio WHERE ano_referencia = 2024 AND dsc_sit_serv_pens ILIKE '%Ativo%' GROUP BY seq_dim_pessoa) SELECT AVG(salario) AS media FROM sal;",
     "tab": "remuneracao_municipio", "cond": "Ativo, AVG", "esp": "sim"},
    {"id": 61, "dif": "facil", "cat": "RH Folha", "q": "Liste 10 servidores com maiores salários em 2024.",
     "sql": "WITH sal AS (SELECT p.nom_pessoa, MAX(CAST(r.vlr_rmn_liquida AS DECIMAL)) AS salario FROM remuneracao_municipio r JOIN pessoa_municipio p ON r.seq_dim_pessoa = p.seq_dim_pessoa WHERE r.ano_referencia = 2024 GROUP BY p.nom_pessoa) SELECT nom_pessoa, salario FROM sal ORDER BY salario DESC LIMIT 10;",
     "tab": "remuneracao_municipio, pessoa_municipio", "cond": "MAX, LIMIT", "esp": "sim"},
    {"id": 62, "dif": "medio", "cat": "RH Folha", "q": "Total pago em folha extra em 2024.",
     "sql": "SELECT SUM(CAST(vlr_rmn_liquida AS DECIMAL)) AS total FROM remuneracao_municipio WHERE ano_referencia = 2024 AND dsc_tipo_pag ILIKE '%Extra%';",
     "tab": "remuneracao_municipio", "cond": "ILIKE", "esp": "sim"},
    # --- Frota (63-68) ---
    {"id": 63, "dif": "medio", "cat": "Frota", "q": "Qual veículo gastou mais combustível em 2024?",
     "sql": "SELECT f.dsc_veiculo, SUM(CAST(g.vlr_gasto AS DECIMAL)) AS total FROM frota_gastoFrota g JOIN frota_frota f ON g.seq_veiculo = f.seq_veiculo WHERE g.ano_referencia = 2024 AND (g.dsc_tipo_gasto ILIKE '%GASOLINA%' OR g.dsc_tipo_gasto ILIKE '%DIESEL%') AND COALESCE(f.dsc_marca, '') NOT ILIKE '%DIVERSOS%' GROUP BY f.dsc_veiculo ORDER BY total DESC LIMIT 1;",
     "tab": "frota_gastoFrota, frota_frota", "cond": "NOT ILIKE, JOIN", "esp": "sim"},
    {"id": 64, "dif": "facil", "cat": "Frota", "q": "Quantos veículos na frota em 2025?",
     "sql": "SELECT COUNT(DISTINCT seq_veiculo) AS qtd FROM frota_frota WHERE ano_referencia = 2025 AND COALESCE(dsc_marca, '') NOT ILIKE '%DIVERSOS%';",
     "tab": "frota_frota", "cond": "NOT ILIKE, COUNT", "esp": "sim"},
    {"id": 65, "dif": "medio", "cat": "Frota", "q": "Total gasto com frota em 2025.",
     "sql": "SELECT SUM(CAST(vlr_gasto AS DECIMAL)) AS total FROM frota_gastoFrota WHERE ano_referencia = 2025;",
     "tab": "frota_gastoFrota", "cond": "SUM", "esp": "sim"},
    {"id": 66, "dif": "medio", "cat": "Frota", "q": "Gasto com gasolina por tipo de veículo em 2024.",
     "sql": "SELECT f.dsc_tipo_veiculo, SUM(CAST(g.vlr_gasto AS DECIMAL)) AS total FROM frota_gastoFrota g JOIN frota_frota f ON g.seq_veiculo = f.seq_veiculo WHERE g.ano_referencia = 2024 AND g.dsc_tipo_gasto ILIKE '%GASOLINA%' AND COALESCE(f.dsc_marca, '') NOT ILIKE '%DIVERSOS%' GROUP BY f.dsc_tipo_veiculo;",
     "tab": "frota_gastoFrota, frota_frota", "cond": "NOT ILIKE, GROUP BY", "esp": "sim"},
    {"id": 67, "dif": "facil", "cat": "Frota", "q": "Marcas de veículos na frota em 2025 excluindo DIVERSOS.",
     "sql": "SELECT DISTINCT dsc_marca FROM frota_frota WHERE ano_referencia = 2025 AND COALESCE(dsc_marca, '') NOT ILIKE '%DIVERSOS%' AND COALESCE(dsc_num_placa, '') NOT IN ('-1', 'NÃO INFORMADO') LIMIT 50;",
     "tab": "frota_frota", "cond": "NOT ILIKE, LIMIT", "esp": "sim"},
    {"id": 68, "dif": "medio", "cat": "Frota", "q": "Km total rodado pela frota em 2025.",
     "sql": "SELECT SUM(CAST(num_marc_final AS INTEGER) - CAST(num_marc_inicial AS INTEGER)) AS km_total FROM frota_gastoFrota WHERE ano_referencia = 2025 AND num_marc_final > num_marc_inicial;",
     "tab": "frota_gastoFrota", "cond": "SUM, CAST", "esp": "sim"},
    # --- Cruzamento Investigativo (69-76) ---
    {"id": 69, "dif": "dificil", "cat": "Cruzamento Investigativo", "q": "Empresas que receberam pagamentos da prefeitura e despesas CGU em 2024.",
     "sql": 'WITH pag AS (SELECT num_doc_credor, SUM(CAST(vlr_pag_fonte AS DECIMAL)) AS total_pag FROM despesa_pagamento WHERE ano_referencia = 2024 AND num_doc_credor NOT IN (\'-1\', \'0\') GROUP BY num_doc_credor), cgu AS (SELECT "Código Favorecido" AS doc, SUM(CAST("Valor Recebido" AS DECIMAL)) AS total_cgu FROM cgu_despesas_favorecidos WHERE ano_referencia = 2024 GROUP BY doc) SELECT p.num_doc_credor, p.total_pag, c.total_cgu FROM pag p JOIN cgu c ON CAST(p.num_doc_credor AS VARCHAR) = CAST(c.doc AS VARCHAR) ORDER BY (p.total_pag + c.total_cgu) DESC LIMIT 10;',
     "tab": "despesa_pagamento, cgu_despesas_favorecidos", "cond": "CAST, JOIN", "esp": "sim"},
    {"id": 70, "dif": "dificil", "cat": "Cruzamento Investigativo", "q": "Servidores ativos em 2023 com Auxílio Brasil em 2023.",
     "sql": 'WITH servidores AS (SELECT TRIM(UPPER(p.nom_pessoa)) AS nome, MAX(CAST(r.vlr_rmn_liquida AS DECIMAL)) AS salario FROM remuneracao_municipio r JOIN pessoa_municipio p ON r.seq_dim_pessoa = p.seq_dim_pessoa WHERE r.ano_referencia = 2023 AND r.dsc_sit_serv_pens ILIKE \'%Ativo%\' GROUP BY nome), benef AS (SELECT TRIM(UPPER("NOME FAVORECIDO")) AS nome, SUM(CAST("VALOR PARCELA" AS DECIMAL)) AS total FROM cgu_auxilio_brasil WHERE ano_referencia = 2023 GROUP BY nome) SELECT s.nome, s.salario, b.total FROM servidores s JOIN benef b ON s.nome = b.nome;',
     "tab": "remuneracao_municipio, cgu_auxilio_brasil", "cond": "TRIM, UPPER", "esp": "indiferente"},
    {"id": 71, "dif": "dificil", "cat": "Cruzamento Investigativo", "q": "Top 3 empresas SICOM pagamentos 2024 que também têm transferências CGU 2024.",
     "sql": 'WITH pag AS (SELECT num_doc_credor, SUM(CAST(vlr_pag_fonte AS DECIMAL)) AS tp FROM despesa_pagamento WHERE ano_referencia = 2024 AND COALESCE(nom_credor, \'\') NOT ILIKE \'%NAO INFORMADO%\' GROUP BY num_doc_credor), tr AS (SELECT "CÓDIGO FAVORECIDO" AS doc, SUM(CAST("VALOR TRANSFERIDO" AS DECIMAL)) AS tt FROM cgu_transferencias WHERE ano_referencia = 2024 GROUP BY doc) SELECT p.num_doc_credor, p.tp, t.tt FROM pag p JOIN tr t ON CAST(p.num_doc_credor AS VARCHAR) = CAST(t.doc AS VARCHAR) ORDER BY (p.tp + t.tt) DESC LIMIT 3;',
     "tab": "despesa_pagamento, cgu_transferencias", "cond": "CAST, LIMIT", "esp": "sim"},
    {"id": 72, "dif": "dificil", "cat": "Cruzamento Investigativo", "q": "Beneficiários do Novo Bolsa Família em 2025 com total acima de 6000 reais.",
     "sql": 'SELECT "NOME FAVORECIDO", SUM(CAST("VALOR PARCELA" AS DECIMAL)) AS total FROM cgu_novo_bolsa_familia WHERE ano_referencia = 2025 GROUP BY "NOME FAVORECIDO" HAVING SUM(CAST("VALOR PARCELA" AS DECIMAL)) > 6000 ORDER BY total DESC LIMIT 20;',
     "tab": "cgu_novo_bolsa_familia", "cond": "HAVING, SUM", "esp": "sim"},
    {"id": 73, "dif": "medio", "cat": "Cruzamento Investigativo", "q": "Total de transferências CGU por tipo em 2025.",
     "sql": 'SELECT "TIPO TRANSFERÊNCIA", SUM(CAST("VALOR TRANSFERIDO" AS DECIMAL)) AS total FROM cgu_transferencias WHERE ano_referencia = 2025 GROUP BY "TIPO TRANSFERÊNCIA" ORDER BY total DESC;',
     "tab": "cgu_transferencias", "cond": "GROUP BY", "esp": "sim"},
    {"id": 74, "dif": "dificil", "cat": "Cruzamento Investigativo", "q": "Credores com pagamentos municipais e bolsa família pagamentos em 2024 (por nome).",
     "sql": 'WITH pag AS (SELECT TRIM(UPPER(nom_credor)) AS nome, SUM(CAST(vlr_pag_fonte AS DECIMAL)) AS total_pag FROM despesa_pagamento WHERE ano_referencia = 2024 AND COALESCE(nom_credor, \'\') NOT ILIKE \'%NAO INFORMADO%\' GROUP BY nome), bf AS (SELECT TRIM(UPPER("NOME FAVORECIDO")) AS nome, SUM(CAST("VALOR PARCELA" AS DECIMAL)) AS total_bf FROM cgu_bolsa_familia_pagamentos WHERE ano_referencia = 2024 GROUP BY nome) SELECT p.nome, p.total_pag, b.total_bf FROM pag p JOIN bf b ON p.nome = b.nome LIMIT 20;',
     "tab": "despesa_pagamento, cgu_bolsa_familia_pagamentos", "cond": "TRIM, JOIN", "esp": "indiferente"},
    {"id": 75, "dif": "medio", "cat": "Cruzamento Investigativo", "q": "Quanto o município recebeu de FPM e ICMS via transferências em 2024?",
     "sql": 'SELECT "NOME PROGRAMA", SUM(CAST("VALOR TRANSFERIDO" AS DECIMAL)) AS total FROM cgu_transferencias WHERE ano_referencia = 2024 AND ("NOME PROGRAMA" ILIKE \'%FPM%\' OR "NOME PROGRAMA" ILIKE \'%ICMS%\') GROUP BY "NOME PROGRAMA";',
     "tab": "cgu_transferencias", "cond": "ILIKE", "esp": "sim"},
    {"id": 76, "dif": "dificil", "cat": "Cruzamento Investigativo", "q": "Top 5 fornecedores pagos em 2024 que também aparecem em despesas favorecidos CGU.",
     "sql": 'WITH pag AS (SELECT num_doc_credor, nom_credor, SUM(CAST(vlr_pag_fonte AS DECIMAL)) AS tp FROM despesa_pagamento WHERE ano_referencia = 2024 GROUP BY num_doc_credor, nom_credor), cgu AS (SELECT "Código Favorecido" AS doc, SUM(CAST("Valor Recebido" AS DECIMAL)) AS tc FROM cgu_despesas_favorecidos WHERE ano_referencia = 2024 GROUP BY doc) SELECT p.nom_credor, p.tp, c.tc FROM pag p JOIN cgu c ON CAST(p.num_doc_credor AS VARCHAR) = CAST(c.doc AS VARCHAR) ORDER BY p.tp DESC LIMIT 5;',
     "tab": "despesa_pagamento, cgu_despesas_favorecidos", "cond": "CAST, LIMIT", "esp": "sim"},
    # --- Regras Especiais (77-80) ---
    {"id": 77, "dif": "medio", "cat": "Regras Especiais", "q": "Total cancelado de RSP pela prefeitura em 2025.",
     "sql": "SELECT SUM(CAST(vlr_movimentacao AS DECIMAL)) AS total FROM empenho_movimentacaoRsp WHERE ano_referencia = 2025 AND dsc_tipo_movimentacao ILIKE '%CANCELAMENTO%' AND CAST(cod_orgao AS INTEGER) IN (2, 4, 5);",
     "tab": "empenho_movimentacaoRsp", "cond": "CANCELAMENTO, IN (2, 4, 5)", "esp": "sim"},
    {"id": 78, "dif": "medio", "cat": "Regras Especiais", "q": "Top 5 maiores licitações por ano em 2024 e 2025 usando ROW_NUMBER.",
     "sql": "WITH base AS (SELECT h.ano_referencia, h.nom_vencedor, SUM(CAST(h.vlr_global AS DECIMAL)) AS total FROM licitacao_homologLicitacao h WHERE h.ano_referencia IN (2024, 2025) GROUP BY h.ano_referencia, h.seq_licitacao, h.nom_vencedor), ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY ano_referencia ORDER BY total DESC) AS rn FROM base) SELECT ano_referencia, nom_vencedor, total FROM ranked WHERE rn <= 5 ORDER BY ano_referencia, total DESC;",
     "tab": "licitacao_homologLicitacao", "cond": "ROW_NUMBER, PARTITION", "esp": "sim"},
    {"id": 79, "dif": "dificil", "cat": "Regras Especiais", "q": "Contratos de 2022 com aditivo de acréscimo em 2023.",
     "sql": "SELECT c.num_contrato, cc.dsc_nome_credor, t.vlr_termo_aditivo FROM contrato_contratos c LEFT JOIN contrato_contContrato cc ON c.seq_contrato = cc.seq_contrato AND c.ano_referencia = cc.ano_referencia LEFT JOIN contrato_termoContrato t ON c.seq_contrato = t.seq_contrato AND t.ano_referencia = 2023 WHERE c.num_ano_contrato = 2022 AND t.dsc_tipo_alteracao ILIKE '%ACRÉSCIMO%';",
     "tab": "contrato_contratos, contrato_termoContrato", "cond": "JOIN, ILIKE", "esp": "indiferente"},
    {"id": 80, "dif": "medio", "cat": "Regras Especiais", "q": "Despesa liquidada por função em 2025 (top 5 funções).",
     "sql": "SELECT dsc_funcao, SUM(CAST(vlr_liquidado AS DECIMAL)) AS total FROM despesa_despesa WHERE ano_referencia = 2025 GROUP BY dsc_funcao ORDER BY total DESC LIMIT 5;",
     "tab": "despesa_despesa", "cond": "GROUP BY, LIMIT", "esp": "sim"},
]


def load_base_10() -> list[dict]:
    src = REPO / "eval" / "datasets" / "golden_dataset_v1.0.csv"
    rows = []
    with open(src, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            if int(row["id_teste"]) <= 10:
                rows.append(row)
    return rows


def extra_to_row(e: dict) -> dict:
    return {
        "id_teste": str(e["id"]),
        "dificuldade": e["dif"],
        "categoria": e["cat"],
        "input_usuario": e["q"],
        "query_referencia": e["sql"],
        "tabelas_esperadas": e["tab"],
        "condicao_esperada": e["cond"],
        "espera_dados": e["esp"],
        "origem": "derivacao",
    }


def validate_all(rows: list[dict], url: str) -> tuple[int, int]:
    ok, fail = 0, 0
    for row in rows:
        tid = row["id_teste"]
        sql = row["query_referencia"].strip()
        res = execute_query(url, sql)
        if res["ok"]:
            ok += 1
            print(f"  [{tid}] OK ({len(res['data'])} linhas)")
        else:
            fail += 1
            print(f"  [{tid}] FAIL: {str(res.get('error', ''))[:100]}")
    return ok, fail


def main() -> None:
    cfg = load_config()
    url = cfg["duckdb_url"]

    print("Carregando base 10...")
    all_rows = load_base_10()
    print(f"Adicionando {len(EXTRA)} derivadas...")
    all_rows.extend(extra_to_row(e) for e in EXTRA)
    all_rows.sort(key=lambda r: int(r["id_teste"]))

    assert len(all_rows) == 80, f"Esperado 80, obteve {len(all_rows)}"

    print("Validando SQL de referência no DuckDB...")
    ok, fail = validate_all(all_rows, url)
    print(f"Validação: OK={ok} FAIL={fail}")

    fieldnames = [
        "id_teste", "dificuldade", "categoria", "input_usuario",
        "query_referencia", "tabelas_esperadas", "condicao_esperada",
        "espera_dados", "origem",
    ]
    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        w.writeheader()
        w.writerows(all_rows)

    print(f"Golden salvo: {OUT} ({len(all_rows)} consultas)")


if __name__ == "__main__":
    main()
