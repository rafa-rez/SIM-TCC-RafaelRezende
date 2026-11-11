"""
Camada Gold de Views Semânticas Pré-Unificadas e Desduplicadas (SIM v4.0).

Elimina multiplicação cartesiana em JOINs multi-ano do SICOM/TCE-MG e expõe 7 views
prontas para Text-to-SQL determinístico e auditoria forense:
1. vw_gold_folha_servidores
2. vw_gold_fornecedores_qsa_360 (1 linha única por CNPJ — zero fan-out sobre pagamentos!)
3. vw_gold_execucao_despesa_credor (chave normalizada SEM_DOC_CREDOR + coluna de Restos a Pagar)
4. vw_gold_frota_consolidada (cadastro cumulativo + gastos de frota via FULL OUTER JOIN + CTB)
5. vw_gold_contratos_vigencia_aditivos (proteção contra contratos de taxa simbólica de R$ 1,00)
6. vw_gold_conflito_interesses_licitacao
7. vw_gold_orcamento_previsto_vs_realizado (LOA prevista vs. Receita arrecadada e Despesa executada 2020-2025)
"""
from __future__ import annotations

import logging
from typing import Dict, Set

import duckdb

logger = logging.getLogger("sim_platform.gold_views")


GOLD_VIEWS_DDL: Dict[str, str] = {
    "vw_gold_folha_servidores": """
        CREATE OR REPLACE VIEW vw_gold_folha_servidores AS
        WITH pessoa_dedup AS (
            SELECT
                seq_dim_pessoa,
                ano_referencia,
                MAX(nom_pessoa) AS nom_pessoa,
                MAX(num_documento) AS num_cpf
            FROM pessoa_municipio
            GROUP BY seq_dim_pessoa, ano_referencia
        )
        SELECT
            r.ano_referencia,
            r.seq_dat_pagamento,
            r.seq_dim_pessoa,
            COALESCE(p.nom_pessoa, 'SERVIDOR SEQ ' || CAST(r.seq_dim_pessoa AS VARCHAR)) AS nom_servidor,
            p.num_cpf,
            r.dsc_cargo,
            CASE
                WHEN COALESCE(r.dsc_lotacao, '') ILIKE '%SEDESMA%'
                THEN r.dsc_lotacao || ' (SEC. DESENV. SUSTENTAVEL E MEIO AMBIENTE)'
                WHEN COALESCE(r.dsc_lotacao, '') ILIKE '%SEMAS%'
                THEN r.dsc_lotacao || ' (SEC. ASSISTENCIA SOCIAL)'
                ELSE r.dsc_lotacao
            END AS dsc_lotacao,
            CASE
                WHEN COALESCE(r.dsc_sit_serv_pens, '') ILIKE '%Inativo%' THEN 'Inativo / Aposentado'
                WHEN COALESCE(r.dsc_sit_serv_pens, '') ILIKE '%Pensionista%' THEN 'Pensionista'
                WHEN COALESCE(r.dsc_sgl_cargo, '') ILIKE '%Efetivo%' THEN 'Efetivo (Concursado)'
                WHEN COALESCE(r.dsc_sgl_cargo, '') ILIKE '%Emprego%' THEN 'Emprego Público (Concursado / Seleção Pública)'
                WHEN COALESCE(r.dsc_sgl_cargo, '') ILIKE '%tempor%' THEN 'Contratado / Servidor Temporário'
                WHEN COALESCE(r.dsc_sgl_cargo, '') ILIKE '%Comissionado%' THEN 'Comissionado (Recrutamento Amplo)'
                WHEN COALESCE(r.dsc_sgl_cargo, '') ILIKE '%Estagi%' THEN 'Estagiário / Aprendiz / Bolsista'
                WHEN COALESCE(r.dsc_sgl_cargo, '') ILIKE '%Agente pol%' THEN 'Agente Político (Prefeito, Vice, Secretários e Vereadores)'
                WHEN COALESCE(r.dsc_sgl_cargo, '') ILIKE '%Honor%' THEN 'Agente Honorífico (Conselheiro)'
                WHEN r.dsc_situacao IS NOT NULL AND TRIM(r.dsc_situacao) NOT IN ('', 'NÃO INFORMADO', 'NAO INFORMADO') THEN r.dsc_situacao
                ELSE COALESCE(NULLIF(TRIM(r.dsc_sgl_cargo), ''), NULLIF(TRIM(r.dsc_sit_serv_pens), ''), 'NÃO INFORMADO')
            END AS dsc_situacao,
            COALESCE(NULLIF(TRIM(r.dsc_sgl_cargo), 'NÃO INFORMADO'), r.dsc_regime) AS dsc_regime,
            r.dsc_sgl_cargo AS tipo_vinculo,
            r.dsc_sit_serv_pens AS situacao_funcional,
            r.dsc_agepol AS cargo_politico,
            TRY_CAST(r.vlr_rmn_bruta AS DECIMAL(18,2)) AS vlr_rmn_bruta,
            TRY_CAST(r.vlr_rmn_liquida AS DECIMAL(18,2)) AS vlr_rmn_liquida,
            TRY_CAST(r.vlr_descontos AS DECIMAL(18,2)) AS vlr_descontos
        FROM remuneracao_municipio r
        LEFT JOIN pessoa_dedup p
          ON r.seq_dim_pessoa = p.seq_dim_pessoa
         AND r.ano_referencia = p.ano_referencia;
    """,
    "vw_gold_fornecedores_qsa_360": """
        CREATE OR REPLACE VIEW vw_gold_fornecedores_qsa_360 AS
        WITH pagamentos_credor AS (
            SELECT
                regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g') AS doc_limpo,
                MAX(nom_credor) AS nom_credor,
                COUNT(*) AS qtd_pagamentos,
                ROUND(SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2))), 2) AS total_pago_historico,
                MIN(ano_referencia) AS primeiro_ano_pagamento,
                MAX(ano_referencia) AS ultimo_ano_pagamento
            FROM despesa_pagamento
            WHERE length(regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g')) >= 11
            GROUP BY 1
        ),
        socios_sicom_dedup AS (
            SELECT
                regexp_replace(COALESCE(h.num_documento, ''), '[^0-9]', '', 'g') AS cnpj_empresa,
                MAX(h.nom_pessoa) AS razao_social,
                COUNT(DISTINCT regexp_replace(COALESCE(q.num_cgc_habilitado, ''), '[^0-9]', '', 'g')) AS qtd_socios_identificados,
                STRING_AGG(DISTINCT UPPER(TRIM(q.nom_pessoa)), ' | ' ORDER BY UPPER(TRIM(q.nom_pessoa))) AS nom_socio,
                STRING_AGG(DISTINCT regexp_replace(COALESCE(q.num_cgc_habilitado, ''), '[^0-9]', '', 'g'), ' | ') AS cpf_cnpj_socio,
                MAX(q.dsc_tipo_participacao) AS dsc_tipo_participacao
            FROM licitacao_quadroSocLicitacao q
            JOIN licitacao_habLicitacao h
              ON q.seq_hab_licitacao = h.seq_hab_licitacao
             AND q.ano_referencia = h.ano_referencia
            WHERE length(regexp_replace(COALESCE(h.num_documento, ''), '[^0-9]', '', 'g')) = 14
            GROUP BY 1
        )
        SELECT
            COALESCE(p.doc_limpo, s.cnpj_empresa) AS num_doc_credor,
            COALESCE(p.nom_credor, s.razao_social) AS nom_credor,
            COALESCE(p.total_pago_historico, 0.00) AS total_pago_historico,
            COALESCE(p.qtd_pagamentos, 0) AS qtd_pagamentos,
            p.primeiro_ano_pagamento,
            p.ultimo_ano_pagamento,
            COALESCE(s.qtd_socios_identificados, 0) AS qtd_socios_identificados,
            s.cpf_cnpj_socio,
            s.nom_socio,
            s.dsc_tipo_participacao
        FROM pagamentos_credor p
        FULL OUTER JOIN socios_sicom_dedup s
          ON p.doc_limpo = s.cnpj_empresa
        WHERE length(COALESCE(p.doc_limpo, s.cnpj_empresa, '')) >= 11;
    """,
    "vw_gold_execucao_despesa_credor": """
        CREATE OR REPLACE VIEW vw_gold_execucao_despesa_credor AS
        WITH orgao_dedup AS (
            SELECT
                seq_orgao,
                MAX(nom_orgao) AS nom_orgao
            FROM orgao_orgao
            GROUP BY seq_orgao
        ),
        credor_emp_dedup AS (
            SELECT
                seq_empenho,
                ano_referencia,
                MAX(
                    CASE
                        WHEN length(regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g')) < 6
                        THEN 'SEM_DOC_CREDOR'
                        ELSE regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g')
                    END
                ) AS doc_credor,
                MAX(nom_credor) AS nom_credor
            FROM empenho_credorEmpenho
            GROUP BY seq_empenho, ano_referencia
        ),
        emp AS (
            SELECT
                e.ano_referencia,
                e.seq_orgao,
                COALESCE(ce.doc_credor, 'SEM_DOC_CREDOR') AS doc_credor,
                MAX(COALESCE(ce.nom_credor, 'FOLHA / CREDOR GLOBAL SEM DETALHAMENTO')) AS nom_credor,
                COUNT(*) AS qtd_empenhos,
                ROUND(SUM(TRY_CAST(e.vlr_empenhado AS DECIMAL(18,2))), 2) AS total_empenhado,
                SUM(CASE WHEN COALESCE(e.ind_dec_licitacao, '') ILIKE '%ADES%O%' THEN TRY_CAST(e.vlr_empenhado AS DECIMAL(18,2)) ELSE 0 END) AS total_empenhado_carona,
                SUM(CASE WHEN COALESCE(e.ind_dec_licitacao, '') ILIKE '%DISPENSA%' OR COALESCE(e.ind_dec_licitacao, '') ILIKE '%INEXIG%' THEN TRY_CAST(e.vlr_empenhado AS DECIMAL(18,2)) ELSE 0 END) AS total_empenhado_dispensa_inexigibilidade
            FROM empenho_empenho e
            LEFT JOIN credor_emp_dedup ce
              ON e.seq_empenho = ce.seq_empenho
             AND e.ano_referencia = ce.ano_referencia
            GROUP BY 1, 2, 3
        ),
        liq AS (
            SELECT
                l.ano_referencia,
                l.seq_orgao,
                COALESCE(ce.doc_credor, 'SEM_DOC_CREDOR') AS doc_credor,
                MAX(COALESCE(ce.nom_credor, 'FOLHA / CREDOR GLOBAL SEM DETALHAMENTO')) AS nom_credor,
                ROUND(SUM(TRY_CAST(l.vlr_liquidacao AS DECIMAL(18,2))), 2) AS total_liquidado
            FROM despesa_liquidacao l
            LEFT JOIN credor_emp_dedup ce
              ON l.seq_empenho = ce.seq_empenho
             AND l.ano_referencia = ce.ano_referencia
            GROUP BY 1, 2, 3
        ),
        pag AS (
            SELECT
                ano_referencia,
                seq_orgao,
                CASE
                    WHEN length(regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g')) < 6
                    THEN 'SEM_DOC_CREDOR'
                    ELSE regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g')
                END AS doc_credor,
                MAX(nom_credor) AS nom_credor,
                ROUND(SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2))), 2) AS total_pago,
                ROUND(SUM(CASE WHEN TRY_CAST(seq_empenho AS INTEGER) = -1 THEN TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2)) ELSE 0 END), 2) AS total_pago_restos_a_pagar
            FROM despesa_pagamento
            GROUP BY 1, 2, 3
        )
        SELECT
            COALESCE(e.ano_referencia, l.ano_referencia, p.ano_referencia) AS ano_referencia,
            COALESCE(e.seq_orgao, l.seq_orgao, p.seq_orgao) AS seq_orgao,
            COALESCE(o.nom_orgao, 'ÓRGÃO ' || CAST(COALESCE(e.seq_orgao, l.seq_orgao, p.seq_orgao) AS VARCHAR)) AS nom_orgao,
            COALESCE(e.doc_credor, l.doc_credor, p.doc_credor) AS num_doc_credor,
            COALESCE(p.nom_credor, e.nom_credor, l.nom_credor) AS nom_credor,
            COALESCE(e.qtd_empenhos, 0) AS qtd_empenhos,
            COALESCE(e.total_empenhado, 0.00) AS total_empenhado,
            COALESCE(l.total_liquidado, 0.00) AS total_liquidado,
            COALESCE(p.total_pago, 0.00) AS total_pago,
            COALESCE(p.total_pago_restos_a_pagar, 0.00) AS total_pago_restos_a_pagar,
            ROUND(COALESCE(e.total_empenhado_carona, 0.00), 2) AS total_empenhado_carona,
            ROUND(COALESCE(e.total_empenhado_dispensa_inexigibilidade, 0.00), 2) AS total_empenhado_dispensa_inexigibilidade
        FROM emp e
        FULL OUTER JOIN liq l
          ON e.ano_referencia = l.ano_referencia
         AND e.seq_orgao = l.seq_orgao
         AND e.doc_credor = l.doc_credor
        FULL OUTER JOIN pag p
          ON COALESCE(e.ano_referencia, l.ano_referencia) = p.ano_referencia
         AND COALESCE(e.seq_orgao, l.seq_orgao) = p.seq_orgao
         AND COALESCE(e.doc_credor, l.doc_credor) = p.doc_credor
        LEFT JOIN orgao_dedup o
          ON COALESCE(e.seq_orgao, l.seq_orgao, p.seq_orgao) = o.seq_orgao;
    """,
    "vw_gold_frota_consolidada": """
        CREATE OR REPLACE VIEW vw_gold_frota_consolidada AS
        WITH veiculo_unico AS (
            SELECT
                seq_veiculo,
                MAX(dsc_num_placa) AS dsc_num_placa,
                MAX(dsc_veiculo) AS dsc_veiculo,
                MAX(dsc_tipo_veiculo) AS dsc_tipo_veiculo,
                MAX(dsc_marca) AS dsc_marca,
                MAX(dsc_modelo) AS dsc_modelo,
                MAX(num_anofabricacao) AS num_anofabricacao,
                MAX(dsc_situacaoveiculo) AS dsc_situacaoveiculo,
                MAX(ano_referencia) AS ano_cadastro_veiculo
            FROM frota_frota
            GROUP BY seq_veiculo
        )
        SELECT
            COALESCE(g.ano_referencia, v.ano_cadastro_veiculo) AS ano_referencia,
            g.num_mes_referencia,
            COALESCE(g.seq_veiculo, v.seq_veiculo) AS seq_veiculo,
            CASE
                WHEN v.dsc_num_placa IS NOT NULL AND TRIM(v.dsc_num_placa) NOT IN ('', '-1', '0', 'S/P')
                THEN v.dsc_num_placa
                WHEN v.seq_veiculo IS NOT NULL AND (
                    COALESCE(v.dsc_tipo_veiculo, '') ILIKE '%MAQUIN%'
                    OR COALESCE(v.dsc_veiculo, '') ILIKE '%RETRO%'
                    OR COALESCE(v.dsc_veiculo, '') ILIKE '%CARREGADEIRA%'
                    OR COALESCE(v.dsc_veiculo, '') ILIKE '%MOTONIVELADORA%'
                    OR COALESCE(v.dsc_veiculo, '') ILIKE '%TRATOR%'
                    OR COALESCE(v.dsc_veiculo, '') ILIKE '%BOBCAT%'
                    OR COALESCE(v.dsc_veiculo, '') ILIKE '%EQUIPAMENTO%'
                    OR COALESCE(v.dsc_veiculo, '') ILIKE '%SERRA%'
                )
                THEN 'ISENTO_CTB_MAQUINARIO'
                ELSE 'SEM_CADASTRO_PLACA'
            END AS dsc_num_placa,
            COALESCE(v.dsc_veiculo, 'VEÍCULO CADASTRADO PRÉ-2020 / ÓRFÃO SEQ ' || CAST(g.seq_veiculo AS VARCHAR)) AS dsc_veiculo,
            v.dsc_tipo_veiculo,
            v.dsc_marca,
            v.dsc_modelo,
            COALESCE(v.dsc_situacaoveiculo, 'ATIVO EM GASTOS DE FROTA (CADASTRO PRÉ-2020)') AS dsc_situacaoveiculo,
            CASE
                WHEN v.seq_veiculo IS NULL THEN 'VEICULO_ORFAO_NO_CADASTRO'
                WHEN v.dsc_num_placa IS NULL OR TRIM(v.dsc_num_placa) IN ('', '-1', '0', 'S/P') THEN 'CADASTRADO_MAQUINARIO_ISENTO_PLACA'
                ELSE 'CADASTRADO'
            END AS status_cadastro_frota,
            COALESCE(g.dsc_tipo_gasto, 'CADASTRO PATRIMONIAL SEM DESPESA NO PERÍODO') AS dsc_tipo_gasto,
            g.dsc_especificacao,
            COALESCE(TRY_CAST(g.num_quant_utilizada AS DECIMAL(18,2)), 0.00) AS qtd_utilizada,
            COALESCE(TRY_CAST(g.vlr_gasto AS DECIMAL(18,2)), 0.00) AS vlr_gasto
        FROM frota_gastoFrota g
        FULL OUTER JOIN veiculo_unico v
          ON g.seq_veiculo = v.seq_veiculo;
    """,
    "vw_gold_contratos_vigencia_aditivos": """
        CREATE OR REPLACE VIEW vw_gold_contratos_vigencia_aditivos AS
        WITH aditivos_agg AS (
            SELECT
                seq_contrato,
                COUNT(*) AS qtd_aditivos,
                ROUND(SUM(TRY_CAST(vlr_termo_aditivo AS DECIMAL(18,2))), 2) AS vlr_total_aditivos,
                MAX(dat_novo_termino) AS dat_termino_apos_aditivo
            FROM contrato_termoContrato
            GROUP BY seq_contrato
        ),
        contratados_agg AS (
            SELECT
                seq_contrato,
                MAX(dsc_nome_credor) AS nom_contratado,
                MAX(num_doc_credor) AS doc_contratado
            FROM contrato_contContrato
            GROUP BY seq_contrato
        ),
        orgao_contrato_dedup AS (
            SELECT
                seq_orgao,
                MAX(cod_orgao) AS cod_orgao,
                MAX(nom_orgao) AS nom_orgao
            FROM orgao_orgao
            GROUP BY seq_orgao
        )
        SELECT
            c.ano_referencia,
            c.seq_contrato,
            c.num_contrato,
            c.num_ano_contrato,
            c.seq_orgao,
            COALESCE(c.cod_orgao, o.cod_orgao) AS cod_orgao,
            COALESCE(o.nom_orgao, 'ÓRGÃO ' || CAST(c.seq_orgao AS VARCHAR)) AS nom_orgao,
            COALESCE(ct.nom_contratado, 'CONTRATADO NÃO INFORMADO') AS nom_contratado,
            ct.doc_contratado AS num_doc_credor,
            c.dsc_naturezaobjeto,
            c.dsc_tipo_processo,
            c.dsc_objetocontrato,
            c.dat_assinatura,
            c.dat_iniciovigencia,
            c.dat_fimvigencia,
            a.dat_termino_apos_aditivo,
            TRY_CAST(c.vlr_contrato AS DECIMAL(18,2)) AS vlr_original_contrato,
            COALESCE(a.qtd_aditivos, 0) AS qtd_aditivos,
            COALESCE(a.vlr_total_aditivos, 0.00) AS vlr_total_aditivos,
            ROUND(
                COALESCE(TRY_CAST(c.vlr_contrato AS DECIMAL(18,2)), 0.00)
                + COALESCE(a.vlr_total_aditivos, 0.00),
                2
            ) AS vlr_global_atualizado,
            TRY_CAST(c.vlr_empenhado AS DECIMAL(18,2)) AS vlr_empenhado,
            TRY_CAST(c.vlr_liquidado AS DECIMAL(18,2)) AS vlr_liquidado,
            TRY_CAST(c.vlr_pago AS DECIMAL(18,2)) AS vlr_pago,
            CASE
                WHEN TRY_CAST(c.vlr_contrato AS DECIMAL(18,2)) > 100.00
                THEN ROUND((COALESCE(a.vlr_total_aditivos, 0.00) / TRY_CAST(c.vlr_contrato AS DECIMAL(18,2))) * 100.0, 2)
                ELSE 0.00
            END AS pct_acrescimo_aditivos
        FROM contrato_contratos c
        LEFT JOIN contratados_agg ct
          ON c.seq_contrato = ct.seq_contrato
        LEFT JOIN aditivos_agg a
          ON c.seq_contrato = a.seq_contrato
        LEFT JOIN orgao_contrato_dedup o
          ON c.seq_orgao = o.seq_orgao;
    """,
    "vw_gold_conflito_interesses_licitacao": """
        CREATE OR REPLACE VIEW vw_gold_conflito_interesses_licitacao AS
        WITH socios_empresas AS (
            SELECT DISTINCT
                regexp_replace(COALESCE(q.num_cgc_habilitado, ''), '[^0-9]', '', 'g') AS cpf_socio,
                q.nom_pessoa AS nom_socio,
                q.dsc_tipo_participacao,
                regexp_replace(COALESCE(h.num_documento, ''), '[^0-9]', '', 'g') AS cnpj_empresa,
                h.nom_pessoa AS nom_empresa,
                MIN(q.ano_referencia) AS primeiro_ano_licitacao,
                MAX(q.ano_referencia) AS ultimo_ano_licitacao
            FROM licitacao_quadroSocLicitacao q
            JOIN licitacao_habLicitacao h
              ON q.seq_hab_licitacao = h.seq_hab_licitacao
             AND q.ano_referencia = h.ano_referencia
            WHERE length(regexp_replace(COALESCE(q.num_cgc_habilitado, ''), '[^0-9]', '', 'g')) = 11
              AND length(regexp_replace(COALESCE(h.num_documento, ''), '[^0-9]', '', 'g')) = 14
            GROUP BY 1, 2, 3, 4, 5
        ),
        servidores AS (
            SELECT
                regexp_replace(COALESCE(p.num_documento, ''), '[^0-9]', '', 'g') AS cpf_servidor,
                MAX(p.nom_pessoa) AS nom_servidor,
                MAX(r.dsc_cargo) AS dsc_cargo,
                MAX(r.dsc_lotacao) AS dsc_lotacao,
                MIN(r.ano_referencia) AS primeiro_ano_folha,
                MAX(r.ano_referencia) AS ultimo_ano_folha,
                ROUND(SUM(COALESCE(TRY_CAST(r.vlr_rmn_bruta AS DECIMAL(18,2)), 0.00)), 2) AS total_remuneracao_bruta
            FROM pessoa_municipio p
            JOIN remuneracao_municipio r
              ON p.seq_dim_pessoa = r.seq_dim_pessoa
             AND p.ano_referencia = r.ano_referencia
            WHERE length(regexp_replace(COALESCE(p.num_documento, ''), '[^0-9]', '', 'g')) = 11
            GROUP BY 1
        ),
        pagamentos_empresa AS (
            SELECT
                regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g') AS cnpj_empresa,
                ROUND(SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2))), 2) AS total_pago_empresa,
                MIN(ano_referencia) AS primeiro_ano_pagamento,
                MAX(ano_referencia) AS ultimo_ano_pagamento
            FROM despesa_pagamento
            GROUP BY 1
        )
        SELECT
            s.cpf_socio AS num_cpf,
            COALESCE(sv.nom_servidor, s.nom_socio) AS nom_pessoa,
            sv.dsc_cargo,
            sv.dsc_lotacao,
            sv.primeiro_ano_folha,
            sv.ultimo_ano_folha,
            sv.total_remuneracao_bruta,
            s.cnpj_empresa,
            s.nom_empresa,
            s.dsc_tipo_participacao,
            COALESCE(pg.total_pago_empresa, 0.00) AS total_pago_empresa,
            pg.primeiro_ano_pagamento,
            pg.ultimo_ano_pagamento
        FROM socios_empresas s
        JOIN servidores sv
          ON s.cpf_socio = sv.cpf_servidor
        LEFT JOIN pagamentos_empresa pg
          ON s.cnpj_empresa = pg.cnpj_empresa;
    """,
    "vw_gold_orcamento_previsto_vs_realizado": """
        CREATE OR REPLACE VIEW vw_gold_orcamento_previsto_vs_realizado AS
        WITH rec_anual AS (
            SELECT
                ano_referencia,
                ROUND(SUM(COALESCE(TRY_CAST(vlr_previsaoinicial AS DECIMAL(18,2)), 0.00)), 2) AS receita_prevista_inicial_loa,
                ROUND(SUM(COALESCE(TRY_CAST(vlr_previsaoatualizada AS DECIMAL(18,2)), 0.00)), 2) AS receita_prevista_atualizada_loa,
                ROUND(SUM(COALESCE(TRY_CAST(vlr_realizadoateperiodo AS DECIMAL(18,2)), 0.00)), 2) AS receita_arrecadada_realizada,
                MAX(TRY_CAST(num_mesexercicio AS INTEGER)) AS ultimo_mes_receita
            FROM receita_receita
            GROUP BY ano_referencia
        ),
        desp_anual AS (
            SELECT
                ano_referencia,
                ROUND(SUM(COALESCE(TRY_CAST(vlr_previsto AS DECIMAL(18,2)), 0.00)), 2) AS despesa_dotacao_inicial_loa,
                ROUND(
                    SUM(
                        COALESCE(TRY_CAST(vlr_previsto AS DECIMAL(18,2)), 0.00)
                        + COALESCE(TRY_CAST(vlr_acrescimo AS DECIMAL(18,2)), 0.00)
                        - ABS(COALESCE(TRY_CAST(vlr_deducao AS DECIMAL(18,2)), 0.00))
                    ),
                    2
                ) AS despesa_dotacao_atualizada,
                ROUND(SUM(COALESCE(TRY_CAST(vlr_empenhado AS DECIMAL(18,2)), 0.00)), 2) AS despesa_empenhada,
                ROUND(SUM(COALESCE(TRY_CAST(vlr_liquidado AS DECIMAL(18,2)), 0.00)), 2) AS despesa_liquidada,
                ROUND(SUM(COALESCE(TRY_CAST(vlr_pago AS DECIMAL(18,2)), 0.00)), 2) AS despesa_paga,
                MAX(TRY_CAST(num_mesexercicio AS INTEGER)) AS ultimo_mes_despesa
            FROM despesa_despesa
            GROUP BY ano_referencia
        )
        SELECT
            COALESCE(r.ano_referencia, d.ano_referencia) AS ano_referencia,
            COALESCE(r.receita_prevista_inicial_loa, d.despesa_dotacao_inicial_loa) AS orcamento_previsto_loa,
            COALESCE(r.receita_prevista_atualizada_loa, d.despesa_dotacao_inicial_loa) AS orcamento_atualizado_loa,
            d.despesa_dotacao_atualizada,
            r.receita_arrecadada_realizada,
            d.despesa_empenhada,
            d.despesa_liquidada,
            d.despesa_paga,
            ROUND(
                100.0 * r.receita_arrecadada_realizada
                / NULLIF(r.receita_prevista_atualizada_loa, 0.00),
                2
            ) AS pct_arrecadacao_loa,
            ROUND(
                100.0 * d.despesa_empenhada
                / NULLIF(d.despesa_dotacao_atualizada, 0.00),
                2
            ) AS pct_despesa_empenhada_dotacao,
            COALESCE(r.ultimo_mes_receita, 12) AS ultimo_mes_receita,
            COALESCE(d.ultimo_mes_despesa, 12) AS ultimo_mes_despesa,
            CASE
                WHEN COALESCE(r.ano_referencia, d.ano_referencia) >= 2025
                     OR LEAST(COALESCE(r.ultimo_mes_receita, 12), COALESCE(d.ultimo_mes_despesa, 12)) < 12
                THEN 'Em andamento (Parcial)'
                ELSE 'Fechado'
            END AS status_exercicio
        FROM rec_anual r
        FULL OUTER JOIN desp_anual d
          ON r.ano_referencia = d.ano_referencia
        ORDER BY 1;
    """,
}


def register_gold_views(con: duckdb.DuckDBPyConnection) -> Set[str]:
    """
    Registra as 6 Gold Views Semânticas sobre a conexão DuckDB se as tabelas base existirem.
    Retorna o conjunto de views registradas com sucesso.
    """
    registered: Set[str] = set()
    try:
        rows = con.execute(
            "SELECT LOWER(table_name) FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        existing = {r[0] for r in rows}
    except Exception:
        return registered

    required_by_view = {
        "vw_gold_folha_servidores": {"remuneracao_municipio", "pessoa_municipio"},
        "vw_gold_fornecedores_qsa_360": {
            "despesa_pagamento",
            "licitacao_quadrosoclicitacao",
            "licitacao_hablicitacao",
        },
        "vw_gold_execucao_despesa_credor": {
            "empenho_empenho",
            "empenho_credorempenho",
            "despesa_liquidacao",
            "despesa_pagamento",
            "orgao_orgao",
        },
        "vw_gold_frota_consolidada": {"frota_frota", "frota_gastofrota"},
        "vw_gold_contratos_vigencia_aditivos": {
            "contrato_contratos",
            "contrato_contcontrato",
            "contrato_termocontrato",
        },
        "vw_gold_conflito_interesses_licitacao": {
            "licitacao_quadrosoclicitacao",
            "licitacao_hablicitacao",
            "pessoa_municipio",
            "remuneracao_municipio",
            "despesa_pagamento",
        },
        "vw_gold_orcamento_previsto_vs_realizado": {
            "receita_receita",
            "despesa_despesa",
        },
    }

    for view_name, ddl in GOLD_VIEWS_DDL.items():
        reqs = required_by_view.get(view_name, set())
        if reqs.issubset(existing):
            try:
                con.execute(ddl)
                registered.add(view_name)
            except Exception as exc:
                if view_name == "vw_gold_folha_servidores":
                    try:
                        con.execute(
                            """
                            CREATE OR REPLACE VIEW vw_gold_folha_servidores AS
                            WITH pessoa_dedup AS (
                                SELECT seq_dim_pessoa, ano_referencia, MAX(nom_pessoa) AS nom_pessoa, MAX(num_documento) AS num_cpf
                                FROM pessoa_municipio GROUP BY seq_dim_pessoa, ano_referencia
                            )
                            SELECT
                                r.ano_referencia, r.seq_dat_pagamento, r.seq_dim_pessoa,
                                COALESCE(p.nom_pessoa, 'SERVIDOR SEQ ' || CAST(r.seq_dim_pessoa AS VARCHAR)) AS nom_servidor,
                                p.num_cpf, r.dsc_cargo, r.dsc_lotacao, r.dsc_situacao, r.dsc_regime,
                                TRY_CAST(r.vlr_rmn_bruta AS DECIMAL(18,2)) AS vlr_rmn_bruta,
                                TRY_CAST(r.vlr_rmn_liquida AS DECIMAL(18,2)) AS vlr_rmn_liquida,
                                TRY_CAST(r.vlr_descontos AS DECIMAL(18,2)) AS vlr_descontos
                            FROM remuneracao_municipio r
                            LEFT JOIN pessoa_dedup p ON r.seq_dim_pessoa = p.seq_dim_pessoa AND r.ano_referencia = p.ano_referencia;
                            """
                        )
                        registered.add(view_name)
                        continue
                    except Exception:
                        pass
                logger.warning("Falha ao criar Gold View %s: %s", view_name, exc)

    return registered
