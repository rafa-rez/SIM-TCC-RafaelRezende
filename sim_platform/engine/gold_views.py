"""
Camada Gold de Views Semânticas Pré-Unificadas e Desduplicadas (SIM v3.0).

Elimina multiplicação cartesiana em JOINs multi-ano do SICOM/TCE-MG e expõe 6 views
prontas para Text-to-SQL determinístico e auditoria forense:
1. vw_gold_folha_servidores
2. vw_gold_fornecedores_qsa_360
3. vw_gold_execucao_despesa_credor
4. vw_gold_frota_consolidada
5. vw_gold_contratos_vigencia_aditivos
6. vw_gold_conflito_interesses_licitacao
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
            r.dsc_lotacao,
            r.dsc_situacao,
            r.dsc_regime,
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
            GROUP BY 1
        ),
        socios_sicom AS (
            SELECT DISTINCT
                regexp_replace(COALESCE(h.num_documento, ''), '[^0-9]', '', 'g') AS cnpj_empresa,
                h.nom_pessoa AS razao_social,
                regexp_replace(COALESCE(q.num_cgc_habilitado, ''), '[^0-9]', '', 'g') AS doc_socio,
                q.nom_pessoa AS nom_socio,
                q.dsc_tipo_participacao
            FROM licitacao_quadroSocLicitacao q
            JOIN licitacao_habLicitacao h
              ON q.seq_hab_licitacao = h.seq_hab_licitacao
             AND q.ano_referencia = h.ano_referencia
            WHERE length(regexp_replace(COALESCE(h.num_documento, ''), '[^0-9]', '', 'g')) = 14
        )
        SELECT
            COALESCE(p.doc_limpo, s.cnpj_empresa) AS num_doc_credor,
            COALESCE(p.nom_credor, s.razao_social) AS nom_credor,
            COALESCE(p.total_pago_historico, 0.00) AS total_pago_historico,
            COALESCE(p.qtd_pagamentos, 0) AS qtd_pagamentos,
            p.primeiro_ano_pagamento,
            p.ultimo_ano_pagamento,
            s.doc_socio AS cpf_cnpj_socio,
            s.nom_socio,
            s.dsc_tipo_participacao
        FROM pagamentos_credor p
        FULL OUTER JOIN socios_sicom s
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
                MAX(regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g')) AS doc_credor,
                MAX(nom_credor) AS nom_credor
            FROM empenho_credorEmpenho
            GROUP BY seq_empenho, ano_referencia
        ),
        emp AS (
            SELECT
                e.ano_referencia,
                e.seq_orgao,
                COALESCE(ce.doc_credor, 'SEM_DOC') AS doc_credor,
                MAX(ce.nom_credor) AS nom_credor,
                COUNT(*) AS qtd_empenhos,
                ROUND(SUM(TRY_CAST(e.vlr_empenhado AS DECIMAL(18,2))), 2) AS total_empenhado,
                SUM(CASE WHEN COALESCE(e.ind_dec_licitacao, '') ILIKE '%ADESÃO%' THEN TRY_CAST(e.vlr_empenhado AS DECIMAL(18,2)) ELSE 0 END) AS total_empenhado_carona
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
                COALESCE(ce.doc_credor, 'SEM_DOC') AS doc_credor,
                MAX(ce.nom_credor) AS nom_credor,
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
                regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g') AS doc_credor,
                MAX(nom_credor) AS nom_credor,
                ROUND(SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2))), 2) AS total_pago
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
            ROUND(COALESCE(e.total_empenhado_carona, 0.00), 2) AS total_empenhado_carona
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
                MAX(num_anofabricacao) AS num_anofabricacao
            FROM frota_frota
            GROUP BY seq_veiculo
        )
        SELECT
            g.ano_referencia,
            g.num_mes_referencia,
            g.seq_veiculo,
            COALESCE(v.dsc_num_placa, 'SEM_CADASTRO_PLACA') AS dsc_num_placa,
            COALESCE(v.dsc_veiculo, 'VEÍCULO ÓRFÃO SEQ ' || CAST(g.seq_veiculo AS VARCHAR)) AS dsc_veiculo,
            v.dsc_tipo_veiculo,
            v.dsc_marca,
            v.dsc_modelo,
            CASE WHEN v.seq_veiculo IS NULL THEN 'VEICULO_ORFAO_NO_CADASTRO' ELSE 'CADASTRADO' END AS status_cadastro_frota,
            g.dsc_tipo_gasto,
            g.dsc_especificacao,
            TRY_CAST(g.num_quant_utilizada AS DECIMAL(18,2)) AS qtd_utilizada,
            TRY_CAST(g.vlr_gasto AS DECIMAL(18,2)) AS vlr_gasto
        FROM frota_gastoFrota g
        LEFT JOIN veiculo_unico v
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
        )
        SELECT
            c.ano_referencia,
            c.seq_contrato,
            c.num_contrato,
            COALESCE(ct.nom_contratado, 'CONTRATADO NÃO INFORMADO') AS nom_contratado,
            ct.doc_contratado AS num_doc_credor,
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
                WHEN TRY_CAST(c.vlr_contrato AS DECIMAL(18,2)) > 0
                THEN ROUND((COALESCE(a.vlr_total_aditivos, 0.00) / TRY_CAST(c.vlr_contrato AS DECIMAL(18,2))) * 100.0, 2)
                ELSE 0.00
            END AS pct_acrescimo_aditivos
        FROM contrato_contratos c
        LEFT JOIN contratados_agg ct
          ON c.seq_contrato = ct.seq_contrato
        LEFT JOIN aditivos_agg a
          ON c.seq_contrato = a.seq_contrato;
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
    }

    for view_name, ddl in GOLD_VIEWS_DDL.items():
        reqs = required_by_view.get(view_name, set())
        if reqs.issubset(existing):
            try:
                con.execute(ddl)
                registered.add(view_name)
            except Exception as exc:
                logger.warning("Falha ao criar Gold View %s: %s", view_name, exc)

    return registered
