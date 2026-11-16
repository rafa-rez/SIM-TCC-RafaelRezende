"""
Camada Gold de Views Semânticas Pré-Unificadas e Desduplicadas (SIM v4.5).

Elimina multiplicação cartesiana em JOINs multi-ano do SICOM/TCE-MG e expõe 8 views
prontas para Text-to-SQL determinístico, multi-município e auditoria forense:
1. vw_gold_folha_servidores (com nom_orgao canônico 1:1, esfera_poder, administracao_tipo e validação de Agente Político)
2. vw_gold_fornecedores_qsa_360 (1 linha única por CNPJ/CPF + PF mascarado preservado + doc_credor_raiz + natureza_credor)
3. vw_gold_execucao_despesa_credor (chave normalizada + PF mascarado + Restos a Pagar + doc_credor_raiz + natureza_credor)
4. vw_gold_frota_consolidada (cadastro cumulativo + gastos de frota via FULL OUTER JOIN + CTB + gasto agregado sem placa)
5. vw_gold_contratos_vigencia_aditivos (proteção contra contratos simbólicos + fallback de signatário + CADASTRADO_NA_BASE)
6. vw_gold_conflito_interesses_licitacao (match exato CPF 11d + match posicional de alta precisão em CPFs mascarados)
7. vw_gold_orcamento_previsto_vs_realizado (LOA consolidada 1 linha/ano do município)
8. vw_gold_orcamento_por_orgao (LOA, dotação atualizada / duodécimo, receita própria e execução por órgão e Poder)
"""
from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Set

import duckdb

logger = logging.getLogger("sim_platform.gold_views")

_CNPJS_PROPRIOS_FROM_ORGAO_MUNICIPIO = """
    SELECT DISTINCT LPAD(regexp_replace(CAST(cod_tip_orgao_1 AS VARCHAR), '[^0-9]', '', 'g'), 14, '0') AS cnpj_orgao_mun
    FROM orgao_municipio
    WHERE length(regexp_replace(CAST(cod_tip_orgao_1 AS VARCHAR), '[^0-9]', '', 'g')) BETWEEN 12 AND 14
    UNION
    SELECT DISTINCT LPAD(regexp_replace(CAST(cnpj_orgao AS VARCHAR), '[^0-9]', '', 'g'), 14, '0') AS cnpj_orgao_mun
    FROM orgao_municipio
    WHERE length(regexp_replace(CAST(cnpj_orgao AS VARCHAR), '[^0-9]', '', 'g')) BETWEEN 12 AND 14
"""

_CNPJS_PROPRIOS_EMPTY = """
    SELECT CAST(NULL AS VARCHAR) AS cnpj_orgao_mun WHERE 1 = 0
"""


def _norm_cod_orgao_key_sql(col_expr: str) -> str:
    """Normaliza chave de órgão de forma resiliente ('02', '2', '002' -> '2')."""
    return f"COALESCE(NULLIF(LTRIM(TRIM(CAST({col_expr} AS VARCHAR)), '0'), ''), '0')"


def _norm_cod_orgao_pad2_sql(col_expr: str) -> str:
    """Formata código de órgão canônico com 2 dígitos ('2' -> '02')."""
    return f"LPAD({_norm_cod_orgao_key_sql(col_expr)}, 2, '0')"


def _credor_doc_key_sql(doc_col: str, nom_col: str, empty_val: str = "'SEM_DOC_CREDOR'") -> str:
    """
    Normaliza documento de credor preservando:
    - CNPJ 14 dígitos e CPF 11 dígitos como dígitos puros;
    - Pessoa Física com CPF pré-mascarado por LGPD (`*` com 6 dígitos visíveis + nome nominal)
      como `<doc_raw_trim> [<NOME_CREDOR>]`, evitando colisão de 6 dígitos e impedindo queda
      indevida em `FOLHA_OU_SEM_DETALHAMENTO`.
    """
    raw_trim = f"TRIM(CAST(COALESCE({doc_col}, '') AS VARCHAR))"
    digits = f"regexp_replace({raw_trim}, '[^0-9]', '', 'g')"
    nom_clean = f"regexp_replace(UPPER(TRIM(CAST(COALESCE({nom_col}, '') AS VARCHAR))), '\\s+', ' ', 'g')"
    return f"""
        CASE
            WHEN length({digits}) IN (11, 14) THEN {digits}
            WHEN CONTAINS({raw_trim}, '*')
                 AND length({digits}) = 6
                 AND {nom_clean} NOT IN ('', 'NÃO INFORMADO', 'NAO INFORMADO', 'CREDOR NÃO INFORMADO', 'CREDOR NAO INFORMADO', '-1')
                 AND NOT {nom_clean} LIKE 'FOLHA%'
                 AND NOT regexp_matches({nom_clean}, '(\\bLTDA\\b|\\bEIRELI\\b|\\bS/A\\b|\\bS\\.A\\.?\\b|\\bMEI\\b|\\bEPP\\b|PREFEITURA|C[AÂ]MARA|FUNDO MUNICIPAL|AUTARQUIA|CONS[OÓ]RCIO|ASSOCIA[CÇ][AÃ]O|FUNDA[CÇ][AÃ]O|INSTITUTO|BANCO |COMPANHIA )')
            THEN {raw_trim} || ' [' || {nom_clean} || ']'
            WHEN length({digits}) >= 11 THEN {digits}
            ELSE {empty_val}
        END
    """


def _natureza_credor_sql(doc_expr: str, nom_expr: str) -> str:
    """
    Classificação universal anti-whack-a-mole da natureza jurídica/contábil do credor
    sem hardcodar nomes locais de cidade específica.
    """
    nom_up = f"UPPER(TRIM(COALESCE({nom_expr}, '')))"
    doc_head = f"SPLIT_PART(COALESCE({doc_expr}, ''), ' [', 1)"
    return f"""
        CASE
            WHEN {doc_expr} IS NULL OR {doc_expr} = 'SEM_DOC_CREDOR'
            THEN 'FOLHA_OU_SEM_DETALHAMENTO'
            WHEN regexp_matches({doc_expr}, '^[0-9]{{11}}$')
                 OR (CONTAINS({doc_head}, '*') AND length(regexp_replace({doc_head}, '[^0-9]', '', 'g')) = 6)
            THEN 'PESSOA_FISICA'
            WHEN NOT regexp_matches({doc_expr}, '^[0-9]{{11,14}}$')
            THEN 'FOLHA_OU_SEM_DETALHAMENTO'
            WHEN {doc_expr} IN (SELECT cnpj_orgao_mun FROM cnpjs_proprios_mun)
                 OR regexp_matches({nom_up}, '(PREFEITURA MUNICIPAL|CAMARA MUNICIPAL|CÂMARA MUNICIPAL|FUNDO MUNICIPAL|SERVI[CÇ]O AUT[OÔ]NOMO DE [AÁ]GUA|\\bSAAE\\b|FUNDA[CÇ][AÃ]O EDUCACIONAL|FUNDA[CÇ][AÃ]O CASA DE CULTURA|SERVIDORES MUNICIPAIS|FOLHA DE PAGAMENTO)')
            THEN 'INTRAGOVERNAMENTAL_MUNICIPIO'
            WHEN SUBSTR({doc_expr}, 1, 8) IN ('29979036', '00394460', '00360305', '18715615', '21154554', '00509968', '00000000', '33657248')
                 OR regexp_matches({nom_up}, '(INSTITUTO NACIONAL DO SEGURO SOCIAL|INSTITUTO NACIONAL DE SEGURIDADE SOCIAL|\\bINSS\\b|RECEITA FEDERAL|\\bPASEP\\b|\\bFGTS\\b|CONS[OÓ]RCIO INTERMUNICIPAL|CONS[OÓ]RCIO P[UÚ]BLICO|INSTITUI[CÇ][AÃ]O DE COOPERA[CÇ][AÃ]O INTERMUNICIPAL|\\bICISMEP\\b|\\bCISMEP\\b|TRIBUNAL DE JUSTI[CÇ]A|TRIBUNAL REGIONAL|TRIBUNAL SUPERIOR|TRIBUNAL DE CONTAS|\\bTRT\\b|\\bTRF\\b|\\bTRE\\b|MINIST[EÉ]RIO |ESTADO DE |SECRETARIA DE ESTADO|TESOURO NACIONAL|FUNDO ESTADUAL|FUNDO NACIONAL|\\bFNDE\\b|\\bFNS\\b|\\bFISTEL\\b|INSTITUTO DE PREVID[EÊ]NCIA|PREVID[EÊ]NCIA MUNICIPAL|CART[OÓ]RIO )')
            THEN 'ENTE_PUBLICO_TRIBUTO_CONSORCIO'
            WHEN regexp_matches({nom_up}, '(\\bCEMIG\\b|\\bCOPASA\\b|\\bSABESP\\b|\\bSANEPAR\\b|\\bEMBASA\\b|\\bCAGECE\\b|\\bCAESB\\b|\\bCEDAE\\b|\\bCORSAN\\b|\\bCELESC\\b|\\bCOPEL\\b|\\bCOELBA\\b|\\bNEOENERGIA\\b|\\bEQUATORIAL\\b|\\bENEL\\b|\\bLIGHT\\b|CORREIOS|BANCO DO BRASIL|CAIXA ECON[OÔ]MICA FEDERAL|BANCO BRADESCO|BANCO SANTANDER|ITA[UÚ] UNIBANCO|BANCO MERCANTIL|BANCO INTER|\\bSICOOB\\b|\\bSICREDI\\b|\\bBDMG\\b|\\bBNDES\\b|\\bSERPRO\\b|\\bDATAPREV\\b|IMPRENSA NACIONAL|COMPANHIA ENERG[EÉ]TICA|COMPANHIA DE SANEAMENTO)')
            THEN 'ESTATAL_CONCESSIONARIA_BANCO'
            WHEN regexp_matches({nom_up}, '(SANTA CASA|BENEFIC[EÊ]NCIA|IRMANDADE|ASSOCIA[CÇ][AÃ]O|FUNDA[CÇ][AÃ]O|SINDICATO|\\bMITRA\\b|DIOCESE|PAR[OÓ]QUIA|\\bAPAE\\b|\\bINSTITUTO\\b|\\bCONSELHO\\b|CAIXA ESCOLAR|CRECHE COMUNIT[AÁ]RIA|LAR DOS IDOSOS|\\bASILO\\b|SOCIEDADE CIVIL|OBRAS SOCIAIS)')
                 AND NOT regexp_matches({nom_up}, '(\\bLTDA\\b|\\bEIRELI\\b|\\bS\\.A\\.?\\b|\\bS/A\\b|\\bSA\\b)')
            THEN 'SEM_FINS_LUCRATIVOS'
            ELSE 'EMPRESA_PRIVADA'
        END
    """


_CTE_DIM_ORGAO_CANONICO = f"""
    dim_orgao_canonico AS (
        SELECT
            cod_orgao_key,
            cod_orgao_norm,
            seq_orgao,
            nom_orgao_oficial,
            tipo_orgao_oficial,
            CASE
                WHEN regexp_matches(UPPER(COALESCE(tipo_orgao_oficial, '') || ' ' || COALESCE(nom_orgao_oficial, '')), '(C[AÂ]MARA|LEGISLATIV)')
                THEN 'LEGISLATIVO'
                ELSE 'EXECUTIVO'
            END AS esfera_poder,
            CASE
                WHEN regexp_matches(UPPER(COALESCE(tipo_orgao_oficial, '') || ' ' || COALESCE(nom_orgao_oficial, '')), '(AUTARQUIA|FUNDA[CÇ][AÃ]O|INSTITUTO|PREVID[EÊ]NCIA|\\bRPPS\\b|EMPRESA|FUNDO|SERVI[CÇ]O AUT[OÔ]NOMO|\\bSAAE\\b)')
                THEN 'INDIRETA_OU_FUNDO'
                ELSE 'DIRETA'
            END AS administracao_tipo
        FROM (
            SELECT
                {_norm_cod_orgao_key_sql("cod_orgao")} AS cod_orgao_key,
                {_norm_cod_orgao_pad2_sql("cod_orgao")} AS cod_orgao_norm,
                seq_orgao,
                TRIM(CAST(nom_orgao AS VARCHAR)) AS nom_orgao_oficial,
                TRIM(CAST(tipo_orgao AS VARCHAR)) AS tipo_orgao_oficial,
                ROW_NUMBER() OVER (
                    PARTITION BY {_norm_cod_orgao_key_sql("cod_orgao")}
                    ORDER BY TRY_CAST(ano_referencia AS INTEGER) DESC NULLS LAST, TRY_CAST(seq_orgao AS BIGINT) DESC NULLS LAST
                ) AS rn
            FROM orgao_orgao
            WHERE cod_orgao IS NOT NULL AND TRIM(CAST(cod_orgao AS VARCHAR)) <> ''
        ) ranked_org
        WHERE rn = 1
    )
"""

_POLITICAL_CARGO_REGEX = (
    r"(PREFEITO|VICE[- ]?PREFEITO|VICE[- ]?PRESIDENTE|PRESIDENTE DA C[AÂ]MARA|"
    r"SECRET[AÁ]RI[OA]|\bSEC\.\s*MUN|\bVEREADOR|AGENTE POL[IÍ]TICO|"
    r"CHEFE DE GABINETE|PROCURADOR|CONTROLADOR|ASSESSOR CH\.\s*CONTROLE INTERNO|DIRETOR\s*-\s*PRESIDENTE)"
)
_FALSE_POLITICAL_SECRETARY_REGEX = r"(SECRET[AÁ]RI[OA]\s+ESCOLAR|AUXILIAR\s+(DE\s+)?SECRET[AÁ]RIA)"
_OPERATIONAL_TECH_CARGO_REGEX = (
    r"(\bPROF\.|\bPROFESSOR|VIGIA|ATENDENTE|ENFERM|AUXILIAR|T[EÉ]CNIC|"
    r"AGENTE COMUNIT|MOTORISTA|SERVENTE|M[EÉ]DIC|PEDAGOG|PSIC[OÓ]LOG|"
    r"ASSISTENTE SOCIAL|OFICIAL ADMINISTRATIV|COVEIRO|MONITOR|ORIENTADOR|INSTRUTOR|"
    r"SECRET[AÁ]RI[OA]\s+ESCOLAR|OPERADOR|ZELADOR|MERENDEIR|GARI|FISCAL)"
)


def _cpf_positional_pattern_11_sql(raw_col: str) -> str:
    """
    Converte qualquer representação de CPF (11 dígitos completos, `***613066**`,
    `***.613.066-**`, `054.613.***-**` ou `012***716**`) em máscara posicional
    canônica de 11 posições `[0-9*]`, ou NULL quando não for CPF válido/mascarado.
    """
    raw_s = f"TRIM(CAST(COALESCE({raw_col}, '') AS VARCHAR))"
    pat = f"regexp_replace({raw_s}, '[^0-9*]', '', 'g')"
    dig = f"regexp_replace({raw_s}, '[^0-9]', '', 'g')"
    return f"""
        CASE
            WHEN length({dig}) = 11 THEN {dig}
            WHEN length({pat}) = 11 AND length({dig}) BETWEEN 6 AND 9 THEN {pat}
            ELSE NULL
        END
    """


def _cpf_patterns_compatible_sql(pat_a: str, pat_b: str) -> str:
    """
    Verifica compatibilidade posicional exata nas 11 posições entre dois padrões `[0-9*]`
    exigindo 0 conflitos e pelo menos 3 dígitos visíveis coincidentes (ex.: posições 4..6).
    """
    no_conflict = " AND ".join(
        f"(SUBSTR({pat_a}, {i}, 1) = '*' OR SUBSTR({pat_b}, {i}, 1) = '*' OR SUBSTR({pat_a}, {i}, 1) = SUBSTR({pat_b}, {i}, 1))"
        for i in range(1, 12)
    )
    overlap_sum = " + ".join(
        f"(CASE WHEN SUBSTR({pat_a}, {i}, 1) <> '*' AND SUBSTR({pat_b}, {i}, 1) <> '*' AND SUBSTR({pat_a}, {i}, 1) = SUBSTR({pat_b}, {i}, 1) THEN 1 ELSE 0 END)"
        for i in range(1, 12)
    )
    return f"({pat_a} IS NOT NULL AND {pat_b} IS NOT NULL AND ({no_conflict}) AND ({overlap_sum}) >= 3)"


GOLD_VIEWS_DDL: Dict[str, str] = {
    "vw_gold_folha_servidores": f"""
        CREATE OR REPLACE VIEW vw_gold_folha_servidores AS
        WITH pessoa_dedup AS (
            SELECT
                seq_dim_pessoa,
                ano_referencia,
                MAX(nom_pessoa) AS nom_pessoa,
                MAX(num_documento) AS num_cpf
            FROM pessoa_municipio
            GROUP BY seq_dim_pessoa, ano_referencia
        ),
        orgao_folha_dedup AS (
            SELECT
                seq_dim_orgao,
                MAX(cod_orgao) AS cod_orgao,
                MAX(nom_orgao) AS nom_orgao_raw,
                MAX(CAST(cnpj_orgao AS VARCHAR)) AS cnpj_orgao_shifted
            FROM orgao_municipio
            GROUP BY seq_dim_orgao
        ),
        {_CTE_DIM_ORGAO_CANONICO}
        SELECT
            r.ano_referencia,
            r.seq_dat_pagamento,
            r.seq_dim_pessoa,
            COALESCE(r.cod_orgao, om.cod_orgao) AS cod_orgao,
            COALESCE(
                dim.nom_orgao_oficial,
                CASE
                    WHEN om.nom_orgao_raw IS NOT NULL
                         AND UPPER(TRIM(CAST(om.nom_orgao_raw AS VARCHAR))) NOT IN ('', 'MUNICIPAL', 'ESTADUAL', 'NÃO INFORMADO', 'NAO INFORMADO')
                    THEN TRIM(CAST(om.nom_orgao_raw AS VARCHAR))
                    WHEN om.cnpj_orgao_shifted IS NOT NULL
                         AND regexp_matches(UPPER(om.cnpj_orgao_shifted), '[A-ZÀ-Ú]')
                    THEN UPPER(TRIM(om.cnpj_orgao_shifted))
                    ELSE NULL
                END,
                'ÓRGÃO ' || CAST(COALESCE(r.cod_orgao, r.seq_dim_orgao) AS VARCHAR)
            ) AS nom_orgao,
            COALESCE(NULLIF(TRIM(r.dsc_tipo_pag), ''), 'Mensal') AS dsc_tipo_pag,
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
                WHEN COALESCE(r.dsc_sgl_cargo, '') ILIKE '%Estagi%'
                     OR UPPER(COALESCE(r.dsc_cargo, '')) LIKE '%ESTAGI%'
                     OR UPPER(COALESCE(r.dsc_cargo, '')) LIKE '%APRENDIZ%'
                     OR UPPER(COALESCE(r.dsc_cargo, '')) LIKE '%BOLSISTA%'
                THEN 'Estagiário / Aprendiz / Bolsista'
                WHEN COALESCE(r.dsc_sgl_cargo, '') ILIKE '%Honor%'
                     OR UPPER(COALESCE(r.dsc_cargo, '')) LIKE '%HONORIF%'
                     OR UPPER(COALESCE(r.dsc_cargo, '')) LIKE '%HONORÍF%'
                THEN 'Agente Honorífico (Conselheiro)'
                WHEN COALESCE(r.dsc_sgl_cargo, '') ILIKE '%Efetivo%' THEN 'Efetivo (Concursado)'
                WHEN COALESCE(r.dsc_sgl_cargo, '') ILIKE '%Emprego%' THEN 'Emprego Público (Concursado / Seleção Pública)'
                WHEN COALESCE(r.dsc_sgl_cargo, '') ILIKE '%tempor%' THEN 'Contratado / Servidor Temporário'
                WHEN COALESCE(r.dsc_sgl_cargo, '') ILIKE '%Comissionado%' THEN 'Comissionado (Recrutamento Amplo)'
                WHEN COALESCE(r.dsc_sgl_cargo, '') ILIKE '%Agente pol%' THEN
                    CASE
                        WHEN regexp_matches(UPPER(COALESCE(r.dsc_cargo, '')), '{_POLITICAL_CARGO_REGEX}')
                             AND NOT regexp_matches(UPPER(COALESCE(r.dsc_cargo, '')), '{_FALSE_POLITICAL_SECRETARY_REGEX}')
                        THEN 'Agente Político (Prefeito, Vice, Secretários e Vereadores)'
                        WHEN regexp_matches(UPPER(COALESCE(r.dsc_cargo, '')), '{_OPERATIONAL_TECH_CARGO_REGEX}')
                        THEN 'Servidor Público (Quadro Geral / Temporário)'
                        ELSE 'Servidor Público (Quadro Geral / Temporário)'
                    END
                WHEN r.dsc_situacao IS NOT NULL AND TRIM(r.dsc_situacao) NOT IN ('', 'NÃO INFORMADO', 'NAO INFORMADO') THEN r.dsc_situacao
                ELSE COALESCE(NULLIF(TRIM(r.dsc_sgl_cargo), ''), NULLIF(TRIM(r.dsc_sit_serv_pens), ''), 'NÃO INFORMADO')
            END AS dsc_situacao,
            COALESCE(NULLIF(TRIM(r.dsc_sgl_cargo), 'NÃO INFORMADO'), r.dsc_regime) AS dsc_regime,
            CASE
                WHEN COALESCE(r.dsc_sgl_cargo, '') ILIKE '%Agente pol%'
                     AND (
                         UPPER(COALESCE(r.dsc_cargo, '')) LIKE '%ESTAGI%'
                         OR UPPER(COALESCE(r.dsc_cargo, '')) LIKE '%APRENDIZ%'
                         OR UPPER(COALESCE(r.dsc_cargo, '')) LIKE '%BOLSISTA%'
                     )
                THEN 'Estagiário/Aluno aprendiz/Bolsista'
                WHEN COALESCE(r.dsc_sgl_cargo, '') ILIKE '%Agente pol%'
                     AND (
                         NOT regexp_matches(UPPER(COALESCE(r.dsc_cargo, '')), '{_POLITICAL_CARGO_REGEX}')
                         OR regexp_matches(UPPER(COALESCE(r.dsc_cargo, '')), '{_FALSE_POLITICAL_SECRETARY_REGEX}')
                     )
                THEN 'Servidor Público (Quadro Geral)'
                ELSE r.dsc_sgl_cargo
            END AS tipo_vinculo,
            r.dsc_sit_serv_pens AS situacao_funcional,
            r.dsc_agepol AS cargo_politico,
            TRY_CAST(r.vlr_rmn_bruta AS DECIMAL(18,2)) AS vlr_rmn_bruta,
            TRY_CAST(r.vlr_rmn_liquida AS DECIMAL(18,2)) AS vlr_rmn_liquida,
            TRY_CAST(r.vlr_descontos AS DECIMAL(18,2)) AS vlr_descontos,
            {_norm_cod_orgao_pad2_sql("COALESCE(r.cod_orgao, om.cod_orgao)")} AS cod_orgao_norm,
            CASE
                WHEN dim.esfera_poder = 'LEGISLATIVO'
                     OR regexp_matches(
                         UPPER(COALESCE(om.nom_orgao_raw, '') || ' ' || COALESCE(om.cnpj_orgao_shifted, '') || ' ' || COALESCE(r.dsc_lotacao, '') || ' ' || COALESCE(r.dsc_cargo, '')),
                         '(C[AÂ]MARA MUNICIPAL|PODER LEGISLATIVO|CORPO LEGISLATIVO|\\bVEREADOR\\b)'
                     )
                THEN 'LEGISLATIVO'
                ELSE COALESCE(dim.esfera_poder, 'EXECUTIVO')
            END AS esfera_poder,
            CASE
                WHEN dim.administracao_tipo = 'INDIRETA_OU_FUNDO'
                     OR regexp_matches(
                         UPPER(COALESCE(om.nom_orgao_raw, '') || ' ' || COALESCE(om.cnpj_orgao_shifted, '') || ' ' || COALESCE(r.dsc_lotacao, '')),
                         '(AUTARQUIA|FUNDA[CÇ][AÃ]O|INSTITUTO DE PREVID|\\bRPPS\\b|SERVI[CÇ]O AUT[OÔ]NOMO|\\bSAAE\\b|\\bF\\.C\\.C\\.C\\.?\\b|\\bF\\.E\\.C\\.?\\b|CASA DE CULTURA|FUNDO MUNICIPAL)'
                     )
                THEN 'INDIRETA_OU_FUNDO'
                ELSE COALESCE(dim.administracao_tipo, 'DIRETA')
            END AS administracao_tipo
        FROM remuneracao_municipio r
        LEFT JOIN pessoa_dedup p
          ON r.seq_dim_pessoa = p.seq_dim_pessoa
         AND r.ano_referencia = p.ano_referencia
        LEFT JOIN orgao_folha_dedup om
          ON r.seq_dim_orgao = om.seq_dim_orgao
        LEFT JOIN dim_orgao_canonico dim
          ON {_norm_cod_orgao_key_sql("COALESCE(r.cod_orgao, om.cod_orgao)")} = dim.cod_orgao_key;
    """,
    "vw_gold_fornecedores_qsa_360": f"""
        CREATE OR REPLACE VIEW vw_gold_fornecedores_qsa_360 AS
        WITH cnpjs_proprios_mun AS (
            {{__CNPJS_PROPRIOS_SQL__}}
        ),
        pagamentos_credor AS (
            SELECT
                {_credor_doc_key_sql("num_doc_credor", "nom_credor")} AS doc_limpo,
                MAX(nom_credor) AS nom_credor,
                COUNT(*) AS qtd_pagamentos,
                ROUND(
                    SUM(
                        COALESCE(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2)), 0.00)
                        - COALESCE(TRY_CAST(vlr_anu_fonte AS DECIMAL(18,2)), 0.00)
                    ),
                    2
                ) AS total_pago_historico,
                MIN(ano_referencia) AS primeiro_ano_pagamento,
                MAX(ano_referencia) AS ultimo_ano_pagamento
            FROM despesa_pagamento
            WHERE {_credor_doc_key_sql("num_doc_credor", "nom_credor")} <> 'SEM_DOC_CREDOR'
            GROUP BY 1
        ),
        socios_sicom_dedup AS (
            SELECT
                regexp_replace(COALESCE(h.num_documento, ''), '[^0-9]', '', 'g') AS cnpj_empresa,
                MAX(h.nom_pessoa) AS razao_social,
                COUNT(DISTINCT COALESCE(NULLIF(TRIM(CAST(q.num_cgc_habilitado AS VARCHAR)), ''), UPPER(TRIM(COALESCE(q.nom_pessoa, ''))))) AS qtd_socios_identificados,
                STRING_AGG(DISTINCT UPPER(TRIM(q.nom_pessoa)), ' | ' ORDER BY UPPER(TRIM(q.nom_pessoa))) AS nom_socio,
                STRING_AGG(DISTINCT TRIM(CAST(COALESCE(q.num_cgc_habilitado, '') AS VARCHAR)), ' | ') AS cpf_cnpj_socio,
                MAX(q.dsc_tipo_participacao) AS dsc_tipo_participacao
            FROM licitacao_quadroSocLicitacao q
            JOIN licitacao_habLicitacao h
              ON q.seq_hab_licitacao = h.seq_hab_licitacao
             AND q.ano_referencia = h.ano_referencia
            WHERE regexp_matches(regexp_replace(COALESCE(h.num_documento, ''), '[^0-9]', '', 'g'), '^[0-9]{{14}}$')
            GROUP BY 1
        )
        SELECT
            COALESCE(p.doc_limpo, s.cnpj_empresa) AS num_doc_credor,
            CASE
                WHEN regexp_matches(COALESCE(p.doc_limpo, s.cnpj_empresa, ''), '^[0-9]{{14}}$')
                THEN SUBSTR(COALESCE(p.doc_limpo, s.cnpj_empresa), 1, 8)
                ELSE NULL
            END AS doc_credor_raiz,
            COALESCE(p.nom_credor, s.razao_social) AS nom_credor,
            {_natureza_credor_sql("COALESCE(p.doc_limpo, s.cnpj_empresa)", "COALESCE(p.nom_credor, s.razao_social)")} AS natureza_credor,
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
        WHERE COALESCE(p.doc_limpo, s.cnpj_empresa, 'SEM_DOC_CREDOR') <> 'SEM_DOC_CREDOR';
    """,
    "vw_gold_execucao_despesa_credor": f"""
        CREATE OR REPLACE VIEW vw_gold_execucao_despesa_credor AS
        WITH cnpjs_proprios_mun AS (
            {{__CNPJS_PROPRIOS_SQL__}}
        ),
        orgao_dedup AS (
            SELECT
                seq_orgao,
                {_norm_cod_orgao_pad2_sql("MAX(cod_orgao)")} AS cod_orgao,
                MAX(nom_orgao) AS nom_orgao,
                MAX(tipo_orgao) AS tipo_orgao
            FROM orgao_orgao
            GROUP BY seq_orgao
        ),
        credor_emp_dedup AS (
            SELECT
                seq_empenho,
                ano_referencia,
                MAX({_credor_doc_key_sql("num_doc_credor", "nom_credor")}) AS doc_credor,
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
                ROUND(SUM(COALESCE(TRY_CAST(e.vlr_empenhado AS DECIMAL(18,2)), 0.00)), 2) AS total_empenhado_bruto,
                ROUND(
                    SUM(
                        COALESCE(TRY_CAST(e.vlr_empenhado AS DECIMAL(18,2)), 0.00)
                        + COALESCE(TRY_CAST(e.vlr_reforco AS DECIMAL(18,2)), 0.00)
                        - COALESCE(TRY_CAST(e.vlr_anulempenho AS DECIMAL(18,2)), 0.00)
                    ),
                    2
                ) AS total_empenhado,
                SUM(
                    CASE
                        WHEN COALESCE(e.ind_dec_licitacao, '') ILIKE '%ADES%O%'
                        THEN COALESCE(TRY_CAST(e.vlr_empenhado AS DECIMAL(18,2)), 0.00)
                             + COALESCE(TRY_CAST(e.vlr_reforco AS DECIMAL(18,2)), 0.00)
                             - COALESCE(TRY_CAST(e.vlr_anulempenho AS DECIMAL(18,2)), 0.00)
                        ELSE 0
                    END
                ) AS total_empenhado_carona,
                SUM(
                    CASE
                        WHEN COALESCE(e.ind_dec_licitacao, '') ILIKE '%DISPENSA%' OR COALESCE(e.ind_dec_licitacao, '') ILIKE '%INEXIG%'
                        THEN COALESCE(TRY_CAST(e.vlr_empenhado AS DECIMAL(18,2)), 0.00)
                             + COALESCE(TRY_CAST(e.vlr_reforco AS DECIMAL(18,2)), 0.00)
                             - COALESCE(TRY_CAST(e.vlr_anulempenho AS DECIMAL(18,2)), 0.00)
                        ELSE 0
                    END
                ) AS total_empenhado_dispensa_inexigibilidade
            FROM empenho_empenho e
            LEFT JOIN credor_emp_dedup ce
              ON e.seq_empenho = ce.seq_empenho
             AND e.ano_referencia = ce.ano_referencia
            GROUP BY 1, 2, 3
        ),
        pag_rp_credor_liq AS (
            SELECT
                ano_referencia,
                seq_liquidacao,
                MAX({_credor_doc_key_sql("num_doc_credor", "nom_credor", empty_val="CAST(NULL AS VARCHAR)")}) AS doc_credor,
                MAX(nom_credor) AS nom_credor
            FROM despesa_pagamento
            WHERE seq_liquidacao IS NOT NULL
              AND TRY_CAST(seq_liquidacao AS BIGINT) > 0
            GROUP BY ano_referencia, seq_liquidacao
        ),
        pag_rp_credor_rsp AS (
            SELECT
                ano_referencia,
                seq_rsp,
                MAX({_credor_doc_key_sql("num_doc_credor", "nom_credor", empty_val="CAST(NULL AS VARCHAR)")}) AS doc_credor,
                MAX(nom_credor) AS nom_credor
            FROM despesa_pagamento
            WHERE seq_rsp IS NOT NULL
              AND TRY_CAST(seq_rsp AS BIGINT) > 0
            GROUP BY ano_referencia, seq_rsp
        ),
        liq AS (
            SELECT
                l.ano_referencia,
                l.seq_orgao,
                COALESCE(ce.doc_credor, prl.doc_credor, prr.doc_credor, 'SEM_DOC_CREDOR') AS doc_credor,
                MAX(COALESCE(ce.nom_credor, prl.nom_credor, prr.nom_credor, 'FOLHA / CREDOR GLOBAL SEM DETALHAMENTO')) AS nom_credor,
                ROUND(SUM(COALESCE(TRY_CAST(l.vlr_liquidacao AS DECIMAL(18,2)), 0.00)), 2) AS total_liquidado_bruto,
                ROUND(
                    SUM(
                        CASE
                            WHEN COALESCE(TRY_CAST(l.seq_empenho AS INTEGER), 0) <> -1
                                 AND COALESCE(l.dsc_tipo_liquidacao, '') NOT ILIKE '%RESTOS A PAGAR%'
                            THEN COALESCE(TRY_CAST(l.vlr_liquidacao AS DECIMAL(18,2)), 0.00)
                                 - COALESCE(TRY_CAST(l.vlr_anu_liquidacao AS DECIMAL(18,2)), 0.00)
                            ELSE 0.00
                        END
                    ),
                    2
                ) AS liquidado_exercicio,
                ROUND(
                    SUM(
                        CASE
                            WHEN COALESCE(TRY_CAST(l.seq_empenho AS INTEGER), 0) = -1
                                 OR COALESCE(l.dsc_tipo_liquidacao, '') ILIKE '%RESTOS A PAGAR%'
                            THEN COALESCE(TRY_CAST(l.vlr_liquidacao AS DECIMAL(18,2)), 0.00)
                                 - COALESCE(TRY_CAST(l.vlr_anu_liquidacao AS DECIMAL(18,2)), 0.00)
                            ELSE 0.00
                        END
                    ),
                    2
                ) AS liquidado_restos_a_pagar,
                ROUND(
                    SUM(
                        COALESCE(TRY_CAST(l.vlr_liquidacao AS DECIMAL(18,2)), 0.00)
                        - COALESCE(TRY_CAST(l.vlr_anu_liquidacao AS DECIMAL(18,2)), 0.00)
                    ),
                    2
                ) AS total_liquidado
            FROM despesa_liquidacao l
            LEFT JOIN credor_emp_dedup ce
              ON l.seq_empenho = ce.seq_empenho
             AND l.ano_referencia = ce.ano_referencia
            LEFT JOIN pag_rp_credor_liq prl
              ON l.ano_referencia = prl.ano_referencia
             AND l.seq_liquidacao = prl.seq_liquidacao
            LEFT JOIN pag_rp_credor_rsp prr
              ON l.ano_referencia = prr.ano_referencia
             AND l.seq_rsp = prr.seq_rsp
            GROUP BY 1, 2, 3
        ),
        pag AS (
            SELECT
                ano_referencia,
                seq_orgao,
                {_credor_doc_key_sql("num_doc_credor", "nom_credor")} AS doc_credor,
                MAX(nom_credor) AS nom_credor,
                ROUND(SUM(COALESCE(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2)), 0.00)), 2) AS total_pago_bruto,
                ROUND(
                    SUM(
                        CASE
                            WHEN COALESCE(TRY_CAST(seq_empenho AS INTEGER), 0) <> -1
                                 AND COALESCE(dsc_tipo_pagamento, '') NOT ILIKE '%RESTOS A PAGAR%'
                            THEN COALESCE(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2)), 0.00)
                                 - COALESCE(TRY_CAST(vlr_anu_fonte AS DECIMAL(18,2)), 0.00)
                            ELSE 0.00
                        END
                    ),
                    2
                ) AS pago_exercicio,
                ROUND(
                    SUM(
                        COALESCE(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2)), 0.00)
                        - COALESCE(TRY_CAST(vlr_anu_fonte AS DECIMAL(18,2)), 0.00)
                    ),
                    2
                ) AS total_pago,
                ROUND(
                    SUM(
                        CASE
                            WHEN COALESCE(TRY_CAST(seq_empenho AS INTEGER), 0) = -1
                                 OR COALESCE(dsc_tipo_pagamento, '') ILIKE '%RESTOS A PAGAR%'
                            THEN COALESCE(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2)), 0.00)
                                 - COALESCE(TRY_CAST(vlr_anu_fonte AS DECIMAL(18,2)), 0.00)
                            ELSE 0.00
                        END
                    ),
                    2
                ) AS total_pago_restos_a_pagar
            FROM despesa_pagamento
            GROUP BY 1, 2, 3
        )
        SELECT
            COALESCE(e.ano_referencia, l.ano_referencia, p.ano_referencia) AS ano_referencia,
            COALESCE(e.seq_orgao, l.seq_orgao, p.seq_orgao) AS seq_orgao,
            o.cod_orgao AS cod_orgao,
            COALESCE(o.nom_orgao, 'ÓRGÃO ' || CAST(COALESCE(e.seq_orgao, l.seq_orgao, p.seq_orgao) AS VARCHAR)) AS nom_orgao,
            CASE
                WHEN regexp_matches(UPPER(COALESCE(o.tipo_orgao, '') || ' ' || COALESCE(o.nom_orgao, '')), '(C[AÂ]MARA|LEGISLATIV)')
                THEN 'LEGISLATIVO'
                ELSE 'EXECUTIVO'
            END AS esfera_poder,
            COALESCE(e.doc_credor, l.doc_credor, p.doc_credor) AS num_doc_credor,
            CASE
                WHEN regexp_matches(COALESCE(e.doc_credor, l.doc_credor, p.doc_credor, ''), '^[0-9]{{14}}$')
                THEN SUBSTR(COALESCE(e.doc_credor, l.doc_credor, p.doc_credor), 1, 8)
                ELSE NULL
            END AS doc_credor_raiz,
            COALESCE(p.nom_credor, e.nom_credor, l.nom_credor) AS nom_credor,
            {_natureza_credor_sql("COALESCE(e.doc_credor, l.doc_credor, p.doc_credor)", "COALESCE(p.nom_credor, e.nom_credor, l.nom_credor)")} AS natureza_credor,
            COALESCE(e.qtd_empenhos, 0) AS qtd_empenhos,
            COALESCE(e.total_empenhado, 0.00) AS total_empenhado,
            COALESCE(e.total_empenhado_bruto, 0.00) AS total_empenhado_bruto,
            COALESCE(l.liquidado_exercicio, 0.00) AS liquidado_exercicio,
            COALESCE(l.liquidado_restos_a_pagar, 0.00) AS liquidado_restos_a_pagar,
            COALESCE(l.total_liquidado, 0.00) AS total_liquidado,
            COALESCE(l.total_liquidado_bruto, 0.00) AS total_liquidado_bruto,
            COALESCE(p.pago_exercicio, 0.00) AS pago_exercicio,
            COALESCE(p.total_pago, 0.00) AS total_pago,
            COALESCE(p.total_pago_bruto, 0.00) AS total_pago_bruto,
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
    "vw_gold_frota_consolidada": f"""
        CREATE OR REPLACE VIEW vw_gold_frota_consolidada AS
        WITH {_CTE_DIM_ORGAO_CANONICO},
        veiculo_unico AS (
            SELECT
                seq_veiculo,
                MAX(seq_orgao) AS seq_orgao_veic,
                MAX(cod_orgao) AS cod_orgao_veic,
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
            COALESCE(g.seq_orgao, v.seq_orgao_veic, dim.seq_orgao) AS seq_orgao,
            COALESCE(dim.cod_orgao_norm, CAST(g.cod_orgao AS VARCHAR), CAST(v.cod_orgao_veic AS VARCHAR)) AS cod_orgao,
            COALESCE(dim.nom_orgao_oficial, 'ADMINISTRAÇÃO MUNICIPAL') AS nom_orgao,
            COALESCE(dim.esfera_poder, 'EXECUTIVO') AS esfera_poder,
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
                WHEN v.seq_veiculo IS NULL AND TRY_CAST(g.seq_veiculo AS BIGINT) <= 0
                THEN 'SEM_PLACA_INDIVIDUALIZADA'
                ELSE 'SEM_CADASTRO_PLACA'
            END AS dsc_num_placa,
            CASE
                WHEN v.dsc_veiculo IS NOT NULL THEN v.dsc_veiculo
                WHEN TRY_CAST(g.seq_veiculo AS BIGINT) <= 0 THEN 'GASTO AGREGADO DE FROTA SEM PLACA INDIVIDUAL'
                ELSE 'VEÍCULO CADASTRADO PRÉ-2020 / ÓRFÃO SEQ ' || CAST(g.seq_veiculo AS VARCHAR)
            END AS dsc_veiculo,
            v.dsc_tipo_veiculo,
            v.dsc_marca,
            v.dsc_modelo,
            CASE
                WHEN v.dsc_situacaoveiculo IS NOT NULL THEN v.dsc_situacaoveiculo
                WHEN TRY_CAST(g.seq_veiculo AS BIGINT) <= 0 THEN 'DESPESA GLOBAL DE FROTA SEM PLACA INDIVIDUALIZADA'
                ELSE 'ATIVO EM GASTOS DE FROTA (CADASTRO PRÉ-2020)'
            END AS dsc_situacaoveiculo,
            CASE
                WHEN v.seq_veiculo IS NULL AND TRY_CAST(g.seq_veiculo AS BIGINT) <= 0 THEN 'SEM_PLACA_INDIVIDUALIZADA'
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
          ON g.seq_veiculo = v.seq_veiculo
        LEFT JOIN dim_orgao_canonico dim
          ON {_norm_cod_orgao_key_sql("COALESCE(g.cod_orgao, v.cod_orgao_veic)")} = dim.cod_orgao_key;
    """,
    "vw_gold_contratos_vigencia_aditivos": f"""
        CREATE OR REPLACE VIEW vw_gold_contratos_vigencia_aditivos AS
        WITH {_CTE_DIM_ORGAO_CANONICO},
        aditivos_agg AS (
            SELECT
                seq_contrato,
                MAX(ano_referencia) AS ano_ref_aditivo,
                MAX(seq_orgao) AS seq_orgao_aditivo,
                MAX(cod_orgao) AS cod_orgao_aditivo,
                MAX(num_contrato) AS num_contrato_aditivo,
                MAX(num_ano_contrato) AS num_ano_contrato_aditivo,
                COUNT(*) AS qtd_aditivos,
                ROUND(
                    SUM(
                        CASE
                            WHEN COALESCE(dsc_tipo_alteracao, '') ILIKE '%DECRESCIMO%'
                            THEN -ABS(COALESCE(TRY_CAST(vlr_termo_aditivo AS DECIMAL(18,2)), 0.00))
                            ELSE COALESCE(TRY_CAST(vlr_termo_aditivo AS DECIMAL(18,2)), 0.00)
                        END
                    ),
                    2
                ) AS vlr_total_aditivos,
                ROUND(
                    SUM(
                        CASE
                            WHEN COALESCE(dsc_tipo_alteracao, '') NOT ILIKE '%DECRESCIMO%'
                                 AND COALESCE(dsc_tipo_termo, '') NOT ILIKE '%REAJUSTE%'
                                 AND COALESCE(dsc_tipo_termo, '') NOT ILIKE '%RECOMPOSI%'
                                 AND COALESCE(dsc_tipo_termo, '') NOT ILIKE '%EQUILIBR%'
                                 AND COALESCE(dsc_tipo_termo, '') NOT ILIKE '%PRAZO%'
                            THEN COALESCE(TRY_CAST(vlr_termo_aditivo AS DECIMAL(18,2)), 0.00)
                            ELSE 0.00
                        END
                    ),
                    2
                ) AS vlr_aditivos_acrescimo_objeto,
                ROUND(
                    SUM(
                        CASE
                            WHEN COALESCE(dsc_tipo_termo, '') ILIKE '%REAJUSTE%'
                                 OR COALESCE(dsc_tipo_termo, '') ILIKE '%RECOMPOSI%'
                                 OR COALESCE(dsc_tipo_termo, '') ILIKE '%EQUILIBR%'
                            THEN COALESCE(TRY_CAST(vlr_termo_aditivo AS DECIMAL(18,2)), 0.00)
                            ELSE 0.00
                        END
                    ),
                    2
                ) AS vlr_reajuste_reequilibrio,
                ROUND(
                    SUM(
                        CASE
                            WHEN COALESCE(dsc_tipo_termo, '') ILIKE '%PRAZO%'
                                 AND COALESCE(dsc_tipo_alteracao, '') NOT ILIKE '%DECRESCIMO%'
                            THEN COALESCE(TRY_CAST(vlr_termo_aditivo AS DECIMAL(18,2)), 0.00)
                            ELSE 0.00
                        END
                    ),
                    2
                ) AS vlr_prorrogacoes_vigencia,
                ROUND(
                    SUM(
                        CASE
                            WHEN COALESCE(dsc_tipo_alteracao, '') ILIKE '%DECRESCIMO%'
                            THEN ABS(COALESCE(TRY_CAST(vlr_termo_aditivo AS DECIMAL(18,2)), 0.00))
                            ELSE 0.00
                        END
                    ),
                    2
                ) AS vlr_supressoes_aditivos,
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
            COALESCE(c.ano_referencia, a.ano_ref_aditivo) AS ano_referencia,
            COALESCE(c.seq_contrato, a.seq_contrato) AS seq_contrato,
            COALESCE(c.num_contrato, a.num_contrato_aditivo) AS num_contrato,
            COALESCE(c.num_ano_contrato, a.num_ano_contrato_aditivo) AS num_ano_contrato,
            COALESCE(c.seq_orgao, a.seq_orgao_aditivo) AS seq_orgao,
            COALESCE(c.cod_orgao, a.cod_orgao_aditivo, o.cod_orgao, dim.cod_orgao_norm) AS cod_orgao,
            COALESCE(
                o.nom_orgao,
                dim.nom_orgao_oficial,
                'ÓRGÃO ' || CAST(COALESCE(c.seq_orgao, a.seq_orgao_aditivo) AS VARCHAR)
            ) AS nom_orgao,
            COALESCE(
                NULLIF(TRIM(ct.nom_contratado), ''),
                CASE
                    WHEN c.dsc_nome_signatario IS NOT NULL
                         AND TRIM(CAST(c.dsc_nome_signatario AS VARCHAR)) <> ''
                         AND NOT regexp_matches(
                             UPPER(TRIM(CAST(c.dsc_nome_signatario AS VARCHAR))),
                             '^(MUNIC[IÍ]PIO DE|PREFEITURA|C[AÂ]MARA MUNICIPAL|SERVI[CÇ]O AUT[OÔ]NOMO|FUNDA[CÇ][AÃ]O CASA|FUNDA[CÇ][AÃ]O EDUCACIONAL|FISCAL DE CONTRATO|N[AÃ]O INFORMADO)'
                         )
                    THEN TRIM(CAST(c.dsc_nome_signatario AS VARCHAR))
                    ELSE NULL
                END,
                'CONTRATADO NÃO INFORMADO'
            ) AS nom_contratado,
            COALESCE(
                NULLIF(TRIM(CAST(ct.doc_contratado AS VARCHAR)), ''),
                NULLIF(TRIM(CAST(c.num_doc_signatario AS VARCHAR)), '')
            ) AS num_doc_credor,
            c.dsc_naturezaobjeto,
            c.dsc_tipo_processo,
            COALESCE(c.dsc_objetocontrato, 'CONTRATO FIRMADO ANTERIORMENTE A 2020 COM ADITIVO VIGENTE') AS dsc_objetocontrato,
            c.dat_assinatura,
            c.dat_iniciovigencia,
            c.dat_fimvigencia,
            a.dat_termino_apos_aditivo,
            CASE
                WHEN c.seq_contrato IS NULL THEN 'CONTRATO_ANTERIOR_A_2020'
                ELSE 'CADASTRADO_NA_BASE'
            END AS status_cadastro_contrato,
            TRY_CAST(c.vlr_contrato AS DECIMAL(18,2)) AS vlr_original_contrato,
            COALESCE(a.qtd_aditivos, 0) AS qtd_aditivos,
            COALESCE(a.vlr_total_aditivos, 0.00) AS vlr_total_aditivos,
            COALESCE(a.vlr_aditivos_acrescimo_objeto, 0.00) AS vlr_aditivos_acrescimo_objeto,
            COALESCE(a.vlr_reajuste_reequilibrio, 0.00) AS vlr_reajuste_reequilibrio,
            COALESCE(a.vlr_prorrogacoes_vigencia, 0.00) AS vlr_prorrogacoes_vigencia,
            COALESCE(a.vlr_supressoes_aditivos, 0.00) AS vlr_supressoes_aditivos,
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
                THEN ROUND((COALESCE(a.vlr_aditivos_acrescimo_objeto, 0.00) / TRY_CAST(c.vlr_contrato AS DECIMAL(18,2))) * 100.0, 2)
                ELSE 0.00
            END AS pct_acrescimo_aditivos
        FROM contrato_contratos c
        FULL OUTER JOIN aditivos_agg a
          ON c.seq_contrato = a.seq_contrato
        LEFT JOIN contratados_agg ct
          ON COALESCE(c.seq_contrato, a.seq_contrato) = ct.seq_contrato
        LEFT JOIN orgao_contrato_dedup o
          ON COALESCE(c.seq_orgao, a.seq_orgao_aditivo) = o.seq_orgao
        LEFT JOIN dim_orgao_canonico dim
          ON {_norm_cod_orgao_key_sql("COALESCE(c.cod_orgao, a.cod_orgao_aditivo)")} = dim.cod_orgao_key;
    """,
    "vw_gold_conflito_interesses_licitacao": f"""
        CREATE OR REPLACE VIEW vw_gold_conflito_interesses_licitacao AS
        WITH socios_empresas AS (
            SELECT
                {_cpf_positional_pattern_11_sql("q.num_cgc_habilitado")} AS cpf_pat_socio,
                regexp_replace(COALESCE(CAST(q.num_cgc_habilitado AS VARCHAR), ''), '[^0-9]', '', 'g') AS cpf_digits_socio,
                MAX(TRIM(CAST(q.num_cgc_habilitado AS VARCHAR))) AS cpf_display_socio,
                regexp_replace(UPPER(TRIM(COALESCE(q.nom_pessoa, ''))), '\\s+', ' ', 'g') AS nom_socio,
                MAX(UPPER(TRIM(q.dsc_tipo_participacao))) AS dsc_tipo_participacao,
                regexp_replace(COALESCE(CAST(h.num_documento AS VARCHAR), ''), '[^0-9]', '', 'g') AS cnpj_empresa,
                MAX(UPPER(TRIM(h.nom_pessoa))) AS nom_empresa,
                MIN(q.ano_referencia) AS primeiro_ano_licitacao,
                MAX(q.ano_referencia) AS ultimo_ano_licitacao
            FROM licitacao_quadroSocLicitacao q
            JOIN licitacao_habLicitacao h
              ON q.seq_hab_licitacao = h.seq_hab_licitacao
             AND q.ano_referencia = h.ano_referencia
            WHERE {_cpf_positional_pattern_11_sql("q.num_cgc_habilitado")} IS NOT NULL
              AND regexp_matches(regexp_replace(COALESCE(CAST(h.num_documento AS VARCHAR), ''), '[^0-9]', '', 'g'), '^[0-9]{{14}}$')
            GROUP BY 1, 2, 4, 6
        ),
        servidores AS (
            SELECT
                {_cpf_positional_pattern_11_sql("p.num_documento")} AS cpf_pat_servidor,
                regexp_replace(COALESCE(CAST(p.num_documento AS VARCHAR), ''), '[^0-9]', '', 'g') AS cpf_digits_servidor,
                MAX(TRIM(CAST(p.num_documento AS VARCHAR))) AS cpf_display_servidor,
                regexp_replace(UPPER(TRIM(COALESCE(p.nom_pessoa, ''))), '\\s+', ' ', 'g') AS nom_servidor,
                COALESCE(
                    MAX(CASE WHEN TRIM(COALESCE(r.dsc_cargo, '')) NOT IN ('', 'NÃO INFORMADO', 'NAO INFORMADO') THEN r.dsc_cargo END),
                    MAX(r.dsc_cargo)
                ) AS dsc_cargo,
                COALESCE(
                    MAX(CASE WHEN TRIM(COALESCE(r.dsc_lotacao, '')) NOT IN ('', 'NÃO INFORMADO', 'NAO INFORMADO') THEN r.dsc_lotacao END),
                    MAX(r.dsc_lotacao)
                ) AS dsc_lotacao,
                MIN(r.ano_referencia) AS primeiro_ano_folha,
                MAX(r.ano_referencia) AS ultimo_ano_folha,
                ROUND(SUM(COALESCE(TRY_CAST(r.vlr_rmn_bruta AS DECIMAL(18,2)), 0.00)), 2) AS total_remuneracao_bruta
            FROM pessoa_municipio p
            JOIN remuneracao_municipio r
              ON p.seq_dim_pessoa = r.seq_dim_pessoa
             AND p.ano_referencia = r.ano_referencia
            WHERE {_cpf_positional_pattern_11_sql("p.num_documento")} IS NOT NULL
            GROUP BY 1, 2, 4
        ),
        pagamentos_empresa AS (
            SELECT
                regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g') AS cnpj_empresa,
                ROUND(
                    SUM(
                        COALESCE(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2)), 0.00)
                        - COALESCE(TRY_CAST(vlr_anu_fonte AS DECIMAL(18,2)), 0.00)
                    ),
                    2
                ) AS total_pago_empresa,
                MIN(ano_referencia) AS primeiro_ano_pagamento,
                MAX(ano_referencia) AS ultimo_ano_pagamento
            FROM despesa_pagamento
            GROUP BY 1
        )
        SELECT
            CASE
                WHEN length(s.cpf_digits_socio) = 11 THEN s.cpf_digits_socio
                WHEN length(sv.cpf_digits_servidor) = 11 THEN sv.cpf_digits_servidor
                ELSE COALESCE(sv.cpf_display_servidor, s.cpf_display_socio)
            END AS num_cpf,
            COALESCE(sv.nom_servidor, s.nom_socio) AS nom_pessoa,
            sv.dsc_cargo,
            sv.dsc_lotacao,
            sv.primeiro_ano_folha,
            sv.ultimo_ano_folha,
            sv.total_remuneracao_bruta,
            s.cnpj_empresa,
            s.nom_empresa,
            s.dsc_tipo_participacao,
            s.primeiro_ano_licitacao,
            s.ultimo_ano_licitacao,
            COALESCE(pg.total_pago_empresa, 0.00) AS total_pago_empresa,
            pg.primeiro_ano_pagamento,
            pg.ultimo_ano_pagamento,
            (
                TRY_CAST(sv.ultimo_ano_folha AS INTEGER) >= TRY_CAST(COALESCE(pg.primeiro_ano_pagamento, s.primeiro_ano_licitacao) AS INTEGER)
                AND TRY_CAST(sv.primeiro_ano_folha AS INTEGER) <= TRY_CAST(COALESCE(pg.ultimo_ano_pagamento, s.ultimo_ano_licitacao) AS INTEGER)
            ) AS ha_sobreposicao_temporal,
            CASE
                WHEN TRY_CAST(sv.ultimo_ano_folha AS INTEGER) >= TRY_CAST(COALESCE(pg.primeiro_ano_pagamento, s.primeiro_ano_licitacao) AS INTEGER)
                     AND TRY_CAST(sv.primeiro_ano_folha AS INTEGER) <= TRY_CAST(COALESCE(pg.ultimo_ano_pagamento, s.ultimo_ano_licitacao) AS INTEGER)
                THEN 'SOBREPOSICAO_CONTEMPORANEA_CONFIRMADA'
                WHEN TRY_CAST(COALESCE(pg.primeiro_ano_pagamento, s.primeiro_ano_licitacao) AS INTEGER) <= 2022
                     AND TRY_CAST(sv.primeiro_ano_folha AS INTEGER) >= 2023
                THEN 'INCONCLUSIVO_LACUNA_FOLHA_2020_2022'
                ELSE 'PERIODOS_DISTINTOS_SEM_SOBREPOSICAO'
            END AS classificacao_temporal_vinculo
        FROM socios_empresas s
        JOIN servidores sv
          ON (
              (length(s.cpf_digits_socio) = 11 AND length(sv.cpf_digits_servidor) = 11 AND s.cpf_digits_socio = sv.cpf_digits_servidor)
              OR (
                  s.nom_socio = sv.nom_servidor
                  AND array_length(
                      regexp_extract_all(
                          regexp_replace(s.nom_socio, '\\b(DAS|DOS|FILHO|JUNIOR|NETO)\\b', '', 'g'),
                          '[A-ZÀ-Ú]{{3,}}'
                      )
                  ) >= 3
                  AND {_cpf_patterns_compatible_sql("s.cpf_pat_socio", "sv.cpf_pat_servidor")}
              )
          )
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
                WHEN LEAST(COALESCE(r.ultimo_mes_receita, 0), COALESCE(d.ultimo_mes_despesa, 0)) < 12
                THEN 'Em andamento (Parcial)'
                ELSE 'Fechado'
            END AS status_exercicio
        FROM rec_anual r
        FULL OUTER JOIN desp_anual d
          ON r.ano_referencia = d.ano_referencia
        ORDER BY 1;
    """,
    "vw_gold_orcamento_por_orgao": f"""
        CREATE OR REPLACE VIEW vw_gold_orcamento_por_orgao AS
        WITH {_CTE_DIM_ORGAO_CANONICO},
        rec_orgao AS (
            SELECT
                ano_referencia,
                {_norm_cod_orgao_key_sql("cod_orgao")} AS cod_orgao_key,
                {_norm_cod_orgao_pad2_sql("cod_orgao")} AS cod_orgao_norm,
                MAX(seq_orgao) AS seq_orgao,
                ROUND(SUM(COALESCE(TRY_CAST(vlr_previsaoinicial AS DECIMAL(18,2)), 0.00)), 2) AS receita_prevista_inicial_orgao,
                ROUND(SUM(COALESCE(TRY_CAST(vlr_previsaoatualizada AS DECIMAL(18,2)), 0.00)), 2) AS receita_prevista_atualizada_orgao,
                ROUND(SUM(COALESCE(TRY_CAST(vlr_realizadoateperiodo AS DECIMAL(18,2)), 0.00)), 2) AS receita_arrecadada_propria_orgao,
                MAX(TRY_CAST(num_mesexercicio AS INTEGER)) AS ultimo_mes_receita
            FROM receita_receita
            GROUP BY 1, 2, 3
        ),
        desp_orgao AS (
            SELECT
                ano_referencia,
                {_norm_cod_orgao_key_sql("cod_orgao")} AS cod_orgao_key,
                {_norm_cod_orgao_pad2_sql("cod_orgao")} AS cod_orgao_norm,
                MAX(seq_orgao) AS seq_orgao,
                ROUND(SUM(COALESCE(TRY_CAST(vlr_previsto AS DECIMAL(18,2)), 0.00)), 2) AS dotacao_inicial_loa,
                ROUND(
                    SUM(
                        COALESCE(TRY_CAST(vlr_previsto AS DECIMAL(18,2)), 0.00)
                        + COALESCE(TRY_CAST(vlr_acrescimo AS DECIMAL(18,2)), 0.00)
                        - ABS(COALESCE(TRY_CAST(vlr_deducao AS DECIMAL(18,2)), 0.00))
                    ),
                    2
                ) AS dotacao_atualizada,
                ROUND(SUM(COALESCE(TRY_CAST(vlr_empenhado AS DECIMAL(18,2)), 0.00)), 2) AS despesa_empenhada,
                ROUND(SUM(COALESCE(TRY_CAST(vlr_liquidado AS DECIMAL(18,2)), 0.00)), 2) AS despesa_liquidada,
                ROUND(SUM(COALESCE(TRY_CAST(vlr_pago AS DECIMAL(18,2)), 0.00)), 2) AS despesa_paga,
                MAX(TRY_CAST(num_mesexercicio AS INTEGER)) AS ultimo_mes_despesa
            FROM despesa_despesa
            GROUP BY 1, 2, 3
        )
        SELECT
            COALESCE(d.ano_referencia, r.ano_referencia) AS ano_referencia,
            COALESCE(d.seq_orgao, r.seq_orgao, dim.seq_orgao) AS seq_orgao,
            COALESCE(dim.cod_orgao_norm, d.cod_orgao_norm, r.cod_orgao_norm) AS cod_orgao,
            COALESCE(dim.nom_orgao_oficial, 'ÓRGÃO ' || COALESCE(d.cod_orgao_norm, r.cod_orgao_norm)) AS nom_orgao,
            CASE
                WHEN dim.esfera_poder = 'LEGISLATIVO'
                     OR regexp_matches(UPPER(COALESCE(dim.nom_orgao_oficial, '')), '(C[AÂ]MARA|LEGISLATIV)')
                THEN 'LEGISLATIVO'
                ELSE COALESCE(dim.esfera_poder, 'EXECUTIVO')
            END AS esfera_poder,
            CASE
                WHEN dim.administracao_tipo = 'INDIRETA_OU_FUNDO'
                     OR regexp_matches(UPPER(COALESCE(dim.nom_orgao_oficial, '')), '(AUTARQUIA|FUNDA[CÇ][AÃ]O|INSTITUTO|PREVID[EÊ]NCIA|\\bRPPS\\b|EMPRESA|FUNDO|SERVI[CÇ]O AUT[OÔ]NOMO|\\bSAAE\\b)')
                THEN 'INDIRETA_OU_FUNDO'
                ELSE COALESCE(dim.administracao_tipo, 'DIRETA')
            END AS administracao_tipo,
            CASE
                WHEN COALESCE(dim.esfera_poder, 'EXECUTIVO') = 'LEGISLATIVO'
                THEN 'REPASSE_DUODECIMO_CONSTITUCIONAL_CF_ART_29A'
                WHEN COALESCE(dim.administracao_tipo, 'DIRETA') = 'INDIRETA_OU_FUNDO'
                THEN 'RECEITA_PROPRIA_E_REPASSES_VINCULADOS'
                ELSE 'TESOURO_CENTRAL_E_TRANSFERENCIAS'
            END AS regime_financiamento,
            COALESCE(d.dotacao_inicial_loa, 0.00) AS dotacao_inicial_loa,
            COALESCE(d.dotacao_atualizada, 0.00) AS dotacao_atualizada,
            COALESCE(r.receita_prevista_atualizada_orgao, 0.00) AS receita_prevista_orgao,
            COALESCE(r.receita_arrecadada_propria_orgao, 0.00) AS receita_arrecadada_propria_orgao,
            COALESCE(d.despesa_empenhada, 0.00) AS despesa_empenhada,
            COALESCE(d.despesa_liquidada, 0.00) AS despesa_liquidada,
            COALESCE(d.despesa_paga, 0.00) AS despesa_paga,
            ROUND(
                100.0 * COALESCE(d.despesa_empenhada, 0.00)
                / NULLIF(d.dotacao_atualizada, 0.00),
                2
            ) AS pct_execucao_dotacao,
            GREATEST(COALESCE(r.ultimo_mes_receita, 0), COALESCE(d.ultimo_mes_despesa, 0)) AS ultimo_mes_exercicio,
            CASE
                WHEN COALESCE(d.ultimo_mes_despesa, r.ultimo_mes_receita, 12) < 12
                THEN 'Em andamento (Parcial)'
                ELSE 'Fechado'
            END AS status_exercicio
        FROM desp_orgao d
        FULL OUTER JOIN rec_orgao r
          ON d.ano_referencia = r.ano_referencia
         AND d.cod_orgao_key = r.cod_orgao_key
        LEFT JOIN dim_orgao_canonico dim
          ON COALESCE(d.cod_orgao_key, r.cod_orgao_key) = dim.cod_orgao_key
        ORDER BY 1, 3;
    """,
}


def register_gold_views(
    con: duckdb.DuckDBPyConnection,
    institutional_cnpjs: Optional[List[str]] = None,
) -> Set[str]:
    """
    Registra as 8 Gold Views Semânticas sobre a conexão DuckDB se as tabelas base existirem.
    Permite injetar `institutional_cnpjs` explícitos do tenant unindo-os a `cnpjs_proprios_mun`.
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
        "vw_gold_frota_consolidada": {"frota_frota", "frota_gastofrota", "orgao_orgao"},
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
        "vw_gold_orcamento_por_orgao": {
            "receita_receita",
            "despesa_despesa",
            "orgao_orgao",
        },
    }

    cnpj_parts: List[str] = []
    if "orgao_municipio" in existing:
        cnpj_parts.append(_CNPJS_PROPRIOS_FROM_ORGAO_MUNICIPIO.strip())
    if institutional_cnpjs:
        clean_explicit = sorted(
            {
                re.sub(r"[^0-9]", "", str(c)).zfill(14)
                for c in institutional_cnpjs
                if c and 12 <= len(re.sub(r"[^0-9]", "", str(c))) <= 14
            }
        )
        for c14 in clean_explicit:
            cnpj_parts.append(f"SELECT '{c14}' AS cnpj_orgao_mun")

    cnpjs_sql = "\n    UNION\n    ".join(cnpj_parts) if cnpj_parts else _CNPJS_PROPRIOS_EMPTY

    for view_name, raw_ddl in GOLD_VIEWS_DDL.items():
        reqs = required_by_view.get(view_name, set())
        if reqs.issubset(existing):
            ddl = raw_ddl.replace("{__CNPJS_PROPRIOS_SQL__}", cnpjs_sql)
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
                                CAST(NULL AS VARCHAR) AS cod_orgao,
                                'MUNICÍPIO' AS nom_orgao,
                                'Mensal' AS dsc_tipo_pag,
                                COALESCE(p.nom_pessoa, 'SERVIDOR SEQ ' || CAST(r.seq_dim_pessoa AS VARCHAR)) AS nom_servidor,
                                p.num_cpf, r.dsc_cargo, r.dsc_lotacao, r.dsc_situacao, r.dsc_regime,
                                r.dsc_situacao AS tipo_vinculo,
                                r.dsc_situacao AS situacao_funcional,
                                CAST(NULL AS VARCHAR) AS cargo_politico,
                                TRY_CAST(r.vlr_rmn_bruta AS DECIMAL(18,2)) AS vlr_rmn_bruta,
                                TRY_CAST(r.vlr_rmn_liquida AS DECIMAL(18,2)) AS vlr_rmn_liquida,
                                TRY_CAST(r.vlr_descontos AS DECIMAL(18,2)) AS vlr_descontos,
                                CAST(NULL AS VARCHAR) AS cod_orgao_norm,
                                'EXECUTIVO' AS esfera_poder,
                                'DIRETA' AS administracao_tipo
                            FROM remuneracao_municipio r
                            LEFT JOIN pessoa_dedup p ON r.seq_dim_pessoa = p.seq_dim_pessoa AND r.ano_referencia = p.ano_referencia;
                            """
                        )
                        registered.add(view_name)
                        continue
                    except Exception:
                        pass
                logger.warning("Falha ao criar Gold View %s: %s", view_name, exc)

    # 9ª Gold View Adaptativa Multi-Modelo: unifica microdados nominais CSV (Caeté) e resumo mensal API REST (Lagoa da Prata / novos municípios)
    cgu_union_parts: List[str] = []
    for cgu_tbl, prog_label in (
        ("cgu_novo_bolsa_familia", "NOVO BOLSA FAMÍLIA"),
        ("cgu_bpc", "BPC (BENEFÍCIO DE PRESTAÇÃO CONTINUADA)"),
        ("cgu_auxilio_brasil", "AUXÍLIO BRASIL"),
    ):
        if cgu_tbl.lower() not in existing:
            continue
        try:
            c_rows = con.execute(f"DESCRIBE {cgu_tbl}").fetchall()
            col_names = {str(r[0]).strip() for r in c_rows}
            col_low_map = {c.lower(): c for c in col_names}
            if "ano_referencia" not in col_low_map or "valor parcela" not in col_low_map:
                continue
            val_col = f'"{col_low_map["valor parcela"]}"'
            mes_col = f'"{col_low_map["mês referência"]}"' if "mês referência" in col_low_map else (
                f'"{col_low_map["mes referencia"]}"' if "mes referencia" in col_low_map else "CAST(NULL AS VARCHAR)"
            )
            safe_val_expr = (
                f"CASE WHEN CONTAINS(CAST({val_col} AS VARCHAR), ',') "
                f"THEN TRY_CAST(REPLACE(REPLACE(CAST({val_col} AS VARCHAR), '.', ''), ',', '.') AS DECIMAL(18,2)) "
                f"ELSE TRY_CAST({val_col} AS DECIMAL(18,2)) END"
            )
            if "qtd_beneficiados" in col_low_map:
                # Modelo API REST Agregado Municipal (1 linha = 1 mês da cidade inteira; VALOR PARCELA já é o total mensal!)
                q_col = f'"{col_low_map["qtd_beneficiados"]}"'
                cgu_union_parts.append(
                    f"SELECT TRY_CAST(ano_referencia AS INTEGER) AS ano_referencia, "
                    f"CAST({mes_col} AS VARCHAR) AS mes_referencia, "
                    f"'{prog_label}' AS programa_social, "
                    f"MAX(TRY_CAST({q_col} AS BIGINT)) AS beneficiarios_no_mes, "
                    f"ROUND(SUM(COALESCE({safe_val_expr}, 0.00)), 2) AS vlr_total_pago_mes "
                    f"FROM {cgu_tbl} WHERE TRY_CAST({q_col} AS BIGINT) > 0 OR COALESCE({safe_val_expr}, 0) > 0 "
                    f"GROUP BY 1, 2"
                )
            else:
                # Modelo CSV Microdados Individuais (1 linha = 1 parcela individual paga ao cidadão)
                nis_col = (
                    f'"{col_low_map["nis favorecido"]}"'
                    if "nis favorecido" in col_low_map
                    else (f'"{col_low_map["nis beneficiário"]}"' if "nis beneficiário" in col_low_map else None)
                )
                cnt_expr = f"COUNT(DISTINCT {nis_col})" if nis_col else "COUNT(*)"
                cgu_union_parts.append(
                    f"SELECT TRY_CAST(ano_referencia AS INTEGER) AS ano_referencia, "
                    f"CAST({mes_col} AS VARCHAR) AS mes_referencia, "
                    f"'{prog_label}' AS programa_social, "
                    f"{cnt_expr} AS beneficiarios_no_mes, "
                    f"ROUND(SUM(COALESCE({safe_val_expr}, 0.00)), 2) AS vlr_total_pago_mes "
                    f"FROM {cgu_tbl} WHERE COALESCE({safe_val_expr}, 0) > 0 "
                    f"GROUP BY 1, 2"
                )
        except Exception:
            continue

    if cgu_union_parts:
        try:
            union_sql = "\n            UNION ALL\n            ".join(cgu_union_parts)
            con.execute(
                f"CREATE OR REPLACE VIEW vw_gold_beneficios_sociais_cgu AS\n"
                f"SELECT * FROM (\n            {union_sql}\n) sub_cgu\n"
                f"WHERE beneficiarios_no_mes > 0 OR vlr_total_pago_mes > 0\n"
                f"ORDER BY ano_referencia, mes_referencia, programa_social;"
            )
            registered.add("vw_gold_beneficios_sociais_cgu")
        except Exception as exc:
            logger.warning("Falha ao criar vw_gold_beneficios_sociais_cgu: %s", exc)

    return registered
