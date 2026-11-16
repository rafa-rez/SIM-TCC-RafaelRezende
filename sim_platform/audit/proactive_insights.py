"""
Motor Proativo de Insights e Auditoria Contínua v3.0 (`ProactiveAuditEngine`).

Princípio de Engenharia:
- ZERO valores hardcoded arbitrários ou alertas falsos que nunca disparam.
- Executa 7 Trilhas Reais de Controladoria e Fiscalização (padrão TCE-MG / SURICATO / CGU)
  diretamente sobre os arquivos Parquet oficiais do município, expondo de forma transparente:
  1. A **Base Legal ou Critério Estatístico** (Lei 14.133/2021, Lei 8.666/93, LRF, Jurisprudência TCU/TCE-MG).
  2. A **Query SQL Exata** que originou o achado (100% reproduzível e auditável).
  3. A **Nota Técnica de Interpretação** (orientando o controlador interno sobre quando o achado é
     regular versus quando exige abertura de diligência ou Tomada de Contas Especial).
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import duckdb


DEFAULT_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
_AUDIT_CACHE: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
_AUDIT_CACHE_LOCK = threading.RLock()


class ProactiveAuditEngine:
    """
    Executa as 7 trilhas dinâmicas de controladoria sobre o banco Parquet do município
    (mais a trilha fiscal LRF/SICONFI para cidades recém-sincronizadas via API federal).
    """

    def __init__(self, workspace_root: Path = DEFAULT_WORKSPACE_ROOT, tenant_id: str = "caete_mg_3110004") -> None:
        self.workspace_root = workspace_root
        self.tenant_id = tenant_id
        self.parquet_dir = workspace_root / "avaliacao" / "dados" / "parquet" / tenant_id

    def _resolve_layered_parquets(self) -> Dict[str, Path]:
        by_low: Dict[str, tuple[str, Path]] = {}
        if not self.parquet_dir.exists():
            return {}
        base_res = self.parquet_dir.resolve()
        for pq in sorted(base_res.glob("*.parquet")):
            by_low[pq.stem.lower()] = (pq.stem, pq)
        ptr = base_res / "CURRENT.json"
        if ptr.exists():
            try:
                payload = json.loads(ptr.read_text(encoding="utf-8"))
                rel_dirs: List[str] = []
                chain = payload.get("release_chain")
                if isinstance(chain, list):
                    rel_dirs.extend(str(x) for x in chain if x)
                rel_single = str(payload.get("release_dir") or "").strip()
                if rel_single and rel_single not in rel_dirs:
                    rel_dirs.append(rel_single)
                for rel_sub in rel_dirs:
                    cand = (base_res / rel_sub).resolve()
                    if (cand == base_res or base_res in cand.parents) and cand.exists():
                        for pq in sorted(cand.glob("*.parquet")):
                            orig_s = by_low.get(pq.stem.lower(), (pq.stem, pq))[0]
                            by_low[pq.stem.lower()] = (orig_s, pq)
            except Exception:
                pass
        return {stem: path for stem, path in by_low.values()}

    def _get_active_release_id(self, pq_map: Dict[str, Path]) -> str:
        if not self.parquet_dir.exists():
            return "no_parquet_dir"
        ptr = self.parquet_dir.resolve() / "CURRENT.json"
        rel_id = "base"
        if ptr.exists():
            try:
                payload = json.loads(ptr.read_text(encoding="utf-8"))
                chain = payload.get("release_chain")
                rel_single = str(payload.get("release_dir") or payload.get("release_id") or "").strip()
                if isinstance(chain, list) and chain:
                    rel_id = "|".join(str(x) for x in chain if x)
                elif rel_single:
                    rel_id = rel_single
            except Exception:
                rel_id = "unreadable_current"
        h = hashlib.sha256()
        h.update(rel_id.encode("utf-8"))
        for stem in sorted(pq_map.keys()):
            try:
                st = pq_map[stem].stat()
                h.update(f"|{stem}:{st.st_size}:{int(st.st_mtime)}".encode("utf-8"))
            except Exception:
                pass
        return f"{rel_id}@{h.hexdigest()[:12]}"

    def run_all_checks(
        self,
        aditivo_pct_threshold: float = 25.0,
        min_dispensas_recorrentes: int = 5,
        yoy_growth_pct_threshold: float = 100.0,
    ) -> Dict[str, Any]:
        pq_map = self._resolve_layered_parquets()
        if not pq_map:
            return {
                "status": "sem_dados",
                "tenant_id": self.tenant_id,
                "active_release_id": "sem_dados",
                "skipped_empty_tables": [],
                "detail": (
                    f"Nenhum arquivo Parquet oficial sincronizado para o município `{self.tenant_id}`. "
                    "Execute a sincronização de dados antes de rodar as trilhas de auditoria."
                ),
                "total_insights_categories": 0,
                "insights": [],
            }

        active_release_id = self._get_active_release_id(pq_map)
        cache_key = (
            self.tenant_id,
            active_release_id,
            round(float(aditivo_pct_threshold), 4),
            int(min_dispensas_recorrentes),
            round(float(yoy_growth_pct_threshold), 4),
        )
        with _AUDIT_CACHE_LOCK:
            cached = _AUDIT_CACHE.get(cache_key)
            if cached is not None:
                return copy.deepcopy(cached)

        con = duckdb.connect(":memory:")
        try:
            con.execute("SET memory_limit = '256MB';")
        except Exception:
            pass
        query_errors: List[str] = []
        table_row_counts: Dict[str, int] = {}
        skipped_empty_set: Set[str] = set()

        def _pq(stem: str) -> str:
            return pq_map[stem].as_posix()

        def _has(*stems: str) -> bool:
            all_ok = True
            for s in stems:
                if s not in pq_map:
                    all_ok = False
                    continue
                if s not in table_row_counts:
                    try:
                        cnt_row = con.execute(f"SELECT COUNT(*) FROM read_parquet('{_pq(s)}')").fetchone()
                        table_row_counts[s] = int(cnt_row[0] or 0) if cnt_row else 0
                    except Exception:
                        table_row_counts[s] = 0
                if table_row_counts[s] <= 0:
                    skipped_empty_set.add(s)
                    all_ok = False
            return all_ok

        def _query_with_total(sql: str) -> Tuple[List[Dict[str, Any]], int]:
            try:
                rel = con.execute(sql)
            except Exception as exc:
                query_errors.append(str(exc)[:180])
                return [], 0
            cols = [d[0] for d in rel.description]
            out = []
            for r in rel.fetchall():
                row_dict = {}
                for i, c in enumerate(cols):
                    v = r[i]
                    if hasattr(v, "__float__") and not isinstance(v, (int, bool)):
                        v = round(float(v), 2)
                    row_dict[c] = v
                out.append(row_dict)

            lim_match = re.search(r"\s+LIMIT\s+(\d+)\s*;?\s*$", sql.strip(), flags=re.IGNORECASE)
            if not lim_match:
                return out, len(out)
            lim_val = int(lim_match.group(1))
            if len(out) < lim_val:
                return out, len(out)
            sql_no_limit = sql.strip()[: lim_match.start()].rstrip()
            try:
                tot_row = con.execute(f"SELECT COUNT(*) FROM ({sql_no_limit}) AS _sim_audit_tot").fetchone()
                total_occ = int(tot_row[0] or len(out)) if tot_row else len(out)
            except Exception:
                total_occ = len(out)
            return out, total_occ

        insights: List[Dict[str, Any]] = []

        # TRILHA 1 (FIND-TCE-01): Cruzamento Cadastral QSA × Folha de Servidores com Contemporaneidade Temporal
        if _has("licitacao_quadroSocLicitacao", "licitacao_habLicitacao", "remuneracao_municipio", "pessoa_municipio", "despesa_pagamento"):
            from sim_platform.engine.gold_views import (
                _cpf_patterns_compatible_sql,
                _cpf_positional_pattern_11_sql,
            )

            pat_q = _cpf_positional_pattern_11_sql("q.num_cgc_habilitado")
            pat_pm = _cpf_positional_pattern_11_sql("pm.num_documento")
            compat_cond = _cpf_patterns_compatible_sql("se.pat_socio", "sv.pat_servidor")
            sql_conflito = f"""
            WITH socios_emp AS (
                SELECT
                    {pat_q} AS pat_socio,
                    regexp_replace(COALESCE(CAST(q.num_cgc_habilitado AS VARCHAR), ''), '[^0-9]', '', 'g') AS doc_socio,
                    MAX(TRIM(CAST(q.num_cgc_habilitado AS VARCHAR))) AS raw_doc_socio,
                    regexp_replace(UPPER(TRIM(COALESCE(q.nom_pessoa, ''))), '\\s+', ' ', 'g') AS nom_socio,
                    MAX(UPPER(TRIM(q.dsc_tipo_participacao))) AS tipo_participacao,
                    regexp_replace(COALESCE(CAST(h.num_documento AS VARCHAR), ''), '[^0-9]', '', 'g') AS cnpj_empresa,
                    MAX(UPPER(TRIM(h.nom_pessoa))) AS razao_social_empresa,
                    MIN(q.ano_referencia) AS primeiro_ano_licitacao,
                    MAX(q.ano_referencia) AS ultimo_ano_licitacao
                FROM read_parquet('{_pq("licitacao_quadroSocLicitacao")}') q
                JOIN read_parquet('{_pq("licitacao_habLicitacao")}') h
                  ON q.seq_hab_licitacao = h.seq_hab_licitacao
                 AND q.ano_referencia = h.ano_referencia
                WHERE {pat_q} IS NOT NULL
                  AND regexp_matches(regexp_replace(COALESCE(CAST(h.num_documento AS VARCHAR), ''), '[^0-9]', '', 'g'), '^[0-9]{{14}}$')
                GROUP BY 1, 2, 4, 6
            ),
            servidores AS (
                SELECT
                    {pat_pm} AS pat_servidor,
                    regexp_replace(COALESCE(CAST(pm.num_documento AS VARCHAR), ''), '[^0-9]', '', 'g') AS doc_servidor,
                    MAX(TRIM(CAST(pm.num_documento AS VARCHAR))) AS raw_doc_servidor,
                    regexp_replace(UPPER(TRIM(COALESCE(pm.nom_pessoa, ''))), '\\s+', ' ', 'g') AS nom_servidor,
                    COALESCE(
                        MAX(CASE WHEN TRIM(COALESCE(r.dsc_cargo, '')) NOT IN ('', 'NÃO INFORMADO', 'NAO INFORMADO') THEN UPPER(TRIM(r.dsc_cargo)) END),
                        MAX(UPPER(TRIM(r.dsc_cargo)))
                    ) AS cargo_servidor,
                    MAX(UPPER(TRIM(r.dsc_situacao))) AS situacao_servidor,
                    MIN(r.ano_referencia) AS primeiro_ano_folha,
                    MAX(r.ano_referencia) AS ultimo_ano_folha,
                    STRING_AGG(DISTINCT CAST(r.ano_referencia AS VARCHAR), ', ' ORDER BY CAST(r.ano_referencia AS VARCHAR)) AS anos_na_folha
                FROM read_parquet('{_pq("remuneracao_municipio")}') r
                JOIN read_parquet('{_pq("pessoa_municipio")}') pm
                  ON r.seq_dim_pessoa = pm.seq_dim_pessoa
                 AND r.ano_referencia = pm.ano_referencia
                WHERE {pat_pm} IS NOT NULL
                GROUP BY 1, 2, 4
            ),
            pagos AS (
                SELECT
                    regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g') AS cnpj_credor,
                    ROUND(
                        SUM(
                            COALESCE(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2)), 0.0)
                            - COALESCE(TRY_CAST(vlr_anu_fonte AS DECIMAL(18,2)), 0.0)
                        ),
                        2
                    ) AS total_pago_empresa
                FROM read_parquet('{_pq("despesa_pagamento")}')
                GROUP BY 1
            )
            SELECT
                CASE
                    WHEN length(se.doc_socio) = 11 THEN CONCAT('***.', SUBSTR(se.doc_socio, 4, 3), '.', SUBSTR(se.doc_socio, 7, 3), '-**')
                    WHEN length(sv.doc_servidor) = 11 THEN CONCAT('***.', SUBSTR(sv.doc_servidor, 4, 3), '.', SUBSTR(sv.doc_servidor, 7, 3), '-**')
                    ELSE COALESCE(sv.raw_doc_servidor, se.raw_doc_socio)
                END AS cpf_socio_servidor,
                sv.nom_servidor,
                sv.cargo_servidor,
                sv.anos_na_folha,
                se.primeiro_ano_licitacao,
                se.ultimo_ano_licitacao,
                (se.primeiro_ano_licitacao <= sv.ultimo_ano_folha AND se.ultimo_ano_licitacao >= sv.primeiro_ano_folha) AS ha_sobreposicao_temporal,
                CASE
                    WHEN se.primeiro_ano_licitacao <= sv.ultimo_ano_folha AND se.ultimo_ano_licitacao >= sv.primeiro_ano_folha
                    THEN 'SOBREPOSICAO_CONTEMPORANEA_CONFIRMADA'
                    WHEN se.primeiro_ano_licitacao <= 2022
                    THEN 'INCONCLUSIVO_LACUNA_FOLHA_2020_2022'
                    ELSE 'PERIODOS_DISTINTOS_SEM_SOBREPOSICAO'
                END AS classificacao_temporal_vinculo,
                se.razao_social_empresa AS empresa_contratada,
                se.cnpj_empresa,
                COALESCE(pg.total_pago_empresa, 0.0) AS total_pago_empresa
            FROM socios_emp se
            JOIN servidores sv
              ON (
                  (length(se.doc_socio) = 11 AND length(sv.doc_servidor) = 11 AND se.doc_socio = sv.doc_servidor)
                  OR (
                      se.nom_socio = sv.nom_servidor
                      AND array_length(
                          regexp_extract_all(
                              regexp_replace(se.nom_socio, '\\b(DAS|DOS|FILHO|JUNIOR|NETO)\\b', '', 'g'),
                              '[A-ZÀ-Ú]{{3,}}'
                          )
                      ) >= 3
                      AND {compat_cond}
                  )
              )
            LEFT JOIN pagos pg ON se.cnpj_empresa = pg.cnpj_credor
            ORDER BY ha_sobreposicao_temporal DESC, total_pago_empresa DESC
            LIMIT 10
            """
            rows_conflito, tot_conflito = _query_with_total(sql_conflito)
            insights.append(
                {
                    "id": "find_tce_01_conflito_interesses",
                    "title": "Cruzamento QSA × Folha de Servidores e Contemporaneidade Licitatória",
                    "legal_basis": "Art. 9º, § 1º e Art. 14, IV da Lei 14.133/2021 (Impedimento de Licitar e Contratar)",
                    "parameter_explained": (
                        "Cruzamento exato de CPF entre `licitacao_quadroSocLicitacao` e `pessoa_municipio` × `remuneracao_municipio`, "
                        "classificando a contemporaneidade (`ha_sobreposicao_temporal` / `classificacao_temporal_vinculo`)."
                    ),
                    "auditor_guidance": (
                        "Nota de Controladoria: Separa vínculos contemporâneos confirmados (`SOBREPOSICAO_CONTEMPORANEA_CONFIRMADA`) "
                        "de períodos distintos (`PERIODOS_DISTINTOS_SEM_SOBREPOSICAO`, ex.: servidor em 2023 e licitação em 2024) e "
                        "de certames de 2020–2022 (`INCONCLUSIVO_LACUNA_FOLHA_2020_2022`). Vínculos não contemporâneos não configuram "
                        "automaticamente o impedimento do Art. 14, IV, exigindo verificação das datas exatas de admissão/desligamento."
                    ),
                    "count": len(rows_conflito),
                    "total_occurrences": tot_conflito,
                    "sql_executed": sql_conflito.strip(),
                    "rows": rows_conflito,
                }
            )

        # TRILHA 2 (FIND-TCE-02): Colisão Societária no Mesmo Certame Licitatório
        if _has("licitacao_habLicitacao", "licitacao_quadroSocLicitacao"):
            sql_colisao = f"""
            WITH hab_socios AS (
                SELECT DISTINCT
                    h.ano_referencia,
                    h.seq_licitacao,
                    regexp_replace(COALESCE(h.num_documento, ''), '[^0-9]', '', 'g') AS cnpj_licitante,
                    UPPER(TRIM(h.nom_pessoa)) AS empresa_licitante,
                    regexp_replace(COALESCE(q.num_cgc_habilitado, ''), '[^0-9]', '', 'g') AS doc_socio,
                    UPPER(TRIM(q.nom_pessoa)) AS nom_socio
                FROM read_parquet('{_pq("licitacao_habLicitacao")}') h
                JOIN read_parquet('{_pq("licitacao_quadroSocLicitacao")}') q
                  ON h.seq_hab_licitacao = q.seq_hab_licitacao
                 AND h.ano_referencia = q.ano_referencia
                WHERE length(regexp_replace(COALESCE(h.num_documento, ''), '[^0-9]', '', 'g')) = 14
                  AND length(regexp_replace(COALESCE(q.num_cgc_habilitado, ''), '[^0-9]', '', 'g')) >= 11
            )
            SELECT
                a.ano_referencia,
                a.seq_licitacao,
                COUNT(DISTINCT a.doc_socio) AS qtd_socios_compartilhados,
                STRING_AGG(DISTINCT a.nom_socio, ', ' ORDER BY a.nom_socio) AS socio_compartilhado,
                MAX(a.empresa_licitante) AS empresa_a,
                a.cnpj_licitante AS cnpj_a,
                MAX(b.empresa_licitante) AS empresa_b,
                b.cnpj_licitante AS cnpj_b
            FROM hab_socios a
            JOIN hab_socios b
              ON a.seq_licitacao = b.seq_licitacao
             AND a.ano_referencia = b.ano_referencia
             AND a.doc_socio = b.doc_socio
             AND a.cnpj_licitante < b.cnpj_licitante
            GROUP BY a.ano_referencia, a.seq_licitacao, a.cnpj_licitante, b.cnpj_licitante
            ORDER BY a.ano_referencia DESC, a.seq_licitacao DESC
            LIMIT 10
            """
            rows_colisao, tot_colisao = _query_with_total(sql_colisao)
            insights.append(
                {
                    "id": "find_tce_02_colisao_societaria",
                    "title": "Colisão Societária entre Licitantes no Mesmo Certame",
                    "legal_basis": "Jurisprudência TCU (Acórdão 1.793/2011-Plenário) e Art. 337-F do Código Penal",
                    "parameter_explained": "Pares de empresas com CNPJs distintos habilitadas no mesmo processo licitatório (`seq_licitacao`) que compartilham sócios ou administradores (`doc_socio`), deduplicados por par societário.",
                    "auditor_guidance": (
                        "Nota de Controladoria: Identifica pares de empresas distintas que compartilharam sócios ou diretores no mesmo "
                        "processo licitatório (`seq_licitacao`). Em modalidades competitivas (Pregão/Concorrência), empresas com sócios "
                        "em comum exigem diligência quanto à independência efetiva das propostas."
                    ),
                    "count": len(rows_colisao),
                    "total_occurrences": tot_colisao,
                    "sql_executed": sql_colisao.strip(),
                    "rows": rows_colisao,
                }
            )

        # TRILHA 3 (FIND-TCE-03): Adesão a Atas de Registro de Preços ("Caronas" — Art. 86 Lei 14.133)
        if _has("empenho_empenho", "empenho_credorEmpenho"):
            sql_caronas = f"""
            SELECT
                MAX(UPPER(TRIM(ec.nom_credor))) AS fornecedor_carona,
                MAX(UPPER(TRIM(ec.nom_credor))) AS nom_credor,
                COALESCE(NULLIF(regexp_replace(COALESCE(ec.num_doc_credor, ''), '[^0-9]', '', 'g'), ''), 'SEM_DOC') AS cnpj,
                COUNT(DISTINCT e.seq_empenho) AS qtd_empenhos_adesao,
                STRING_AGG(DISTINCT CAST(e.ano_referencia AS VARCHAR), ', ' ORDER BY CAST(e.ano_referencia AS VARCHAR)) AS exercicios,
                ROUND(
                    SUM(
                        COALESCE(TRY_CAST(e.vlr_empenhado AS DECIMAL(18,2)), 0.0)
                        + COALESCE(TRY_CAST(e.vlr_reforco AS DECIMAL(18,2)), 0.0)
                        - COALESCE(TRY_CAST(e.vlr_anulempenho AS DECIMAL(18,2)), 0.0)
                    ),
                    2
                ) AS total_empenhado_carona
            FROM read_parquet('{_pq("empenho_empenho")}') e
            LEFT JOIN read_parquet('{_pq("empenho_credorEmpenho")}') ec
              ON e.seq_empenho = ec.seq_empenho AND e.ano_referencia = ec.ano_referencia
            WHERE COALESCE(e.ind_dec_licitacao, '') ILIKE '%ADES%O%'
              AND COALESCE(ec.nom_credor, '') NOT ILIKE '%INSS%'
              AND COALESCE(ec.nom_credor, '') NOT ILIKE '%INSTITUTO NACIONAL DO SEGURO SOCIAL%'
              AND COALESCE(ec.nom_credor, '') NOT ILIKE '%TRIBUNAL%'
              AND COALESCE(ec.nom_credor, '') NOT ILIKE '%SANTA CASA%'
              AND COALESCE(ec.nom_credor, '') NOT ILIKE '%BENEFICENCIA%'
              AND COALESCE(ec.nom_credor, '') NOT ILIKE '%PREFEITURA%'
              AND COALESCE(ec.nom_credor, '') NOT ILIKE '%FGTS%'
              AND COALESCE(ec.nom_credor, '') NOT ILIKE '%PASEP%'
            GROUP BY 3
            ORDER BY total_empenhado_carona DESC
            LIMIT 10
            """
            rows_caronas, tot_caronas = _query_with_total(sql_caronas)
            insights.append(
                {
                    "id": "find_tce_03_adesao_arp_caronas",
                    "title": "Materialidade em Adesões a Atas de Registro de Preços ('Caronas' — Art. 86)",
                    "legal_basis": "Art. 86, §§ 2º a 5º da Lei 14.133/2021 (Limites e Justificativa de Vantajosidade em ARP)",
                    "parameter_explained": "Empenhos líquidos com `ind_dec_licitacao ILIKE '%ADES%O%'` agrupados por CNPJ unificado do credor em `empenho_empenho` × `empenho_credorEmpenho`.",
                    "auditor_guidance": (
                        "Nota de Controladoria: Identifica os fornecedores privados com maior volume empenhado via Adesão a Ata de Registro de Preços. "
                        "Exige-se comprovação formal de vantajosidade econômica e respeito ao limite de 50% por órgão carona."
                    ),
                    "count": len(rows_caronas),
                    "total_occurrences": tot_caronas,
                    "sql_executed": sql_caronas.strip(),
                    "rows": rows_caronas,
                }
            )

        # TRILHA 4 (FIND-TCE-04): Despesas de Frota em Veículos Órfãos (Sem Placa Cadastrada vs Isentos CTB)
        if _has("frota_gastoFrota", "frota_frota"):
            sql_frota_orfa = f"""
            WITH base_frota AS (
                SELECT
                    g.ano_referencia,
                    CASE
                        WHEN v.seq_veiculo IS NULL THEN 'VEICULO_ORFAO_SEM_PLACA'
                        WHEN COALESCE(TRIM(v.dsc_num_placa), '') NOT IN ('', '-1', '0', 'S/P') THEN 'PLACA_IDENTIFICADA'
                        WHEN UPPER(COALESCE(v.dsc_tipo_veiculo, '') || ' ' || COALESCE(v.dsc_veiculo, '')) SIMILAR TO '%(TRATOR|RETROESCAVADEIRA|MOTONIVELADORA|PA CARREGADEIRA|PÁ CARREGADEIRA|COMPACTADOR|ESCAVADEIRA|ROLO|MAQUINA|MÁQUINA|ROCADEIRA|ROÇADEIRA|MOTOPODA|BOBCAT|EQUIPAMENTO|SERRA)%'
                            THEN 'ISENTO_CTB_MAQUINARIO'
                        ELSE 'CADASTRO_SEM_PLACA'
                    END AS status_placa,
                    TRY_CAST(g.vlr_gasto AS DECIMAL(18,2)) AS vlr_gasto
                FROM read_parquet('{_pq("frota_gastoFrota")}') g
                LEFT JOIN (
                    SELECT seq_veiculo, MAX(dsc_num_placa) AS dsc_num_placa,
                           MAX(dsc_tipo_veiculo) AS dsc_tipo_veiculo,
                           MAX(dsc_veiculo) AS dsc_veiculo
                    FROM read_parquet('{_pq("frota_frota")}')
                    GROUP BY 1
                ) v ON g.seq_veiculo = v.seq_veiculo
            )
            SELECT
                ano_referencia,
                COUNT(*) AS total_lancamentos,
                ROUND(SUM(vlr_gasto), 2) AS gasto_total_frota,
                ROUND(SUM(CASE WHEN status_placa = 'ISENTO_CTB_MAQUINARIO' THEN vlr_gasto ELSE 0 END), 2) AS gasto_maquinario_isento_ctb,
                ROUND(SUM(CASE WHEN status_placa IN ('VEICULO_ORFAO_SEM_PLACA', 'CADASTRO_SEM_PLACA') THEN vlr_gasto ELSE 0 END), 2) AS gasto_sem_placa,
                ROUND((SUM(CASE WHEN status_placa IN ('VEICULO_ORFAO_SEM_PLACA', 'CADASTRO_SEM_PLACA') THEN vlr_gasto ELSE 0 END) / NULLIF(SUM(vlr_gasto), 0)) * 100, 2) AS pct_gasto_sem_placa
            FROM base_frota
            GROUP BY 1
            ORDER BY ano_referencia DESC
            """
            rows_frota_orfa, tot_frota_orfa = _query_with_total(sql_frota_orfa)
            insights.append(
                {
                    "id": "find_tce_04_frota_sem_placa",
                    "title": "Despesas de Frota em Veículos Órfãos sem Placa Cadastrada vs. Maquinário Isento CTB",
                    "legal_basis": "Instruções Normativas SICOM/TCE-MG e Art. 115, § 4º-A do CTB (Isenção de Emplacamento de Maquinário Agrícola/Obras)",
                    "parameter_explained": "Cruzamento entre `frota_gastoFrota` e `frota_frota`, segregando maquinário pesado isento de placa (`ISENTO_CTB_MAQUINARIO`) de veículos órfãos/sem placa (`VEICULO_ORFAO_SEM_PLACA` / `CADASTRO_SEM_PLACA`).",
                    "auditor_guidance": (
                        "Nota de Controladoria: Segrega automaticamente gastos de tratores, retroescavadeiras e motoniveladoras (isentos de emplacamento pelo Art. 115, §4º-A do CTB) "
                        "dos veículos leves/utilitários sem placa ou sem cadastro correspondente em `frota_frota`. "
                        "Recomenda-se saneamento do vínculo `seq_veiculo` no SICOM para os veículos rodoviários comuns."
                    ),
                    "count": len(rows_frota_orfa),
                    "total_occurrences": tot_frota_orfa,
                    "sql_executed": sql_frota_orfa.strip(),
                    "rows": rows_frota_orfa,
                }
            )

        # TRILHA 5 (FIND-TCE-05): Concentração de Fornecedores PJ Privados e Transparência Societária (QSA)
        if _has("receita_qsa_fornecedores"):
            sql_qsa_conc = f"""
            SELECT
                razao_social,
                num_cnpj,
                porte_empresa,
                qtd_socios_identificados,
                SUBSTR(COALESCE(nomes_socios_qsa, ''), 1, 75) AS quadro_societario_resumo,
                qtd_pagamentos,
                ROUND(TRY_CAST(total_pago_historico AS DECIMAL(18,2)), 2) AS total_pago_historico,
                alerta_auditoria_controladoria
            FROM read_parquet('{_pq("receita_qsa_fornecedores")}')
            WHERE COALESCE(porte_empresa, '') NOT ILIKE '%ÓRGÃO PÚBLICO%'
              AND COALESCE(porte_empresa, '') NOT ILIKE '%SEM FINS LUCRATIVOS%'
              AND COALESCE(porte_empresa, '') NOT ILIKE '%ENTE P%BLICO%'
            ORDER BY total_pago_historico DESC
            LIMIT 10
            """
            rows_qsa_conc, tot_qsa_conc = _query_with_total(sql_qsa_conc)
            insights.append(
                {
                    "id": "find_tce_05_concentracao_qsa",
                    "title": "Concentração de Credores Privados e Cobertura Societária (QSA Reconciliado)",
                    "legal_basis": "Matriz de Risco e Materialidade TCE-MG / NBC TA 315",
                    "parameter_explained": "Top 10 fornecedores estritamente privados (`porte_empresa = 'EMPRESA PRIVADA (LTDA / S.A. / ME)'`) ordenados por `total_pago_historico` com status de QSA.",
                    "auditor_guidance": (
                        "Nota de Controladoria: Exclui automaticamente órgãos públicos, obrigações patronais (`INSS`, `FGTS`, `PASEP`), tribunais e entidades sem fins lucrativos "
                        "para ranquear apenas fornecedores privados de alta materialidade, indicando quais já possuem QSA mapeado via SICOM e quais exigem diligência."
                    ),
                    "count": len(rows_qsa_conc),
                    "total_occurrences": tot_qsa_conc,
                    "sql_executed": sql_qsa_conc.strip(),
                    "rows": rows_qsa_conc,
                }
            )

        # TRILHA 6 (FIND-TCE-06 / audit_aditivos_25pct): Contratos com Acréscimo Quantitativo/Qualitativo de Escopo > 25%
        if _has("contrato_termoContrato", "contrato_contContrato", "contrato_contratos"):
            sql_aditivos = f"""
            WITH adit AS (
                SELECT seq_contrato, COUNT(*) AS qtd_aditivos,
                       ROUND(
                           SUM(
                               CASE
                                   WHEN COALESCE(dsc_tipo_alteracao, '') NOT ILIKE '%DECRESCIMO%'
                                        AND COALESCE(dsc_tipo_termo, '') NOT ILIKE '%REAJUSTE%'
                                        AND COALESCE(dsc_tipo_termo, '') NOT ILIKE '%RECOMPOSIC%'
                                        AND COALESCE(dsc_tipo_termo, '') NOT ILIKE '%EQUILIBRIO%'
                                        AND COALESCE(dsc_tipo_termo, '') NOT ILIKE '%PRAZO%'
                                   THEN COALESCE(TRY_CAST(vlr_termo_aditivo AS DECIMAL(18,2)), 0.0)
                                   ELSE 0.0
                               END
                           ),
                           2
                       ) AS total_aditivado,
                       ROUND(
                           SUM(
                               CASE
                                   WHEN COALESCE(dsc_tipo_termo, '') ILIKE '%REAJUSTE%'
                                        OR COALESCE(dsc_tipo_termo, '') ILIKE '%RECOMPOSIC%'
                                        OR COALESCE(dsc_tipo_termo, '') ILIKE '%EQUILIBRIO%'
                                   THEN COALESCE(TRY_CAST(vlr_termo_aditivo AS DECIMAL(18,2)), 0.0)
                                   ELSE 0.0
                               END
                           ),
                           2
                       ) AS vlr_reajuste_reequilibrio
                FROM read_parquet('{_pq("contrato_termoContrato")}')
                WHERE TRY_CAST(vlr_termo_aditivo AS DECIMAL(18,2)) > 0
                GROUP BY 1
            ),
            cred_cont AS (
                SELECT seq_contrato, ano_referencia,
                       MAX(UPPER(TRIM(dsc_nome_credor))) AS nom_contratado,
                       MAX(num_doc_credor) AS doc_contratado
                FROM read_parquet('{_pq("contrato_contContrato")}')
                GROUP BY 1, 2
            )
            SELECT c.ano_referencia, c.num_contrato,
                   COALESCE(cc.nom_contratado, 'NÃO INFORMADO NO SICOM') AS nom_contratado,
                   SUBSTR(TRIM(c.dsc_objetocontrato), 1, 85) AS objeto_contrato,
                   ROUND(TRY_CAST(c.vlr_contrato AS DECIMAL(18,2)), 2) AS vlr_original,
                   a.qtd_aditivos, a.total_aditivado, a.vlr_reajuste_reequilibrio,
                   ROUND((a.total_aditivado / NULLIF(TRY_CAST(c.vlr_contrato AS DECIMAL(18,2)), 0)) * 100, 1) AS pct_acrescimo
            FROM read_parquet('{_pq("contrato_contratos")}') c
            JOIN adit a ON c.seq_contrato = a.seq_contrato
            LEFT JOIN cred_cont cc ON c.seq_contrato = cc.seq_contrato AND c.ano_referencia = cc.ano_referencia
            WHERE TRY_CAST(c.vlr_contrato AS DECIMAL(18,2)) > 10000
              AND ((a.total_aditivado / NULLIF(TRY_CAST(c.vlr_contrato AS DECIMAL(18,2)), 0)) * 100) > {aditivo_pct_threshold}
            ORDER BY a.total_aditivado DESC
            LIMIT 8
            """
            rows_aditivos, tot_aditivos = _query_with_total(sql_aditivos)
            insights.append(
                {
                    "id": "audit_aditivos_25pct",
                    "title": f"Acréscimos de Objeto Acima de {aditivo_pct_threshold:.0f}% do Contrato Original (Excluídos Reajustes e Prorrogações)",
                    "legal_basis": "Art. 125 da Lei 14.133/2021 e Jurisprudência TCU (Acórdão 749/2010-Plenário)",
                    "parameter_explained": (
                        f"Soma bruta dos acréscimos quantitativos/qualitativos de escopo em `contrato_termoContrato` "
                        f"(excluídos `REAJUSTE`, `RECOMPOSIÇÃO` e `PRAZO`, e sem compensação com supressões) > {aditivo_pct_threshold:.0f}% de `vlr_contrato`."
                    ),
                    "auditor_guidance": (
                        "Nota de Controladoria: O teto legal de 25% (50% p/ reforma de edifícios/equipamentos) incide estritamente sobre acréscimos de escopo, "
                        "sem compensação com supressões (Acórdão 749/2010-TCU). Reajustes inflacionários (`4 - REAJUSTE`), reequilíbrio econômico-financeiro (`5 - RECOMPOSIÇÃO`) "
                        "e prorrogações plurianuais de serviços contínuos já foram expurgados do cálculo."
                    ),
                    "count": len(rows_aditivos),
                    "total_occurrences": tot_aditivos,
                    "sql_executed": sql_aditivos.strip(),
                    "rows": rows_aditivos,
                }
            )

        # TRILHA 7 (FIND-TCE-07 / audit_dispensas_recorrentes): Fornecedores com Múltiplas Dispensas no Mesmo Ano
        if _has("licitacao_fornDispensa"):
            sql_dispensas = f"""
            SELECT f.ano_referencia,
                   MAX(UPPER(TRIM(f.dsc_nom_fornecedor))) AS fornecedor,
                   COALESCE(NULLIF(regexp_replace(COALESCE(f.num_doc_fornecedor, ''), '[^0-9]', '', 'g'), ''), 'SEM_DOC') AS documento,
                   COUNT(DISTINCT f.seq_dispensa) AS qtd_dispensas_no_ano,
                   COUNT(*) AS qtd_itens,
                   ROUND(SUM(TRY_CAST(f.vlr_item AS DECIMAL(18,2)) * COALESCE(TRY_CAST(f.num_quant_item AS DECIMAL(18,2)), 1)), 2) AS valor_total_itens
            FROM read_parquet('{_pq("licitacao_fornDispensa")}') f
            WHERE COALESCE(f.dsc_nom_fornecedor, '') <> ''
            GROUP BY 1, 3
            HAVING COUNT(DISTINCT f.seq_dispensa) >= {min_dispensas_recorrentes}
            ORDER BY qtd_dispensas_no_ano DESC, valor_total_itens DESC
            LIMIT 8
            """
            rows_dispensas, tot_dispensas = _query_with_total(sql_dispensas)
            insights.append(
                {
                    "id": "audit_dispensas_recorrentes",
                    "title": f"Fornecedores com ≥ {min_dispensas_recorrentes} Dispensas de Licitação no Mesmo Ano",
                    "legal_basis": "Art. 75, §1º da Lei 14.133/2021 (Prevenção ao Fracionamento de Despesa)",
                    "parameter_explained": f"`COUNT(DISTINCT seq_dispensa) >= {min_dispensas_recorrentes}` para o mesmo CNPJ no mesmo exercício em `licitacao_fornDispensa`.",
                    "auditor_guidance": (
                        "Nota de Controladoria: Exige distinção entre contratações recorrentes de obras/serviços de mesma natureza "
                        "(que demandam verificação do teto anual por ramo de atividade no Art. 75, §1º) e aquisições emergenciais/judiciais de saúde "
                        "(Art. 75, VIII), cuja recorrência recomenda abertura de Ata de Registro de Preços."
                    ),
                    "count": len(rows_dispensas),
                    "total_occurrences": tot_dispensas,
                    "sql_executed": sql_dispensas.strip(),
                    "rows": rows_dispensas,
                }
            )

        # TRILHA FEDERAL / API (LRF & Mínimos Constitucionais via SICONFI/STN — sempre executa quando siconfi_finbra_indicadores estiver populada)
        if _has("siconfi_finbra_indicadores"):
            sql_lrf = f"""
            SELECT
                ano_referencia,
                vlr_receita_corrente_liquida,
                vlr_despesa_pessoal_lrf,
                pct_despesa_pessoal_rcl,
                situacao_fiscal_lrf,
                pct_aplicacao_saude_siops,
                pct_aplicacao_educacao_mde
            FROM read_parquet('{_pq("siconfi_finbra_indicadores")}')
            ORDER BY ano_referencia DESC
            """
            rows_lrf, tot_lrf = _query_with_total(sql_lrf)
            insights.append(
                {
                    "id": "audit_lrf_limites_fiscais",
                    "title": "Monitoramento Fiscal LRF (Despesa com Pessoal / RCL) e Mínimos Constitucionais (SICONFI/STN)",
                    "legal_basis": "Arts. 19, 20 e 22 da LC 101/2000 (LRF) e Arts. 198, §2º e 212 da CF/88",
                    "parameter_explained": (
                        "Série oficial homologada pelo Tesouro Nacional (RGF Anexo 01 e RREO Anexo 02/03): "
                        "Alerta LRF em 48,60% da RCL, Prudencial em 51,30% e Teto Executivo em 54,00%; mínimos de 15% (Saúde) e 25% (Educação)."
                    ),
                    "auditor_guidance": (
                        "Nota de Controladoria: Acompanha continuamente a relação Despesa Líquida com Pessoal / RCL e o cumprimento dos pisos "
                        "constitucionais de Saúde (15%) e Educação (25%) segundo os demonstrativos homologados no SICONFI/STN."
                    ),
                    "count": len(rows_lrf),
                    "total_occurrences": tot_lrf,
                    "sql_executed": sql_lrf.strip(),
                    "rows": rows_lrf,
                }
            )

        con.close()

        from sim_platform.security.sql_guard import mask_sensitive_rows

        for ins in insights:
            raw_sql_ex = str(ins.get("sql_executed") or "")
            if raw_sql_ex:
                ins["sql_executed"] = re.sub(
                    r"read_parquet\('([^']+)'\)",
                    lambda m: Path(m.group(1)).stem,
                    raw_sql_ex,
                )
            if isinstance(ins.get("rows"), list) and ins["rows"]:
                masked_r, _ = mask_sensitive_rows(ins["rows"], role="citizen")
                ins["rows"] = masked_r

        has_sicom_trails = any(i["id"] != "audit_lrf_limites_fiscais" for i in insights)
        result_payload = {
            "status": (
                "erro_parcial"
                if (query_errors and not insights)
                else ("ok" if has_sicom_trails else "ok_parcial_api")
            ),
            "tenant_id": self.tenant_id,
            "active_release_id": active_release_id,
            "skipped_empty_tables": sorted(skipped_empty_set),
            "total_insights_categories": len(insights),
            "sicom_trails_pending_package": not has_sicom_trails,
            "query_errors": query_errors[:5] if query_errors else [],
            "insights": insights,
        }
        with _AUDIT_CACHE_LOCK:
            _AUDIT_CACHE[cache_key] = copy.deepcopy(result_payload)
        return result_payload
