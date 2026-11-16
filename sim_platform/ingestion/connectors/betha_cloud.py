"""
Conector Padrão `BethaCloudConnector` (Betha Sistemas — Transparência Cloud).

Atende prefeituras mineiras hospedadas na infraestrutura Betha Cloud (ex.: Lagoa da Prata/MG e ~200+ cidades):
1. Deduplicação estrita por `_id` canônico da API Betha Cloud (eliminando duplicatas causadas por paginação mutável `offset`/`limit`).
2. Mapeamento tipado para tabelas Silver compatíveis 1:1 com as 8 Gold Views (`empenho_empenho`, `empenho_credorEmpenho`,
   `despesa_liquidacao`, `despesa_pagamento`, `despesa_despesa`, `remuneracao_municipio`, `pessoa_municipio`,
   `contrato_contratos`, `contrato_contContrato`) usando `deterministic_seq_id`.
3. Conciliação automática de despesas por função de governo (`Saúde`, `Educação`, `Administração`, etc.) via
   SICONFI `RREO-Anexo 02` (`reconcile_functions_with_siconfi`), auditando divergências e preenchendo funções
   consolidadas de autarquias/fundos quando fragmentadas no portal local.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from sim_platform.ingestion.connectors.base import (
     DEFAULT_WORKSPACE_ROOT,
    BaseMunicipalConnector,
    ConnectorProfile,
    ConnectorRunResult,
    deterministic_seq_id,
    digits_only,
    normalize_ascii_upper,
    parse_date_int_yyyymmdd,
    parse_ptbr_money,
)


_SICONFI_STANDARD_FUNCTIONS: Dict[str, str] = {
    "LEGISLATIVA": "01 - LEGISLATIVA",
    "JUDICIARIA": "02 - JUDICIÁRIA",
    "ESSENCIAL A JUSTICA": "03 - ESSENCIAL À JUSTIÇA",
    "ADMINISTRACAO": "04 - ADMINISTRAÇÃO",
    "SEGURANCA PUBLICA": "06 - SEGURANÇA PÚBLICA",
    "ASSISTENCIA SOCIAL": "08 - ASSISTÊNCIA SOCIAL",
    "PREVIDENCIA SOCIAL": "09 - PREVIDÊNCIA SOCIAL",
    "SAUDE": "10 - SAÚDE",
    "EDUCACAO": "12 - EDUCAÇÃO",
    "CULTURA": "13 - CULTURA",
    "URBANISMO": "15 - URBANISMO",
    "HABITACAO": "16 - HABITAÇÃO",
    "SANEAMENTO": "17 - SANEAMENTO",
    "GESTAO AMBIENTAL": "18 - GESTÃO AMBIENTAL",
    "AGRICULTURA": "20 - AGRICULTURA",
    "COMERCIO E SERVICOS": "23 - COMÉRCIO E SERVIÇOS",
    "TRANSPORTE": "26 - TRANSPORTE",
    "DESPORTO E LAZER": "27 - DESPORTO E LAZER",
    "ENCARGOS ESPECIAIS": "28 - ENCARGOS ESPECIAIS",
}


class BethaCloudConnector(BaseMunicipalConnector):
    """Conector oficial padronizado para portais de transparência Betha Cloud em Minas Gerais."""

    profile = ConnectorProfile(
        connector_id="betha_cloud",
        vendor_family="Betha Sistemas / Transparência Cloud",
        sync_mode="AUTO_API",
        requires_human_captcha=False,
        default_window_id="portais_abertos_pncp_diarios",
        target_silver_tables=(
            "empenho_empenho",
            "empenho_credorEmpenho",
            "despesa_liquidacao",
            "despesa_pagamento",
            "despesa_despesa",
            "remuneracao_municipio",
            "pessoa_municipio",
            "contrato_contratos",
            "contrato_contContrato",
        ),
        description=(
            "Deduplicação por `_id` Betha Cloud, síntese de chaves `seq_*` de 52 bits e "
            "conciliação funcional de despesas via SICONFI RREO-Anexo 02."
        ),
    )

    def __init__(
        self,
        tenant_id: str,
        workspace_root: Path = DEFAULT_WORKSPACE_ROOT,
        tenant_profile: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(
            tenant_id=tenant_id,
            workspace_root=workspace_root,
            tenant_profile=tenant_profile,
        )

    @staticmethod
    def deduplicate_by_betha_id(
        records: Iterable[Dict[str, Any]],
        fallback_key_fields: Sequence[str] = ("numero", "exercicio", "data", "valor"),
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Deduplica registros paginados do Betha Cloud usando `_id` (ou `id`) como chave primária.
        Quando um mesmo `_id` aparece em múltiplas páginas, preserva a última versão consolidada.
        Retorna `(unique_records, duplicates_removed)`.
        """
        by_key: Dict[str, Dict[str, Any]] = {}
        duplicates = 0
        for idx, rec in enumerate(records):
            if not isinstance(rec, dict):
                continue
            raw_id = str(rec.get("_id") or rec.get("id") or "").strip()
            if raw_id:
                k = f"id::{raw_id}"
            else:
                fb_vals = [str(rec.get(f) or "").strip() for f in fallback_key_fields]
                if any(fb_vals):
                    k = "fb::" + "|".join(fb_vals)
                else:
                    k = f"row::{idx}"
            if k in by_key:
                duplicates += 1
            by_key[k] = rec
        return list(by_key.values()), duplicates

    def _resolve_orgao_codes(self, raw_orgao_code: Any, raw_orgao_name: Any = None) -> Tuple[int, str]:
        cod_str = str(raw_orgao_code or "").strip()
        if not cod_str:
            up_nm = normalize_ascii_upper(raw_orgao_name)
            if "CAMARA" in up_nm or "LEGISLATIV" in up_nm:
                cod_str = "01"
            elif "AGUA" in up_nm or "SAAE" in up_nm:
                cod_str = "03"
            elif "SAUDE" in up_nm:
                cod_str = "04"
            else:
                cod_str = "02"
        cod_norm = cod_str.zfill(2) if cod_str.isdigit() and len(cod_str) < 2 else cod_str
        seq_orgao = deterministic_seq_id(self.ibge_code, "orgao", cod_norm)
        return seq_orgao, cod_norm

    def map_empenhos(self, raw_empenhos: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
        dedup_list, dup_cnt = self.deduplicate_by_betha_id(
            raw_empenhos,
            fallback_key_fields=("numeroEmpenho", "numero", "exercicio", "ano", "valorEmpenhado"),
        )
        emp_rows: List[Dict[str, Any]] = []
        cred_rows: List[Dict[str, Any]] = []
        cod_mun_int = int(self.ibge_code)

        for rec in dedup_list:
            betha_id = str(rec.get("_id") or rec.get("id") or "").strip()
            ano = int(rec.get("exercicio") or rec.get("ano") or rec.get("ano_referencia") or 2025)
            dt_emp = parse_date_int_yyyymmdd(rec.get("dataEmpenho") or rec.get("data"))
            mes = int(rec.get("mes") or (str(dt_emp)[4:6] if dt_emp else 1))
            num_emp_raw = digits_only(rec.get("numeroEmpenho") or rec.get("numero") or "0") or "0"
            num_emp = int(num_emp_raw)
            org_obj = rec.get("entidade") if isinstance(rec.get("entidade"), dict) else {}
            org_nm = rec.get("nomeOrgao") or org_obj.get("nome") or rec.get("orgao") or ""
            seq_orgao, cod_orgao = self._resolve_orgao_codes(rec.get("codOrgao") or org_obj.get("codigo"), org_nm)

            seq_emp = deterministic_seq_id(
                self.ibge_code,
                "betha_empenho",
                betha_id or f"{ano}-{cod_orgao}-{num_emp}",
            )
            vlr_emp = parse_ptbr_money(rec.get("valorEmpenhado") or rec.get("valorOriginal") or rec.get("valor"))
            vlr_ref = parse_ptbr_money(rec.get("valorReforco") or 0.0)
            vlr_anu = parse_ptbr_money(rec.get("valorAnulado") or 0.0)

            func_raw = rec.get("funcao")
            dsc_funcao = (
                str(func_raw.get("descricao") or "")
                if isinstance(func_raw, dict)
                else str(func_raw or rec.get("descricaoFuncao") or "NÃO INFORMADO")
            ).strip()

            emp_rows.append(
                {
                    "seq_empenho": seq_emp,
                    "cod_municipio": cod_mun_int,
                    "seq_orgao": seq_orgao,
                    "cod_orgao": cod_orgao,
                    "num_anoexercicio": ano,
                    "num_mesexercicio": mes,
                    "num_empenho": num_emp,
                    "dat_empenho": dt_emp,
                    "dsc_modalidade": str(rec.get("modalidadeLicitacao") or rec.get("modalidade") or ""),
                    "dsc_tipo_empenho": str(rec.get("tipoEmpenho") or rec.get("especie") or "ORDINÁRIO"),
                    "dsc_empenho": str(rec.get("historico") or rec.get("objeto") or "").strip(),
                    "ind_dec_licitacao": str(rec.get("modalidadeLicitacao") or ""),
                    "dsc_funcao": dsc_funcao,
                    "dsc_subfuncao": str(rec.get("subfuncao") or ""),
                    "dsc_programa": str(rec.get("programa") or ""),
                    "dsc_acao": str(rec.get("acao") or ""),
                    "dsc_naturezadespesa": str(rec.get("naturezaDespesa") or rec.get("elementoDespesa") or ""),
                    "vlr_empenhado": vlr_emp,
                    "vlr_reforco": vlr_ref,
                    "vlr_anulempenho": vlr_anu,
                    "ano_referencia": ano,
                }
            )

            cred_obj = rec.get("credor") if isinstance(rec.get("credor"), dict) else {}
            doc_cred = digits_only(
                rec.get("cpfCnpjCredor") or cred_obj.get("cpfCnpj") or cred_obj.get("documento") or rec.get("documentoCredor")
            )
            nom_cred = str(
                rec.get("nomeCredor") or cred_obj.get("nome") or rec.get("favorecido") or "NÃO INFORMADO"
            ).strip()
            cred_rows.append(
                {
                    "seq_emp_credor": deterministic_seq_id(self.ibge_code, "betha_emp_credor", seq_emp, doc_cred or nom_cred),
                    "seq_empenho": seq_emp,
                    "seq_orgao": seq_orgao,
                    "cod_orgao": cod_orgao,
                    "num_ano_referencia": ano,
                    "num_mes_referencia": mes,
                    "num_doc_credor": doc_cred,
                    "nom_credor": nom_cred,
                    "ano_referencia": ano,
                }
            )
        return emp_rows, cred_rows, dup_cnt

    def map_liquidacoes(self, raw_liquidacoes: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        dedup_list, dup_cnt = self.deduplicate_by_betha_id(
            raw_liquidacoes,
            fallback_key_fields=("numeroLiquidacao", "numeroEmpenho", "exercicio", "valorLiquidado"),
        )
        rows: List[Dict[str, Any]] = []
        for rec in dedup_list:
            betha_id = str(rec.get("_id") or rec.get("id") or "").strip()
            ano = int(rec.get("exercicio") or rec.get("ano") or rec.get("ano_referencia") or 2025)
            dt_liq = parse_date_int_yyyymmdd(rec.get("dataLiquidacao") or rec.get("data"))
            mes = int(rec.get("mes") or (str(dt_liq)[4:6] if dt_liq else 1))
            seq_orgao, cod_orgao = self._resolve_orgao_codes(rec.get("codOrgao"), rec.get("nomeOrgao") or rec.get("orgao"))
            emp_ref_id = str(rec.get("empenhoId") or rec.get("_empenhoId") or "").strip()
            num_emp = int(digits_only(rec.get("numeroEmpenho") or "0") or "0")
            seq_emp = deterministic_seq_id(
                self.ibge_code,
                "betha_empenho",
                emp_ref_id or f"{ano}-{cod_orgao}-{num_emp}",
            )
            num_liq = int(digits_only(rec.get("numeroLiquidacao") or rec.get("numero") or "0") or "0")
            seq_liq = deterministic_seq_id(
                self.ibge_code,
                "betha_liquidacao",
                betha_id or f"{ano}-{cod_orgao}-{num_emp}-{num_liq}",
            )
            rows.append(
                {
                    "seq_liquidacao": seq_liq,
                    "seq_orgao": seq_orgao,
                    "cod_orgao": cod_orgao,
                    "seq_empenho": seq_emp,
                    "seq_rsp": 0,
                    "num_ano_referencia": ano,
                    "num_mes_referencia": mes,
                    "num_liquidacao": num_liq,
                    "dat_liquidacao": dt_liq,
                    "dsc_tipo_liquidacao": str(rec.get("tipoLiquidacao") or "EXERCÍCIO"),
                    "vlr_liquidacao": parse_ptbr_money(rec.get("valorLiquidado") or rec.get("valor")),
                    "vlr_anu_liquidacao": parse_ptbr_money(rec.get("valorAnulado") or 0.0),
                    "ano_referencia": ano,
                }
            )
        return rows, dup_cnt

    def map_pagamentos(self, raw_pagamentos: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        dedup_list, dup_cnt = self.deduplicate_by_betha_id(
            raw_pagamentos,
            fallback_key_fields=("numeroPagamento", "numeroEmpenho", "exercicio", "valorPago"),
        )
        rows: List[Dict[str, Any]] = []
        for rec in dedup_list:
            betha_id = str(rec.get("_id") or rec.get("id") or "").strip()
            ano = int(rec.get("exercicio") or rec.get("ano") or rec.get("ano_referencia") or 2025)
            dt_pag = parse_date_int_yyyymmdd(rec.get("dataPagamento") or rec.get("data"))
            mes = int(rec.get("mes") or (str(dt_pag)[4:6] if dt_pag else 1))
            seq_orgao, cod_orgao = self._resolve_orgao_codes(rec.get("codOrgao"), rec.get("nomeOrgao") or rec.get("orgao"))
            emp_ref_id = str(rec.get("empenhoId") or rec.get("_empenhoId") or "").strip()
            num_emp = int(digits_only(rec.get("numeroEmpenho") or "0") or "0")
            is_rp = bool(rec.get("restosAPagar")) or ("RESTOS" in normalize_ascii_upper(rec.get("tipoPagamento") or ""))
            seq_emp = (
                -1
                if is_rp and not emp_ref_id and num_emp == 0
                else deterministic_seq_id(self.ibge_code, "betha_empenho", emp_ref_id or f"{ano}-{cod_orgao}-{num_emp}")
            )
            num_pag = int(digits_only(rec.get("numeroPagamento") or rec.get("numero") or "0") or "0")
            seq_pag = deterministic_seq_id(
                self.ibge_code,
                "betha_pagamento",
                betha_id or f"{ano}-{cod_orgao}-{num_emp}-{num_pag}",
            )
            cred_obj = rec.get("credor") if isinstance(rec.get("credor"), dict) else {}
            doc_cred = digits_only(
                rec.get("cpfCnpjCredor") or cred_obj.get("cpfCnpj") or cred_obj.get("documento") or rec.get("documentoCredor")
            )
            nom_cred = str(
                rec.get("nomeCredor") or cred_obj.get("nome") or rec.get("favorecido") or "NÃO INFORMADO"
            ).strip()
            rows.append(
                {
                    "seq_pagamento": seq_pag,
                    "seq_orgao": seq_orgao,
                    "cod_orgao": cod_orgao,
                    "seq_empenho": seq_emp,
                    "seq_rsp": deterministic_seq_id(self.ibge_code, "betha_rsp", betha_id) if is_rp else 0,
                    "seq_liquidacao": 0,
                    "num_ano_referencia": ano,
                    "num_mes_referencia": mes,
                    "num_doc_credor": doc_cred,
                    "nom_credor": nom_cred,
                    "num_empenho": num_emp,
                    "num_ord_pagamento": num_pag,
                    "dat_pagamento": dt_pag,
                    "dsc_pagamento": str(rec.get("historico") or rec.get("descricao") or "").strip(),
                    "dsc_tipo_pagamento": "RESTOS A PAGAR" if is_rp else "ORÇAMENTÁRIO",
                    "dsc_fonte_recurso": str(rec.get("fonteRecurso") or ""),
                    "vlr_pag_fonte": parse_ptbr_money(rec.get("valorPago") or rec.get("valor")),
                    "vlr_ret_fonte": parse_ptbr_money(rec.get("valorRetido") or 0.0),
                    "vlr_anu_fonte": parse_ptbr_money(rec.get("valorAnulado") or 0.0),
                    "ano_referencia": ano,
                }
            )
        return rows, dup_cnt

    def reconcile_functions_with_siconfi(
        self,
        betha_despesas_rows: Sequence[Dict[str, Any]],
        siconfi_rreo_a02_items: Sequence[Dict[str, Any]],
        ano_referencia: int,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Concilia despesas por Função de Governo do portal Betha Cloud com o demonstrativo oficial
        SICONFI `RREO-Anexo 02` (Demonstrativo da Execução das Despesas por Função/Subfunção).
        - Se o portal Betha omitir funções de fundos/autarquias (ex.: Saúde / Educação / Saneamento)
          ou divergir acima de 1%, consolida/completa com lastro contábil auditável do SICONFI.
        """
        dedup_desp, dup_cnt = self.deduplicate_by_betha_id(
            betha_despesas_rows,
            fallback_key_fields=("funcao", "dsc_funcao", "mes", "valorLiquidado"),
        )
        cod_mun_int = int(self.ibge_code)
        seq_orgao_pref, cod_orgao_pref = self._resolve_orgao_codes("02", self.municipality_name)

        local_by_func: Dict[str, Dict[str, float]] = {}
        silver_rows: List[Dict[str, Any]] = []

        for r in dedup_desp:
            raw_fn = str(r.get("dsc_funcao") or r.get("funcao") or "ADMINISTRAÇÃO").strip()
            norm_fn = re.sub(r"^\d+\s*[-–]\s*", "", normalize_ascii_upper(raw_fn)).strip()
            canon_label = _SICONFI_STANDARD_FUNCTIONS.get(norm_fn, raw_fn.upper())
            v_prev = parse_ptbr_money(r.get("vlr_previsto") or r.get("dotacaoInicial") or r.get("dotacaoAtualizada"))
            v_emp = parse_ptbr_money(r.get("vlr_empenhado") or r.get("valorEmpenhado"))
            v_liq = parse_ptbr_money(r.get("vlr_liquidado") or r.get("valorLiquidado"))
            v_pag = parse_ptbr_money(r.get("vlr_pago") or r.get("valorPago"))

            agg = local_by_func.setdefault(norm_fn, {"previsto": 0.0, "empenhado": 0.0, "liquidado": 0.0, "pago": 0.0})
            agg["previsto"] = round(agg["previsto"] + v_prev, 2)
            agg["empenhado"] = round(agg["empenhado"] + v_emp, 2)
            agg["liquidado"] = round(agg["liquidado"] + v_liq, 2)
            agg["pago"] = round(agg["pago"] + v_pag, 2)

            silver_rows.append(
                {
                    "seq_orgao": seq_orgao_pref,
                    "cod_municipio": cod_mun_int,
                    "cod_orgao": cod_orgao_pref,
                    "num_anoexercicio": int(ano_referencia),
                    "num_mesexercicio": int(r.get("num_mesexercicio") or r.get("mes") or 12),
                    "dsc_funcao": canon_label,
                    "dsc_subfuncao": str(r.get("dsc_subfuncao") or r.get("subfuncao") or ""),
                    "dsc_fonterecurso": str(r.get("dsc_fonterecurso") or r.get("fonteRecurso") or "CONSOLIDADO PORTAL"),
                    "vlr_previsto": v_prev,
                    "vlr_acrescimo": 0.0,
                    "vlr_deducao": 0.0,
                    "vlr_empenhado": v_emp,
                    "vlr_liquidado": v_liq,
                    "vlr_pago": v_pag,
                    "vlr_rspprocessado": 0.0,
                    "vlr_rspnprocessado": 0.0,
                    "ano_referencia": int(ano_referencia),
                }
            )

        # Extrair totais por função principal do SICONFI RREO-Anexo 02 (evitando dupla contagem de subfunções)
        siconfi_by_func: Dict[str, Dict[str, float]] = {}
        for it in siconfi_rreo_a02_items:
            if not isinstance(it, dict):
                continue
            conta_norm = normalize_ascii_upper(it.get("conta") or "")
            if conta_norm not in _SICONFI_STANDARD_FUNCTIONS:
                continue
            col_norm = normalize_ascii_upper(it.get("coluna") or "")
            val = parse_ptbr_money(it.get("valor"))
            if val <= 0:
                continue
            bucket = siconfi_by_func.setdefault(
                conta_norm,
                {"previsto": 0.0, "empenhado": 0.0, "liquidado": 0.0, "pago": 0.0},
            )
            if "DOTACAO ATUALIZADA" in col_norm or ("DOTACAO INICIAL" in col_norm and bucket["previsto"] == 0.0):
                bucket["previsto"] = val
            elif "EMPENHADAS ATE O BIMESTRE" in col_norm:
                bucket["empenhado"] = val
            elif "LIQUIDADAS ATE O BIMESTRE" in col_norm:
                bucket["liquidado"] = val
            elif "PAGAS ATE O BIMESTRE" in col_norm:
                bucket["pago"] = val

        reconciled_adjustments: List[Dict[str, Any]] = []
        for fn_key, sic_tot in sorted(siconfi_by_func.items()):
            loc_tot = local_by_func.get(fn_key)
            canon_label = _SICONFI_STANDARD_FUNCTIONS[fn_key]
            sic_liq = sic_tot["liquidado"]
            loc_liq = loc_tot["liquidado"] if loc_tot else 0.0
            diff_liq = round(sic_liq - loc_liq, 2)

            # Se a função não veio no portal ou faltam > 1% do consolidado municipal (ex.: Fundo de Saúde / SAAE separado)
            if sic_liq > 0 and (loc_tot is None or (diff_liq > 0 and (diff_liq / sic_liq) > 0.01)):
                delta_prev = max(0.0, round(sic_tot["previsto"] - (loc_tot["previsto"] if loc_tot else 0.0), 2))
                delta_emp = max(0.0, round(sic_tot["empenhado"] - (loc_tot["empenhado"] if loc_tot else 0.0), 2))
                delta_pag = max(0.0, round(sic_tot["pago"] - (loc_tot["pago"] if loc_tot else 0.0), 2))
                silver_rows.append(
                    {
                        "seq_orgao": seq_orgao_pref,
                        "cod_municipio": cod_mun_int,
                        "cod_orgao": cod_orgao_pref,
                        "num_anoexercicio": int(ano_referencia),
                        "num_mesexercicio": 12,
                        "dsc_funcao": canon_label,
                        "dsc_subfuncao": "CONCILIAÇÃO SICONFI RREO-ANEXO 02",
                        "dsc_fonterecurso": "CONCILIAÇÃO OFICIAL SICONFI/STN",
                        "vlr_previsto": delta_prev,
                        "vlr_acrescimo": 0.0,
                        "vlr_deducao": 0.0,
                        "vlr_empenhado": delta_emp,
                        "vlr_liquidado": diff_liq,
                        "vlr_pago": delta_pag,
                        "vlr_rspprocessado": 0.0,
                        "vlr_rspnprocessado": 0.0,
                        "ano_referencia": int(ano_referencia),
                    }
                )
                reconciled_adjustments.append(
                    {
                        "funcao": canon_label,
                        "portal_liquidado": loc_liq,
                        "siconfi_liquidado": sic_liq,
                        "complemento_conciliado": diff_liq,
                    }
                )

        report = {
            "ano_referencia": int(ano_referencia),
            "duplicates_removed": dup_cnt,
            "siconfi_functions_checked": len(siconfi_by_func),
            "functions_reconciled_count": len(reconciled_adjustments),
            "adjustments": reconciled_adjustments,
        }
        return silver_rows, report

    def transform_raw_payload(self, raw_payload: Any, **kwargs: Any) -> ConnectorRunResult:
        payload = raw_payload if isinstance(raw_payload, dict) else {}
        ano_ref = int(kwargs.get("ano_referencia") or payload.get("ano_referencia") or 2025)
        tables: Dict[str, List[Dict[str, Any]]] = {}
        total_dups = 0
        audit: Dict[str, Any] = {}

        if payload.get("empenhos"):
            emp_rows, cred_rows, d_emp = self.map_empenhos(payload["empenhos"])
            tables["empenho_empenho"] = emp_rows
            tables["empenho_credorEmpenho"] = cred_rows
            total_dups += d_emp

        if payload.get("liquidacoes"):
            liq_rows, d_liq = self.map_liquidacoes(payload["liquidacoes"])
            tables["despesa_liquidacao"] = liq_rows
            total_dups += d_liq

        if payload.get("pagamentos"):
            pag_rows, d_pag = self.map_pagamentos(payload["pagamentos"])
            tables["despesa_pagamento"] = pag_rows
            total_dups += d_pag

        if payload.get("despesas_funcao") is not None or payload.get("siconfi_rreo_a02"):
            desp_rows, rec_rep = self.reconcile_functions_with_siconfi(
                betha_despesas_rows=payload.get("despesas_funcao") or [],
                siconfi_rreo_a02_items=payload.get("siconfi_rreo_a02") or [],
                ano_referencia=ano_ref,
            )
            if desp_rows:
                tables["despesa_despesa"] = desp_rows
            total_dups += int(rec_rep.get("duplicates_removed") or 0)
            audit["siconfi_reconciliation"] = rec_rep

        return ConnectorRunResult(
            connector_id=self.profile.connector_id,
            tenant_id=self.tenant_id,
            ibge_code=self.ibge_code,
            status="sucesso",
            tables_normalized=tables,
            deduplicated_count=total_dups,
            audit_metrics=audit,
        )
