"""
Conector Padrão `PortalFacilElConnector` — Família M4 (`Actcon / PortalFácil + E&L / SMARapd Open Data API`).

Atende polos regionais de Minas Gerais (ex.: Governador Valadares `3127701` — `idCliente=94` / E&L,
Ipatinga `3131307` — `idCliente=97` / SMARapd) que disponibilizam API REST JSON/CSV em
`https://dadosabertos-portalfacil.azurewebsites.net/api/`.

Assimetrias reais tratadas:
1. Receita hierárquica mista (`1.0.0.0.00.00.00` sintética junto com `1.1.1.2.50.01.00` analítica):
   elimina contas sintéticas-pai por prefixo PCASP para impedir quintuplicação da arrecadação.
2. Pagamentos de Restos a Pagar trazem `numExercicio` antigo (ex.: `"2014"`) dentro da consulta de `2026`:
   separa `ano_referencia` (exercício financeiro do desembolso) de `dsc_tipo_pagamento = 'RESTOS A PAGAR'`.
3. Higienização LGPD na origem: mascara CPFs de 11 dígitos expostos em auxílios a pessoas físicas
   (`OUTROS AUXÍLIOS FINANCEIROS A PESSOAS FÍSICAS`), preservando CNPJs de 14 dígitos intactos.
4. Normalização de `descTpFolha` (`"FOLHA DO MÊS"` -> `'Mensal'`, `"13º"` -> `'13º Salário'`).
"""

from __future__ import annotations

import re
from datetime import date
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
from sim_platform.ingestion.connectors.betha_cloud import BethaCloudConnector
from sim_platform.ingestion.connectors.cidade360 import Cidade360Connector
from sim_platform.ingestion.connectors.memory_portal import (
    MemoryPortalConnector,
    derive_pcasp_revenue_hierarchy,
    mask_cpf_if_individual,
)
from sim_platform.security.sql_guard import mask_text_cpfs


def _significant_pcasp_prefix(raw_code: str) -> str:
    """Remove apenas segmentos finais inteiramente zerados ('1.1.1.2.50.00.00' -> '1.1.1.2.50') sem amputar o zero de '.50'."""
    s = str(raw_code or "").strip()
    if "." in s:
        parts = [digits_only(p) for p in s.split(".") if digits_only(p) != ""]
        while len(parts) > 1 and set(parts[-1]) == {"0"}:
            parts.pop()
        return ".".join(parts)
    d = digits_only(s)
    while len(d) > 2 and d.endswith("00"):
        d = d[:-2]
    return d


def _ci_dict(rec: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k or "").strip().upper(): v for k, v in rec.items() if k is not None}


class PortalFacilElConnector(BaseMunicipalConnector):
    """Conector oficial para portais `Actcon / PortalFácil` integrados a ERPs `E&L` e `SMARapd`."""

    profile = ConnectorProfile(
        connector_id="portalfacil_el",
        vendor_family="Actcon / PortalFácil + E&L / SMARapd Open Data",
        sync_mode="AUTO_REST_CSV_OPEN_API",
        requires_human_captcha=False,
        default_window_id="portais_abertos_pncp_diarios",
        target_silver_tables=(
            "orgao_orgao",
            "orgao_municipio",
            "orgao_orgaoUnidade",
            "pessoa_municipio",
            "remuneracao_municipio",
            "empenho_empenho",
            "empenho_credorEmpenho",
            "despesa_liquidacao",
            "despesa_pagamento",
            "despesa_despesa",
            "receita_receita",
            "licitacao_licitacao",
            "licitacao_habLicitacao",
            "contrato_contratos",
            "contrato_contContrato",
        ),
        description=(
            "Ingestão automatizada via API pública `dadosabertos-portalfacil.azurewebsites.net` "
            "(E&L / SMARapd), com expurgo de contas sintéticas PCASP, higienização LGPD de CPFs e conciliação SICONFI."
        ),
    )

    def _classify_folha_tipo(self, raw_tp: Any) -> str:
        up = normalize_ascii_upper(raw_tp)
        if "13" in up or "DECIMO" in up or "GRATIFICACAO NATALINA" in up:
            return "13º Salário"
        if "FERIAS" in up:
            return "Férias"
        if "RESCIS" in up:
            return "Rescisão"
        if "COMPLEMENT" in up or "SUPLEMENT" in up:
            return "Complementar"
        return "Mensal"

    def _classify_political_agent_strict(self, cargo: str, funcao: str, vinculo: str) -> Tuple[str, str, str]:
        """
        Classifica exclusivamente Agentes Políticos constitucionais titulares (CF/88 art. 29 e 39 §4º:
        Prefeito, Vice-Prefeito, Secretário Municipal titular e Vereador), evitando que cargos técnicos
        ou assessores inflacionem o limite de sanidade `1 <= political_agents <= 80`.
        """
        c_up = normalize_ascii_upper(f"{cargo} {funcao}")
        v_up = normalize_ascii_upper(vinculo)
        is_adj_or_assessor = any(
            x in c_up
            for x in ("ADJUNTO", "ASSESSOR", "CHEFE DE", "DIRETOR", "COORDENADOR", "GERENTE", "SUBSECRETARIO", "AUXILIAR", "TECNICO", "AGENTE")
        )
        if not is_adj_or_assessor:
            if re.search(r"\bVICE[\s-]*PREFEIT[OA]?\b", c_up):
                return "AP", "Agente Político", "Vice-Prefeito"
            if re.search(r"\bPREFEIT[OA]\b", c_up):
                return "AP", "Agente Político", "Prefeito"
            if re.search(r"\bVEREADOR[A]?\b", c_up):
                return "AP", "Agente Político", "Vereador"
            if re.search(r"\bSECRETARI[OA]\s+MUNICIPAL\b", c_up) or c_up in ("SECRETARIO", "SECRETARIA"):
                return "AP", "Agente Político", "Secretário Municipal"

        if "INATIV" in v_up or "APOSENTAD" in v_up:
            return "INA", "Inativo", ""
        if "PENSION" in v_up:
            return "PEN", "Pensionista", ""
        if "COMISS" in v_up or "CONFIANCA" in v_up:
            return "COM", "Cargo Comissionado", ""
        if "CONTRAT" in v_up or "TEMPOR" in v_up or "DESIGNAD" in v_up:
            return "TEM", "Contratado Temporário", ""
        return "EFE", "Servidor Efetivo", ""

    @staticmethod
    def decode_portal_csv_bytes(raw_bytes: bytes) -> str:
        """Decodifica CSVs de portais municipais suportando UTF-16LE/BE (padrão Actcon/E&L), UTF-8-SIG e CP1252."""
        if not raw_bytes:
            return ""
        if raw_bytes.startswith((b"\xff\xfe", b"\xfe\xff")):
            return raw_bytes.decode("utf-16", errors="replace")
        # Detectar UTF-16LE sem BOM (bytes nulos alternados nos primeiros 100 bytes)
        sample = raw_bytes[:120]
        if sample.count(b"\x00") >= max(10, len(sample) // 4):
            return raw_bytes.decode("utf-16-le", errors="replace")
        try:
            return raw_bytes.decode("utf-8-sig")
        except UnicodeDecodeError:
            return raw_bytes.decode("cp1252", errors="replace")

    def map_servidores(self, raw_rows: Iterable[Dict[str, Any]], default_year: int = 2026) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
        seen: set[Tuple[Any, ...]] = set()
        rem_rows: List[Dict[str, Any]] = []
        pessoas: Dict[Tuple[int, int], Dict[str, Any]] = {}
        dups = 0
        cod_mun = int(self.ibge_code)

        for rec_raw in raw_rows:
            if not isinstance(rec_raw, dict):
                continue
            # Normalizar chaves case-insensitive (JSON camelCase `descServidor` vs CSV UTF-16 `DESCSERVIDOR`)
            rec = {str(k or "").strip().upper(): v for k, v in rec_raw.items() if k is not None}
            nome = str(rec.get("DESCSERVIDOR") or rec.get("NOME_SERVIDOR") or "").strip()
            if not nome:
                continue
            comp = str(rec.get("DTCOMPETENCIA") or f"10/{default_year}").strip()
            m_comp = re.match(r"^(\d{1,2})/(\d{4})$", comp)
            if m_comp:
                mes_ref, ano_ref = int(m_comp.group(1)), int(m_comp.group(2))
            else:
                ano_ref, mes_ref = default_year, 10
            seq_dat_pag = int(f"{ano_ref}{mes_ref:02d}")

            matricula = str(rec.get("NUMMATRICULA") or "").strip()
            unidade = str(rec.get("DESCUNIDADE") or "").strip()
            cod_org_int = 1 if "CAMARA" in normalize_ascii_upper(unidade) else (3 if any(k in normalize_ascii_upper(unidade) for k in ("SAAE", "IPREM", "AUTARQUIA")) else 2)

            vlr_bruto = parse_ptbr_money(rec.get("VLPROVENTO") or rec.get("VLBASE"))
            vlr_desc = parse_ptbr_money(rec.get("VLDESCONTO") or 0.0)
            vlr_liq_raw = parse_ptbr_money(rec.get("VLLIQUIDO") or 0.0)
            vlr_liq = vlr_liq_raw if vlr_liq_raw > 0 else max(0.0, round(vlr_bruto - vlr_desc, 2))
            if vlr_bruto <= 0 and vlr_liq <= 0:
                continue

            tp_folha = self._classify_folha_tipo(rec.get("DESCTPFOLHA"))
            dkey = (cod_org_int, normalize_ascii_upper(matricula), normalize_ascii_upper(nome), seq_dat_pag, tp_folha, vlr_bruto)
            if dkey in seen:
                dups += 1
                continue
            seen.add(dkey)

            cargo = str(rec.get("DESCCARGO") or "NÃO INFORMADO").strip()
            funcao = str(rec.get("DESCFUNCAO") or "").strip()
            vinculo = str(rec.get("DESCVINCULO") or "EFETIVO").strip()
            sgl_cargo, sit_clean, dsc_agepol = self._classify_political_agent_strict(cargo, funcao, vinculo)

            seq_dim_orgao = deterministic_seq_id(self.ibge_code, "dim_orgao", cod_org_int)
            mat_key = (matricula.lstrip("0") or matricula) if matricula else cargo
            seq_dim_pessoa = deterministic_seq_id(self.ibge_code, "pf_pessoa", normalize_ascii_upper(nome), mat_key)
            cpf_masked = mask_cpf_if_individual(rec.get("NUMCPF") or "") or "***.000.***-**"

            rem_rows.append(
                {
                    "seq_dim_orgao": seq_dim_orgao,
                    "seq_dim_pessoa": seq_dim_pessoa,
                    "dsc_cargo": funcao if (dsc_agepol and funcao) else cargo,
                    "dsc_lotacao": unidade,
                    "seq_dat_pagamento": seq_dat_pag,
                    "dsc_regime": vinculo,
                    "dsc_req_crg": "",
                    "dsc_cessao": "",
                    "dsc_tipo_pag": tp_folha,
                    "dsc_sgl_cargo": sgl_cargo,
                    "dsc_situacao": sit_clean,
                    "cod_municipio": cod_mun,
                    "cod_orgao": cod_org_int,
                    "seq_dat_ingresso_cargo": 0,
                    "seq_dat_exclusao": 0,
                    "seq_dat_concessao": 0,
                    "dsc_sgl_cargo_1": sgl_cargo,
                    "cod_vinculo": 1,
                    "num_matricula": matricula,
                    "cod_agepol": "1" if dsc_agepol else "",
                    "dsc_agepol": dsc_agepol,
                    "cod_tipdep": 0,
                    "dsc_tipdep": "",
                    "dsc_dependencia": "",
                    "seq_dat_afast_pre": 0,
                    "qtd_hora_semana": 40.0,
                    "vlr_rmn_bruta": vlr_bruto,
                    "vlr_rmn_liquida": vlr_liq,
                    "vlr_descontos": vlr_desc,
                    "cod_sit_serv_pens": 1,
                    "dsc_sit_serv_pens": sit_clean,
                    "codcbo": "",
                    "seq_dat_obito_instituidor": 0,
                    "seq_dat_comissionado": 0,
                    "seq_dat_comissionado_exc": 0,
                    "seq_dim_pessoa_instituidor": 0,
                    "dsc_req_cargo": "",
                    "dsc_tipo_pagext": "",
                    "ind_natureza_saldo": "",
                    "seq_dat_transf_reserva": 0,
                    "ind_dedic_exclusiva": "",
                    "filename": f"portalfacil_servidores_{ano_ref}.json",
                    "ano_referencia": ano_ref,
                }
            )
            pessoas[(seq_dim_pessoa, ano_ref)] = {
                "seq_dim_pessoa": seq_dim_pessoa,
                "tip_documento": 1,
                "num_documento": cpf_masked,
                "nom_pessoa": nome.upper(),
                "ind_sexo": "",
                "seq_dat_nascimento": 0,
                "cod_orgao": cod_org_int,
                "filename": f"portalfacil_pessoas_{ano_ref}.json",
                "ano_referencia": ano_ref,
            }
        return rem_rows, list(pessoas.values()), dups

    def map_receitas(self, raw_rows: Sequence[Dict[str, Any]], default_year: int = 2025) -> Tuple[List[Dict[str, Any]], int]:
        """Filtra apenas rubricas analíticas folha (expurgando sintéticas-pai PCASP) e mapeia para `receita_receita`."""
        by_year: Dict[int, List[Dict[str, Any]]] = {}
        for r_raw in raw_rows:
            if not isinstance(r_raw, dict):
                continue
            r = _ci_dict(r_raw)
            yr = int(digits_only(r.get("NUMEXERCICIO") or r.get("ANO_REFERENCIA") or default_year) or default_year)
            by_year.setdefault(yr, []).append(r)

        out: List[Dict[str, Any]] = []
        synthetic_dropped = 0
        seq_org = deterministic_seq_id(self.ibge_code, "orgao", "02")

        for yr, items in sorted(by_year.items()):
            prefixes = [_significant_pcasp_prefix(str(it.get("NUMRECEITA") or "")) for it in items]
            uniq_prefixes = {p for p in prefixes if p}
            eff_mes = max(1, date.today().month - 1) if yr >= date.today().year and date.today().month < 12 else 12
            for it, pref in zip(items, prefixes):
                sep = "." if "." in pref else ""
                if pref and any(other != pref and other.startswith(pref + sep) for other in uniq_prefixes):
                    synthetic_dropped += 1
                    continue
                cod_rec = str(it.get("NUMRECEITA") or "").strip()
                desc_rec = str(it.get("DESCRECEITA") or "").strip()
                v_prev = parse_ptbr_money(it.get("VLPREVISTO") or it.get("VLORCADO"))
                v_real = parse_ptbr_money(it.get("VLREALIZADO") or it.get("VLARRECADADO"))
                if v_prev == 0.0 and v_real == 0.0:
                    continue
                is_ded = cod_rec.startswith("9") or "DEDUCAO" in normalize_ascii_upper(desc_rec) or v_real < 0
                if is_ded and v_real > 0:
                    v_real = -abs(v_real)
                    v_prev = -abs(v_prev)
                cat, orig, esp = derive_pcasp_revenue_hierarchy(cod_rec, desc_rec)
                out.append(
                    {
                        "seq_orgao": seq_org,
                        "cod_orgao": "02",
                        "num_anoexercicio": yr,
                        "num_mesexercicio": eff_mes,
                        "dsc_cateconomica": cat,
                        "dsc_origem": orig,
                        "dsc_especie": esp,
                        "dsc_rubrica": desc_rec,
                        "dsc_alinea": desc_rec,
                        "dsc_subalinea": "",
                        "dsc_tipo": "DEDUÇÃO" if is_ded else "RECEITA ORÇAMENTÁRIA",
                        "dsc_fonterecurso": str(it.get("DESCFONTE") or "RECURSOS MUNICIPAIS"),
                        "dsc_codorcamentario": cod_rec,
                        "dsc_tipodeducao": "FUNDEB / DEDUÇÃO" if is_ded else "",
                        "vlr_previsaoinicial": v_prev,
                        "vlr_previsaoatualizada": v_prev,
                        "vlr_realizadoateperiodo": v_real,
                        "filename": f"portalfacil_receita_{yr}.json",
                        "ano_referencia": yr,
                    }
                )
        return out, synthetic_dropped

    def map_empenhos_pagamentos(
        self,
        raw_empenhos: Sequence[Dict[str, Any]],
        raw_liquidacoes: Sequence[Dict[str, Any]],
        raw_pagamentos: Sequence[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], int]:
        emp_rows: List[Dict[str, Any]] = []
        cred_rows: List[Dict[str, Any]] = []
        liq_rows: List[Dict[str, Any]] = []
        pag_rows: List[Dict[str, Any]] = []
        dups = 0
        cod_mun = int(self.ibge_code)
        seq_org2 = deterministic_seq_id(self.ibge_code, "orgao", "02")

        seen_emp: set[str] = set()
        for raw_r in raw_empenhos:
            if not isinstance(raw_r, dict):
                continue
            ci = _ci_dict(raw_r)
            ano = int(digits_only(ci.get("NUMEXERCICIO") or ci.get("ANO_REFERENCIA") or 2025) or 2025)
            num_emp = int(digits_only(ci.get("NUMEMPENHO") or "0") or "0")
            unid = str(ci.get("DESCUNIDADE") or "")
            ukey = f"{ano}::{num_emp}::{unid}"
            if ukey in seen_emp:
                dups += 1
                continue
            seen_emp.add(ukey)
            dt_e = parse_date_int_yyyymmdd(ci.get("DTEMPENHO")) or int(f"{ano}0101")
            mes = int(str(dt_e)[4:6])
            seq_e = deterministic_seq_id(self.ibge_code, "pf_emp", ano, num_emp)
            vlr_e = parse_ptbr_money(ci.get("VLEMPENHO"))
            doc_c = mask_cpf_if_individual(ci.get("NUMCPFCNPJ"))
            nom_c = str(ci.get("DESCFORNECEDOR") or "CREDOR NÃO INFORMADO").strip()
            emp_rows.append(
                {
                    "seq_empenho": seq_e,
                    "cod_municipio": cod_mun,
                    "seq_orgao": seq_org2,
                    "cod_orgao": "02",
                    "seq_unidade": 0,
                    "cod_unidade": str(ci.get("NUMUNIDADE") or ""),
                    "cod_subunidade": "",
                    "num_anoexercicio": ano,
                    "num_mesexercicio": mes,
                    "num_empenho": num_emp,
                    "dat_empenho": dt_e,
                    "dsc_modalidade": str(ci.get("DESCMODALIDADE") or ""),
                    "dsc_tipo_empenho": str(ci.get("TPEMPENHO") or "ORDINÁRIO"),
                    "dsc_empenho": mask_text_cpfs(str(ci.get("DESCHISTORICO") or ci.get("DESCDESPESA") or "").strip()),
                    "ind_dec_contrato": "",
                    "ind_dec_convenio": "",
                    "ind_dec_licitacao": "",
                    "ind_dec_instr_conge": "",
                    "seq_contrato": 0,
                    "seq_termo_aditivo": 0,
                    "seq_convenio": 0,
                    "seq_licitacao": 0,
                    "seq_dispensa": 0,
                    "seq_instr_conge": 0,
                    "dsc_dotacao": "",
                    "dsc_funcao": str(ci.get("DESCFUNCAO") or "NÃO INFORMADO"),
                    "dsc_subfuncao": str(ci.get("DESCSUBFUNCAO") or ""),
                    "dsc_programa": str(ci.get("DESCPROGRAMA") or ""),
                    "dsc_acao": str(ci.get("DESCPROJETO") or ""),
                    "dsc_subacao": "",
                    "dsc_naturezadespesa": str(ci.get("DESCDESPESA") or ""),
                    "vlr_empenhado": vlr_e,
                    "vlr_reforco": 0.0,
                    "vlr_anulempenho": parse_ptbr_money(ci.get("VLANULADO") or 0.0),
                    "filename": f"pf_empenho_{ano}.json",
                    "ano_referencia": ano,
                }
            )
            cred_rows.append(
                {
                    "seq_emp_credor": deterministic_seq_id(self.ibge_code, "pf_cred", seq_e, doc_c or nom_c),
                    "seq_empenho": seq_e,
                    "seq_orgao": seq_org2,
                    "cod_orgao": "02",
                    "seq_unidade": 0,
                    "num_ano_referencia": ano,
                    "num_mes_referencia": mes,
                    "num_doc_credor": doc_c,
                    "nom_credor": nom_c,
                    "filename": f"pf_credor_{ano}.json",
                    "ano_referencia": ano,
                }
            )

        seen_liq: set[str] = set()
        for raw_r in raw_liquidacoes:
            if not isinstance(raw_r, dict):
                continue
            ci = _ci_dict(raw_r)
            dt_l = parse_date_int_yyyymmdd(ci.get("DTLIQUIDACAO"))
            ano_l = int(ci.get("ANO_CONSULTA") or (str(dt_l)[:4] if dt_l else (digits_only(ci.get("NUMEXERCICIO")) or 2025)))
            mes_l = int(str(dt_l)[4:6]) if dt_l else 1
            num_l = int(digits_only(ci.get("NUMLIQUIDACAO") or "0") or "0")
            num_emp = int(digits_only(ci.get("NUMEMPENHO") or "0") or "0")
            ano_emp = int(digits_only(ci.get("NUMEXERCICIO") or ano_l) or ano_l)
            ukey = f"{ano_l}::{num_l}::{num_emp}"
            if ukey in seen_liq:
                dups += 1
                continue
            seen_liq.add(ukey)
            liq_rows.append(
                {
                    "seq_liquidacao": deterministic_seq_id(self.ibge_code, "pf_liq", ukey),
                    "seq_orgao": seq_org2,
                    "cod_orgao": "02",
                    "seq_unidade": 0,
                    "seq_empenho": deterministic_seq_id(self.ibge_code, "pf_emp", ano_emp, num_emp),
                    "seq_rsp": 0,
                    "num_ano_referencia": ano_l,
                    "num_mes_referencia": mes_l,
                    "num_liquidacao": num_l,
                    "dat_liquidacao": dt_l or int(f"{ano_l}0101"),
                    "dsc_tipo_liquidacao": "EXERCÍCIO",
                    "num_doc_liquidante": "",
                    "nom_liquidante": "",
                    "vlr_liquidacao": parse_ptbr_money(ci.get("VLLIQUIDADO") or ci.get("VLLIQUIDACAO") or ci.get("VALOR")),
                    "vlr_anu_liquidacao": 0.0,
                    "filename": f"pf_liq_{ano_l}.json",
                    "ano_referencia": ano_l,
                }
            )

        seen_pag: set[str] = set()
        for idx, raw_r in enumerate(raw_pagamentos):
            if not isinstance(raw_r, dict):
                continue
            ci = _ci_dict(raw_r)
            dt_p = parse_date_int_yyyymmdd(ci.get("DTPAGAMENTO"))
            ano_p = int(ci.get("ANO_CONSULTA") or (str(dt_p)[:4] if dt_p else 2025))
            mes_p = int(str(dt_p)[4:6]) if dt_p else 1
            ano_emp = int(digits_only(ci.get("NUMEXERCICIO") or ano_p) or ano_p)
            num_emp = int(digits_only(ci.get("NUMEMPENHO") or "0") or "0")
            num_pag = int(digits_only(ci.get("NUMPAGAMENTO") or str(idx + 1)) or str(idx + 1))
            vlr_p = parse_ptbr_money(ci.get("VLPAGO") or ci.get("VLPAGAMENTO") or ci.get("VALOR"))
            ukey = f"{ano_p}::{ano_emp}::{num_emp}::{num_pag}::{vlr_p}"
            if ukey in seen_pag:
                dups += 1
                continue
            seen_pag.add(ukey)
            is_rp = ano_emp < ano_p
            doc_c = mask_cpf_if_individual(ci.get("NUMCPFCNPJ"))
            nom_c = str(ci.get("DESCFORNECEDOR") or "CREDOR NÃO INFORMADO").strip()
            pag_rows.append(
                {
                    "seq_pagamento": deterministic_seq_id(self.ibge_code, "pf_pag", ukey),
                    "seq_orgao": seq_org2,
                    "cod_orgao": "02",
                    "seq_unidade": 0,
                    "seq_empenho": deterministic_seq_id(self.ibge_code, "pf_emp", ano_emp, num_emp),
                    "seq_rsp": deterministic_seq_id(self.ibge_code, "pf_rsp", ukey) if is_rp else 0,
                    "seq_liquidacao": 0,
                    "seq_orgao_empenho": seq_org2,
                    "seq_unid_empenho": 0,
                    "num_ano_referencia": ano_p,
                    "num_mes_referencia": mes_p,
                    "num_doc_resp": "",
                    "nom_resp": "",
                    "num_doc_credor": doc_c,
                    "nom_credor": nom_c,
                    "num_empenho": num_emp,
                    "dat_empenho": dt_p or int(f"{ano_emp}0101"),
                    "num_liquidacao": 0,
                    "dat_liquidacao": dt_p or int(f"{ano_p}0101"),
                    "num_ord_pagamento": num_pag,
                    "dat_pagamento": dt_p or int(f"{ano_p}0101"),
                    "dsc_pagamento": mask_text_cpfs(str(ci.get("DESCDESPESA") or ci.get("DESCHISTORICO") or "").strip()),
                    "dsc_tipo_pagamento": "RESTOS A PAGAR" if is_rp else "ORÇAMENTÁRIO",
                    "dsc_fonte_recurso": str(ci.get("DESCFONTE") or ""),
                    "dsc_cod_orcamentario": str(ci.get("NUMDESPESA") or ""),
                    "vlr_pag_fonte": vlr_p,
                    "vlr_ret_fonte": parse_ptbr_money(ci.get("VLRETIDO") or 0.0),
                    "vlr_ant_fonte": 0.0,
                    "vlr_anu_fonte": 0.0,
                    "filename": f"pf_pag_{ano_p}.json",
                    "ano_referencia": ano_p,
                }
            )
        return emp_rows, cred_rows, liq_rows, pag_rows, dups

    def map_contratos(self, raw_contratos: Iterable[Dict[str, Any]], default_year: int = 2025) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
        seen: set[str] = set()
        c_rows: List[Dict[str, Any]] = []
        cc_rows: List[Dict[str, Any]] = []
        dups = 0
        cod_mun = int(self.ibge_code)
        seq_org = deterministic_seq_id(self.ibge_code, "orgao", "02")

        for raw_r in raw_contratos:
            if not isinstance(raw_r, dict):
                continue
            ci = _ci_dict(raw_r)
            num_str = str(ci.get("NUMCONTRATO") or "").strip()
            proc_str = str(ci.get("NUMPROCESSO") or "").strip()
            doc_f = mask_cpf_if_individual(ci.get("NUMCPFCNPJ"))
            nom_f = str(ci.get("DESCFORN") or ci.get("DESCFORNECEDOR") or "").strip()
            if not nom_f:
                nom_f = f"CONTRATADO ({doc_f})" if doc_f else "FORNECEDOR IDENTIFICADO"
            ukey = f"{num_str}::{proc_str}::{doc_f}"
            if not num_str or ukey in seen:
                dups += 1
                continue
            seen.add(ukey)

            dt_ass = parse_date_int_yyyymmdd(ci.get("DTASSINATURA") or ci.get("DTINICIO"))
            dt_fim = parse_date_int_yyyymmdd(ci.get("DTVENCIMENTO") or ci.get("DTFIM"))
            m_yr = re.search(r"/(20\d{2})\b", num_str)
            ano = int(ci.get("ANO_REFERENCIA") or (m_yr.group(1) if m_yr else (str(dt_ass)[:4] if dt_ass else default_year)))
            mes = int(str(dt_ass)[4:6]) if dt_ass else 1
            num_c = int(digits_only(num_str.split("/")[0]) or "0")
            seq_c = deterministic_seq_id(self.ibge_code, "pf_contrato", ukey)

            c_rows.append(
                {
                    "seq_contrato": seq_c,
                    "seq_orgao": seq_org,
                    "cod_municipio": cod_mun,
                    "cod_orgao": "02",
                    "seq_unidade": 0,
                    "cod_unidade": "",
                    "cod_subunidade": "",
                    "seq_licitacao": 0,
                    "seq_dispensa": 0,
                    "num_anoexercicio": ano,
                    "num_mesexercicio": mes,
                    "dsc_decorrentelicitacao": "SIM",
                    "dsc_tipo_processo": str(ci.get("DESCMODALIDADE") or "Processo Licitatório"),
                    "dsc_naturezaobjeto": "COMPRAS E SERVIÇOS",
                    "dsc_tipo_cadastro": "INICIAL",
                    "num_contrato": num_c,
                    "dat_assinatura": dt_ass or int(f"{ano}0101"),
                    "num_ano_contrato": ano,
                    "dsc_objetocontrato": mask_text_cpfs(str(ci.get("DESCOBJETO") or f"Contrato {num_str}").strip()),
                    "num_doc_signatario": "",
                    "dsc_nome_signatario": "",
                    "tipo_instrumento": "Contrato",
                    "dat_iniciovigencia": dt_ass or int(f"{ano}0101"),
                    "dat_fimvigencia": dt_fim or int(f"{ano}1231"),
                    "dsc_fornecimento": "",
                    "dsc_formapagamento": "",
                    "dsc_unid_med_prazo": "",
                    "dsc_prazoexecucao": "",
                    "dsc_multarecisoria": "",
                    "dsc_multa_inadimplencia": "",
                    "dsc_garantia": "",
                    "dat_publicacao": dt_ass or int(f"{ano}0101"),
                    "dsc_veiculopublicacao": "Portal da Transparência / Diário Oficial Eletrônico",
                    "vlr_contrato": parse_ptbr_money(ci.get("VLCONTRATO") or ci.get("VLGLOBAL") or ci.get("VALOR")),
                    "vlr_empenhado": 0.0,
                    "vlr_liquidado": 0.0,
                    "vlr_pago": 0.0,
                    "vlr_rspprocessado": 0.0,
                    "vlr_rspnprocessado": 0.0,
                    "filename": f"pf_contrato_{ano}.json",
                    "ano_referencia": ano,
                }
            )
            cc_rows.append(
                {
                    "seq_contratado": deterministic_seq_id(self.ibge_code, "pf_contratado", seq_c, doc_f or nom_f),
                    "seq_contrato": seq_c,
                    "seq_orgao": seq_org,
                    "cod_orgao": "02",
                    "num_ano_referencia": ano,
                    "num_mes_referencia": mes,
                    "num_doc_representante": "",
                    "dsc_nome_representante": "",
                    "num_doc_credor": doc_f,
                    "dsc_nome_credor": nom_f,
                    "filename": f"pf_contratado_{ano}.json",
                    "ano_referencia": ano,
                }
            )
        return c_rows, cc_rows, dups

    def transform_raw_payload(self, raw_payload: Any, **kwargs: Any) -> ConnectorRunResult:
        payload = raw_payload if isinstance(raw_payload, dict) else {}
        default_year = int(kwargs.get("ano_referencia") or payload.get("ano_referencia") or 2025)
        tables: Dict[str, List[Dict[str, Any]]] = {}
        total_dups = 0
        audit: Dict[str, Any] = {}
        years_seen: set[int] = {default_year}

        if payload.get("servidores"):
            rem_r, pess_r, d1 = self.map_servidores(payload["servidores"], default_year=default_year)
            if rem_r:
                tables["remuneracao_municipio"] = rem_r
                tables["pessoa_municipio"] = pess_r
                years_seen.update(int(r["ano_referencia"]) for r in rem_r)
            total_dups += d1

        if payload.get("receitas"):
            rec_r, syn_drop = self.map_receitas(payload["receitas"], default_year=default_year)
            if rec_r:
                tables["receita_receita"] = rec_r
                years_seen.update(int(r["ano_referencia"]) for r in rec_r)
            audit["synthetic_revenue_rows_dropped"] = syn_drop

        emp_r, cred_r, liq_r, pag_r, d2 = self.map_empenhos_pagamentos(
            payload.get("empenhos") or [],
            payload.get("liquidacoes") or [],
            payload.get("pagamentos") or [],
        )
        if emp_r:
            tables["empenho_empenho"] = emp_r
            tables["empenho_credorEmpenho"] = cred_r
            years_seen.update(int(r["ano_referencia"]) for r in emp_r)
        if liq_r:
            tables["despesa_liquidacao"] = liq_r
        if pag_r:
            tables["despesa_pagamento"] = pag_r
        total_dups += d2

        c_all: List[Dict[str, Any]] = []
        cc_all: List[Dict[str, Any]] = []
        if payload.get("contratos"):
            cr, ccr, d3 = self.map_contratos(payload["contratos"], default_year=default_year)
            c_all.extend(cr)
            cc_all.extend(ccr)
            total_dups += d3
        if payload.get("pncp_contratos"):
            c360 = Cidade360Connector(tenant_id=self.tenant_id, workspace_root=self.workspace_root, tenant_profile=self.tenant_profile)
            cr2, ccr2, d4 = c360.map_pncp_contratos(payload["pncp_contratos"])
            c_all.extend(cr2)
            cc_all.extend(ccr2)
            total_dups += d4
        if c_all:
            tables["contrato_contratos"] = c_all
            tables["contrato_contContrato"] = cc_all
            years_seen.update(int(r["ano_referencia"]) for r in c_all)

        if payload.get("siconfi_rreo_a02_by_year"):
            betha_helper = BethaCloudConnector(tenant_id=self.tenant_id, workspace_root=self.workspace_root, tenant_profile=self.tenant_profile)
            desp_all: List[Dict[str, Any]] = []
            for yr, items in sorted(payload["siconfi_rreo_a02_by_year"].items()):
                dr, _ = betha_helper.reconcile_functions_with_siconfi([], items, int(yr))
                desp_all.extend(dr)
                years_seen.add(int(yr))
            if desp_all:
                tables["despesa_despesa"] = desp_all

        mem_helper = MemoryPortalConnector(tenant_id=self.tenant_id, workspace_root=self.workspace_root, tenant_profile=self.tenant_profile)
        if "receita_receita" not in tables and payload.get("siconfi_rreo_a01_by_year"):
            rec_sic = mem_helper.map_siconfi_rreo_receitas(payload["siconfi_rreo_a01_by_year"])
            if rec_sic:
                tables["receita_receita"] = rec_sic
                years_seen.update(int(r["ano_referencia"]) for r in rec_sic)

        org_r, org_mun_r = mem_helper.build_orgaos_tables(sorted(years_seen))
        tables["orgao_orgao"] = org_r
        tables["orgao_municipio"] = org_mun_r

        gaz_acts: List[Dict[str, Any]] = []
        today_iso = date.today().isoformat()
        for act in payload.get("gazette_acts") or []:
            if isinstance(act, dict) and str(act.get("page_content") or "").strip():
                dt_pub = min(str(act.get("data_publicacao") or today_iso)[:10], today_iso)
                pc = mask_text_cpfs(re.sub(r"^\[Município:[^\]]+\]\s*", "", str(act["page_content"]).strip()))
                gaz_acts.append({**act, "data_publicacao": dt_pub, "page_content": pc})

        return ConnectorRunResult(
            connector_id=self.profile.connector_id,
            tenant_id=self.tenant_id,
            ibge_code=self.ibge_code,
            status="sucesso",
            tables_normalized=tables,
            gazette_acts=gaz_acts,
            deduplicated_count=total_dups,
            audit_metrics={**audit, "years_covered": sorted(years_seen)},
        )
