"""
Conector Padrão `MemoryPortalConnector` — Família M3 (`Memory Sistemas / OpenKnowledge FastAPI`).

Atende prefeituras e câmaras municipais de Minas Gerais que operam o ecossistema
`ilai.memory.com.br` + `https://publico.memory.com.br/openknowledge` (ex.: Matozinhos `3141108`,
Curvelo `3120904`, Conselheiro Pena, Manhumirim, Maravilhas).

Assimetrias tratadas nativamente:
1. Multi-entidade na mesma cidade (`entidade=1` Prefeitura `9BHSQH` -> `cod_orgao='02'` EXECUTIVO;
   `entidade=2` Câmara `9BHSQY` -> `cod_orgao='01'` LEGISLATIVO; `entidade=3` Consórcio/Autarquia -> `'03'`).
2. Folha em `/lai/pessoal/servidor/` traz `proventos` como string decimal de alta precisão
   (`"7795.800000000000"`) sem descontos explícitos, e mistura `situacao_admissional` com cargos
   políticos (`PREFEITO`, `VICE-PREFEITO`, `SECRETARIO MUNICIPAL`, `VEREADOR`) — classificados sem
   contaminação por `departamento_lotacao`.
3. Higienização LGPD na ingestão: CPFs de 11 dígitos de pessoas físicas em empenhos/pagamentos
   são mascarados preventivamente quando expostos sem máscara, preservando CNPJs de 14 dígitos intactos.
4. Conciliação funcional com `SICONFI RREO-Anexo 02` e `RREO-Anexo 01` para `despesa_despesa` e `receita_receita`.
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
from sim_platform.security.sql_guard import mask_text_cpfs


def mask_cpf_if_individual(doc_raw: Any) -> str:
    """Preserva CNPJ (14 dígitos) para cruzamento QSA/PNCP e mascara CPF no padrão ANPD `***.XXX.XXX-**`."""
    raw_s = str(doc_raw or "").strip()
    if not raw_s:
        return ""
    if "*" in raw_s:
        return mask_text_cpfs(raw_s)
    d = digits_only(raw_s)
    if not d:
        return ""
    if len(d) == 14:
        return d
    if len(d) in (12, 13) and "/" in raw_s:
        return d.zfill(14)
    if 7 <= len(d) <= 11:
        cpf11 = d.zfill(11)
        return f"***.{cpf11[3:6]}.{cpf11[6:9]}-**"
    return "***.000.***-**"


def derive_pcasp_revenue_hierarchy(cod_raw: str, desc_raw: str) -> Tuple[str, str, str]:
    """
    Deriva `(dsc_cateconomica, dsc_origem, dsc_especie)` a partir do código nacional PCASP/STN
    (`1.1.1.2.50...` ou `111250...`), garantindo compatibilidade universal com `vw_gold_orcamento_previsto_vs_realizado`.
    """
    d = digits_only(cod_raw)
    up = normalize_ascii_upper(desc_raw)
    if d.startswith("2") or "CAPITAL" in up or "OPERACOES DE CREDITO" in up or "ALIENACAO" in up:
        cat = "Receitas de Capital"
    else:
        cat = "Receitas Correntes"

    p2 = d[:2]
    if p2 == "11" or any(k in up for k in ("IMPOSTO", "IPTU", "ISS", "ITBI", "IRRF", "TAXA")):
        orig = "Impostos, Taxas e Contribuições de Melhoria"
        esp = "Impostos" if d.startswith("111") or "IMPOSTO" in up else "Taxas"
    elif p2 == "12" or "CONTRIBUIC" in up or "COSIP" in up or "CIP" in up:
        orig = "Contribuições"
        esp = "Contribuições Econômicas / Iluminação Pública"
    elif p2 == "13" or "PATRIMONIAL" in up or "RENDIMENTO" in up:
        orig = "Receita Patrimonial"
        esp = "Valores Mobiliários"
    elif p2 == "16" or "SERVICO" in up:
        orig = "Receita de Serviços"
        esp = "Serviços Administrativos"
    elif p2 in ("17", "24") or "TRANSFERENCIA" in up or "COTA-PARTE" in up or "FPM" in up or "FUNDEB" in up:
        orig = "Transferências Correntes" if cat == "Receitas Correntes" else "Transferências de Capital"
        esp = "Transferências Intergovernamentais"
    else:
        orig = "Outras Receitas Correntes" if cat == "Receitas Correntes" else "Outras Receitas de Capital"
        esp = "Demais Receitas"
    return cat, orig, esp


class MemoryPortalConnector(BaseMunicipalConnector):
    """Conector oficial para portais Memory Sistemas (`OpenKnowledge` + `SICONFI` + `PNCP` + `DiOE/DOSP`)."""

    profile = ConnectorProfile(
        connector_id="memory_portal",
        vendor_family="Memory Sistemas (iLAI / OpenKnowledge REST)",
        sync_mode="AUTO_REST_OPENKNOWLEDGE",
        requires_human_captcha=False,
        default_window_id="portais_abertos_pncp_diarios",
        target_silver_tables=(
            "orgao_orgao",
            "orgao_municipio",
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
            "licitacao_homologLicitacao",
            "contrato_contratos",
            "contrato_contContrato",
        ),
        description=(
            "Ingestão automatizada via FastAPI pública `publico.memory.com.br/openknowledge` "
            "(Prefeitura + Câmara + Autarquias) conciliada com SICONFI/STN e PNCP."
        ),
    )

    def _resolve_orgao(self, raw_cod: Any, raw_desc: Any = "") -> Tuple[int, str, str]:
        up = normalize_ascii_upper(raw_desc)
        d = digits_only(raw_cod)
        if d in ("1", "01") or "CAMARA" in up or "LEGISLATIVO" in up:
            cod = "01"
            nom = (self.tenant_profile.get("orgaos_map") or {}).get("01") or f"CÂMARA MUNICIPAL DE {self.municipality_name.upper()}"
        elif d in ("3", "03") or any(k in up for k in ("AUTARQUIA", "SAAE", "CONSORCIO", "CISREC", "INSTITUTO", "PREVIDENCIA")):
            cod = "03"
            nom = (self.tenant_profile.get("orgaos_map") or {}).get("03") or str(raw_desc or "ADMINISTRAÇÃO INDIRETA").strip().upper()
        else:
            cod = "02"
            nom = (self.tenant_profile.get("orgaos_map") or {}).get("02") or f"PREFEITURA MUNICIPAL DE {self.municipality_name.upper()}"
        return deterministic_seq_id(self.ibge_code, "orgao", cod), cod, nom

    def build_orgaos_tables(self, anos: Sequence[int]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        cod_mun = int(self.ibge_code)
        orgaos_map = self.tenant_profile.get("orgaos_map") or {
            "01": f"CÂMARA MUNICIPAL DE {self.municipality_name.upper()}",
            "02": f"PREFEITURA MUNICIPAL DE {self.municipality_name.upper()}",
        }
        cnpjs_by_cod: Dict[str, str] = {}
        for cnpj, lbl in (self.tenant_profile.get("institutional_cnpjs") or {}).items():
            ul = normalize_ascii_upper(lbl)
            if "CAMARA" in ul:
                cnpjs_by_cod["01"] = digits_only(cnpj)
            elif "PREFEITURA" in ul:
                cnpjs_by_cod["02"] = digits_only(cnpj)
            else:
                cnpjs_by_cod.setdefault("03", digits_only(cnpj))

        orgao_rows: List[Dict[str, Any]] = []
        mun_rows: List[Dict[str, Any]] = []
        latest_yr = max(anos) if anos else 2025
        for cod, nom in sorted(orgaos_map.items()):
            seq_org = deterministic_seq_id(self.ibge_code, "orgao", cod)
            seq_dim = deterministic_seq_id(self.ibge_code, "dim_orgao", int(digits_only(cod) or "2"))
            is_cam = cod == "01" or "CAMARA" in normalize_ascii_upper(nom)
            for yr in sorted(set(anos)):
                orgao_rows.append(
                    {
                        "seq_orgao": seq_org,
                        "num_anoexercicio": int(yr),
                        "cod_orgao": cod,
                        "nom_orgao": nom,
                        "tipo_orgao": "01" if is_cam else ("02" if cod == "02" else "03"),
                        "cod_municipio": cod_mun,
                        "nom_municipio": self.municipality_name.upper(),
                        "cod_uf": 31,
                        "sgl_uf": self.state_uf,
                        "nom_uf": "MINAS GERAIS",
                        "dsc_regiaoplanejamento": "MINAS GERAIS",
                        "filename": f"memory_orgao_{yr}.json",
                        "ano_referencia": int(yr),
                    }
                )
            mun_rows.append(
                {
                    "seq_dim_orgao": seq_dim,
                    "cod_municipio": cod_mun,
                    "cod_orgao": int(digits_only(cod) or "2"),
                    "cod_tip_orgao": 1 if is_cam else 2,
                    "cod_tip_orgao_1": 1 if is_cam else 2,
                    "cnpj_orgao": cnpjs_by_cod.get(cod, ""),
                    "nom_orgao": nom,
                    "nom_grupo_jurisdicionado": self.municipality_name.upper(),
                    "dsc_tip_orgao": "PODER LEGISLATIVO" if is_cam else "PODER EXECUTIVO",
                    "dsc_esfera": "MUNICIPAL",
                    "filename": f"memory_orgao_municipio_{latest_yr}.json",
                    "ano_referencia": int(latest_yr),
                }
            )
        return orgao_rows, mun_rows

    def map_servidores(
        self,
        raw_servidores: Iterable[Dict[str, Any]],
        default_year: int = 2025,
        default_month: int = 10,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
        seen: set[Tuple[Any, ...]] = set()
        rem_rows: List[Dict[str, Any]] = []
        pessoas: Dict[Tuple[int, int], Dict[str, Any]] = {}
        dups = 0
        cod_mun = int(self.ibge_code)

        for rec in raw_servidores:
            if not isinstance(rec, dict):
                continue
            nome = str(rec.get("nome_servidor") or rec.get("nom_servidor") or "").strip()
            if not nome:
                continue
            situacao_atual = normalize_ascii_upper(rec.get("situacao_atual") or "ATIVO")
            if "DEMITIDO" in situacao_atual or "EXONERADO" in situacao_atual or "FALECIDO" in situacao_atual:
                # Ignorar vínculos históricos encerrados com provento zerado
                if parse_ptbr_money(rec.get("proventos")) <= 0:
                    continue

            ano_ref = int(rec.get("ano_referencia") or rec.get("exercicio") or default_year)
            mes_ref = int(rec.get("mes_referencia") or default_month)
            seq_dat_pag = int(f"{ano_ref}{mes_ref:02d}")
            matricula = str(rec.get("numero_matricula") or rec.get("num_matricula") or "").strip()
            cod_orgao_str = str(rec.get("cod_orgao") or ("01" if int(rec.get("cod_grupoentidade") or 2) == 2 else "02")).zfill(2)
            cod_orgao_int = int(digits_only(cod_orgao_str) or "2")

            vlr_bruto = parse_ptbr_money(rec.get("proventos") or rec.get("vlr_rmn_bruta"))
            if vlr_bruto <= 0:
                continue
            vlr_desc = parse_ptbr_money(rec.get("descontos") or 0.0)
            vlr_liq = parse_ptbr_money(rec.get("liquido") or max(0.0, round(vlr_bruto - vlr_desc, 2)))

            dkey = (cod_orgao_int, normalize_ascii_upper(matricula), normalize_ascii_upper(nome), seq_dat_pag, vlr_bruto)
            if dkey in seen:
                dups += 1
                continue
            seen.add(dkey)

            cargo = str(rec.get("funcao") or rec.get("cargo") or "NÃO INFORMADO").strip()
            lotacao = str(rec.get("setor_lotacao") or rec.get("departamento_lotacao") or "").strip()
            vinculo = str(rec.get("situacao_admissional") or rec.get("regime_trabalhista") or "EFETIVO").strip()

            c_up = normalize_ascii_upper(cargo)
            v_up = normalize_ascii_upper(vinculo)
            is_adj = any(x in c_up for x in ("ADJUNTO", "ASSESSOR", "CHEFE DE", "DIRETOR", "COORDENADOR", "GERENTE", "SUBSECRETARIO", "AUXILIAR", "TECNICO", "AGENTE "))
            if not is_adj and re.search(r"\bVICE[\s-]*PREFEIT[OA]?\b", c_up):
                sgl_cargo, sit_clean, dsc_agepol = "AP", "Agente Político", "Vice-Prefeito"
            elif not is_adj and re.search(r"\bPREFEIT[OA]\b", c_up):
                sgl_cargo, sit_clean, dsc_agepol = "AP", "Agente Político", "Prefeito"
            elif not is_adj and re.search(r"\bVEREADOR[A]?\b", c_up):
                sgl_cargo, sit_clean, dsc_agepol = "AP", "Agente Político", "Vereador"
            elif not is_adj and (re.search(r"\bSECRETARI[OA]\s+MUNICIPAL\b", c_up) or c_up in ("SECRETARIO", "SECRETARIA")):
                sgl_cargo, sit_clean, dsc_agepol = "AP", "Agente Político", "Secretário Municipal"
            elif "INATIV" in v_up or "APOSENTAD" in v_up:
                sgl_cargo, sit_clean, dsc_agepol = "INA", "Inativo", ""
            elif "PENSION" in v_up:
                sgl_cargo, sit_clean, dsc_agepol = "PEN", "Pensionista", ""
            elif "COMISS" in v_up or "CONFIANCA" in v_up:
                sgl_cargo, sit_clean, dsc_agepol = "COM", "Cargo Comissionado", ""
            elif "CONTRAT" in v_up or "TEMPOR" in v_up:
                sgl_cargo, sit_clean, dsc_agepol = "TEM", "Contratado Temporário", ""
            else:
                sgl_cargo, sit_clean, dsc_agepol = "EFE", "Servidor Efetivo", ""
            seq_dim_orgao = deterministic_seq_id(self.ibge_code, "dim_orgao", cod_orgao_int)
            seq_dim_pessoa = deterministic_seq_id(self.ibge_code, "memory_pessoa", normalize_ascii_upper(nome), matricula or cargo)
            cpf_masked = mask_cpf_if_individual(rec.get("cpf") or "")
            if not cpf_masked:
                # Gerar máscara LGPD determinística não identificável (`***.000.***-**`) quando a API omite CPF
                cpf_masked = "***.000.***-**"

            hrs_raw = digits_only(rec.get("carga_horaria") or "40")
            hrs = float(hrs_raw) if hrs_raw and int(hrs_raw) <= 60 else 40.0

            rem_rows.append(
                {
                    "seq_dim_orgao": seq_dim_orgao,
                    "seq_dim_pessoa": seq_dim_pessoa,
                    "dsc_cargo": cargo,
                    "dsc_lotacao": lotacao,
                    "seq_dat_pagamento": seq_dat_pag,
                    "dsc_regime": str(rec.get("regime_trabalhista") or vinculo),
                    "dsc_req_crg": "",
                    "dsc_cessao": "",
                    "dsc_tipo_pag": "Mensal",
                    "dsc_sgl_cargo": sgl_cargo,
                    "dsc_situacao": sit_clean,
                    "cod_municipio": cod_mun,
                    "cod_orgao": cod_orgao_int,
                    "seq_dat_ingresso_cargo": parse_date_int_yyyymmdd(rec.get("data_admissao")) or 0,
                    "seq_dat_exclusao": 0,
                    "seq_dat_concessao": 0,
                    "dsc_sgl_cargo_1": sgl_cargo,
                    "cod_vinculo": int(rec.get("codigo_situacao") or 1),
                    "num_matricula": matricula,
                    "cod_agepol": "1" if dsc_agepol else "",
                    "dsc_agepol": dsc_agepol or "",
                    "cod_tipdep": 0,
                    "dsc_tipdep": "",
                    "dsc_dependencia": "",
                    "seq_dat_afast_pre": 0,
                    "qtd_hora_semana": hrs,
                    "vlr_rmn_bruta": vlr_bruto,
                    "vlr_rmn_liquida": vlr_liq,
                    "vlr_descontos": vlr_desc,
                    "cod_sit_serv_pens": 1,
                    "dsc_sit_serv_pens": sit_clean,
                    "codcbo": str(rec.get("cod_cbo") or ""),
                    "seq_dat_obito_instituidor": 0,
                    "seq_dat_comissionado": 0,
                    "seq_dat_comissionado_exc": 0,
                    "seq_dim_pessoa_instituidor": 0,
                    "dsc_req_cargo": "",
                    "dsc_tipo_pagext": "",
                    "ind_natureza_saldo": "",
                    "seq_dat_transf_reserva": 0,
                    "ind_dedic_exclusiva": "",
                    "filename": f"memory_servidores_{ano_ref}.json",
                    "ano_referencia": ano_ref,
                }
            )
            sx = normalize_ascii_upper(rec.get("sexo"))
            ind_sexo = "F" if sx.startswith("F") else ("M" if sx.startswith("M") else "")
            pessoas[(seq_dim_pessoa, ano_ref)] = {
                "seq_dim_pessoa": seq_dim_pessoa,
                "tip_documento": 1,
                "num_documento": cpf_masked,
                "nom_pessoa": nome.upper(),
                "ind_sexo": ind_sexo,
                "seq_dat_nascimento": 0,
                "cod_orgao": cod_orgao_int,
                "filename": f"memory_pessoas_{ano_ref}.json",
                "ano_referencia": ano_ref,
            }
        return rem_rows, list(pessoas.values()), dups

    def map_empenhos(self, raw_empenhos: Iterable[Dict[str, Any]], default_year: int = 2025) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
        seen: set[str] = set()
        emp_rows: List[Dict[str, Any]] = []
        cred_rows: List[Dict[str, Any]] = []
        dups = 0
        cod_mun = int(self.ibge_code)

        for rec in raw_empenhos:
            if not isinstance(rec, dict):
                continue
            emp_id = str(rec.get("id_empenho") or f"{rec.get('ano_referencia', default_year)}-{rec.get('numero_empenho')}").strip()
            cod_org_hint = rec.get("cod_orgao") or rec.get("codigo_orgao")
            seq_org, cod_org, _ = self._resolve_orgao(cod_org_hint, rec.get("descricao_orgao") or "")
            ukey = f"{cod_org}::{emp_id}"
            if ukey in seen:
                dups += 1
                continue
            seen.add(ukey)

            dt_emp = parse_date_int_yyyymmdd(rec.get("data_empenho"))
            ano = int(rec.get("ano_referencia") or (str(dt_emp)[:4] if dt_emp else default_year))
            mes = int(str(dt_emp)[4:6]) if dt_emp else 1
            num_emp = int(digits_only(rec.get("numero_empenho") or "0") or "0")
            seq_emp = deterministic_seq_id(self.ibge_code, "memory_empenho", ukey)
            vlr = parse_ptbr_money(rec.get("valor_empenho") or rec.get("valor"))

            emp_rows.append(
                {
                    "seq_empenho": seq_emp,
                    "cod_municipio": cod_mun,
                    "seq_orgao": seq_org,
                    "cod_orgao": cod_org,
                    "seq_unidade": 0,
                    "cod_unidade": str(rec.get("codigo_unidade") or ""),
                    "cod_subunidade": "",
                    "num_anoexercicio": ano,
                    "num_mesexercicio": mes,
                    "num_empenho": num_emp,
                    "dat_empenho": dt_emp or int(f"{ano}0101"),
                    "dsc_modalidade": str(rec.get("modalidade") or ""),
                    "dsc_tipo_empenho": str(rec.get("tipo_empenho") or "ORDINÁRIO"),
                    "dsc_empenho": mask_text_cpfs(str(rec.get("historico") or rec.get("especificacao") or rec.get("descricao_unidade") or "").strip()),
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
                    "dsc_funcao": str(rec.get("descricao_funcao") or "NÃO INFORMADO"),
                    "dsc_subfuncao": str(rec.get("descricao_subfuncao") or ""),
                    "dsc_programa": str(rec.get("descricao_programa") or ""),
                    "dsc_acao": str(rec.get("descricao_projeto_atividade") or ""),
                    "dsc_subacao": "",
                    "dsc_naturezadespesa": str(rec.get("natureza_despesa") or ""),
                    "vlr_empenhado": vlr,
                    "vlr_reforco": parse_ptbr_money(rec.get("valor_reforco") or 0.0),
                    "vlr_anulempenho": parse_ptbr_money(rec.get("valor_anulado") or 0.0),
                    "filename": f"memory_empenho_{ano}.json",
                    "ano_referencia": ano,
                }
            )
            doc_cred = mask_cpf_if_individual(rec.get("fornecedor_cpf_cnpj"))
            nom_cred = str(rec.get("fornecedor_nome") or "CREDOR NÃO INFORMADO").strip()
            cred_rows.append(
                {
                    "seq_emp_credor": deterministic_seq_id(self.ibge_code, "memory_emp_credor", seq_emp, doc_cred or nom_cred),
                    "seq_empenho": seq_emp,
                    "seq_orgao": seq_org,
                    "cod_orgao": cod_org,
                    "seq_unidade": 0,
                    "num_ano_referencia": ano,
                    "num_mes_referencia": mes,
                    "num_doc_credor": doc_cred,
                    "nom_credor": nom_cred,
                    "filename": f"memory_credor_{ano}.json",
                    "ano_referencia": ano,
                }
            )
        return emp_rows, cred_rows, dups

    def map_liquidacoes(self, raw_liq: Iterable[Dict[str, Any]], default_year: int = 2025) -> Tuple[List[Dict[str, Any]], int]:
        seen: set[str] = set()
        rows: List[Dict[str, Any]] = []
        dups = 0
        for rec in raw_liq:
            if not isinstance(rec, dict):
                continue
            cod_org = str(rec.get("cod_orgao") or "02").zfill(2)
            seq_org = deterministic_seq_id(self.ibge_code, "orgao", cod_org)
            emp_id = str(rec.get("id_empenho") or rec.get("numero_empenho") or "0").strip()
            num_liq = int(digits_only(rec.get("numero_liquidacao") or "0") or "0")
            ukey = f"{cod_org}::{emp_id}::{num_liq}"
            if ukey in seen:
                dups += 1
                continue
            seen.add(ukey)
            dt_liq = parse_date_int_yyyymmdd(rec.get("data_liquidacao"))
            ano = int(rec.get("ano_referencia") or (str(dt_liq)[:4] if dt_liq else default_year))
            mes = int(str(dt_liq)[4:6]) if dt_liq else 1
            seq_emp = deterministic_seq_id(self.ibge_code, "memory_empenho", f"{cod_org}::{emp_id}")
            rows.append(
                {
                    "seq_liquidacao": deterministic_seq_id(self.ibge_code, "memory_liq", ukey),
                    "seq_orgao": seq_org,
                    "cod_orgao": cod_org,
                    "seq_unidade": 0,
                    "seq_empenho": seq_emp,
                    "seq_rsp": 0,
                    "num_ano_referencia": ano,
                    "num_mes_referencia": mes,
                    "num_liquidacao": num_liq,
                    "dat_liquidacao": dt_liq or int(f"{ano}0101"),
                    "dsc_tipo_liquidacao": "EXERCÍCIO",
                    "num_doc_liquidante": "",
                    "nom_liquidante": "",
                    "vlr_liquidacao": parse_ptbr_money(rec.get("valor_liquidacao") or rec.get("valor")),
                    "vlr_anu_liquidacao": 0.0,
                    "filename": f"memory_liq_{ano}.json",
                    "ano_referencia": ano,
                }
            )
        return rows, dups

    def map_pagamentos(self, raw_pag: Iterable[Dict[str, Any]], default_year: int = 2025) -> Tuple[List[Dict[str, Any]], int]:
        seen: set[str] = set()
        rows: List[Dict[str, Any]] = []
        dups = 0
        for idx, rec in enumerate(raw_pag):
            if not isinstance(rec, dict):
                continue
            cod_org = str(rec.get("cod_orgao") or "02").zfill(2)
            seq_org = deterministic_seq_id(self.ibge_code, "orgao", cod_org)
            emp_id = str(rec.get("id_empenho") or rec.get("numero_empenho") or "0").strip()
            dt_pag = parse_date_int_yyyymmdd(rec.get("data_pagamento") or rec.get("datapagamento"))
            ano = int(rec.get("ano_referencia") or (str(dt_pag)[:4] if dt_pag else default_year))
            mes = int(str(dt_pag)[4:6]) if dt_pag else 1
            vlr_pag = parse_ptbr_money(rec.get("valor_pago") or rec.get("valor_pagamento") or rec.get("valor"))
            num_ord = int(digits_only(rec.get("nro_pagamento") or rec.get("numero_pagamento") or str(idx + 1)) or str(idx + 1))
            ukey = f"{cod_org}::{emp_id}::{num_ord}::{dt_pag}::{vlr_pag}"
            if ukey in seen:
                dups += 1
                continue
            seen.add(ukey)
            seq_emp = deterministic_seq_id(self.ibge_code, "memory_empenho", f"{cod_org}::{emp_id}")
            doc_cred = mask_cpf_if_individual(rec.get("cpf_cnpj") or rec.get("fornecedor_cpf_cnpj"))
            nom_cred = str(rec.get("fornecedor_nome") or rec.get("favorecido") or "CREDOR NÃO INFORMADO").strip()
            rows.append(
                {
                    "seq_pagamento": deterministic_seq_id(self.ibge_code, "memory_pag", ukey),
                    "seq_orgao": seq_org,
                    "cod_orgao": cod_org,
                    "seq_unidade": 0,
                    "seq_empenho": seq_emp,
                    "seq_rsp": 0,
                    "seq_liquidacao": 0,
                    "seq_orgao_empenho": seq_org,
                    "seq_unid_empenho": 0,
                    "num_ano_referencia": ano,
                    "num_mes_referencia": mes,
                    "num_doc_resp": "",
                    "nom_resp": "",
                    "num_doc_credor": doc_cred,
                    "nom_credor": nom_cred,
                    "num_empenho": int(digits_only(rec.get("numero_empenho") or "0") or "0"),
                    "dat_empenho": dt_pag or int(f"{ano}0101"),
                    "num_liquidacao": int(digits_only(rec.get("nro_liquidacao") or "0") or "0"),
                    "dat_liquidacao": dt_pag or int(f"{ano}0101"),
                    "num_ord_pagamento": num_ord,
                    "dat_pagamento": dt_pag or int(f"{ano}0101"),
                    "dsc_pagamento": mask_text_cpfs(str(rec.get("des_despesa") or rec.get("historico") or "").strip()),
                    "dsc_tipo_pagamento": "ORÇAMENTÁRIO",
                    "dsc_fonte_recurso": str(rec.get("fonte_recurso") or ""),
                    "dsc_cod_orcamentario": str(rec.get("cod_despesa") or ""),
                    "vlr_pag_fonte": vlr_pag,
                    "vlr_ret_fonte": parse_ptbr_money(rec.get("valor_retido") or 0.0),
                    "vlr_ant_fonte": 0.0,
                    "vlr_anu_fonte": 0.0,
                    "filename": f"memory_pag_{ano}.json",
                    "ano_referencia": ano,
                }
            )
        return rows, dups

    def map_contratos(self, raw_contratos: Iterable[Dict[str, Any]], default_year: int = 2025) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
        seen: set[str] = set()
        c_rows: List[Dict[str, Any]] = []
        cc_rows: List[Dict[str, Any]] = []
        dups = 0
        cod_mun = int(self.ibge_code)

        for rec in raw_contratos:
            if not isinstance(rec, dict):
                continue
            cid = str(rec.get("id_contrato") or rec.get("numero_contrato") or "").strip()
            cod_org = str(rec.get("cod_orgao") or "02").zfill(2)
            ukey = f"{cod_org}::{cid}"
            if not cid or ukey in seen:
                dups += 1
                continue
            seen.add(ukey)

            nom_forn = str(rec.get("fornecedor_nome") or rec.get("dsc_nome_credor") or "").strip()
            doc_forn = mask_cpf_if_individual(rec.get("fornecedor_cpf_cnpj") or rec.get("num_doc_credor"))
            if not nom_forn:
                nom_forn = f"CONTRATADO ({doc_forn})" if doc_forn else "FORNECEDOR IDENTIFICADO NO PORTAL"

            dt_ini = parse_date_int_yyyymmdd(rec.get("inicio_vigencia") or rec.get("data_assinatura"))
            dt_fim = parse_date_int_yyyymmdd(rec.get("fim_vigencia") or rec.get("termino_vigencia"))
            num_str = str(rec.get("numero_contrato") or "0")
            m_yr = re.search(r"/(20\d{2})\b", num_str)
            ano = int(rec.get("ano_referencia") or (m_yr.group(1) if m_yr else (str(dt_ini)[:4] if dt_ini else default_year)))
            mes = int(str(dt_ini)[4:6]) if dt_ini else 1
            num_c = int(digits_only(num_str.split("/")[0]) or "0")
            seq_org = deterministic_seq_id(self.ibge_code, "orgao", cod_org)
            seq_c = deterministic_seq_id(self.ibge_code, "memory_contrato", ukey)

            c_rows.append(
                {
                    "seq_contrato": seq_c,
                    "seq_orgao": seq_org,
                    "cod_municipio": cod_mun,
                    "cod_orgao": cod_org,
                    "seq_unidade": 0,
                    "cod_unidade": "",
                    "cod_subunidade": "",
                    "seq_licitacao": 0,
                    "seq_dispensa": 0,
                    "num_anoexercicio": ano,
                    "num_mesexercicio": mes,
                    "dsc_decorrentelicitacao": "SIM",
                    "dsc_tipo_processo": str(rec.get("modalidade") or "Pregão / Processo Licitatório"),
                    "dsc_naturezaobjeto": "COMPRAS E SERVIÇOS",
                    "dsc_tipo_cadastro": "INICIAL",
                    "num_contrato": num_c,
                    "dat_assinatura": dt_ini or int(f"{ano}0101"),
                    "num_ano_contrato": ano,
                    "dsc_objetocontrato": str(rec.get("objeto") or rec.get("descricao_objeto") or f"Contrato {num_str} - {nom_forn}").strip(),
                    "num_doc_signatario": "",
                    "dsc_nome_signatario": "",
                    "tipo_instrumento": str(rec.get("tipo_ato") or "Contrato"),
                    "dat_iniciovigencia": dt_ini or int(f"{ano}0101"),
                    "dat_fimvigencia": dt_fim or int(f"{ano}1231"),
                    "dsc_fornecimento": "",
                    "dsc_formapagamento": "",
                    "dsc_unid_med_prazo": "",
                    "dsc_prazoexecucao": "",
                    "dsc_multarecisoria": "",
                    "dsc_multa_inadimplencia": "",
                    "dsc_garantia": "",
                    "dat_publicacao": dt_ini or int(f"{ano}0101"),
                    "dsc_veiculopublicacao": "Portal da Transparência Memory / DiOE",
                    "vlr_contrato": parse_ptbr_money(rec.get("valor_contrato") or rec.get("valor")),
                    "vlr_empenhado": 0.0,
                    "vlr_liquidado": 0.0,
                    "vlr_pago": 0.0,
                    "vlr_rspprocessado": 0.0,
                    "vlr_rspnprocessado": 0.0,
                    "filename": f"memory_contrato_{ano}.json",
                    "ano_referencia": ano,
                }
            )
            cc_rows.append(
                {
                    "seq_contratado": deterministic_seq_id(self.ibge_code, "memory_contratado", seq_c, doc_forn or nom_forn),
                    "seq_contrato": seq_c,
                    "seq_orgao": seq_org,
                    "cod_orgao": cod_org,
                    "num_ano_referencia": ano,
                    "num_mes_referencia": mes,
                    "num_doc_representante": "",
                    "dsc_nome_representante": "",
                    "num_doc_credor": doc_forn,
                    "dsc_nome_credor": nom_forn,
                    "filename": f"memory_contratado_{ano}.json",
                    "ano_referencia": ano,
                }
            )
        return c_rows, cc_rows, dups

    def map_licitacoes(self, raw_lic: Iterable[Dict[str, Any]], default_year: int = 2025) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        lic_rows: List[Dict[str, Any]] = []
        hab_rows: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for rec in raw_lic:
            if not isinstance(rec, dict):
                continue
            lid = str(rec.get("id_processo") or rec.get("numero_licitacao") or "").strip()
            cod_org = str(rec.get("cod_orgao") or "02").zfill(2)
            ukey = f"{cod_org}::{lid}"
            if not lid or ukey in seen:
                continue
            seen.add(ukey)
            dt_ab = parse_date_int_yyyymmdd(rec.get("abertura") or rec.get("data_licitacao"))
            num_str = str(rec.get("numero_licitacao") or "0")
            m_yr = re.search(r"/(20\d{2})\b", num_str)
            ano = int(rec.get("ano_referencia") or (m_yr.group(1) if m_yr else (str(dt_ab)[:4] if dt_ab else default_year)))
            mes = int(str(dt_ab)[4:6]) if dt_ab else 1
            seq_org = deterministic_seq_id(self.ibge_code, "orgao", cod_org)
            seq_lic = deterministic_seq_id(self.ibge_code, "memory_lic", ukey)
            vlr_hom = parse_ptbr_money(rec.get("valor_homologado") or 0.0)
            lic_rows.append(
                {
                    "seq_licitacao": seq_lic,
                    "seq_orgao": seq_org,
                    "cod_orgao": cod_org,
                    "seq_unidade": 0,
                    "num_ano_referencia": ano,
                    "num_mes_referencia": mes,
                    "num_processo": num_str.split("/")[0],
                    "num_ano_processo": ano,
                    "dsc_tipo_cadastro": "INICIAL",
                    "dsc_modalidade": str(rec.get("modalidade") or "Pregão"),
                    "num_modalidade": int(digits_only(num_str.split("/")[0]) or "0"),
                    "dsc_nat_processo": str(rec.get("situacao") or "Homologado").strip(),
                    "dat_abert_proc_adm": dt_ab or int(f"{ano}0101"),
                    "dat_conv_edital": dt_ab or int(f"{ano}0101"),
                    "dat_pub_edital": dt_ab or int(f"{ano}0101"),
                    "dsc_veic_pub_001": "Portal Memory / DiOE",
                    "dat_veic_pub_001": dt_ab or int(f"{ano}0101"),
                    "dsc_veic_pub_002": "",
                    "dat_veic_pub_002": 0,
                    "dat_receb_prev_doc": dt_ab or int(f"{ano}0101"),
                    "num_exercicio_edital": ano,
                    "dsc_tipo_licitacao": "MENOR PREÇO",
                    "dsc_nat_objeto": str(rec.get("natureza") or "Compras e Serviços"),
                    "dsc_objeto_licitacao": str(rec.get("objeto") or rec.get("descricao_motivo") or f"Processo Licitatório {num_str}").strip(),
                    "dsc_regime_exec_obra": "",
                    "num_convidados": 0,
                    "dsc_clausula_prorrog": "",
                    "dsc_unid_medida": "",
                    "num_prazo_execucao": 0,
                    "dsc_forma_pagamento": "",
                    "dsc_criterio_aceit": "",
                    "dsc_criterio_adjud": "",
                    "dsc_processo_lote": "",
                    "dsc_ind_pref_micro": "",
                    "dsc_ind_exclusiva_micro": "",
                    "dsc_ind_sub_contrato_micro": "",
                    "dsc_ind_reserv_micro": "",
                    "vlr_empenhado": vlr_hom,
                    "vlr_liquidado": vlr_hom,
                    "vlr_pago": vlr_hom,
                    "vlr_rsp_processado": 0.0,
                    "vlr_rsp_nao_processado": 0.0,
                    "filename": f"memory_lic_{ano}.json",
                    "ano_referencia": ano,
                }
            )
            if rec.get("fornecedor_cpf_cnpj") or rec.get("fornecedor_nome"):
                doc_h = mask_cpf_if_individual(rec.get("fornecedor_cpf_cnpj"))
                nom_h = str(rec.get("fornecedor_nome") or "LICITANTE HABILITADO").strip()
                hab_rows.append(
                    {
                        "seq_hab_licitacao": deterministic_seq_id(self.ibge_code, "memory_hab", seq_lic, doc_h or nom_h),
                        "seq_licitacao": seq_lic,
                        "seq_orgao": seq_org,
                        "cod_orgao": cod_org,
                        "num_ano_referencia": ano,
                        "num_mes_referencia": mes,
                        "num_documento": doc_h,
                        "nom_pessoa": nom_h,
                        "dsc_objeto_social": "",
                        "dsc_tipo_orgao_resp": "",
                        "dat_registro": 0,
                        "num_registro": "",
                        "dat_registro_cvm": 0,
                        "num_registro_cvm": "",
                        "num_inscr_estadual": "",
                        "dsc_estado_inscr": "MG",
                        "num_certidao_inss": "",
                        "dat_emi_cert_inss": 0,
                        "dat_val_cert_inss": 0,
                        "num_certidao_fgts": "",
                        "dat_emi_cert_fgts": 0,
                        "dat_val_cert_fgts": 0,
                        "num_cndt": "",
                        "dat_emi_cndt": 0,
                        "dat_val_cndt": 0,
                        "dat_habilitacao": dt_ab or int(f"{ano}0101"),
                        "dsc_ind_presenca_licit": "SIM",
                        "dsc_ind_renuncia": "NÃO",
                        "filename": f"memory_hab_{ano}.json",
                        "ano_referencia": ano,
                    }
                )
        return lic_rows, hab_rows

    def map_siconfi_rreo_receitas(self, rreo_a01_by_year: Dict[int, Sequence[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Converte demonstrativos oficiais SICONFI `RREO-Anexo 01` em `receita_receita` sem dupla contagem sintética."""
        rows: List[Dict[str, Any]] = []
        seq_org, cod_org, _ = self._resolve_orgao("02")
        LEAF_ACCOUNTS = {
            "IMPOSTOS, TAXAS E CONTRIBUICOES DE MELHORIA": ("11100000", "Receitas Correntes", "Impostos, Taxas e Contribuições de Melhoria"),
            "CONTRIBUICOES": ("12000000", "Receitas Correntes", "Contribuições"),
            "RECEITA PATRIMONIAL": ("13000000", "Receitas Correntes", "Receita Patrimonial"),
            "RECEITA DE SERVICOS": ("16000000", "Receitas Correntes", "Receita de Serviços"),
            "TRANSFERENCIAS CORRENTES": ("17000000", "Receitas Correntes", "Transferências Correntes"),
            "OUTRAS RECEITAS CORRENTES": ("19000000", "Receitas Correntes", "Outras Receitas Correntes"),
            "OPERACOES DE CREDITO": ("21000000", "Receitas de Capital", "Operações de Crédito"),
            "ALIENACAO DE BENS": ("22000000", "Receitas de Capital", "Alienação de Bens"),
            "TRANSFERENCIAS DE CAPITAL": ("24000000", "Receitas de Capital", "Transferências de Capital"),
            "OUTRAS RECEITAS DE CAPITAL": ("29000000", "Receitas de Capital", "Outras Receitas de Capital"),
        }
        for yr, items in sorted(rreo_a01_by_year.items()):
            buckets: Dict[str, Dict[str, float]] = {}
            for it in items:
                if not isinstance(it, dict):
                    continue
                cn = normalize_ascii_upper(it.get("conta") or "")
                # Ignorar linhas intraorçamentárias ou sintéticas
                if cn not in LEAF_ACCOUNTS:
                    continue
                col = normalize_ascii_upper(it.get("coluna") or "")
                val = parse_ptbr_money(it.get("valor"))
                b = buckets.setdefault(cn, {"ini": 0.0, "atu": 0.0, "real": 0.0})
                if "PREVISAO INICIAL" in col:
                    b["ini"] = max(b["ini"], val)
                elif "PREVISAO ATUALIZADA" in col:
                    b["atu"] = max(b["atu"], val)
                elif "ATE O BIMESTRE" in col and "PERCENTUAL" not in col and "%" not in col:
                    b["real"] = max(b["real"], val)
            for cn, vals in buckets.items():
                if vals["ini"] <= 0 and vals["atu"] <= 0 and vals["real"] <= 0:
                    continue
                cod_orc, cat, orig = LEAF_ACCOUNTS[cn]
                eff_mes = max(1, date.today().month - 1) if int(yr) >= date.today().year and date.today().month < 12 else 12
                rows.append(
                    {
                        "seq_orgao": seq_org,
                        "cod_orgao": cod_org,
                        "num_anoexercicio": int(yr),
                        "num_mesexercicio": eff_mes,
                        "dsc_cateconomica": cat,
                        "dsc_origem": orig,
                        "dsc_especie": cn.title(),
                        "dsc_rubrica": cn.title(),
                        "dsc_alinea": cn.title(),
                        "dsc_subalinea": "",
                        "dsc_tipo": "RECEITA ORÇAMENTÁRIA",
                        "dsc_fonterecurso": "CONSOLIDADO SICONFI/STN RREO-ANEXO 01",
                        "dsc_codorcamentario": cod_orc,
                        "dsc_tipodeducao": "",
                        "vlr_previsaoinicial": vals["ini"] or vals["atu"],
                        "vlr_previsaoatualizada": vals["atu"] or vals["ini"],
                        "vlr_realizadoateperiodo": vals["real"],
                        "filename": f"siconfi_rreo_a01_{yr}.json",
                        "ano_referencia": int(yr),
                    }
                )
        return rows

    def transform_raw_payload(self, raw_payload: Any, **kwargs: Any) -> ConnectorRunResult:
        payload = raw_payload if isinstance(raw_payload, dict) else {}
        default_year = int(kwargs.get("ano_referencia") or payload.get("ano_referencia") or 2025)
        tables: Dict[str, List[Dict[str, Any]]] = {}
        total_dups = 0
        years_seen: set[int] = {default_year}

        if payload.get("servidores"):
            rem_r, pess_r, d1 = self.map_servidores(payload["servidores"], default_year=default_year)
            if rem_r:
                tables["remuneracao_municipio"] = rem_r
                tables["pessoa_municipio"] = pess_r
                years_seen.update(int(r["ano_referencia"]) for r in rem_r)
            total_dups += d1

        if payload.get("empenhos"):
            emp_r, cred_r, d2 = self.map_empenhos(payload["empenhos"], default_year=default_year)
            if emp_r:
                tables["empenho_empenho"] = emp_r
                tables["empenho_credorEmpenho"] = cred_r
                years_seen.update(int(r["ano_referencia"]) for r in emp_r)
            total_dups += d2

        if payload.get("liquidacoes"):
            liq_r, d3 = self.map_liquidacoes(payload["liquidacoes"], default_year=default_year)
            if liq_r:
                tables["despesa_liquidacao"] = liq_r
            total_dups += d3

        if payload.get("pagamentos"):
            pag_r, d4 = self.map_pagamentos(payload["pagamentos"], default_year=default_year)
            if pag_r:
                tables["despesa_pagamento"] = pag_r
            total_dups += d4

        c_all: List[Dict[str, Any]] = []
        cc_all: List[Dict[str, Any]] = []
        if payload.get("contratos"):
            cr, ccr, d5 = self.map_contratos(payload["contratos"], default_year=default_year)
            c_all.extend(cr)
            cc_all.extend(ccr)
            total_dups += d5
        if payload.get("pncp_contratos"):
            c360 = Cidade360Connector(tenant_id=self.tenant_id, workspace_root=self.workspace_root, tenant_profile=self.tenant_profile)
            cr2, ccr2, d6 = c360.map_pncp_contratos(payload["pncp_contratos"])
            c_all.extend(cr2)
            cc_all.extend(ccr2)
            total_dups += d6
        if c_all:
            tables["contrato_contratos"] = c_all
            tables["contrato_contContrato"] = cc_all
            years_seen.update(int(r["ano_referencia"]) for r in c_all)

        if payload.get("licitacoes"):
            lic_r, hab_r = self.map_licitacoes(payload["licitacoes"], default_year=default_year)
            if lic_r:
                tables["licitacao_licitacao"] = lic_r
            if hab_r:
                tables["licitacao_habLicitacao"] = hab_r

        if payload.get("siconfi_rreo_a02_by_year"):
            betha_helper = BethaCloudConnector(tenant_id=self.tenant_id, workspace_root=self.workspace_root, tenant_profile=self.tenant_profile)
            desp_all: List[Dict[str, Any]] = []
            for yr, items in sorted(payload["siconfi_rreo_a02_by_year"].items()):
                dr, _ = betha_helper.reconcile_functions_with_siconfi([], items, int(yr))
                desp_all.extend(dr)
                years_seen.add(int(yr))
            if desp_all:
                tables["despesa_despesa"] = desp_all

        if payload.get("siconfi_rreo_a01_by_year"):
            rec_r = self.map_siconfi_rreo_receitas(payload["siconfi_rreo_a01_by_year"])
            if rec_r:
                tables["receita_receita"] = rec_r
                years_seen.update(int(r["ano_referencia"]) for r in rec_r)

        org_r, org_mun_r = self.build_orgaos_tables(sorted(years_seen))
        tables["orgao_orgao"] = org_r
        tables["orgao_municipio"] = org_mun_r

        gaz_acts: List[Dict[str, Any]] = []
        today_iso = date.today().isoformat()
        for act in payload.get("gazette_acts") or []:
            if not isinstance(act, dict):
                continue
            dt_pub = str(act.get("data_publicacao") or today_iso)[:10]
            if dt_pub > today_iso:
                dt_pub = today_iso
            pc = str(act.get("page_content") or "").strip()
            if pc.startswith("[Município:"):
                pc = re.sub(r"^\[Município:[^\]]+\]\s*", "", pc).strip()
            pc = mask_text_cpfs(pc)
            if pc:
                gaz_acts.append({**act, "data_publicacao": dt_pub, "page_content": pc})

        return ConnectorRunResult(
            connector_id=self.profile.connector_id,
            tenant_id=self.tenant_id,
            ibge_code=self.ibge_code,
            status="sucesso",
            tables_normalized=tables,
            gazette_acts=gaz_acts,
            deduplicated_count=total_dups,
            audit_metrics={"years_covered": sorted(years_seen)},
        )
