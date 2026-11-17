"""
Conector Padrão `CkanPortalConnector` — Família M5 (`CKAN v3 Open Data + DOM API — Grande Porte`).

Atende capitais e polos regionais de Minas Gerais que operam portais próprios CKAN v3
(`/api/3/action/package_show`, ex.: Belo Horizonte `dados.pbh.gov.br` IBGE `3106200` e
Ouro Preto `dados.ouropreto.mg.gov.br` IBGE `3146107`) combinados com APIs abertas de Diário Oficial.

Assimetrias reais tratadas:
1. Unpivot contábil de matrizes mensais de receita (`arrecadacao-mensal-do-municipio-receitas`):
   filtra estritamente `escrituracao == 'S'` (contas escrituráveis analíticas) para impedir
   quintuplicação por contas sintéticas (`1.0.0.0.00.0.0 RECEITAS CORRENTES`, `escrituracao == 'N'`).
2. Unpivot de tabelas matriciais de frota (`frota-de-veiculos-por-setor-e-tipo`): transforma colunas
   de categorias de veículos (`CAMINHAO`, `MICRO ONIBUS`, `PICAPE`, `UTILITARIO`, `VAN`) em registros
   individuais compatíveis com `vw_gold_frota_consolidada`.
3. Demonstrativo de Execução Orçamentária (`UO 101 CM - CAMARA MUNICIPAL` vs. Unidades do Executivo):
   separa Poder Legislativo (`cod_orgao='01'`) e Poder Executivo (`cod_orgao='02'`) preservando
   `Funcao descricao`, `Subfuncao descricao`, `Programa descricao` e `Acao descricao`.
4. Classificação estrita de Agentes Políticos titulares em metrópoles (`Prefeito`, `Vice-Prefeito`,
   `Secretário Municipal` titular e `Vereador`, sem `Secretário Adjunto` nem assessores).
"""

from __future__ import annotations

import csv
import io
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
from sim_platform.ingestion.connectors.portalfacil_el import PortalFacilElConnector
from sim_platform.security.sql_guard import mask_text_cpfs


_CKAN_MONTH_MAP: Tuple[Tuple[str, int], ...] = (
    ("JANEIRO", 1),
    ("FEVEREIRO", 2),
    ("MARCO", 3),
    ("MARO", 3),
    ("ABRIL", 4),
    ("MAIO", 5),
    ("JUNHO", 6),
    ("JULHO", 7),
    ("AGOSTO", 8),
    ("SETEMBRO", 9),
    ("OUTUBRO", 10),
    ("NOVEMBRO", 11),
    ("DEZEMBRO", 12),
)


class CkanPortalConnector(BaseMunicipalConnector):
    """Conector oficial para portais CKAN v3 (`dados.pbh.gov.br`, `dados.ouropreto.mg.gov.br`, `dados.mg.gov.br`)."""

    profile = ConnectorProfile(
        connector_id="ckan_portal",
        vendor_family="CKAN v3 Open Data + Portal Próprio / DOM REST",
        sync_mode="AUTO_REST_CKAN",
        requires_human_captcha=False,
        default_window_id="portais_abertos_pncp_diarios",
        target_silver_tables=(
            "orgao_orgao",
            "orgao_municipio",
            "pessoa_municipio",
            "remuneracao_municipio",
            "despesa_despesa",
            "receita_receita",
            "empenho_empenho",
            "empenho_credorEmpenho",
            "despesa_liquidacao",
            "despesa_pagamento",
            "frota_frota",
            "contrato_contratos",
            "contrato_contContrato",
            "licitacao_licitacao",
            "licitacao_habLicitacao",
        ),
        description=(
            "Ingestão automatizada via API CKAN v3 (`/api/3/action/package_show`) com unpivot de matrizes "
            "mensais de receita (`escrituracao='S'`) e frota, conciliada com PNCP, SICONFI e DOM Eletrônico."
        ),
    )

    @staticmethod
    def parse_semicolon_csv_text(csv_text: str) -> List[Dict[str, str]]:
        cleaned = (csv_text or "").lstrip("\ufeff")
        first_line = cleaned.splitlines()[0] if cleaned.splitlines() else ""
        delim = "\t" if ("\t" in first_line and ";" not in first_line) else ";"
        reader = csv.DictReader(io.StringIO(cleaned), delimiter=delim)
        rows: List[Dict[str, str]] = []
        for r in reader:
            norm_r = {str(k or "").strip().strip('"'): str(v or "").strip().strip('"') for k, v in r.items() if k is not None}
            if any(norm_r.values()):
                rows.append(norm_r)
        return rows

    def unpivot_ckan_monthly_receitas_csv(self, csv_text: str, ano_referencia: int) -> Tuple[List[Dict[str, Any]], int]:
        """
        Despivota CSVs mensais de arrecadação CKAN (`receita;nome_receita;escrituracao;Janeiro..Dezembro`),
        descartando rigorosamente linhas sintéticas (`escrituracao != 'S'`).
        """
        raw_rows = self.parse_semicolon_csv_text(csv_text)
        out: List[Dict[str, Any]] = []
        synthetic_skipped = 0
        seq_org = deterministic_seq_id(self.ibge_code, "orgao", "02")

        for r in raw_rows:
            escr = normalize_ascii_upper(r.get("escrituracao") or "S")
            if escr and escr != "S":
                synthetic_skipped += 1
                continue
            cod_rec = str(r.get("receita") or r.get("cod_receita") or "").strip()
            nom_rec = str(r.get("nome_receita") or r.get("dsc_rubrica") or "").strip()
            if not cod_rec and not nom_rec:
                continue
            cat, orig, esp = derive_pcasp_revenue_hierarchy(cod_rec, nom_rec)
            is_ded = cod_rec.startswith("9") or "DEDUCAO" in normalize_ascii_upper(nom_rec)

            # Mapear colunas dos meses presentes
            norm_col_map = {normalize_ascii_upper(k): v for k, v in r.items()}
            for m_name, m_num in _CKAN_MONTH_MAP:
                raw_val = norm_col_map.get(m_name, "")
                if not raw_val or raw_val in ("-", "- ", "0", "0,00"):
                    continue
                val = parse_ptbr_money(raw_val)
                if val == 0.0:
                    continue
                out.append(
                    {
                        "seq_orgao": seq_org,
                        "cod_orgao": "02",
                        "num_anoexercicio": int(ano_referencia),
                        "num_mesexercicio": int(m_num),
                        "dsc_cateconomica": cat,
                        "dsc_origem": orig,
                        "dsc_especie": esp,
                        "dsc_rubrica": nom_rec,
                        "dsc_alinea": nom_rec,
                        "dsc_subalinea": "",
                        "dsc_tipo": "DEDUÇÃO" if (is_ded or val < 0) else "RECEITA ORÇAMENTÁRIA",
                        "dsc_fonterecurso": "TESOURO / VINCULADOS CKAN",
                        "dsc_codorcamentario": cod_rec,
                        "dsc_tipodeducao": "DEDUÇÃO DE RECEITA" if (is_ded or val < 0) else "",
                        "vlr_previsaoinicial": 0.0,
                        "vlr_previsaoatualizada": 0.0,
                        "vlr_realizadoateperiodo": val,
                        "filename": f"ckan_receita_mensal_{ano_referencia}.csv",
                        "ano_referencia": int(ano_referencia),
                    }
                )
        return out, synthetic_skipped

    def unpivot_ckan_fleet_matrix_csv(self, csv_text: str, ano_referencia: int = 2026) -> List[Dict[str, Any]]:
        """Despivota matrizes `Setor x Tipo de Veículo` do CKAN em registros individuais de `frota_frota`."""
        raw_rows = self.parse_semicolon_csv_text(csv_text)
        vehicles: List[Dict[str, Any]] = []
        cod_mun = int(self.ibge_code)
        seq_org = deterministic_seq_id(self.ibge_code, "orgao", "02")
        serial = 0

        non_count_col_keywords = (
            "SETOR",
            "ORGAO",
            "UNIDADE",
            "SECRETARIA",
            "TOTAL",
            "ANO",
            "MES",
            "DATA",
            "COMPETENCIA",
            "COD",
            "ID_",
            "ID ",
            "VALOR",
            "CUSTO",
            "OBS",
        )
        max_vehicles_per_cell = 5000
        max_fleet_total = 25000

        for r in raw_rows:
            sector = ""
            counts_by_type: List[Tuple[str, int]] = []
            for k, v in r.items():
                ku = normalize_ascii_upper(k)
                if "SETOR" in ku or "ORGAO" in ku:
                    sector = str(v or "").strip()
                elif ku and not any(kw in ku for kw in non_count_col_keywords):
                    v_clean = str(v or "").strip()
                    if re.fullmatch(r"\d{1,4}", v_clean):
                        qty = min(int(v_clean), max_vehicles_per_cell)
                        if qty > 0:
                            counts_by_type.append((ku.replace("_", " ").strip(), qty))
            if not sector or "TOTAL" in normalize_ascii_upper(sector):
                continue
            for vtype, qty in counts_by_type:
                for idx in range(1, qty + 1):
                    if serial >= max_fleet_total:
                        return vehicles
                    serial += 1
                    seq_v = deterministic_seq_id(self.ibge_code, "ckan_veiculo", sector, vtype, idx)
                    # Placa sintética institucional auditável de frota oficial quando o CKAN omite placa por segurança pública
                    plate_tag = f"OFICIAL-{serial:04d}"
                    vehicles.append(
                        {
                            "seq_veiculo": seq_v,
                            "seq_orgao": seq_org,
                            "cod_orgao": "02",
                            "seq_unidade": 0,
                            "cod_unidade": "",
                            "cod_subunidade": "",
                            "num_anoexercicio": int(ano_referencia),
                            "num_mesexercicio": 9,
                            "cod_municipio": cod_mun,
                            "cod_veiculo": str(serial),
                            "dsc_tipo_veiculo": vtype.split()[0],
                            "dsc_sub_tipo_veiculo": vtype,
                            "dsc_veiculo": f"{vtype} ({sector})",
                            "dsc_marca": "FROTA OFICIAL",
                            "dsc_modelo": vtype,
                            "num_anofabricacao": 2022,
                            "dsc_num_placa": plate_tag,
                            "dsc_numchassi": "",
                            "num_renavam": 0,
                            "dsc_numserie": "",
                            "dsc_situacaoveiculo": "ATIVO",
                            "dsc_tipdeslocamento": sector,
                            "num_docproprietario": "",
                            "nom_proprietario": f"PREFEITURA MUNICIPAL DE {self.municipality_name.upper()}",
                            "filename": f"ckan_frota_{ano_referencia}.csv",
                            "ano_referencia": int(ano_referencia),
                        }
                    )
        return vehicles

    def map_ckan_funcionalismo_csv(
        self,
        csv_text: str,
        ano_referencia: int = 2026,
        mes_referencia: int = 9,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
        raw_rows = self.parse_semicolon_csv_text(csv_text)
        pf_helper = PortalFacilElConnector(tenant_id=self.tenant_id, workspace_root=self.workspace_root, tenant_profile=self.tenant_profile)
        seen: set[Tuple[Any, ...]] = set()
        rem_rows: List[Dict[str, Any]] = []
        pessoas: Dict[Tuple[int, int], Dict[str, Any]] = {}
        dups = 0
        cod_mun = int(self.ibge_code)
        seq_dat_pag = int(f"{ano_referencia}{mes_referencia:02d}")

        for idx, r in enumerate(raw_rows):
            norm_r = {normalize_ascii_upper(k): v for k, v in r.items()}
            nome = norm_r.get("NOME") or norm_r.get("SERVIDOR") or ""
            if not nome:
                continue
            vlr_bruto = parse_ptbr_money(
                norm_r.get("REMUNERACAO TOTAL")
                or norm_r.get("PROVENTOS")
                or norm_r.get("SALARIO")
                or norm_r.get("VALOR BRUTO")
            )
            if vlr_bruto <= 0:
                continue
            vlr_desc = parse_ptbr_money(norm_r.get("DESCONTOS") or 0.0)
            vlr_liq = parse_ptbr_money(norm_r.get("LIQUIDO") or max(0.0, round(vlr_bruto - vlr_desc, 2)))

            orgao_txt = norm_r.get("ORGAO") or norm_r.get("ENTIDADE/ORGAO") or ""
            lotacao = norm_r.get("UNIDADE DE LOTACAO") or orgao_txt
            cargo = norm_r.get("CARGO") or "SERVIDOR PÚBLICO"
            vinculo = norm_r.get("VINCULO") or "EFETIVO ESTATUTÁRIO"
            cod_org_int = 1 if "CAMARA" in normalize_ascii_upper(orgao_txt) else 2

            dkey = (cod_org_int, normalize_ascii_upper(nome), normalize_ascii_upper(cargo), normalize_ascii_upper(lotacao), vlr_bruto)
            if dkey in seen:
                dups += 1
                continue
            seen.add(dkey)

            sgl_cargo, sit_clean, dsc_agepol = pf_helper._classify_political_agent_strict(cargo, "", vinculo)
            seq_dim_org = deterministic_seq_id(self.ibge_code, "dim_orgao", cod_org_int)
            seq_dim_pes = deterministic_seq_id(self.ibge_code, "ckan_pessoa", normalize_ascii_upper(nome), normalize_ascii_upper(cargo), normalize_ascii_upper(lotacao))

            rem_rows.append(
                {
                    "seq_dim_orgao": seq_dim_org,
                    "seq_dim_pessoa": seq_dim_pes,
                    "dsc_cargo": cargo,
                    "dsc_lotacao": lotacao,
                    "seq_dat_pagamento": seq_dat_pag,
                    "dsc_regime": vinculo,
                    "dsc_req_crg": "",
                    "dsc_cessao": "",
                    "dsc_tipo_pag": "Mensal",
                    "dsc_sgl_cargo": sgl_cargo,
                    "dsc_situacao": sit_clean,
                    "cod_municipio": cod_mun,
                    "cod_orgao": cod_org_int,
                    "seq_dat_ingresso_cargo": 0,
                    "seq_dat_exclusao": 0,
                    "seq_dat_concessao": 0,
                    "dsc_sgl_cargo_1": sgl_cargo,
                    "cod_vinculo": 1,
                    "num_matricula": str(norm_r.get("BM") or norm_r.get("MATRICULA") or f"BM{idx + 1}"),
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
                    "filename": f"ckan_funcionalismo_{ano_referencia}.csv",
                    "ano_referencia": int(ano_referencia),
                }
            )
            pessoas[(seq_dim_pes, ano_referencia)] = {
                "seq_dim_pessoa": seq_dim_pes,
                "tip_documento": 1,
                "num_documento": "***.000.***-**",
                "nom_pessoa": nome.upper(),
                "ind_sexo": "",
                "seq_dat_nascimento": 0,
                "cod_orgao": cod_org_int,
                "filename": f"ckan_pessoas_{ano_referencia}.csv",
                "ano_referencia": int(ano_referencia),
            }
        return rem_rows, list(pessoas.values()), dups

    def map_ckan_execucao_orcamentaria_csv(self, csv_text: str) -> List[Dict[str, Any]]:
        """Agrega o Demonstrativo de Execução Orçamentária CKAN por Órgão/Função/Subfunção/Programa/Ação/Ano."""
        raw_rows = self.parse_semicolon_csv_text(csv_text)
        cod_mun = int(self.ibge_code)
        agg: Dict[Tuple[Any, ...], Dict[str, float]] = {}

        for r in raw_rows:
            norm_r = {normalize_ascii_upper(k): v for k, v in r.items()}
            ano = int(digits_only(norm_r.get("ANO") or "0") or "0")
            if ano < 2018:
                continue
            uo_desc = norm_r.get("UO DESCRICAO") or ""
            uo_norm = normalize_ascii_upper(uo_desc)
            fn_cod = digits_only(norm_r.get("FUNCAO") or "0").zfill(2)
            fn_desc = norm_r.get("FUNCAO DESCRICAO") or "ADMINISTRAÇÃO"
            is_camara = (
                "CAMARA" in uo_norm
                or "LEGISLATIV" in uo_norm
                or fn_cod == "01"
                or str(norm_r.get("UNIDADE ORCAMENTARIA UO") or "").strip() in ("101", "01", "1")
            )
            cod_org = "01" if is_camara else "02"
            dsc_fn = f"{fn_cod} - {fn_desc}" if fn_cod != "00" else fn_desc
            sub_fn = norm_r.get("SUBFUNCAO DESCRICAO") or ""
            prog = norm_r.get("PROGRAMA DESCRICAO") or ""
            acao = norm_r.get("ACAO DESCRICAO") or ""
            nat = norm_r.get("NATUREZA DESCRICAO") or ""
            fonte = norm_r.get("FONTE RECURSO DESCRICAO") or "TESOURO MUNICIPAL"

            key = (ano, cod_org, dsc_fn, sub_fn, prog, acao, nat, fonte)
            b = agg.setdefault(key, {"orc": 0.0, "sup": 0.0, "anu": 0.0, "emp": 0.0, "liq": 0.0, "pag": 0.0})
            b["orc"] += parse_ptbr_money(norm_r.get("VALOR ORCADO"))
            b["sup"] += parse_ptbr_money(norm_r.get("SUPLEMENTACAO")) + parse_ptbr_money(norm_r.get("REALOCACAO ACRESCIMO"))
            b["anu"] += parse_ptbr_money(norm_r.get("ANULACAO")) + parse_ptbr_money(norm_r.get("REALOCACAO DECRESCIMO"))
            b["emp"] += parse_ptbr_money(norm_r.get("EMPENHO"))
            b["liq"] += parse_ptbr_money(norm_r.get("LIQUIDACAO"))
            b["pag"] += parse_ptbr_money(norm_r.get("PAGAMENTO"))

        today_dt = date.today()
        out: List[Dict[str, Any]] = []
        for (ano, cod_org, dsc_fn, sub_fn, prog, acao, nat, fonte), v in agg.items():
            seq_org = deterministic_seq_id(self.ibge_code, "orgao", cod_org)
            eff_mes = min(12, max(1, today_dt.month - 1)) if int(ano) >= today_dt.year else 12
            out.append(
                {
                    "seq_orgao": seq_org,
                    "cod_municipio": cod_mun,
                    "cod_orgao": cod_org,
                    "seq_unidade": 0,
                    "cod_unidade": "",
                    "cod_subunidade": "",
                    "num_anoexercicio": int(ano),
                    "num_mesexercicio": eff_mes,
                    "dsc_funcao": dsc_fn,
                    "dsc_subfuncao": sub_fn,
                    "dsc_programa": prog,
                    "dsc_acao": acao,
                    "dsc_subacao": "",
                    "dsc_naturezadespesa": nat,
                    "dsc_fonterecurso": fonte,
                    "dsc_cod_orcamentario": "",
                    "vlr_previsto": round(v["orc"], 2),
                    "vlr_acrescimo": round(v["sup"], 2),
                    "vlr_deducao": round(v["anu"], 2),
                    "vlr_empenhado": round(v["emp"], 2),
                    "vlr_liquidado": round(v["liq"], 2),
                    "vlr_pago": round(v["pag"], 2),
                    "vlr_rspprocessado": 0.0,
                    "vlr_rspnprocessado": 0.0,
                    "filename": f"ckan_execucao_{ano}.csv",
                    "ano_referencia": int(ano),
                }
            )
        return out

    def map_ckan_contratos_csv(self, csv_texts: Iterable[str], default_year: int = 2026) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
        seen: set[str] = set()
        c_rows: List[Dict[str, Any]] = []
        cc_rows: List[Dict[str, Any]] = []
        dups = 0
        cod_mun = int(self.ibge_code)
        seq_org = deterministic_seq_id(self.ibge_code, "orgao", "02")

        for txt in csv_texts:
            for r in self.parse_semicolon_csv_text(txt):
                nr = {normalize_ascii_upper(k): v for k, v in r.items()}
                num_str = nr.get("N DO CONTRATO") or nr.get("NUMERO CONTRATO") or ""
                inst = nr.get("INSTRUMENTO") or "Contrato"
                nom_f = (nr.get("NOME DA CONTRATADA") or nr.get("CONTRATADA") or "").strip()
                doc_f = mask_cpf_if_individual(nr.get("CNPJ") or nr.get("CPF/CNPJ"))
                if not nom_f:
                    nom_f = f"CONTRATADO ({doc_f})" if doc_f else "CONTRATADA IDENTIFICADA CKAN"
                dt_ass = parse_date_int_yyyymmdd(nr.get("DATA ASSINATURA"))
                dt_ini = parse_date_int_yyyymmdd(nr.get("INICIO VIGENCIA")) or dt_ass
                dt_fim = parse_date_int_yyyymmdd(nr.get("TERMINO VIGENCIA"))
                ano = int(str(dt_ass)[:4]) if dt_ass else default_year
                mes = int(str(dt_ass)[4:6]) if dt_ass else 1
                ukey = f"{ano}::{num_str}::{inst}::{doc_f or nom_f}"
                if not num_str or ukey in seen:
                    dups += 1
                    continue
                seen.add(ukey)
                seq_c = deterministic_seq_id(self.ibge_code, "ckan_contrato", ukey)
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
                        "dsc_tipo_processo": nr.get("MODALIDADE LICITACAO") or "Processo Licitatório",
                        "dsc_naturezaobjeto": "COMPRAS E SERVIÇOS",
                        "dsc_tipo_cadastro": "INICIAL",
                        "num_contrato": int(digits_only(num_str.split("/")[0]) or "0"),
                        "dat_assinatura": dt_ass or int(f"{ano}0101"),
                        "num_ano_contrato": ano,
                        "dsc_objetocontrato": (nr.get("OBJETO") or f"{inst} {num_str}").strip(),
                        "num_doc_signatario": "",
                        "dsc_nome_signatario": "",
                        "tipo_instrumento": inst,
                        "dat_iniciovigencia": dt_ini or int(f"{ano}0101"),
                        "dat_fimvigencia": dt_fim or int(f"{ano}1231"),
                        "dsc_fornecimento": "",
                        "dsc_formapagamento": "",
                        "dsc_unid_med_prazo": "",
                        "dsc_prazoexecucao": "",
                        "dsc_multarecisoria": "",
                        "dsc_multa_inadimplencia": "",
                        "dsc_garantia": "",
                        "dat_publicacao": parse_date_int_yyyymmdd(nr.get("DATA PUBLICACAO DOM EXTRATO")) or dt_ass or int(f"{ano}0101"),
                        "dsc_veiculopublicacao": "Diário Oficial do Município (DOM)",
                        "vlr_contrato": parse_ptbr_money(nr.get("VALOR DO INSTRUMENTO")),
                        "vlr_empenhado": 0.0,
                        "vlr_liquidado": 0.0,
                        "vlr_pago": 0.0,
                        "vlr_rspprocessado": 0.0,
                        "vlr_rspnprocessado": 0.0,
                        "filename": f"ckan_contratos_{ano}.csv",
                        "ano_referencia": ano,
                    }
                )
                cc_rows.append(
                    {
                        "seq_contratado": deterministic_seq_id(self.ibge_code, "ckan_contratado", seq_c, doc_f or nom_f),
                        "seq_contrato": seq_c,
                        "seq_orgao": seq_org,
                        "cod_orgao": "02",
                        "num_ano_referencia": ano,
                        "num_mes_referencia": mes,
                        "num_doc_representante": "",
                        "dsc_nome_representante": "",
                        "num_doc_credor": doc_f,
                        "dsc_nome_credor": nom_f,
                        "filename": f"ckan_contratados_{ano}.csv",
                        "ano_referencia": ano,
                    }
                )
        return c_rows, cc_rows, dups

    def transform_raw_payload(self, raw_payload: Any, **kwargs: Any) -> ConnectorRunResult:
        payload = raw_payload if isinstance(raw_payload, dict) else {}
        default_year = int(kwargs.get("ano_referencia") or payload.get("ano_referencia") or 2026)
        tables: Dict[str, List[Dict[str, Any]]] = {}
        total_dups = 0
        audit: Dict[str, Any] = {}
        years_seen: set[int] = {default_year}

        if payload.get("funcionalismo_csv_by_year"):
            rem_all: List[Dict[str, Any]] = []
            pes_all: List[Dict[str, Any]] = []
            for yr_mes, csv_txt in sorted(payload["funcionalismo_csv_by_year"].items()):
                yr = int(str(yr_mes)[:4])
                mes = int(str(yr_mes)[4:6]) if len(str(yr_mes)) >= 6 else 9
                rr, pr, d1 = self.map_ckan_funcionalismo_csv(csv_txt, ano_referencia=yr, mes_referencia=mes)
                rem_all.extend(rr)
                pes_all.extend(pr)
                total_dups += d1
                years_seen.add(yr)
            if rem_all:
                tables["remuneracao_municipio"] = rem_all
                tables["pessoa_municipio"] = pes_all

        if payload.get("receitas_mensais_csv_by_year"):
            rec_all: List[Dict[str, Any]] = []
            syn_tot = 0
            for yr, csv_txt in sorted(payload["receitas_mensais_csv_by_year"].items()):
                rr, syn = self.unpivot_ckan_monthly_receitas_csv(csv_txt, int(yr))
                rec_all.extend(rr)
                syn_tot += syn
                years_seen.add(int(yr))
            if rec_all:
                tables["receita_receita"] = rec_all
            audit["ckan_synthetic_revenue_skipped"] = syn_tot

        mem_helper = MemoryPortalConnector(tenant_id=self.tenant_id, workspace_root=self.workspace_root, tenant_profile=self.tenant_profile)
        if payload.get("siconfi_rreo_a01_by_year"):
            # Completar anos históricos de receita e previsão LOA com RREO-Anexo 01 nos anos sem CSV CKAN ou para previsão inicial
            sic_rec = mem_helper.map_siconfi_rreo_receitas(payload["siconfi_rreo_a01_by_year"])
            existing_rec_years = {int(r["ano_referencia"]) for r in tables.get("receita_receita", [])}
            extra_rec = [r for r in sic_rec if int(r["ano_referencia"]) not in existing_rec_years]
            if extra_rec:
                tables.setdefault("receita_receita", []).extend(extra_rec)
            # Se o CSV mensal CKAN não traz coluna de previsão LOA (`vlr_previsaoatualizada == 0`), injeta linha de meta LOA no mês 1 com `vlr_realizadoateperiodo = 0`
            for r in sic_rec:
                yr_r = int(r["ano_referencia"])
                if yr_r in existing_rec_years and (r["vlr_previsaoinicial"] > 0 or r["vlr_previsaoatualizada"] > 0):
                    tables["receita_receita"].append(
                        {
                            **r,
                            "num_mesexercicio": 1,
                            "vlr_realizadoateperiodo": 0.0,
                            "dsc_rubrica": f"{r['dsc_rubrica']} (PREVISÃO LOA)",
                        }
                    )

        desp_all: List[Dict[str, Any]] = []
        ckan_desp_years: set[int] = set()
        for csv_txt in payload.get("execucao_orcamentaria_csvs") or []:
            dr = self.map_ckan_execucao_orcamentaria_csv(csv_txt)
            desp_all.extend(dr)
            ckan_desp_years.update(int(x["ano_referencia"]) for x in dr)
        if payload.get("siconfi_rreo_a02_by_year"):
            betha_helper = BethaCloudConnector(tenant_id=self.tenant_id, workspace_root=self.workspace_root, tenant_profile=self.tenant_profile)
            for yr, items in sorted(payload["siconfi_rreo_a02_by_year"].items()):
                if int(yr) not in ckan_desp_years:
                    dr_sic, _ = betha_helper.reconcile_functions_with_siconfi([], items, int(yr))
                    desp_all.extend(dr_sic)
                    years_seen.add(int(yr))
        if desp_all:
            tables["despesa_despesa"] = desp_all
            years_seen.update(int(x["ano_referencia"]) for x in desp_all)

        if payload.get("frota_matrix_csv"):
            fr = self.unpivot_ckan_fleet_matrix_csv(payload["frota_matrix_csv"], ano_referencia=default_year)
            if fr:
                tables["frota_frota"] = fr

        c_all: List[Dict[str, Any]] = []
        cc_all: List[Dict[str, Any]] = []
        if payload.get("contratos_csvs"):
            cr, ccr, dc = self.map_ckan_contratos_csv(payload["contratos_csvs"], default_year=default_year)
            c_all.extend(cr)
            cc_all.extend(ccr)
            total_dups += dc
        if payload.get("pncp_contratos"):
            c360 = Cidade360Connector(tenant_id=self.tenant_id, workspace_root=self.workspace_root, tenant_profile=self.tenant_profile)
            cr2, ccr2, dc2 = c360.map_pncp_contratos(payload["pncp_contratos"])
            c_all.extend(cr2)
            cc_all.extend(ccr2)
            total_dups += dc2

        # Popula empenhos/liquidações/pagamentos apenas quando o CKAN fornece datasets reais de execução individualizada
        # (nunca sintetizar pagamentos/liquidações de exercício a partir do valor global plurianual de contratos PNCP — MCASP art. 62/63 Lei 4.320/64)
        if payload.get("empenhos") or payload.get("pagamentos"):
            pf_emp_helper = PortalFacilElConnector(
                tenant_id=self.tenant_id,
                workspace_root=self.workspace_root,
                tenant_profile=self.tenant_profile,
            )
            emp_r, cred_r, liq_r, pag_r, de = pf_emp_helper.map_empenhos_pagamentos(
                payload.get("empenhos") or [],
                payload.get("liquidacoes") or [],
                payload.get("pagamentos") or [],
            )
            total_dups += de
            if emp_r:
                tables["empenho_empenho"] = emp_r
                tables["empenho_credorEmpenho"] = cred_r
            if liq_r:
                tables["despesa_liquidacao"] = liq_r
            if pag_r:
                tables["despesa_pagamento"] = pag_r

        if c_all:
            tables["contrato_contratos"] = c_all
            tables["contrato_contContrato"] = cc_all
            years_seen.update(int(r["ano_referencia"]) for r in c_all)

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
