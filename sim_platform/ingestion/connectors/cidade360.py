"""
Conector Padrão `Cidade360Connector` (Portais Cidade360 + Integração PNCP Contratos).

Regras de engenharia de dados blindadas para escala MG:
1. Deduplicação estrita da folha de servidores pela tupla canônica:
   `(Matricula, NomeServidor, Competencia, vlr_bruto, vlr_liquido, vlr_descontos)`,
   eliminando linhas idênticas repetidas nas APIs/CSVs do Cidade360 sem colapsar vínculos distintos
   legítimos (ex.: professor com duas matrículas distintas).
2. Zero contaminação de `Agente Político` com `Lotacao`:
   A classificação de `Agente Político` analisa EXCLUSIVAMENTE `Cargo` e `TipoVinculo`/`Regime`
   (Prefeito, Vice-Prefeito, Secretário Municipal, Vereador), JAMAIS inspecionando `Lotacao`
   (para que motoristas, recepcionistas ou assessores lotados no `"GABINETE DO PREFEITO"` ou
   `"SECRETARIA MUNICIPAL"` nunca sejam falsamente classificados como Agentes Políticos!).
3. Integração PNCP (`Portal Nacional de Contratações Públicas` — Lei 14.133/2021):
   Normaliza contratos abertos do PNCP diretamente para o par de tabelas Silver
   `contrato_contratos` + `contrato_contContrato` exigido por `vw_gold_contratos_vigencia_aditivos`.
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


_RE_AGENTE_POLITICO_CARGO = re.compile(
    r"^(?:"
    r"PREFEIT[OA](?:\s+MUNICIPAL)?|"
    r"VICE[\s-]*PREFEIT[OA](?:\s+MUNICIPAL)?|"
    r"SECRET[AÁ]RI[OA]\s+MUNICIPAL(?:\b.*)|"
    r"SECRET[AÁ]RI[OA]\s+DE\s+GOVERNO(?:\b.*)|"
    r"VEREADOR(?:A)?(?:\b.*)"
    r")$",
    re.IGNORECASE,
)


class Cidade360Connector(BaseMunicipalConnector):
    """Conector padronizado para prefeituras com portal Cidade360 + contratos via PNCP."""

    profile = ConnectorProfile(
        connector_id="cidade360",
        vendor_family="Cidade360 / Transparência Municipal + PNCP",
        sync_mode="AUTO_API",
        requires_human_captcha=False,
        default_window_id="portais_abertos_pncp_diarios",
        target_silver_tables=(
            "remuneracao_municipio",
            "pessoa_municipio",
            "contrato_contratos",
            "contrato_contContrato",
        ),
        description=(
            "Deduplicação de folha (Matricula, NomeServidor, Competencia, Valores) sem contaminação "
            "de Agente Político por Lotação + contratos via API oficial PNCP."
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
    def classify_vinculo_without_lotacao_contamination(
        cargo: Any,
        vinculo: Any,
        situacao: Any = None,
    ) -> Tuple[str, str, Optional[str]]:
        """
        Classifica `(dsc_sgl_cargo, dsc_situacao, dsc_agepol)` avaliando APENAS `cargo`, `vinculo` e `situacao`.
        NUNCA recebe nem inspeciona `Lotacao`!
        """
        cargo_norm = normalize_ascii_upper(cargo)
        vinc_norm = normalize_ascii_upper(vinculo)
        sit_norm = normalize_ascii_upper(situacao)

        is_agente_politico = bool(
            "AGENTE POLITICO" in vinc_norm
            or "ELETIVO" in vinc_norm
            or "SUBSIDIO" in vinc_norm
            or _RE_AGENTE_POLITICO_CARGO.match(cargo_norm)
        )
        if is_agente_politico:
            return "Agente Político", sit_norm or "ATIVO", str(cargo or "AGENTE POLÍTICO").strip()

        if "INATIV" in sit_norm or "APOSENTAD" in sit_norm or "INATIV" in vinc_norm:
            return "Inativo", "Inativo", None
        if "PENSIONISTA" in sit_norm or "PENSIONISTA" in vinc_norm:
            return "Pensionista", "Pensionista", None
        if "EFETIVO" in vinc_norm or "ESTATUTARIO" in vinc_norm or "CONCURSADO" in vinc_norm:
            return "Servidor Efetivo", sit_norm or "ATIVO", None
        if "COMISSIONADO" in vinc_norm or "COMISSAO" in vinc_norm or "RECRUTAMENTO AMPLO" in vinc_norm:
            return "Cargo Comissionado", sit_norm or "ATIVO", None
        if "TEMPORARIO" in vinc_norm or "CONTRATADO" in vinc_norm or "PROCESSO SELETIVO" in vinc_norm:
            return "Servidor Temporário", sit_norm or "ATIVO", None
        if "ESTAGI" in vinc_norm or "ESTAGI" in cargo_norm:
            return "Estagiário", sit_norm or "ATIVO", None
        if "EMPREGO PUBLICO" in vinc_norm or "CELETISTA" in vinc_norm or "CLT" in vinc_norm:
            return "Emprego Público", sit_norm or "ATIVO", None
        return (str(vinculo or "NÃO INFORMADO").strip() or "NÃO INFORMADO"), (sit_norm or "ATIVO"), None

    @staticmethod
    def _parse_competencia_yyyymm(raw_comp: Any, default_year: int = 2025) -> Tuple[int, int]:
        s = str(raw_comp or "").strip()
        m_ym = re.match(r"^(20\d{2})[-/]?([01]\d)$", s)
        if m_ym:
            yr = int(m_ym.group(1))
            mo = int(m_ym.group(2))
            return yr, yr * 100 + mo
        m_my = re.match(r"^([01]?\d)[-/](20\d{2})$", s)
        if m_my:
            yr = int(m_my.group(2))
            mo = int(m_my.group(1))
            return yr, yr * 100 + mo
        d = digits_only(s)
        if len(d) == 6 and d[:4].startswith("20"):
            return int(d[:4]), int(d)
        return default_year, default_year * 100 + 1

    def deduplicate_and_map_folha(
        self,
        raw_folha: Iterable[Dict[str, Any]],
        default_year: int = 2025,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
        """
        Deduplica registros de folha por `(Matricula, NomeServidor, Competencia, vlr_bruto, vlr_liquido, vlr_descontos)`
        e gera `remuneracao_municipio` + `pessoa_municipio` sem contaminar `Agente Político` com `Lotacao`.
        """
        seen_keys: set[Tuple[str, str, int, float, float, float]] = set()
        rem_rows: List[Dict[str, Any]] = []
        pessoas_by_key: Dict[Tuple[int, int], Dict[str, Any]] = {}
        duplicates = 0
        cod_mun_int = int(self.ibge_code)

        for rec in raw_folha:
            if not isinstance(rec, dict):
                continue
            matricula = str(
                rec.get("Matricula") or rec.get("matricula") or rec.get("num_matricula") or ""
            ).strip()
            nome = str(
                rec.get("NomeServidor") or rec.get("nomeServidor") or rec.get("nome") or rec.get("nom_servidor") or ""
            ).strip()
            if not nome:
                continue
            comp_raw = rec.get("Competencia") or rec.get("competencia") or rec.get("mesAno") or rec.get("seq_dat_pagamento")
            ano_ref, seq_dat_pag = self._parse_competencia_yyyymm(comp_raw, default_year=default_year)

            vlr_bruto = parse_ptbr_money(
                rec.get("ValorBruto") or rec.get("valorBruto") or rec.get("remuneracaoBruta") or rec.get("vlr_rmn_bruta")
            )
            vlr_liq = parse_ptbr_money(
                rec.get("ValorLiquido") or rec.get("valorLiquido") or rec.get("remuneracaoLiquida") or rec.get("vlr_rmn_liquida")
            )
            vlr_desc = parse_ptbr_money(
                rec.get("ValorDescontos")
                or rec.get("valorDescontos")
                or rec.get("descontos")
                or rec.get("vlr_descontos")
                or max(0.0, round(vlr_bruto - vlr_liq, 2))
            )

            dedup_key = (
                normalize_ascii_upper(matricula),
                normalize_ascii_upper(nome),
                seq_dat_pag,
                vlr_bruto,
                vlr_liq,
                vlr_desc,
            )
            if dedup_key in seen_keys:
                duplicates += 1
                continue
            seen_keys.add(dedup_key)

            cargo = str(rec.get("Cargo") or rec.get("cargo") or rec.get("dsc_cargo") or "NÃO INFORMADO").strip()
            lotacao = str(rec.get("Lotacao") or rec.get("lotacao") or rec.get("secretaria") or rec.get("dsc_lotacao") or "").strip()
            vinculo = rec.get("TipoVinculo") or rec.get("vinculo") or rec.get("regime") or rec.get("dsc_regime")
            situacao = rec.get("Situacao") or rec.get("situacao") or rec.get("dsc_situacao")

            # Classificação estritamente isolada de `lotacao`!
            sgl_cargo, sit_clean, dsc_agepol = self.classify_vinculo_without_lotacao_contamination(
                cargo=cargo,
                vinculo=vinculo,
                situacao=situacao,
            )

            cpf_digits = digits_only(rec.get("CPF") or rec.get("cpf") or rec.get("num_cpf"))
            cod_orgao_int = int(digits_only(rec.get("cod_orgao") or "2") or "2")
            seq_dim_orgao = deterministic_seq_id(self.ibge_code, "dim_orgao", cod_orgao_int)
            seq_dim_pessoa = deterministic_seq_id(
                self.ibge_code,
                "pessoa",
                cpf_digits if len(cpf_digits) == 11 else normalize_ascii_upper(nome),
            )

            rem_rows.append(
                {
                    "seq_dim_orgao": seq_dim_orgao,
                    "seq_dim_pessoa": seq_dim_pessoa,
                    "dsc_cargo": cargo,
                    "dsc_lotacao": lotacao,
                    "seq_dat_pagamento": seq_dat_pag,
                    "dsc_regime": str(vinculo or sgl_cargo),
                    "dsc_req_crg": "",
                    "dsc_tipo_pag": str(rec.get("TipoFolha") or rec.get("dsc_tipo_pag") or "Mensal"),
                    "dsc_sgl_cargo": sgl_cargo,
                    "dsc_situacao": sit_clean,
                    "cod_municipio": cod_mun_int,
                    "cod_orgao": cod_orgao_int,
                    "num_matricula": matricula,
                    "cod_agepol": "1" if dsc_agepol else "",
                    "dsc_agepol": dsc_agepol or "",
                    "dsc_sit_serv_pens": sit_clean,
                    "vlr_rmn_bruta": vlr_bruto,
                    "vlr_rmn_liquida": vlr_liq,
                    "vlr_descontos": vlr_desc,
                    "ano_referencia": ano_ref,
                }
            )

            pessoas_by_key[(seq_dim_pessoa, ano_ref)] = {
                "seq_dim_pessoa": seq_dim_pessoa,
                "tip_documento": 1,
                "num_documento": cpf_digits,
                "nom_pessoa": nome.upper(),
                "cod_orgao": cod_orgao_int,
                "ano_referencia": ano_ref,
            }

        return rem_rows, list(pessoas_by_key.values()), duplicates

    @staticmethod
    def build_pncp_contratos_url(cnpj_orgao: str, data_inicial_yyyymmdd: str, data_final_yyyymmdd: str, pagina: int = 1) -> str:
        clean_cnpj = digits_only(cnpj_orgao)
        if len(clean_cnpj) != 14:
            raise ValueError(f"CNPJ do órgão inválido para consulta PNCP: '{cnpj_orgao}'.")
        return (
            f"https://pncp.gov.br/api/consulta/v1/contratos"
            f"?dataInicial={digits_only(data_inicial_yyyymmdd)}"
            f"&dataFinal={digits_only(data_final_yyyymmdd)}"
            f"&cnpjOrgao={clean_cnpj}&pagina={int(pagina)}&tamanhoPagina=500"
        )

    def map_pncp_contratos(
        self,
        raw_pncp_items: Iterable[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
        """
        Mapeia contratos da API oficial do PNCP (`pncp.gov.br/api/consulta/v1/contratos`)
        para as duas tabelas Silver `contrato_contratos` + `contrato_contContrato`.
        """
        seen_ctrl: set[str] = set()
        contratos_rows: List[Dict[str, Any]] = []
        contratados_rows: List[Dict[str, Any]] = []
        duplicates = 0
        cod_mun_int = int(self.ibge_code)

        for it in raw_pncp_items:
            if not isinstance(it, dict):
                continue
            pncp_id = str(
                it.get("numeroControlePNCP")
                or it.get("sequencialContrato")
                or f"{it.get('anoContrato')}-{it.get('numeroContratoEmpenho')}"
            ).strip()
            if pncp_id in seen_ctrl:
                duplicates += 1
                continue
            seen_ctrl.add(pncp_id)

            ano_cont = int(it.get("anoContrato") or it.get("ano_referencia") or 2025)
            num_raw = digits_only(it.get("numeroContratoEmpenho") or it.get("sequencialContrato") or "0") or "0"
            num_cont = int(num_raw)
            dt_ass = parse_date_int_yyyymmdd(it.get("dataAssinatura"))
            dt_ini = parse_date_int_yyyymmdd(it.get("dataVigenciaInicio"))
            dt_fim = parse_date_int_yyyymmdd(it.get("dataVigenciaFim"))
            dt_pub = parse_date_int_yyyymmdd(it.get("dataPublicacaoPncp"))
            mes_ex = int(str(dt_ass or dt_pub or f"{ano_cont}0101")[4:6])

            org_ent = it.get("orgaoEntidade") if isinstance(it.get("orgaoEntidade"), dict) else {}
            poder_id = str(org_ent.get("poderId") or "E").upper()
            cod_orgao = "01" if poder_id == "L" else "02"
            seq_orgao = deterministic_seq_id(self.ibge_code, "orgao", cod_orgao)
            seq_contrato = deterministic_seq_id(self.ibge_code, "pncp_contrato", pncp_id)

            vlr_global = parse_ptbr_money(
                it.get("valorGlobal") or it.get("valorInicial") or it.get("valorAcumulado") or it.get("vlr_contrato")
            )
            cat_proc = it.get("categoriaProcesso") if isinstance(it.get("categoriaProcesso"), dict) else {}
            tipo_cont = it.get("tipoContrato") if isinstance(it.get("tipoContrato"), dict) else {}

            contratos_rows.append(
                {
                    "seq_contrato": seq_contrato,
                    "seq_orgao": seq_orgao,
                    "cod_municipio": cod_mun_int,
                    "cod_orgao": cod_orgao,
                    "num_anoexercicio": ano_cont,
                    "num_mesexercicio": mes_ex,
                    "dsc_tipo_processo": str(it.get("modalidadeNome") or cat_proc.get("nome") or "PNCP Lei 14.133/2021"),
                    "dsc_naturezaobjeto": str(cat_proc.get("nome") or it.get("naturezaObjeto") or "COMPRAS E SERVIÇOS"),
                    "num_contrato": num_cont,
                    "dat_assinatura": dt_ass,
                    "num_ano_contrato": ano_cont,
                    "dsc_objetocontrato": str(it.get("objetoContrato") or it.get("dsc_objetocontrato") or "").strip(),
                    "tipo_instrumento": str(tipo_cont.get("nome") or "Contrato"),
                    "dat_iniciovigencia": dt_ini,
                    "dat_fimvigencia": dt_fim,
                    "dat_publicacao": dt_pub,
                    "dsc_veiculopublicacao": "Portal Nacional de Contratações Públicas (PNCP)",
                    "vlr_contrato": vlr_global,
                    "vlr_empenhado": 0.0,
                    "vlr_liquidado": 0.0,
                    "vlr_pago": 0.0,
                    "ano_referencia": ano_cont,
                }
            )

            doc_forn = digits_only(it.get("niFornecedor") or it.get("num_doc_credor"))
            nom_forn = str(
                it.get("nomeRazaoSocialFornecedor") or it.get("dsc_nome_credor") or "FORNECEDOR NÃO INFORMADO"
            ).strip()
            contratados_rows.append(
                {
                    "seq_contratado": deterministic_seq_id(self.ibge_code, "pncp_contratado", seq_contrato, doc_forn or nom_forn),
                    "seq_contrato": seq_contrato,
                    "seq_orgao": seq_orgao,
                    "cod_orgao": cod_orgao,
                    "num_ano_referencia": ano_cont,
                    "num_mes_referencia": mes_ex,
                    "num_doc_representante": "",
                    "dsc_nome_representante": "",
                    "num_doc_credor": doc_forn,
                    "dsc_nome_credor": nom_forn,
                    "ano_referencia": ano_cont,
                }
            )

        return contratos_rows, contratados_rows, duplicates

    def transform_raw_payload(self, raw_payload: Any, **kwargs: Any) -> ConnectorRunResult:
        payload = raw_payload if isinstance(raw_payload, dict) else {}
        default_year = int(kwargs.get("ano_referencia") or payload.get("ano_referencia") or 2025)
        tables: Dict[str, List[Dict[str, Any]]] = {}
        total_dups = 0
        audit: Dict[str, Any] = {}

        if payload.get("folha"):
            rem_rows, pes_rows, d_folha = self.deduplicate_and_map_folha(
                payload["folha"],
                default_year=default_year,
            )
            if rem_rows:
                tables["remuneracao_municipio"] = rem_rows
            if pes_rows:
                tables["pessoa_municipio"] = pes_rows
            total_dups += d_folha
            audit["folha_servidores_unicos"] = len(rem_rows)
            audit["folha_duplicatas_removidas"] = d_folha

        if payload.get("pncp_contratos"):
            c_rows, ct_rows, d_pncp = self.map_pncp_contratos(payload["pncp_contratos"])
            if c_rows:
                tables["contrato_contratos"] = c_rows
            if ct_rows:
                tables["contrato_contContrato"] = ct_rows
            total_dups += d_pncp
            audit["pncp_contratos_mapeados"] = len(c_rows)

        return ConnectorRunResult(
            connector_id=self.profile.connector_id,
            tenant_id=self.tenant_id,
            ibge_code=self.ibge_code,
            status="sucesso",
            tables_normalized=tables,
            deduplicated_count=total_dups,
            audit_metrics=audit,
        )
