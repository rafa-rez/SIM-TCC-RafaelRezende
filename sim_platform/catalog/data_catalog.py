"""
Catálogo Central de Dados Públicos e Selecionador Semântico Dinâmico de Schema (DataCatalog).

Resolve os achados:
- FIND-005: Divergência de tipos (253 colunas) entre schema do prompt e banco DuckDB real
- FIND-009: Overfitting do Prompt B e contradições de regras ad-hoc
- FIND-010: Envio estático das tabelas (~16.5k-30k tokens) em toda consulta Text-to-SQL
- Seções 10 e 11: Catálogo independente de workflow visual + seleção automática, manual, por fonte (`allowed_sources`) e híbrida
- Expansão Multi-Fonte (79 tabelas em 7 famílias SQL + Diário Oficial RAG):
  `SICOM`, `CGU`, `SICONFI`, `IBGE`, `INEP`, `DATASUS`, `RECEITA_QSA`.
"""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import duckdb


def _norm(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", (text or "").lower())
    return "".join(c for c in nfkd if not unicodedata.combining(c))


@dataclass
class ColumnMetadata:
    name: str
    physical_type: str
    semantic_type: str
    needs_numeric_cast: bool = False
    description: str = ""


@dataclass
class TableMetadata:
    table_name: str
    friendly_name: str
    segment: str
    source_family: str  # "SICOM", "CGU", "SICONFI", "IBGE", "INEP", "DATASUS", "RECEITA_QSA"
    origin: str
    description: str
    update_frequency: str
    access_level: str  # "public", "internal", "restricted"
    years_available: List[int] = field(default_factory=list)
    source_files: List[str] = field(default_factory=list)
    join_keys: List[str] = field(default_factory=list)
    related_tables: List[str] = field(default_factory=list)
    business_rules: List[str] = field(default_factory=list)
    columns: List[ColumnMetadata] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "friendly_name": self.friendly_name,
            "segment": self.segment,
            "source_family": self.source_family,
            "origin": self.origin,
            "description": self.description,
            "update_frequency": self.update_frequency,
            "access_level": self.access_level,
            "years_available": self.years_available,
            "source_files_count": len(self.source_files),
            "join_keys": self.join_keys,
            "related_tables": self.related_tables,
            "business_rules": self.business_rules,
            "columns": [
                {
                    "name": c.name,
                    "physical_type": c.physical_type,
                    "semantic_type": c.semantic_type,
                    "needs_numeric_cast": c.needs_numeric_cast,
                }
                for c in self.columns
            ],
        }


@dataclass
class SchemaSelectionResult:
    selected_tables: List[str]
    selected_segments: List[str]
    selection_mode: str  # "automatic", "manual_segment", "manual_tables", "source_filtered", "hybrid"
    total_catalog_tables: int
    selected_count: int
    reduction_pct: float
    dynamic_ddl: str
    domain_rules: List[str]
    transparency_notes: List[str]
    allowed_sources: List[str] = field(default_factory=list)


def infer_source_family(table_name: str, default_family: str = "SICOM") -> str:
    t = table_name.lower()
    if t.startswith("cgu_"):
        return "CGU"
    if t.startswith("siconfi_"):
        return "SICONFI"
    if t.startswith("ibge_"):
        return "IBGE"
    if t.startswith("inep_"):
        return "INEP"
    if t.startswith("datasus_"):
        return "DATASUS"
    if t.startswith("receita_qsa_"):
        return "RECEITA_QSA"
    return default_family


SEGMENT_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "execucao_despesa": {
        "title": "Execução da Despesa, Empenhos, Liquidações e Pagamentos",
        "origin": "SICOM / TCE-MG",
        "source_family": "SICOM",
        "tables": [
            "despesa_despesa",
            "despesa_pagamento",
            "despesa_liquidacao",
            "despesa_liquidacaoFonte",
            "despesa_liquidacaoNfs",
            "despesa_movPagamento",
            "despesa_notasfiscais",
            "despesa_itemNfs",
            "empenho_empenho",
            "empenho_empenhoFonte",
            "empenho_credorEmpenho",
        ],
        "keywords": [
            "despesa", "gasto", "gastou", "pago", "pagou", "pagamento", "pagamentos",
            "empenho", "empenhado", "empenhos", "liquidado", "liquidacao", "credor",
            "fornecedor", "fornecedores", "nota fiscal", "notas fiscais", "diaria",
            "diarias", "merenda", "alimentacao", "obra", "obras", "manutencao",
            "ieq-c", "ieq", "isf-m", "isf", "ies-c", "ies", "orcamento", "secretaria",
            "valor pago", "maiores pagamentos", "favorecido",
        ],
        "rules": [
            "Para pagamentos por credor/órgão/função, use `despesa_pagamento` (p) ou `empenho_empenho` (e) unido por `p.seq_empenho = e.seq_empenho AND p.ano_referencia = e.ano_referencia`.",
            "Colunas `vlr_*` em `VIEW`s não tipadas podem ser VARCHAR: use `CAST(vlr_... AS DECIMAL(18,2))` ou `TRY_CAST(vlr_... AS DECIMAL(18,2))` em somas.",
            "Para filtrar credores reais em rankings, exclua sentinelas: `COALESCE(nom_credor, '') NOT ILIKE '%NAO INFORMADO%' AND COALESCE(num_doc_credor, '') NOT IN ('-1', '0')`.",
        ],
    },
    "orcamento_planejamento": {
        "title": "Receita Orçamentária, Leis, Decretos e Alterações Orçamentárias",
        "origin": "SICOM / TCE-MG",
        "source_family": "SICOM",
        "tables": [
            "receita_receita",
            "despesa_altOrcamentaria",
            "despesa_decretos",
            "despesa_leisDecretos",
        ],
        "keywords": [
            "receita", "arrecadacao", "arrecadado", "previsto", "previsao", "imposto",
            "iptu", "iss", "itbi", "icms", "fpm", "tributo", "decreto", "suplementacao",
            "alteracao orcamentaria", "credito adicional", "loa", "lei orcamentaria",
            "isf-m", "isf", "saude financeira", "receita corrente", "superavit", "deficit",
        ],
        "rules": [
            "Em `receita_receita`, receita realizada acumulada no exercício usa `vlr_realizadoateperiodo`; receita orçada usa `vlr_previsaoatualizada` ou `vlr_orcado`.",
            "Para o Índice de Saúde Financeira Municipal (ISF-M), divida a Receita Corrente (`receita_receita` onde `dsc_cateconomica ILIKE '%Corrente%'`) pela Despesa Corrente Liquidada (`despesa_despesa` onde `dsc_naturezadespesa LIKE '3%'`).",
        ],
    },
    "restos_a_pagar": {
        "title": "Restos a Pagar (RAP) e Movimentações de Exercícios Anteriores",
        "origin": "SICOM / TCE-MG",
        "source_family": "SICOM",
        "tables": [
            "empenho_rsp",
            "empenho_movimentacaoRsp",
            "empenho_movFonteRsp",
            "empenho_credorRsp",
            "empenho_credorMovResp",
        ],
        "keywords": [
            "restos a pagar", "resto a pagar", "rap", "rsp", "cancelamento de restos",
            "inscritos em restos", "exercicios anteriores", "movimentacao rsp",
        ],
        "rules": [
            "Para Restos a Pagar, utilize `empenho_rsp` e `empenho_movimentacaoRsp` sempre unindo por `seq_rsp` e `ano_referencia`.",
        ],
    },
    "contratos_aditivos": {
        "title": "Contratos Administrativos, Termos Aditivos, Apostilamentos e Rescisões",
        "origin": "SICOM / TCE-MG",
        "source_family": "SICOM",
        "tables": [
            "contrato_contratos",
            "contrato_contContrato",
            "contrato_itemContrato",
            "contrato_termoContrato",
            "contrato_itemTrmContrato",
            "contrato_apostContrato",
            "contrato_creditoContrato",
            "contrato_resContrato",
        ],
        "keywords": [
            "contrato", "contratos", "contratado", "contratada", "vigente", "vigencia",
            "aditivo", "termo aditivo", "aditivos", "apostilamento", "rescisao",
            "objeto do contrato", "valor contratado", "fiscal de contrato",
            "empresa", "empresas", "prestador", "prestadores", "presta servico",
            "prestacao de servico", "trabalhando com o municipio", "terceirizada",
        ],
        "rules": [
            "Em `contrato_contratos` (c), os dados de fornecedor/contratado estão em `contrato_contContrato` (cc) unido por `c.seq_contrato = cc.seq_contrato AND c.ano_referencia = cc.ano_referencia`.",
            "Para obter o nome do contratado caso não conste na ponte, una `contrato_contContrato.num_doc_contratado = pessoa_municipio.num_documento AND contrato_contContrato.ano_referencia = pessoa_municipio.ano_referencia`.",
            "Termos aditivos estão em `contrato_termoContrato` unido por `seq_contrato` e `ano_referencia`.",
        ],
    },
    "licitacoes_compras": {
        "title": "Processos Licitatórios, Dispensas, Inexigibilidades, Atas e Quadro Societário",
        "origin": "SICOM / TCE-MG",
        "source_family": "SICOM",
        "tables": [
            "licitacao_licitacao",
            "licitacao_itemLicitacao",
            "licitacao_cotacaoLicitacao",
            "licitacao_credLicitacao",
            "licitacao_habLicitacao",
            "licitacao_julgLicitacao",
            "licitacao_homologLicitacao",
            "licitacao_parecLicitacao",
            "licitacao_refLicitacao",
            "licitacao_recLicitacao",
            "licitacao_respLicitacao",
            "licitacao_comissaoLicitacao",
            "licitacao_quadroSocLicitacao",
            "licitacao_dispensa",
            "licitacao_itemDispensa",
            "licitacao_cotDispensa",
            "licitacao_credDispensa",
            "licitacao_fornDispensa",
            "licitacao_recDispensa",
            "licitacao_respDispensa",
            "licitacao_regadesao",
            "licitacao_itemRegadeso",
            "licitacao_cotRegadesao",
            "licitacao_vencRegadesao",
        ],
        "keywords": [
            "licitacao", "licitacoes", "pregao", "concorrencia", "tomada de precos",
            "dispensa", "inexigibilidade", "homologacao", "homologado", "vencedor",
            "licitante", "habilitacao", "habilitado", "julgamento", "ata de registro",
            "adesao", "carona", "comissao de licitacao", "socio", "socios",
            "quadro societario", "modalidade", "edital", "compra direta",
        ],
        "rules": [
            "Processos licitatórios ficam em `licitacao_licitacao` (`seq_licitacao`, `ano_referencia`); vencedores homologados ficam em `licitacao_homologLicitacao`.",
            "Sócios de empresas licitantes ficam em `licitacao_quadroSocLicitacao`; para cadastro consolidado da Receita Federal + QSA dos fornecedores pagos, consulte também `receita_qsa_fornecedores`.",
            "Contratações diretas (dispensa e inexigibilidade) ficam em `licitacao_dispensa` (`seq_dispensa`, `ano_referencia`).",
        ],
    },
    "folha_rh": {
        "title": "Folha de Pagamento, Remuneração de Servidores e Despesa com Pessoal",
        "origin": "SICOM / TCE-MG",
        "source_family": "SICOM",
        "tables": [
            "remuneracao_municipio",
            "desp_pessoal_despPessoal",
            "pessoa_municipio",
        ],
        "keywords": [
            "servidor", "servidores", "salario", "salarios", "remuneracao", "folha",
            "contracheque", "vencimento", "bruto", "liquido", "desconto", "cargo",
            "lotacao", "professor", "medico", "prefeito", "secretario", "funcionalismo",
            "pessoal", "despesa com pessoal", "cpf", "pessoa", "cadastro",
        ],
        "rules": [
            "LACUNA DE TRANSPARÊNCIA 2020-2022: `remuneracao_municipio` possui dados apenas para 2023, 2024 e 2025. Nunca retorne R$ 0,00 para 2020-2022 sem explicitar a lacuna de publicação do TCE-MG.",
            "Em `remuneracao_municipio`, `vlr_rmn_bruta` é o salário bruto e `vlr_rmn_liquida` é o líquido. Para nome e documento do servidor, una com `pessoa_municipio` por `seq_dim_pessoa`.",
        ],
    },
    "frota_veiculos": {
        "title": "Frota Municipal, Veículos, Abastecimento e Manutenção",
        "origin": "SICOM / TCE-MG",
        "source_family": "SICOM",
        "tables": [
            "frota_frota",
            "frota_gastoFrota",
            "frota_transFrota",
        ],
        "keywords": [
            "frota", "veiculo", "veiculos", "placa", "combustivel", "gasolina",
            "diesel", "etanol", "abastecimento", "manutencao de veiculo", "carro",
            "caminhao", "onibus", "ambulancia", "quilometragem", "pecas",
        ],
        "rules": [
            "Cadastro de veículos está em `frota_frota` (`seq_veiculo`, `placa`, `dsc_veiculo`); despesas de abastecimento e manutenção estão em `frota_gastoFrota` unida por `seq_veiculo` e `ano_referencia`.",
        ],
    },
    "educacao_saude": {
        "title": "Indicadores e Execução Específica de Educação (MDE/FUNDEB/IDEB) e Saúde (ASPS/SIOPS)",
        "origin": "SICOM / TCE-MG + INEP/MEC + DATASUS/SIOPS",
        "source_family": "SICOM",
        "tables": [
            "educacao_educacao",
            "saude_saude",
            "inep_ideb_educacao",
            "datasus_siops_saude",
        ],
        "keywords": [
            "educacao", "ensino", "escola", "escolar", "creche", "fundeb", "mde",
            "ideb", "inep", "matricula", "matriculas", "aluno", "alunos", "aprovacao",
            "saude", "hospital", "ubs", "posto de saude", "medicamento", "sus", "santa casa",
            "asps", "siops", "datasus", "cnes", "leito", "leitos", "atencao basica",
            "ies-c", "ies", "ieq-c", "ieq", "25%", "15%", "esforco social", "social",
        ],
        "rules": [
            "As tabelas `educacao_educacao` e `saude_saude` (SICOM) consolidam gastos setoriais; `inep_ideb_educacao` traz o IDEB Anos Iniciais/Finais, escolas, matrículas e gasto por aluno; `datasus_siops_saude` traz o percentual SIOPS em saúde, UBS, CNES e leitos SUS em Caeté.",
        ],
    },
    "estrutura_orgaos_cadastros": {
        "title": "Órgãos, Unidades Orçamentárias, Responsáveis e Cadastro de Pessoas",
        "origin": "SICOM / TCE-MG",
        "source_family": "SICOM",
        "tables": [
            "orgao_municipio",
            "orgao_orgao",
            "orgao_orgaoResp",
            "orgao_orgaoUnidade",
            "orgao_remessaMunicipio",
            "pessoa_municipio",
        ],
        "keywords": [
            "orgao", "orgaos", "unidade orcamentaria", "secretaria", "responsavel",
            "gestor", "ordenador", "prefeitura", "camara", "autarquia", "fundacao",
            "remessa", "cadastro", "pessoa", "cnpj",
        ],
        "rules": [
            "Órgãos e secretarias estão mapeados em `orgao_orgao` e `orgao_orgaoUnidade` (`cod_orgao`, `cod_unidade`, `dsc_orgao`, `dsc_unidade`).",
        ],
    },
    "cgu_transferencias_beneficios": {
        "title": "Transferências Federais, Convênios, Emendas e Benefícios Sociais (Portal da Transparência CGU)",
        "origin": "CGU — Portal da Transparência Federal",
        "source_family": "CGU",
        "tables": [
            "cgu_auxilio_brasil",
            "cgu_auxilio_emergencial",
            "cgu_bolsa_familia_pagamentos",
            "cgu_bpc",
            "cgu_despesas_favorecidos",
            "cgu_novo_bolsa_familia",
            "cgu_transferencias",
            "cgu_convenios",
            "cgu_emendas",
        ],
        "keywords": [
            "cgu", "bolsa familia", "novo bolsa familia", "auxilio brasil",
            "auxilio emergencial", "bpc", "beneficio de prestacao continuada",
            "beneficiario", "beneficiarios", "nis", "governo federal", "uniao",
            "transferencia federal", "transferencias federais", "recursos federais",
            "favorecido federal", "programa social", "assistencia social",
            "convenio", "convenios", "emenda", "emendas", "parlamentar", "emenda pix",
        ],
        "rules": [
            "Nas tabelas `cgu_*`, valores monetários podem estar em formato texto ou numérico: use `TRY_CAST(REPLACE(CAST(coluna AS VARCHAR), ',', '.') AS DECIMAL(18,2))` quando necessário.",
            "Série histórica de benefícios: `cgu_bolsa_familia_pagamentos` (2020-2021), `cgu_auxilio_emergencial` (2020-2021), `cgu_auxilio_brasil` (2021-2022) e `cgu_novo_bolsa_familia` (2023-2025).",
            "Convênios federais com Caeté estão em `cgu_convenios` e emendas parlamentares destinadas ao município estão em `cgu_emendas`.",
        ],
    },
    "indicadores_ibge_siconfi": {
        "title": "Demografia IBGE (População, PIB, IDHM, Per Capita) e Indicadores Fiscais SICONFI/Tesouro (RCL, LRF, CAPAG)",
        "origin": "IBGE + SICONFI / Secretaria do Tesouro Nacional",
        "source_family": "IBGE",
        "tables": [
            "ibge_indicadores_municipais",
            "siconfi_finbra_indicadores",
        ],
        "keywords": [
            "ibge", "populacao", "habitantes", "censo", "per capita", "por habitante",
            "pib", "idhm", "idh", "densidade", "area territorial",
            "siconfi", "finbra", "tesouro nacional", "rcl", "receita corrente liquida",
            "capag", "limite prudencial", "lrf", "responsabilidade fiscal", "divida consolidada",
        ],
        "rules": [
            "Para calcular qualquer indicador *per capita* (gasto por habitante, arrecadação por habitante), faça JOIN com `ibge_indicadores_municipais` por `ano_referencia` e divida por `populacao_residente`.",
            "Para limites da Lei de Responsabilidade Fiscal (LRF), RCL, despesa com pessoal sobre a RCL e nota CAPAG do Tesouro Nacional, utilize `siconfi_finbra_indicadores`.",
        ],
    },
    "auditoria_fornecedores_qsa": {
        "title": "Cadastro Receita Federal CNPJ, Porte, CNAE e Quadro Societário (QSA) de Fornecedores",
        "origin": "Receita Federal do Brasil + SICOM Habilitação",
        "source_family": "RECEITA_QSA",
        "tables": [
            "receita_qsa_fornecedores",
            "licitacao_quadroSocLicitacao",
        ],
        "keywords": [
            "receita federal", "qsa", "quadro societario", "socio", "socios",
            "dono da empresa", "porte da empresa", "cnae", "ramo de atividade",
            "auditoria de fornecedor", "auditoria de fornecedores", "cnpj fornecedor",
            "materialidade", "controladoria",
        ],
        "rules": [
            "A tabela `receita_qsa_fornecedores` consolida os fornecedores PJ da Prefeitura de Caeté com `num_cnpj`, `razao_social`, `porte_empresa`, `cnae_principal_descricao`, `qtd_socios_identificados`, `nomes_socios_qsa`, `total_pago_historico` e `alerta_auditoria_controladoria`.",
        ],
    },
}

# Tabelas-ponte essenciais incluídas quando a fonte SICOM está permitida
CORE_BRIDGE_TABLES: List[str] = [
    "empenho_empenho",
    "despesa_despesa",
    "despesa_pagamento",
    "receita_receita",
    "pessoa_municipio",
    "orgao_orgao",
    "orgao_orgaoUnidade",
]


class DataCatalog:
    """
    Catálogo central de dados e vinculador semântico de schema (Schema Linker)
    com suporte a filtro estrito por Família de Fonte (`allowed_sources`) e por Tabela (`explicit_tables`).
    """

    def __init__(
        self,
        data_dir: Path,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
        tenant_id: str = "caete_mg_3110004",
    ) -> None:
        self.data_dir = Path(data_dir)
        self.conn = conn
        self.tenant_id = tenant_id
        self.tables: Dict[str, TableMetadata] = {}
        self._build_catalog()

    def _build_catalog(self) -> None:
        self.tables.clear()
        csv_years: Dict[str, Set[int]] = {}
        csv_files: Dict[str, List[str]] = {}

        if self.data_dir.exists():
            for p in sorted(self.data_dir.glob("*.csv")):
                name = p.name
                parts = name.split(".")
                if len(parts) >= 5 and parts[0].isdigit():
                    yr = int(parts[0])
                    tname = f"{parts[2]}_{parts[3]}"
                    csv_years.setdefault(tname.lower(), set()).add(yr)
                    csv_files.setdefault(tname.lower(), []).append(name)
                elif len(parts) >= 4 and parts[0].isdigit():
                    yr = int(parts[0])
                    tname = parts[2]
                    csv_years.setdefault(tname.lower(), set()).add(yr)
                    csv_files.setdefault(tname.lower(), []).append(name)
                else:
                    stem = p.stem
                    if "_" in stem and stem.rsplit("_", 1)[-1].isdigit():
                        base, yr_s = stem.rsplit("_", 1)
                        csv_years.setdefault(base.lower(), set()).add(int(yr_s))
                        csv_files.setdefault(base.lower(), []).append(name)

        db_columns: Dict[str, List[ColumnMetadata]] = {}
        canonical_names: Dict[str, str] = {}

        if self.conn is not None:
            try:
                rows = self.conn.execute(
                    "SELECT table_name, column_name, data_type "
                    "FROM information_schema.columns "
                    "WHERE table_schema = 'main' "
                    "ORDER BY table_name, ordinal_position"
                ).fetchall()
                for tname, cname, dtype in rows:
                    t_low = tname.lower()
                    canonical_names[t_low] = tname
                    c_low = cname.lower()
                    is_num_prefix = c_low.startswith(("vlr_", "val_", "valor", "total_", "pct_", "pib_", "ideb_"))
                    is_varchar = "VARCHAR" in dtype.upper()
                    sem_type = dtype
                    if is_num_prefix and is_varchar:
                        sem_type = "VARCHAR (numérico: usar TRY_CAST(... AS DECIMAL(18,2)))"
                    elif c_low.startswith(("dat_", "dt_", "data_")):
                        sem_type = f"{dtype} (data)"
                    db_columns.setdefault(t_low, []).append(
                        ColumnMetadata(
                            name=cname,
                            physical_type=dtype,
                            semantic_type=sem_type,
                            needs_numeric_cast=(is_num_prefix and is_varchar),
                        )
                    )
            except Exception:
                pass

        for seg_id, seg_info in SEGMENT_DEFINITIONS.items():
            for raw_tname in seg_info["tables"]:
                t_low = raw_tname.lower()
                # Se a tabela já foi registrada em outro segmento (ex: pessoa_municipio), mantém a primeira mas atualiza regras
                if t_low in self.tables:
                    continue
                # Para as 7 novas tabelas enriquecidas, registra se existir no DuckDB (ou se conn is None)
                if t_low in (
                    "cgu_convenios",
                    "cgu_emendas",
                    "siconfi_finbra_indicadores",
                    "ibge_indicadores_municipais",
                    "inep_ideb_educacao",
                    "datasus_siops_saude",
                    "receita_qsa_fornecedores",
                ):
                    if self.conn is not None and t_low not in canonical_names:
                        continue

                real_name = canonical_names.get(t_low, raw_tname)
                source_fam = infer_source_family(real_name, seg_info.get("source_family", "SICOM"))
                yrs = sorted(csv_years.get(t_low, {2020, 2021, 2022, 2023, 2024, 2025}))
                if t_low == "remuneracao_municipio":
                    yrs = [2023, 2024, 2025]
                files = csv_files.get(t_low, [f"{real_name}.parquet"])
                cols = db_columns.get(t_low, [])
                join_keys = [
                    c.name
                    for c in cols
                    if c.name.lower().startswith(("seq_", "id_", "cod_", "num_doc", "num_cnpj", "ano_referencia"))
                ]
                self.tables[t_low] = TableMetadata(
                    table_name=real_name,
                    friendly_name=f"[{source_fam}] {seg_info['title']} — {real_name}",
                    segment=seg_id,
                    source_family=source_fam,
                    origin=seg_info["origin"],
                    description=f"Tabela oficial '{real_name}' da fonte '{source_fam}' ({seg_info['title']}).",
                    update_frequency="Mensal / Anual",
                    access_level="public",
                    years_available=yrs,
                    source_files=files,
                    join_keys=join_keys[:8],
                    related_tables=[t for t in seg_info["tables"] if t.lower() != t_low][:6],
                    business_rules=seg_info["rules"],
                    columns=cols,
                )

    def get_allowed_table_names(
        self,
        role: str = "citizen",
        allowed_sources: Optional[List[str]] = None,
        explicit_tables: Optional[List[str]] = None,
    ) -> Set[str]:
        base = set(self.tables.keys())
        if explicit_tables:
            exp_set = {t.lower() for t in explicit_tables if t.lower() in self.tables}
            if exp_set:
                return exp_set
        if allowed_sources:
            norm_sources = {s.upper().strip() for s in allowed_sources if s.upper().strip() != "DIARIO_OFICIAL"}
            if norm_sources:
                filtered = {
                    t_low
                    for t_low, meta in self.tables.items()
                    if meta.source_family.upper() in norm_sources
                }
                if filtered:
                    return filtered
        return base

    def get_table_years_map(self) -> Dict[str, List[int]]:
        return {t_low: meta.years_available for t_low, meta in self.tables.items()}

    def select_tables_for_query(
        self,
        question: str = "",
        explicit_segments: Optional[List[str]] = None,
        explicit_tables: Optional[List[str]] = None,
        allowed_sources: Optional[List[str]] = None,
        include_bridges: bool = True,
    ) -> SchemaSelectionResult:
        """
        Seleciona dinamicamente o subconjunto ótimo de tabelas e regras para uma consulta,
        respeitando estritamente o filtro de fontes (`allowed_sources`) e tabelas (`explicit_tables`)
        escolhido pelo usuário.
        """
        q_norm = _norm(question)
        matched_segments: List[str] = []
        selected_set: Set[str] = set()
        mode = "automatic"

        norm_sources: List[str] = []
        if allowed_sources:
            for s in allowed_sources:
                su = s.upper().strip()
                if su in ("INEP_DATASUS", "EDUCACAO_SAUDE_FEDERAL"):
                    norm_sources.extend(["INEP", "DATASUS"])
                elif su != "DIARIO_OFICIAL":
                    norm_sources.append(su)
            norm_sources = list(dict.fromkeys(norm_sources))

        # 1. Seleção explícita de segmentos
        if explicit_segments:
            mode = "manual_segment"
            for seg in explicit_segments:
                if seg in SEGMENT_DEFINITIONS:
                    matched_segments.append(seg)
                    for t in SEGMENT_DEFINITIONS[seg]["tables"]:
                        if t.lower() in self.tables:
                            selected_set.add(t.lower())

        # 2. Seleção explícita de tabelas
        if explicit_tables:
            mode = "manual_tables" if not explicit_segments else "hybrid"
            for t in explicit_tables:
                t_low = t.lower()
                if t_low in self.tables:
                    selected_set.add(t_low)
                    seg = self.tables[t_low].segment
                    if seg not in matched_segments:
                        matched_segments.append(seg)

        # 3. Seleção semântica automática pela pergunta do usuário
        if question and not explicit_tables:
            if explicit_segments or norm_sources:
                mode = "source_filtered" if norm_sources else "hybrid"
            for seg_id, seg_info in SEGMENT_DEFINITIONS.items():
                for kw in seg_info["keywords"]:
                    kw_norm = _norm(kw)
                    if kw_norm in q_norm:
                        if seg_id not in matched_segments:
                            matched_segments.append(seg_id)
                        for t in seg_info["tables"]:
                            if t.lower() in self.tables:
                                selected_set.add(t.lower())
                        break

            for t_low, meta in self.tables.items():
                short_suffix = t_low.split("_", 1)[-1]
                if t_low in q_norm or (len(short_suffix) >= 5 and short_suffix in q_norm):
                    selected_set.add(t_low)
                    if meta.segment not in matched_segments:
                        matched_segments.append(meta.segment)

        # 4. Se não houve match específico, usar defaults
        if not matched_segments and not selected_set:
            matched_segments = ["execucao_despesa", "orcamento_planejamento", "educacao_saude", "indicadores_ibge_siconfi"]
            for seg in matched_segments:
                if seg in SEGMENT_DEFINITIONS:
                    for t in SEGMENT_DEFINITIONS[seg]["tables"]:
                        if t.lower() in self.tables:
                            selected_set.add(t.lower())

        # 5. Incluir pontes centrais quando permitido
        if include_bridges and not explicit_tables:
            if not norm_sources or "SICOM" in norm_sources:
                for bt in CORE_BRIDGE_TABLES:
                    if bt.lower() in self.tables:
                        selected_set.add(bt.lower())

        if any(w in q_norm for w in ("social", "ies", "saude", "educacao", "ensino", "fundeb", "ideb", "siops")) and not explicit_tables:
            for t in SEGMENT_DEFINITIONS["educacao_saude"]["tables"]:
                if t.lower() in self.tables:
                    selected_set.add(t.lower())
            if "educacao_saude" not in matched_segments:
                matched_segments.append("educacao_saude")

        # 6. APLICAR RESTRIÇÃO ESTRITA DE FONTES (`allowed_sources`) OU TABELAS (`explicit_tables`)
        if explicit_tables:
            exp_low = {t.lower() for t in explicit_tables if t.lower() in self.tables}
            if exp_low:
                selected_set = exp_low
                mode = "manual_tables"
        elif norm_sources:
            mode = "source_filtered"
            filtered_by_source = {
                t_low
                for t_low in selected_set
                if t_low in self.tables and self.tables[t_low].source_family.upper() in norm_sources
            }
            if not filtered_by_source:
                # Se a pergunta não citou palavras-chave exatas daquela fonte, inclui todas as tabelas da(s) fonte(s) selecionada(s) (até 18)
                all_in_sources = [
                    t_low
                    for t_low, meta in self.tables.items()
                    if meta.source_family.upper() in norm_sources
                ]
                filtered_by_source = set(all_in_sources[:18])
            selected_set = filtered_by_source
            matched_segments = list(
                dict.fromkeys(self.tables[t_low].segment for t_low in selected_set if t_low in self.tables)
            )

        ordered_tables = sorted(
            self.tables[t_low].table_name
            for t_low in selected_set
            if t_low in self.tables
        )

        total_tables = len(self.tables)
        sel_count = len(ordered_tables)
        reduction_pct = round(100.0 * (1.0 - (sel_count / max(1, total_tables))), 2)

        ddl = self.build_dynamic_ddl(ordered_tables)
        rules: List[str] = []
        for seg in matched_segments:
            if seg in SEGMENT_DEFINITIONS:
                for r in SEGMENT_DEFINITIONS[seg]["rules"]:
                    if r not in rules:
                        rules.append(r)

        notes: List[str] = []
        if "remuneracao_municipio" in selected_set:
            notes.append(
                "ATENÇÃO: `remuneracao_municipio` contém dados apenas de 2023 a 2025 (lacuna de transparência ativa em 2020–2022)."
            )
        if norm_sources:
            notes.append(
                f"FILTRO DE ESCOPO ATIVO: Consulta restrita pelo usuário exclusivamente às fontes: {', '.join(norm_sources)}."
            )

        return SchemaSelectionResult(
            selected_tables=ordered_tables,
            selected_segments=matched_segments,
            selection_mode=mode,
            total_catalog_tables=total_tables,
            selected_count=sel_count,
            reduction_pct=reduction_pct,
            dynamic_ddl=ddl,
            domain_rules=rules,
            transparency_notes=notes,
            allowed_sources=norm_sources,
        )

    def build_dynamic_ddl(self, table_names: List[str]) -> str:
        """
        Gera representação compacta e fiel aos tipos reais do DuckDB apenas para as tabelas selecionadas.
        """
        lines: List[str] = []
        for tname in table_names:
            meta = self.tables.get(tname.lower())
            if not meta:
                continue
            yrs_str = f"{min(meta.years_available)}-{max(meta.years_available)}" if meta.years_available else "2020-2025"
            col_strs = [f"{c.name} ({c.physical_type})" for c in meta.columns]
            if col_strs:
                lines.append(f"-- [Fonte: {meta.source_family} | Segmento: {meta.segment} | Cobertura: {yrs_str}]")
                lines.append(f"TABLE {meta.table_name}: {', '.join(col_strs)};")
            else:
                lines.append(f"TABLE {meta.table_name} -- [Fonte: {meta.source_family} | Cobertura: {yrs_str}];")
        return "\n".join(lines)
