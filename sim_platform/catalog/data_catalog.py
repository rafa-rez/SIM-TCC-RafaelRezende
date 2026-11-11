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
    sample_values: List[str] = field(default_factory=list)


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
    grain_type: str = "ANNUAL_FLOW"
    counting_rule: str = ""
    is_gold_view: bool = False
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
            "grain_type": self.grain_type,
            "counting_rule": self.counting_rule,
            "is_gold_view": self.is_gold_view,
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
                    "sample_values": c.sample_values,
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
            "vw_gold_execucao_despesa_credor",
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
            "DISTINÇÃO ESSENCIAL FUNÇÃO TEMÁTICA vs. CREDOR: Para gastos por função temática (Saúde `'10 - SAÚDE'`, Educação `'12 - EDUCAÇÃO'`, Assistência Social `'08 - ASSISTÊNCIA SOCIAL'`, Urbanismo `'15 - URBANISMO'`, Gestão Ambiental `'18 - GESTÃO AMBIENTAL'`, etc.), use SEMPRE `despesa_despesa` filtrando `dsc_funcao` (ex.: `WHERE dsc_funcao ILIKE '%SAÚDE%'` ou `dsc_funcao = '10 - SAÚDE'`), pois `vw_gold_execucao_despesa_credor` NÃO possui `dsc_funcao`!",
            "Para pagamentos/empenhos consolidados por credor/fornecedor/órgão, caronas (`total_empenhado_carona > 0`) ou dispensa/inexigibilidade agregada por credor (`total_empenhado_dispensa_inexigibilidade > 0`), use `vw_gold_execucao_despesa_credor`. Já para listar notas de empenho individuais (`num_empenho`, `dat_empenho`, `dsc_empenho`, `ind_dec_licitacao`), use `empenho_empenho` (e) unido a `empenho_credorEmpenho` (ce) por `e.seq_empenho = ce.seq_empenho AND e.ano_referencia = ce.ano_referencia`.",
            "Colunas `vlr_*` em `VIEW`s não tipadas podem ser VARCHAR: use `CAST(vlr_... AS DECIMAL(18,2))` ou `TRY_CAST(vlr_... AS DECIMAL(18,2))` em somas.",
            "Para filtrar credores reais em rankings, exclua sentinelas: `COALESCE(nom_credor, '') NOT ILIKE '%NAO INFORMADO%' AND COALESCE(num_doc_credor, '') NOT IN ('-1', '0', 'SEM_DOC_CREDOR')`.",
        ],
    },
    "orcamento_planejamento": {
        "title": "Orçamento Municipal (LOA Prevista vs. Realizada), Receitas, Leis e Alterações Orçamentárias",
        "origin": "SICOM / TCE-MG",
        "source_family": "SICOM",
        "tables": [
            "vw_gold_orcamento_previsto_vs_realizado",
            "receita_receita",
            "despesa_altOrcamentaria",
            "despesa_decretos",
            "despesa_leisDecretos",
        ],
        "keywords": [
            "orcamento", "orcamentario", "orcamentaria", "orcado", "dotacao", "projetado",
            "receita", "arrecadacao", "arrecadado", "previsto", "previsao", "imposto",
            "iptu", "iss", "itbi", "icms", "fpm", "tributo", "decreto", "suplementacao",
            "alteracao orcamentaria", "credito adicional", "loa", "lei orcamentaria",
            "isf-m", "isf", "saude financeira", "receita corrente", "superavit", "deficit",
        ],
        "rules": [
            "PRIORIDADE MÁXIMA PARA ORÇAMENTO ANUAL: Para consultar o orçamento do município (LOA prevista vs. receita arrecadada e despesa empenhada/liquidada/paga por ano de 2020 a 2025), use `vw_gold_orcamento_previsto_vs_realizado`! NUNCA confunda `receita_arrecadada_realizada` parcial de 2025 com `orcamento_previsto_loa` / `orcamento_atualizado_loa`.",
            "Para anos futuros ainda não publicados no SICOM (ex.: 2026 ou 2027), NUNCA gere `SELECT 'texto'` sem `FROM` nem filtre apenas `WHERE ano_referencia = 2027` (que retornaria 0 linhas): consulte `SELECT * FROM vw_gold_orcamento_previsto_vs_realizado ORDER BY ano_referencia` (2020 a 2025) para apresentar a série histórica oficial aprovada até 2025 como referência.",
            "Em `receita_receita` (detalhe por rubrica/tributo), receita realizada acumulada usa `vlr_realizadoateperiodo`; receita prevista na LOA usa `vlr_previsaoatualizada` ou `vlr_previsaoinicial`.",
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
            "vw_gold_contratos_vigencia_aditivos",
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
            "Em `contrato_contratos` (c), os dados do fornecedor/contratado (`num_doc_credor`, `dsc_nome_credor`) estão em `contrato_contContrato` (cc) unido por `c.seq_contrato = cc.seq_contrato AND c.ano_referencia = cc.ano_referencia`.",
            "Para visão consolidada de contratos, contratados e acréscimos de aditivos sem duplicação, consulte `vw_gold_contratos_vigencia_aditivos`.",
            "Termos aditivos detalhados estão em `contrato_termoContrato` unido por `seq_contrato`.",
        ],
    },
    "licitacoes_compras": {
        "title": "Processos Licitatórios, Dispensas, Inexigibilidades, Atas e Quadro Societário",
        "origin": "SICOM / TCE-MG",
        "source_family": "SICOM",
        "tables": [
            "vw_gold_conflito_interesses_licitacao",
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
            "quadro societario", "modalidade", "edital", "compra direta", "conflito de interesse",
        ],
        "rules": [
            "Processos licitatórios ficam em `licitacao_licitacao` (`seq_licitacao`, `ano_referencia`); vencedores homologados ficam em `licitacao_homologLicitacao`.",
            "Sócios de empresas licitantes ficam em `licitacao_quadroSocLicitacao` (q) unido a `licitacao_habLicitacao` (h) por `q.seq_hab_licitacao = h.seq_hab_licitacao AND q.ano_referencia = h.ano_referencia` (onde `h.num_documento` é o CNPJ da empresa e `q.num_cgc_habilitado` é o CPF/CNPJ do sócio). Para conflito de interesses (sócios que são servidores), consulte `vw_gold_conflito_interesses_licitacao`.",
            "Contratações diretas (dispensa e inexigibilidade) ficam em `licitacao_dispensa` (`seq_dispensa`, `ano_referencia`); adesões a atas (caronas) têm `ind_dec_licitacao ILIKE '%ADESÃO%'` em `empenho_empenho` ou constam em `licitacao_regadesao`.",
        ],
    },
    "folha_rh": {
        "title": "Folha de Pagamento, Remuneração de Servidores e Despesa com Pessoal",
        "origin": "SICOM / TCE-MG",
        "source_family": "SICOM",
        "tables": [
            "vw_gold_folha_servidores",
            "remuneracao_municipio",
            "desp_pessoal_despPessoal",
            "pessoa_municipio",
        ],
        "keywords": [
            "servidor", "servidores", "funcionario", "funcionarios", "salario", "salarios",
            "remuneracao", "folha", "contracheque", "vencimento", "bruto", "liquido",
            "desconto", "cargo", "cargos", "lotacao", "professor", "medico", "prefeito",
            "vice-prefeito", "secretario", "secretaria", "vereador", "funcionalismo",
            "pessoal", "despesa com pessoal", "concursado", "concursados", "efetivo",
            "efetivos", "comissionado", "comissionados", "temporario", "temporarios",
            "estagiario", "vinculo", "sedesma", "semas", "cpf", "pessoa",
        ],
        "rules": [
            "PRIORIDADE MÁXIMA PARA FOLHA E SERVIDORES: Use SEMPRE `vw_gold_folha_servidores` em vez de `remuneracao_municipio` bruta! Ela já traz `nom_servidor`, `num_cpf`, `dsc_cargo`, `dsc_lotacao`, `dsc_situacao` e `tipo_vinculo` desduplicados e higienizados.",
            "GRANULARIDADE MENSAL (`MONTHLY_PAYROLL_SNAPSHOT`): 1 linha em `vw_gold_folha_servidores` = 1 pagamento mensal (`seq_dat_pagamento` formato YYYYMMDD, ex.: 20250801). PROIBIDO usar `COUNT(*)` para contar pessoas! Para contar servidores únicos no ano use `COUNT(DISTINCT num_cpf)` (ou `COUNT(DISTINCT seq_dim_pessoa)`); para fotografia atual do último mês (salário atual do prefeito/secretários ou quadro ativo vigente), filtre `seq_dat_pagamento = (SELECT MAX(seq_dat_pagamento) FROM vw_gold_folha_servidores WHERE ano_referencia = 2025)`.",
            "VÍNCULOS REAIS EM `vw_gold_folha_servidores`: em `tipo_vinculo` os valores são `'Efetivo'`, `'Emprego público'`, `'Servidor temporário'`, `'Comissionado de recrutamento amplo'`, `'Estagiário/Aluno aprendiz/Bolsista'`, `'Agente político'`, `'Agente Honorífico'`. Em `dsc_situacao`, `'Efetivo (Concursado)'` + `'Emprego Público (Concursado / Seleção Pública)'` representam os concursados/seleção pública, `'Contratado / Servidor Temporário'` os contratados temporários, e `'Comissionado (Recrutamento Amplo)'` os comissionados.",
            "LACUNA DE TRANSPARÊNCIA 2020-2022: `remuneracao_municipio` e `vw_gold_folha_servidores` possuem dados apenas para 2023, 2024 e 2025.",
        ],
    },
    "frota_veiculos": {
        "title": "Frota Municipal, Veículos, Abastecimento e Manutenção",
        "origin": "SICOM / TCE-MG",
        "source_family": "SICOM",
        "tables": [
            "vw_gold_frota_consolidada",
            "frota_frota",
            "frota_gastoFrota",
            "frota_transFrota",
        ],
        "keywords": [
            "frota", "veiculo", "veiculos", "placa", "combustivel", "gasolina",
            "diesel", "etanol", "abastecimento", "manutencao de veiculo", "carro",
            "carros", "caminhao", "onibus", "ambulancia", "quilometragem", "pecas", "maquinario",
        ],
        "rules": [
            "PRIORIDADE MÁXIMA PARA FROTA (`CONSOLIDATED_ENTITY_VIEW`): Consulte SEMPRE `vw_gold_frota_consolidada` ISOLADAMENTE! Ela JÁ UNE `frota_frota` + `frota_gastoFrota` (`vlr_gasto`, `dsc_tipo_gasto`): PROIBIDO fazer `JOIN` de `vw_gold_frota_consolidada` com `frota_gastoFrota` ou `frota_frota` (causaria produto cartesiano e multiplicaria `vlr_gasto`!).",
            "Para contar quantos veículos a Prefeitura possui no total, use `COUNT(DISTINCT seq_veiculo)` em `vw_gold_frota_consolidada` SEM filtrar `ano_referencia = 2025`! Filtre `ano_referencia = YYYY` APENAS para somar `SUM(vlr_gasto)` anual. Para filtrar apenas veículos emplacados (excluindo maquinário pesado isento e sem placa), use `status_cadastro_frota = 'CADASTRADO'`.",
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
            "As tabelas `educacao_educacao` e `saude_saude` (SICOM) consolidam gastos setoriais; `inep_ideb_educacao` traz o IDEB Anos Iniciais/Finais (anos ímpares: 2019, 2021, 2023), escolas e matrículas; `datasus_siops_saude` traz o percentual SIOPS em saúde, UBS, CNES e leitos SUS em Caeté.",
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
            "Órgãos estão em `orgao_orgao` (`seq_orgao`, `cod_orgao`, `nom_orgao`) e unidades em `orgao_orgaoUnidade` (`seq_unidade`, `seq_orgao`, `cod_orgao`, `cod_unidade`, `dsc_unidade`). Como se repetem mensalmente/anualmente, sempre desduplique por `seq_orgao` / `seq_unidade` antes de fazer JOIN.",
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
            "Nas tabelas `cgu_*`, colunas com espaços no nome devem estar entre aspas duplas (ex: `\"VALOR TRANSFERIDO\"`), e valores monetários em formato brasileiro com vírgula decimal usam `TRY_CAST(REPLACE(REPLACE(CAST(coluna AS VARCHAR), '.', ''), ',', '.') AS DECIMAL(18,2))`.",
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
            "vw_gold_fornecedores_qsa_360",
            "vw_gold_conflito_interesses_licitacao",
            "receita_qsa_fornecedores",
            "licitacao_quadroSocLicitacao",
            "licitacao_habLicitacao",
        ],
        "keywords": [
            "receita federal", "qsa", "quadro societario", "socio", "socios",
            "dono da empresa", "porte da empresa", "cnae", "ramo de atividade",
            "auditoria de fornecedor", "auditoria de fornecedores", "cnpj fornecedor",
            "materialidade", "controladoria", "conflito", "parente", "servidor socio",
        ],
        "rules": [
            "A tabela `receita_qsa_fornecedores` consolida os fornecedores PJ da Prefeitura de Caeté com `num_cnpj`, `razao_social`, `porte_empresa`, `cnae_principal_descricao`, `qtd_socios_identificados`, `nomes_socios_qsa`, `total_pago_historico` e `alerta_auditoria_controladoria`.",
            "As views `vw_gold_fornecedores_qsa_360` e `vw_gold_conflito_interesses_licitacao` cruzam diretamente pagamentos, sócios habilitados no SICOM (`licitacao_quadroSocLicitacao` + `licitacao_habLicitacao`) e folha de pagamento de servidores (`pessoa_municipio` + `remuneracao_municipio`).",
        ],
    },
}

# Semântica declarativa de granularidade temporal e regra de contagem (Semantic Layer Pattern)
TABLE_GRAIN_SPEC: Dict[str, tuple[str, str]] = {
    "vw_gold_orcamento_previsto_vs_realizado": (
        "ANNUAL_SUMMARY_VIEW",
        "1 linha = 1 exercício anual (2020-2025). USE ESTA GOLD VIEW COMO PRIORIDADE MÁXIMA para orçamento municipal, LOA prevista vs. receita arrecadada e despesa executada! Para anos futuros (2026, 2027), consulte a série 2020-2025 desta view sem filtrar o ano inexistente.",
    ),
    "vw_gold_folha_servidores": (
        "MONTHLY_PAYROLL_SNAPSHOT",
        "1 linha = 1 vínculo/contracheque mensal (`seq_dat_pagamento` formato YYYYMMDD). PROIBIDO `COUNT(*)` para contar servidores! Use `COUNT(DISTINCT num_cpf)` no ano ou filtre o último mês `seq_dat_pagamento = (SELECT MAX(seq_dat_pagamento) FROM vw_gold_folha_servidores WHERE ano_referencia = 2025)`. Consulte `dsc_situacao` / `tipo_vinculo` para separar concursados, temporários e comissionados.",
    ),
    "remuneracao_municipio": (
        "MONTHLY_PAYROLL_SNAPSHOT",
        "1 linha = 1 contracheque mensal bruto (2023-2025). EVITE consultar diretamente: prefira SEMPRE `vw_gold_folha_servidores`, pois na tabela bruta `dsc_sit_serv_pens` e `dsc_situacao` possuem lacunas e siglas não higienizadas.",
    ),
    "vw_gold_frota_consolidada": (
        "CONSOLIDATED_ENTITY_VIEW",
        "View já pré-unificada (`frota_frota` + `frota_gastoFrota` com `vlr_gasto`, `dsc_tipo_gasto`, `status_cadastro_frota`). PROIBIDO fazer JOIN desta view com `frota_gastoFrota`! Para contar a frota geral da Prefeitura (`qual a frota` / `quantos veículos`), use `SELECT status_cadastro_frota, COUNT(DISTINCT seq_veiculo) AS qtd_veiculos FROM vw_gold_frota_consolidada GROUP BY status_cadastro_frota` SEM filtrar `ano_referencia` (mostrando emplacados, maquinário pesado isento e pré-2020 ativos). Filtre `status_cadastro_frota = 'CADASTRADO'` apenas quando a pergunta pedir especificamente veículos emplacados, e filtre `ano_referencia = YYYY` apenas para gastos anuais.",
    ),
    "frota_frota": (
        "INCREMENTAL_CADASTRE_LOG",
        "Tabela bruta de tombamento incremental (`ano_referencia` indica apenas o ano de registro inicial no SICOM). PROIBIDO filtrar `ano_referencia = 2025` para contar a frota total! Prefira SEMPRE `vw_gold_frota_consolidada` com `COUNT(DISTINCT seq_veiculo)`.",
    ),
    "vw_gold_fornecedores_qsa_360": (
        "CONSOLIDATED_ENTITY_VIEW",
        "1 linha = 1 fornecedor único consolidado por `num_doc_credor` (CNPJ/CPF), com `total_pago_historico` (2020-2025) e `nom_socio`. Não possui coluna `ano_referencia` (use `primeiro_ano_pagamento` / `ultimo_ano_pagamento`).",
    ),
    "receita_qsa_fornecedores": (
        "CONSOLIDATED_ENTITY_VIEW",
        "1 linha = 1 empresa fornecedora PJ consolidada por `num_cnpj` com porte, CNAE e QSA.",
    ),
    "vw_gold_execucao_despesa_credor": (
        "ANNUAL_FLOW",
        "1 linha = 1 credor agregado por `(ano_referencia, seq_orgao, num_doc_credor)` com `total_empenhado`, `total_liquidado`, `total_pago`, `total_empenhado_carona` e `total_empenhado_dispensa_inexigibilidade`. ATENÇÃO: NÃO possui `dsc_funcao` nem `ind_dec_licitacao`! Para gastos por função temática (Saúde, Educação, Assistência Social, etc.), use `despesa_despesa` (`dsc_funcao`). Para empenhos individuais (`num_empenho`, `ind_dec_licitacao`), use `empenho_empenho` unido a `empenho_credorEmpenho`.",
    ),
    "vw_gold_contratos_vigencia_aditivos": (
        "ANNUAL_FLOW",
        "1 linha = 1 contrato administrativo com `seq_contrato`, `num_contrato`, `num_ano_contrato`, `seq_orgao`, `cod_orgao`, `nom_orgao`, `nom_contratado`, `num_doc_credor`, `dsc_objetocontrato`, `vlr_original_contrato`, `qtd_aditivos`, `vlr_total_aditivos`, `vlr_global_atualizado` e `pct_acrescimo_aditivos`. Use `COUNT(DISTINCT seq_contrato)` para contagem.",
    ),
    "vw_gold_conflito_interesses_licitacao": (
        "CONSOLIDATED_ENTITY_VIEW",
        "1 linha = 1 vínculo societário entre servidor público municipal e empresa fornecedora/licitante.",
    ),
}

# Colunas categóricas prioritárias para Auto-Profiling de valores reais no startup (CHESS Value Profiling)
PROFILED_CATEGORICAL_COLUMNS: Set[str] = {
    "dsc_situacao",
    "tipo_vinculo",
    "situacao_funcional",
    "cargo_politico",
    "dsc_sgl_cargo",
    "dsc_regime",
    "status_cadastro_frota",
    "dsc_situacaoveiculo",
    "dsc_tipo_veiculo",
    "dsc_tipo_gasto",
    "status_exercicio",
    "dsc_modalidade_licitacao",
    "dsc_modalidade",
    "ind_dec_licitacao",
    "dsc_cateconomica",
    "dsc_origem",
    "dsc_funcao",
    "nom_orgao",
    "tipo_orgao",
    "dsc_tipo_resp",
    "porte_empresa",
    "situacao_fiscal_lrf",
    "nota_capag_estimada",
    "rede",
    "etapa_ensino",
    "dsc_lotacao",
    "dsc_cargo",
}

PROFILED_TABLES_PRIORITY: Set[str] = {
    "vw_gold_orcamento_previsto_vs_realizado",
    "vw_gold_folha_servidores",
    "vw_gold_frota_consolidada",
    "vw_gold_execucao_despesa_credor",
    "vw_gold_contratos_vigencia_aditivos",
    "vw_gold_fornecedores_qsa_360",
    "vw_gold_conflito_interesses_licitacao",
    "remuneracao_municipio",
    "frota_frota",
    "frota_gastofrota",
    "despesa_despesa",
    "receita_receita",
    "empenho_empenho",
    "licitacao_licitacao",
    "licitacao_dispensa",
    "orgao_orgao",
    "orgao_orgaoresp",
    "receita_qsa_fornecedores",
    "siconfi_finbra_indicadores",
    "inep_ideb_educacao",
    "datasus_siops_saude",
}


def _infer_grain_and_rule(table_low: str, source_family: str) -> tuple[str, str]:
    if table_low in TABLE_GRAIN_SPEC:
        return TABLE_GRAIN_SPEC[table_low]
    if source_family in ("IBGE", "SICONFI", "INEP", "DATASUS"):
        return (
            "ANNUAL_INDICATOR_SERIES",
            "1 linha = 1 indicador anual oficial (ou por rede/etapa no IDEB). Filtre ou agrupe por `ano_referencia`.",
        )
    if table_low in ("pessoa_municipio", "orgao_orgao", "orgao_orgaounidade", "orgao_orgaoresp"):
        return (
            "ANNUAL_CADASTRE_SNAPSHOT",
            "Cadastro replicado por exercício/remessa: sempre use `DISTINCT` pela chave natural (`seq_orgao`, `seq_unidade`, `num_documento`) antes de contar ou fazer JOIN.",
        )
    return (
        "ANNUAL_FLOW",
        " Fluxo transacional anualizado: filtre `ano_referencia` ou agrupe com `GROUP BY ano_referencia`. Em 2025 reflete execução parcial acumulada.",
    )
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
        self.min_data_year: int = 2020
        self.max_data_year: int = 2025
        self.latest_remittance_month: int = 8
        self.latest_remittance_human: str = "agosto de 2025 (última remessa consolidada no SICOM/TCE-MG)"
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
                # Para as 7 novas tabelas enriquecidas e Gold Views, registra se existir no DuckDB (ou se conn is None)
                if t_low in (
                    "cgu_convenios",
                    "cgu_emendas",
                    "siconfi_finbra_indicadores",
                    "ibge_indicadores_municipais",
                    "inep_ideb_educacao",
                    "datasus_siops_saude",
                    "receita_qsa_fornecedores",
                ) or t_low.startswith("vw_gold_"):
                    if self.conn is not None and t_low not in canonical_names:
                        continue

                real_name = canonical_names.get(t_low, raw_tname)
                source_fam = infer_source_family(real_name, seg_info.get("source_family", "SICOM"))
                yrs = sorted(csv_years.get(t_low, {2020, 2021, 2022, 2023, 2024, 2025}))
                if t_low in ("remuneracao_municipio", "vw_gold_folha_servidores"):
                    yrs = [2023, 2024, 2025]
                elif t_low == "inep_ideb_educacao":
                    yrs = [2019, 2021, 2023]
                files = csv_files.get(t_low, [f"{real_name}.parquet"])
                cols = db_columns.get(t_low, [])
                join_keys = [
                    c.name
                    for c in cols
                    if c.name.lower().startswith(("seq_", "id_", "cod_", "num_doc", "num_cnpj", "ano_referencia"))
                ]
                grain_type, counting_rule = _infer_grain_and_rule(t_low, source_fam)
                is_gold = t_low.startswith("vw_gold_")
                self.tables[t_low] = TableMetadata(
                    table_name=real_name,
                    friendly_name=f"[{source_fam}] {seg_info['title']} — {real_name}",
                    segment=seg_id,
                    source_family=source_fam,
                    origin=seg_info["origin"],
                    description=f"Tabela oficial '{real_name}' da fonte '{source_fam}' ({seg_info['title']}).",
                    update_frequency="Mensal / Anual",
                    access_level="public",
                    grain_type=grain_type,
                    counting_rule=counting_rule.strip(),
                    is_gold_view=is_gold,
                    years_available=yrs,
                    source_files=files,
                    join_keys=join_keys[:8],
                    related_tables=[t for t in seg_info["tables"] if t.lower() != t_low][:6],
                    business_rules=seg_info["rules"],
                    columns=cols,
                )

        if self.conn is not None:
            self._profile_categorical_columns()
            self._profile_temporal_freshness()

    def _profile_temporal_freshness(self) -> None:
        """
        Inspeciona no startup o teto real de atualização das remessas carregadas no DuckDB
        para que o agente diferencie sempre a data civil de hoje (`date.today()`) da última
        competência efetivamente publicada nos dados abertos.
        """
        if self.conn is None:
            return
        months_pt = {
            1: "janeiro", 2: "fevereiro", 3: "março", 4: "abril",
            5: "maio", 6: "junho", 7: "julho", 8: "agosto",
            9: "setembro", 10: "outubro", 11: "novembro", 12: "dezembro",
        }
        cur = self.conn.cursor()
        sicom_years: Set[int] = set()
        for meta in self.tables.values():
            if meta.source_family == "SICOM":
                sicom_years.update(y for y in meta.years_available if 1990 <= y <= 2100)
        if sicom_years:
            self.min_data_year = min(sicom_years)
            self.max_data_year = max(sicom_years)

        best_month: Optional[int] = None
        if "vw_gold_orcamento_previsto_vs_realizado" in self.tables:
            try:
                row = cur.execute(
                    "SELECT ano_referencia, GREATEST(COALESCE(ultimo_mes_despesa, 0), COALESCE(ultimo_mes_receita, 0)) "
                    "FROM vw_gold_orcamento_previsto_vs_realizado "
                    "WHERE COALESCE(despesa_empenhada, 0) > 0 OR COALESCE(receita_arrecadada_realizada, 0) > 0 "
                    "ORDER BY ano_referencia DESC LIMIT 1"
                ).fetchone()
                if row and row[0]:
                    self.max_data_year = int(row[0])
                    if row[1] and 1 <= int(row[1]) <= 12:
                        best_month = int(row[1])
            except Exception:
                pass

        if "vw_gold_folha_servidores" in self.tables:
            try:
                prow = cur.execute(
                    "SELECT MAX(CAST(seq_dat_pagamento AS VARCHAR)) FROM vw_gold_folha_servidores"
                ).fetchone()
                if prow and prow[0] and len(str(prow[0])) >= 6 and str(prow[0])[:6].isdigit():
                    p_yr = int(str(prow[0])[:4])
                    p_mo = int(str(prow[0])[4:6])
                    if p_yr >= self.max_data_year and 1 <= p_mo <= 12:
                        self.max_data_year = p_yr
                        best_month = max(best_month or 0, p_mo)
            except Exception:
                pass

        if best_month and 1 <= best_month <= 12:
            self.latest_remittance_month = best_month
            m_name = months_pt[best_month]
            suffix = (
                f"exercício {self.max_data_year} em remessa parcial acumulada no SICOM/TCE-MG"
                if best_month < 12
                else f"exercício {self.max_data_year} encerrado (12 meses) no SICOM/TCE-MG"
            )
            self.latest_remittance_human = f"{m_name} de {self.max_data_year} ({suffix})"
        else:
            self.latest_remittance_human = f"exercício {self.max_data_year} (última remessa consolidada no SICOM/TCE-MG)"

    def _profile_categorical_columns(self) -> None:
        """
        Auto-Profiling de valores categóricos reais no startup (inspirado em CHESS, Stanford 2024).
        Inspeciona colunas VARCHAR categóricas de baixa/média cardinalidade nas Gold Views e tabelas-chave
        para injetar valores distintos reais diretamente no DDL dinâmico enviado à LLM.
        """
        if self.conn is None:
            return
        cur = self.conn.cursor()
        for t_low in sorted(PROFILED_TABLES_PRIORITY):
            meta = self.tables.get(t_low)
            if not meta:
                continue
            for col in meta.columns:
                c_low = col.name.lower()
                if c_low not in PROFILED_CATEGORICAL_COLUMNS:
                    continue
                if "VARCHAR" not in col.physical_type.upper():
                    continue
                col_ident = f'"{col.name}"'
                limit_n = 10 if meta.is_gold_view else 7
                try:
                    rows = cur.execute(
                        f"SELECT TRIM(CAST({col_ident} AS VARCHAR)) AS val, COUNT(*) AS cnt "
                        f"FROM {meta.table_name} "
                        f"WHERE {col_ident} IS NOT NULL "
                        f"AND TRIM(CAST({col_ident} AS VARCHAR)) NOT IN ('', '-1', '0', 'NULL') "
                        f"GROUP BY 1 ORDER BY cnt DESC LIMIT {limit_n}"
                    ).fetchall()
                    samples: List[str] = []
                    for val, _cnt in rows:
                        if not val:
                            continue
                        s_val = str(val).strip()
                        if len(s_val) > 64:
                            s_val = s_val[:61] + "..."
                        samples.append(s_val)
                    if samples:
                        col.sample_values = samples
                except Exception:
                    continue

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
        if "remuneracao_municipio" in selected_set or "vw_gold_folha_servidores" in selected_set:
            notes.append(
                "ATENÇÃO: `remuneracao_municipio` e `vw_gold_folha_servidores` contêm dados apenas de 2023 a 2025 (lacuna de transparência ativa em 2020–2022)."
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

    def build_dynamic_ddl(self, table_names: List[str], role: Optional[str] = None) -> str:
        """
        Gera representação compacta, semântica e fiel aos tipos e valores categóricos reais do DuckDB.
        Ordena Gold Views (`vw_gold_*`) primeiro como Camada Semântica Primária recomendada,
        explicitando `grain_type`, `counting_rule` e `sample_values` auto-perfilados.
        """
        gold_tables = [t for t in table_names if t.lower().startswith("vw_gold_")]
        base_tables = [t for t in table_names if not t.lower().startswith("vw_gold_")]
        prioritized = gold_tables + base_tables

        lines: List[str] = []
        if gold_tables:
            lines.append(
                "-- === CAMADA SEMÂNTICA PRIMÁRIA (GOLD VIEWS PRÉ-DESDUPLICADAS — USE PREFERENCIALMENTE!) ==="
            )

        emitted_base_header = False
        for tname in prioritized:
            meta = self.tables.get(tname.lower())
            if not meta:
                continue
            if not meta.is_gold_view and gold_tables and not emitted_base_header:
                lines.append("-- === TABELAS BASE DETALHADAS (USE QUANDO PRECISAR DE GRANULARIDADE EXTRA) ===")
                emitted_base_header = True

            yrs_str = (
                f"{min(meta.years_available)}-{max(meta.years_available)}"
                if meta.years_available
                else "2020-2025"
            )
            priority_tag = " | PRIORIDADE MÁXIMA (GOLD VIEW)" if meta.is_gold_view else ""
            col_strs = []
            for c in meta.columns:
                cname_fmt = f'"{c.name}"' if (" " in c.name or "-" in c.name) else c.name
                if c.sample_values:
                    vals_preview = ", ".join(f"'{v}'" for v in c.sample_values[:6])
                    col_strs.append(f"{cname_fmt} ({c.semantic_type} | valores reais: [{vals_preview}])")
                else:
                    col_strs.append(f"{cname_fmt} ({c.semantic_type})")

            if col_strs:
                lines.append(
                    f"-- [Fonte: {meta.source_family} | Segmento: {meta.segment} | Cobertura: {yrs_str} | Grão: {meta.grain_type}{priority_tag}]"
                )
                if meta.counting_rule:
                    lines.append(f"-- Regra de Grão/Contagem ({meta.table_name}): {meta.counting_rule}")
                lines.append(f"TABLE {meta.table_name}: {', '.join(col_strs)};")
            else:
                lines.append(
                    f"TABLE {meta.table_name} -- [Fonte: {meta.source_family} | Cobertura: {yrs_str} | Grão: {meta.grain_type}];"
                )
        return "\n".join(lines)
