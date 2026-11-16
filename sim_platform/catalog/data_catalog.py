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

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date
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
    distinct_count: int = 0


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
    row_count: int = 0
    is_populated: bool = True
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
            "row_count": self.row_count,
            "is_populated": self.is_populated,
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
            "Para pagamentos/empenhos consolidados por credor/fornecedor/órgão, caronas (`total_empenhado_carona > 0`) ou dispensa/inexigibilidade agregada por credor (`total_empenhado_dispensa_inexigibilidade > 0`), use `vw_gold_execucao_despesa_credor`. ATENÇÃO CONTÁBIL (MCASP / Lei 4.320/64): `pago_exercicio` e `liquidado_exercicio` segregam estritamente a execução orçamentária do exercício corrente (reconciliando com `despesa_despesa` e `vw_gold_orcamento_previsto_vs_realizado`), enquanto `total_pago_restos_a_pagar` traz os Restos a Pagar quitados no ano e `total_pago` representa o desembolso total de caixa no ano (`pago_exercicio + total_pago_restos_a_pagar`). Já para listar notas de empenho individuais (`num_empenho`, `dat_empenho`, `dsc_empenho`, `ind_dec_licitacao`), use `empenho_empenho` (e) unido a `empenho_credorEmpenho` (ce) por `e.seq_empenho = ce.seq_empenho AND e.ano_referencia = ce.ano_referencia`.",
            "Colunas `vlr_*` em `VIEW`s não tipadas podem ser VARCHAR: use `CAST(vlr_... AS DECIMAL(18,2))` ou `TRY_CAST(vlr_... AS DECIMAL(18,2))` em somas.",
            "RANKING DE FORNECEDORES / EMPRESAS PRIVADAS vs. REPASSES PÚBLICOS: Em `vw_gold_execucao_despesa_credor` e `vw_gold_fornecedores_qsa_360`, quando a pergunta pedir maiores fornecedores, empresas contratadas ou prestadores privados, filtre SEMPRE `WHERE natureza_credor = 'EMPRESA_PRIVADA'`, o que expurga automaticamente folha sem detalhamento (`FOLHA_OU_SEM_DETALHAMENTO`), repasses intragovernamentais (`INTRAGOVERNAMENTAL_MUNICIPIO`), tributos/consórcios/entes públicos (`ENTE_PUBLICO_TRIBUTO_CONSORCIO`, como INSS/FGTS/RFB/TJ), concessionárias estatais (`ESTATAL_CONCESSIONARIA_BANCO`, como energia/saneamento/bancos) e filantrópicas/Santas Casas (`SEM_FINS_LUCRATIVOS`). Agrupe por `COALESCE(doc_credor_raiz, num_doc_credor)` usando `MAX(nom_credor)` e `MAX(num_doc_credor)` para consolidar filiais do mesmo grupo CNPJ e variações de grafia sem duplicar linhas!",
        ],
    },
    "orcamento_planejamento": {
        "title": "Orçamento Municipal (LOA Prevista vs. Realizada), Receitas, Leis e Alterações Orçamentárias",
        "origin": "SICOM / TCE-MG",
        "source_family": "SICOM",
        "tables": [
            "vw_gold_orcamento_previsto_vs_realizado",
            "vw_gold_orcamento_por_orgao",
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
            "duodecimo", "camara", "legislativo", "autarquia", "saae", "fundacao",
        ],
        "rules": [
            "ORÇAMENTO GERAL DO MUNICÍPIO vs. ORÇAMENTO POR ÓRGÃO / CÂMARA MUNICIPAL: Para o orçamento consolidado do município inteiro (1 linha por ano, incluindo 2026), use `vw_gold_orcamento_previsto_vs_realizado`! NUNCA confunda `receita_arrecadada_realizada` parcial do exercício em curso com `orcamento_previsto_loa` / `orcamento_atualizado_loa`. Já quando a pergunta especificar um ÓRGÃO ou PODER (ex.: Câmara Municipal / Poder Legislativo, Prefeitura / Poder Executivo, SAAE / Autarquia, Fundações), use SEMPRE `vw_gold_orcamento_por_orgao` filtrando `esfera_poder = 'LEGISLATIVO'` (ou `nom_orgao` / `administracao_tipo`)! ATENÇÃO CONSTITUCIONAL (CF/88, art. 29-A): Câmaras Municipais NÃO arrecadam receita tributária própria (`receita_arrecadada_propria_orgao = 0.00`), sendo mantidas por repasse de duodécimo (`regime_financiamento = 'REPASSE_DUODECIMO_CONSTITUCIONAL_CF_ART_29A'`); portanto, o orçamento da Câmara é aferido por `dotacao_inicial_loa`, `dotacao_atualizada` e pela execução `despesa_empenhada`, `despesa_liquidada` e `despesa_paga`!",
            "Para anos futuros ainda não iniciados na base (ex.: 2027 ou 2028), NUNCA gere `SELECT 'texto'` sem `FROM` nem filtre apenas `WHERE ano_referencia = 2027` (que retornaria 0 linhas): consulte `SELECT * FROM vw_gold_orcamento_previsto_vs_realizado ORDER BY ano_referencia` (ou `vw_gold_orcamento_por_orgao` se for por órgão) para apresentar a série histórica oficial aprovada até 2026 como referência.",
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
            "Para visão consolidada de contratos, contratados e acréscimos de aditivos sem duplicação, consulte `vw_gold_contratos_vigencia_aditivos`. Nela, `pct_acrescimo_aditivos` e `vlr_aditivos_acrescimo_objeto` apuram estritamente acréscimos de escopo (Art. 125 da Lei 14.133/2021, sem compensação com `vlr_supressoes_aditivos` e expurgando `vlr_reajuste_reequilibrio` e `vlr_prorrogacoes_vigencia`), além de incluir contratos anteriores a 2020 (`status_cadastro_contrato = 'CONTRATO_ANTERIOR_A_2020'`).",
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
            "Sócios de empresas licitantes ficam em `licitacao_quadroSocLicitacao` (q) unido a `licitacao_habLicitacao` (h) por `q.seq_hab_licitacao = h.seq_hab_licitacao AND q.ano_referencia = h.ano_referencia` (onde `h.num_documento` é o CNPJ da empresa e `q.num_cgc_habilitado` é o CPF/CNPJ do sócio). Para cruzamento entre sócios e folha de servidores, consulte `vw_gold_conflito_interesses_licitacao`, verificando SEMPRE `ha_sobreposicao_temporal` e `classificacao_temporal_vinculo` (não classifique vínculos em períodos distintos ou com lacuna 2020–2022 como ilegalidade consumada).",
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
            "PRIORIDADE MÁXIMA PARA FOLHA E SERVIDORES: Use SEMPRE `vw_gold_folha_servidores` em vez de `remuneracao_municipio` bruta! Ela já traz `nom_servidor`, `num_cpf`, `dsc_cargo`, `dsc_lotacao`, `dsc_situacao`, `tipo_vinculo`, `cod_orgao`, `cod_orgao_norm`, `nom_orgao`, `esfera_poder` (`'EXECUTIVO'` vs `'LEGISLATIVO'`), `administracao_tipo` (`'DIRETA'` vs `'INDIRETA_OU_FUNDO'`) e `dsc_tipo_pag` desduplicados e higienizados.",
            "ÓRGÃOS E AGENTES POLÍTICOS NA FOLHA: Para filtrar servidores da Câmara vs. Prefeitura vs. Autarquias/Fundações, use `esfera_poder`, `administracao_tipo` ou os valores reais de `nom_orgao` exibidos no DDL (NUNCA chute códigos `'01'`/`'02'` às cegas!). Ao listar Secretários Municipais ou Agentes Políticos (`cargo_politico` ou `dsc_cargo ILIKE '%SECRET%'`), inclua SEMPRE `nom_servidor`, `dsc_cargo`, `dsc_lotacao` (secretaria/pasta), `vlr_rmn_bruta` E `vlr_rmn_liquida` filtrando o último mês mensal (`seq_dat_pagamento = (SELECT MAX(seq_dat_pagamento) FROM vw_gold_folha_servidores) AND COALESCE(dsc_tipo_pag, 'Mensal') = 'Mensal'`).",
            "GRANULARIDADE MENSAL (`MONTHLY_PAYROLL_SNAPSHOT`): 1 linha em `vw_gold_folha_servidores` = 1 pagamento mensal (`seq_dat_pagamento` formato YYYYMMDD, ex.: 20250801). PROIBIDO usar `COUNT(*)` ou `COUNT(DISTINCT num_cpf)` para contar pessoas, pois `num_cpf` vem mascarado por LGPD em portais municipais e pode colidir entre homônimos de prefixo/dígitos! Para contar servidores únicos use SEMPRE `COUNT(DISTINCT seq_dim_pessoa)`; para fotografia atual do último mês (salário atual do prefeito/secretários ou quadro ativo vigente), filtre `seq_dat_pagamento = (SELECT MAX(seq_dat_pagamento) FROM vw_gold_folha_servidores) AND COALESCE(dsc_tipo_pag, 'Mensal') = 'Mensal'`. Para médias salariais mensais sem distorção de 13º salário ou folha extra em dezembro, filtre `COALESCE(dsc_tipo_pag, 'Mensal') = 'Mensal'`.",
            "VÍNCULOS REAIS EM `vw_gold_folha_servidores`: em `tipo_vinculo` os valores são `'Efetivo'`, `'Emprego público'`, `'Servidor temporário'`, `'Comissionado de recrutamento amplo'`, `'Estagiário/Aluno aprendiz/Bolsista'`, `'Agente político'`, `'Agente Honorífico'`. Em `dsc_situacao`, `'Efetivo (Concursado)'` + `'Emprego Público (Concursado / Seleção Pública)'` representam os concursados/seleção pública, `'Contratado / Servidor Temporário'` os contratados temporários, e `'Comissionado (Recrutamento Amplo)'` os comissionados.",
            "COBERTURA TEMPORAL DA FOLHA: Verifique os anos efetivamente disponíveis de `vw_gold_folha_servidores` no DDL (em Caeté há lacuna histórica de folha em 2020–2022, iniciando em 2023).",
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
            "Para contar quantos veículos a Prefeitura possui no total, use `COUNT(DISTINCT CASE WHEN TRY_CAST(seq_veiculo AS BIGINT) > 0 THEN seq_veiculo END)` em `vw_gold_frota_consolidada` SEM filtrar `ano_referencia` (linhas com `status_cadastro_frota = 'SEM_PLACA_INDIVIDUALIZADA'` representam gastos agregados de frota sem placa individualizada)! Filtre `ano_referencia = YYYY` APENAS para somar `SUM(vlr_gasto)` anual. Para filtrar apenas veículos emplacados (excluindo maquinário pesado isento e sem placa), use `status_cadastro_frota = 'CADASTRADO'`.",
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
            "As tabelas `educacao_educacao` e `saude_saude` (SICOM) consolidam gastos setoriais; `inep_ideb_educacao` traz o IDEB Anos Iniciais/Finais (anos ímpares: 2019, 2021, 2023), escolas e matrículas; `datasus_siops_saude` traz o percentual SIOPS em saúde, UBS, CNES e leitos SUS.",
        ],
    },
    "estrutura_orgaos_cadastros": {
        "title": "Órgãos, Unidades Orçamentárias, Responsáveis e Cadastro de Pessoas",
        "origin": "SICOM / TCE-MG",
        "source_family": "SICOM",
        "tables": [
            "vw_gold_orcamento_por_orgao",
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
            "remessa", "cadastro", "pessoa", "cnpj", "legislativo", "executivo", "saae",
        ],
        "rules": [
            "Para orçamento, dotação, duodécimo e execução financeira por órgão (Câmara Municipal, Prefeitura, SAAE/Autarquia, Fundações), prefira `vw_gold_orcamento_por_orgao`.",
            "Órgãos estão em `orgao_orgao` (`seq_orgao`, `cod_orgao`, `nom_orgao`, `tipo_orgao`) e unidades em `orgao_orgaoUnidade` (`seq_unidade`, `seq_orgao`, `cod_orgao`, `cod_unidade`, `dsc_unidade`). Como se repetem mensalmente/anualmente, sempre desduplique por `seq_orgao` / `seq_unidade` antes de fazer JOIN.",
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
            "Nas tabelas `cgu_*`, colunas com espaços no nome devem estar entre aspas duplas (ex: `\"VALOR PARCELA\"`, `\"VALOR TRANSFERIDO\"`). Para valores monetários, NUNCA remova `.` cegamente se a coluna já for numérica ou usar ponto decimal: utilize `CASE WHEN CONTAINS(CAST(coluna AS VARCHAR), ',') THEN TRY_CAST(REPLACE(REPLACE(CAST(coluna AS VARCHAR), '.', ''), ',', '.') AS DECIMAL(18,2)) ELSE TRY_CAST(coluna AS DECIMAL(18,2)) END`.",
            "Série histórica de benefícios: `cgu_bolsa_familia_pagamentos` (2020-2021), `cgu_auxilio_emergencial` (2020-2021), `cgu_auxilio_brasil` (2021-2022) e `cgu_novo_bolsa_familia` (2023-2025). Verifique no DDL se a tabela contém `qtd_beneficiados` (resumo mensal municipal: use `MAX(qtd_beneficiados)` no ano/mês) ou `\"NIS FAVORECIDO\"` (microdados nominais: use `COUNT(DISTINCT \"NIS FAVORECIDO\")`).",
            "Convênios federais com o município estão em `cgu_convenios` e emendas parlamentares destinadas ao município estão em `cgu_emendas`.",
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
            "A tabela `receita_qsa_fornecedores` consolida os fornecedores PJ da Prefeitura com `num_cnpj`, `razao_social`, `porte_empresa`, `cnae_principal_descricao`, `qtd_socios_identificados`, `nomes_socios_qsa`, `total_pago_historico` e `alerta_auditoria_controladoria`. Em `vw_gold_fornecedores_qsa_360`, use `natureza_credor = 'EMPRESA_PRIVADA'` para filtrar fornecedores privados.",
            "ATENÇÃO A CPFs EM QSA vs. CONFLITO DE INTERESSES: Na base pública da Receita Federal (`receita_qsa_fornecedores`), CPFs de sócios vêm descaracterizados (`***...`), portanto NUNCA faça JOIN de CPF entre `receita_qsa_fornecedores` e `vw_gold_folha_servidores`! Para cruzar sócios de empresas licitantes/fornecedoras com servidores municipais (conflito de interesses / porta giratória), consulte SEMPRE DIRETAMENTE `vw_gold_conflito_interesses_licitacao`, que já realiza o cruzamento exato de CPF de 11 dígitos e compatibilidade posicional de alta precisão em CPFs mascarados!",
        ],
    },
}

# Semântica declarativa de granularidade temporal e regra de contagem (Semantic Layer Pattern)
TABLE_GRAIN_SPEC: Dict[str, tuple[str, str]] = {
    "vw_gold_orcamento_previsto_vs_realizado": (
        "ANNUAL_SUMMARY_VIEW",
        "1 linha = 1 exercício anual consolidado de TODO o município. USE ESTA GOLD VIEW COMO PRIORIDADE MÁXIMA para orçamento geral do município! Para orçamento específico de um órgão (Câmara Municipal, Prefeitura, SAAE, Fundações), use `vw_gold_orcamento_por_orgao`.",
    ),
    "vw_gold_orcamento_por_orgao": (
        "ANNUAL_ORGAN_BUDGET_VIEW",
        "1 linha = 1 órgão por exercício anual (`ano_referencia`, `cod_orgao`, `nom_orgao`, `esfera_poder`, `administracao_tipo`, `regime_financiamento`). USE ESTA GOLD VIEW para orçamento/duodécimo/execução da Câmara Municipal (`esfera_poder = 'LEGISLATIVO'`), Prefeitura, SAAE ou Fundações! Na Câmara Municipal, `receita_arrecadada_propria_orgao` é `0.00` por desenho constitucional (CF/88 art. 29-A: financiamento via duodécimo); consulte `dotacao_inicial_loa`, `dotacao_atualizada`, `despesa_empenhada`, `despesa_liquidada` e `despesa_paga`.",
    ),
    "vw_gold_folha_servidores": (
        "MONTHLY_PAYROLL_SNAPSHOT",
        "1 linha = 1 vínculo/contracheque mensal (`seq_dat_pagamento` formato YYYYMMDD, `dsc_tipo_pag` = `'Mensal'`, `'13º (Décimo Terceiro)'` ou `'Extra'`). Já traz `nom_orgao` oficial canônico, `esfera_poder` (`'EXECUTIVO'` / `'LEGISLATIVO'`) e `administracao_tipo`. PROIBIDO `COUNT(*)` ou `COUNT(DISTINCT num_cpf)` para contar servidores (`num_cpf` vem mascarado por LGPD em portais municipais e pode colidir entre homônimos de prefixo)! Use SEMPRE `COUNT(DISTINCT seq_dim_pessoa)` no ano ou filtre o último mês `seq_dat_pagamento = (SELECT MAX(seq_dat_pagamento) FROM vw_gold_folha_servidores) AND COALESCE(dsc_tipo_pag, 'Mensal') = 'Mensal'`.",
    ),
    "remuneracao_municipio": (
        "MONTHLY_PAYROLL_SNAPSHOT",
        "1 linha = 1 contracheque mensal bruto. EVITE consultar diretamente: prefira SEMPRE `vw_gold_folha_servidores`, pois na tabela bruta `dsc_sit_serv_pens` e `dsc_situacao` possuem lacunas e siglas não higienizadas.",
    ),
    "vw_gold_frota_consolidada": (
        "CONSOLIDATED_ENTITY_VIEW",
        "View já pré-unificada (`frota_frota` + `frota_gastoFrota` com `vlr_gasto`, `dsc_tipo_gasto`, `status_cadastro_frota`). PROIBIDO fazer JOIN desta view com `frota_gastoFrota`! Para contar a frota geral da Prefeitura (`qual a frota` / `quantos veículos`), use `SELECT status_cadastro_frota, COUNT(DISTINCT CASE WHEN TRY_CAST(seq_veiculo AS BIGINT) > 0 THEN seq_veiculo END) AS qtd_veiculos FROM vw_gold_frota_consolidada GROUP BY status_cadastro_frota` SEM filtrar `ano_referencia`. Filtre `status_cadastro_frota = 'CADASTRADO'` apenas quando a pergunta pedir especificamente veículos emplacados, e filtre `ano_referencia = YYYY` apenas para gastos anuais.",
    ),
    "frota_frota": (
        "INCREMENTAL_CADASTRE_LOG",
        "Tabela bruta de tombamento incremental (`ano_referencia` indica apenas o ano de registro inicial no SICOM). PROIBIDO filtrar `ano_referencia = 2025` para contar a frota total! Prefira SEMPRE `vw_gold_frota_consolidada` com `COUNT(DISTINCT seq_veiculo)`.",
    ),
    "vw_gold_fornecedores_qsa_360": (
        "CONSOLIDATED_ENTITY_VIEW",
        "1 linha = 1 fornecedor único consolidado por `num_doc_credor` (CNPJ/CPF), com `doc_credor_raiz` (8 dígitos apenas para CNPJ), `natureza_credor`, `total_pago_historico` líquido e `nom_socio`. Não possui coluna `ano_referencia` (use `primeiro_ano_pagamento` / `ultimo_ano_pagamento`). Para ranking de empresas privadas, filtre `natureza_credor = 'EMPRESA_PRIVADA'`. Para conflito de interesses com servidores, consulte diretamente `vw_gold_conflito_interesses_licitacao`!",
    ),
    "receita_qsa_fornecedores": (
        "CONSOLIDATED_ENTITY_VIEW",
        "1 linha = 1 empresa fornecedora PJ consolidada por `num_cnpj` com porte, CNAE e QSA.",
    ),
    "vw_gold_execucao_despesa_credor": (
        "ANNUAL_FLOW",
        "1 linha = 1 credor agregado por `(ano_referencia, seq_orgao, num_doc_credor)` com `cod_orgao`, `nom_orgao`, `esfera_poder`, `doc_credor_raiz` (8 dígitos para CNPJ, NULL para CPF), `natureza_credor`, `total_empenhado`, `total_liquidado`, `total_pago`, `pago_exercicio`, `total_empenhado_carona` e `total_empenhado_dispensa_inexigibilidade`. Para ranking de empresas/fornecedores privados, filtre `WHERE natureza_credor = 'EMPRESA_PRIVADA'` e agrupe por `COALESCE(doc_credor_raiz, num_doc_credor)`! ATENÇÃO: NÃO possui `dsc_funcao` (use `despesa_despesa` para Saúde/Educação/etc.).",
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

# Tabelas e colunas bloqueadas para Auto-Profiling por proteção estrita de PII / LGPD
PII_SENSITIVE_TABLES_NO_PROFILE: Set[str] = {
    "pessoa_municipio",
    "cgu_bolsa_familia_pagamentos",
    "cgu_novo_bolsa_familia",
    "cgu_auxilio_brasil",
    "cgu_auxilio_emergencial",
    "cgu_bpc",
    "cgu_despesas_favorecidos",
}

PII_SENSITIVE_COLUMN_RE = re.compile(
    r"(cpf|cnpj|documento|nis|rg|titulo_eleitor|email|telefone|celular|endereco|"
    r"nom_pessoa|nom_servidor|nom_socio|nom_favorecido|nom_credor|nom_contratado|razao_social|beneficiario)",
    re.IGNORECASE,
)

_CPF_LIKE_SAMPLE_RE = re.compile(r"(?<![\d/])\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b(?![\d/])")

# Colunas categóricas prioritárias para Auto-Profiling de valores reais no startup (CHESS Value Profiling)
PROFILED_CATEGORICAL_COLUMNS: Set[str] = {
    "cod_orgao",
    "cod_orgao_norm",
    "esfera_poder",
    "administracao_tipo",
    "natureza_credor",
    "regime_financiamento",
    "classificacao_temporal_vinculo",
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
    "nota_capag_tesouro",
    "dsc_tipo_pag",
    "rede",
    "etapa_ensino",
    "dsc_lotacao",
    "dsc_cargo",
}

PROFILED_TABLES_PRIORITY: Set[str] = {
    "vw_gold_orcamento_previsto_vs_realizado",
    "vw_gold_orcamento_por_orgao",
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
        parquet_dir: Optional[Path] = None,
        resolved_parquet_files: Optional[List[Path]] = None,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.conn = conn
        self.tenant_id = tenant_id
        self.parquet_dir = Path(parquet_dir) if parquet_dir is not None else None
        self.resolved_parquet_files: List[Path] = list(resolved_parquet_files or [])
        if self.parquet_dir is None:
            cand_pq = self.data_dir.parent / "parquet" / self.tenant_id
            if cand_pq.exists():
                self.parquet_dir = cand_pq
        self.tables: Dict[str, TableMetadata] = {}
        self._table_latest_month: Dict[str, Optional[int]] = {}
        self.populated_tables_count: int = 0
        self.empty_tables_count: int = 0
        self.min_data_year: int = 2020
        self.max_data_year: int = 2025
        self.latest_remittance_month: int = 8
        self.latest_remittance_human: str = "agosto de 2025 (última remessa consolidada no SICOM/TCE-MG)"
        self.source_freshness_map: Dict[str, str] = {}
        self.missing_source_families: List[str] = []
        self.rag_acts_count: int = 0
        self.rag_latest_date_iso: Optional[str] = None
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
                # Anti-Phantom Table (P0-1): quando há conexão DuckDB ativa (`self.conn is not None`),
                # SOMENTE registra no catálogo tabelas/views que efetivamente existem no DuckDB do município!
                # (Quando `self.conn is None`, mantém o catálogo estático para testes unitários offline.)
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

        # Auto-descoberta universal (Anti-Whack-a-Mole): qualquer tabela/view adicional legitimamente
        # montada no DuckDB do tenant (ex.: CSV/Parquet customizado enviado via Upload Localhost)
        # entra automaticamente no catálogo semântico e na allowlist do SQLGuard, sem exigir hardcode.
        for t_low, real_name in sorted(canonical_names.items()):
            if t_low in self.tables or t_low.startswith(("sqlite_", "duckdb_", "pragma_")):
                continue
            source_fam = infer_source_family(real_name, "SICOM")
            cols = db_columns.get(t_low, [])
            join_keys = [
                c.name
                for c in cols
                if c.name.lower().startswith(("seq_", "id_", "cod_", "num_doc", "num_cnpj", "ano_referencia"))
            ]
            grain_type, counting_rule = _infer_grain_and_rule(t_low, source_fam)
            self.tables[t_low] = TableMetadata(
                table_name=real_name,
                friendly_name=f"[{source_fam}] Base Suplementar Municipal — {real_name}",
                segment="custom_municipal_uploads",
                source_family=source_fam,
                origin="Upload / Sincronização Municipal Local",
                description=f"Tabela suplementar '{real_name}' ingerida no catálogo analítico do município.",
                update_frequency="Sob demanda / Incremental",
                access_level="public",
                grain_type=grain_type,
                counting_rule=counting_rule.strip(),
                is_gold_view=t_low.startswith("vw_gold_"),
                years_available=sorted(csv_years.get(t_low, {2024, 2025})),
                source_files=[f"{real_name}.parquet"],
                join_keys=join_keys[:8],
                related_tables=[],
                business_rules=[
                    "Tabela suplementar ingerida via pipeline municipal; valide chaves e competência (`ano_referencia`) antes de agregar."
                ],
                columns=cols,
            )

        if self.conn is not None:
            self._profile_categorical_columns()
            self._profile_temporal_freshness()
        else:
            self.populated_tables_count = len(self.tables)
            self.empty_tables_count = 0

    def _extract_table_latest_month(
        self,
        cur: duckdb.DuckDBPyConnection,
        meta: TableMetadata,
        col_names: Dict[str, str],
        yr_col: Optional[str],
        max_yr: int,
    ) -> Optional[int]:
        yr_filter = f"WHERE TRY_CAST({yr_col} AS INTEGER) = {max_yr}" if yr_col else ""
        try:
            if "seq_dat_pagamento" in col_names:
                c_pag = col_names["seq_dat_pagamento"]
                r = cur.execute(
                    f"SELECT MAX(CAST({c_pag} AS VARCHAR)) FROM {meta.table_name} {yr_filter}"
                ).fetchone()
                if r and r[0] and len(str(r[0])) >= 6 and str(r[0])[:6].isdigit():
                    mo = int(str(r[0])[4:6])
                    if 1 <= mo <= 12:
                        return mo
            if "ultimo_mes_despesa" in col_names and "ultimo_mes_receita" in col_names:
                r = cur.execute(
                    f"SELECT GREATEST(COALESCE(MAX(TRY_CAST(ultimo_mes_despesa AS INTEGER)), 0), "
                    f"COALESCE(MAX(TRY_CAST(ultimo_mes_receita AS INTEGER)), 0)) "
                    f"FROM {meta.table_name} {yr_filter}"
                ).fetchone()
                if r and r[0] and 1 <= int(r[0]) <= 12:
                    return int(r[0])
            if "ultimo_mes_exercicio" in col_names:
                r = cur.execute(
                    f"SELECT MAX(TRY_CAST(ultimo_mes_exercicio AS INTEGER)) FROM {meta.table_name} {yr_filter}"
                ).fetchone()
                if r and r[0] and 1 <= int(r[0]) <= 12:
                    return int(r[0])
            for m_key in ("num_mesexercicio", "num_mes_referencia"):
                if m_key in col_names:
                    mc = col_names[m_key]
                    r = cur.execute(
                        f'SELECT MAX(TRY_CAST("{mc}" AS INTEGER)) FROM {meta.table_name} {yr_filter}'
                    ).fetchone()
                    if r and r[0] and 1 <= int(r[0]) <= 12:
                        return int(r[0])
            for d_key in ("dat_empenho", "dat_pagamento", "dat_liquidacao", "dat_assinatura", "dat_abertura", "dat_pub_edital"):
                if d_key in col_names:
                    dc = col_names[d_key]
                    r = cur.execute(
                        f'SELECT MAX(CAST("{dc}" AS VARCHAR)) FROM {meta.table_name} {yr_filter}'
                    ).fetchone()
                    if r and r[0]:
                        ds = str(r[0]).strip()
                        if len(ds) >= 6 and ds[:6].isdigit():
                            mo = int(ds[4:6])
                            if 1 <= mo <= 12:
                                return mo
                        elif re.match(r"^\d{4}-\d{2}", ds):
                            mo = int(ds[5:7])
                            if 1 <= mo <= 12:
                                return mo
        except Exception:
            pass
        return None

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

        present_families = {meta.source_family for meta in self.tables.values() if meta.is_populated}
        self.missing_source_families = [
            fam for fam in ("SICOM", "CGU", "SICONFI", "IBGE", "INEP", "DATASUS") if fam not in present_families
        ]

        sicom_years: Set[int] = set()
        all_years: Set[int] = set()
        for meta in self.tables.values():
            if not meta.is_populated:
                continue
            valid_y = {y for y in meta.years_available if 1990 <= y <= 2100}
            all_years.update(valid_y)
            if meta.source_family == "SICOM":
                sicom_years.update(valid_y)

        # Backfill de anos para tabelas consolidadas sem coluna anual própria (ex.: receita_qsa_fornecedores)
        ref_exec_yrs: List[int] = []
        if "despesa_pagamento" in self.tables and self.tables["despesa_pagamento"].is_populated:
            ref_exec_yrs = list(self.tables["despesa_pagamento"].years_available)
        elif sicom_years:
            ref_exec_yrs = sorted(sicom_years)
        elif all_years:
            ref_exec_yrs = sorted(all_years)
        for meta in self.tables.values():
            if meta.is_populated and not meta.years_available and ref_exec_yrs:
                meta.years_available = list(ref_exec_yrs)

        if sicom_years:
            self.min_data_year = min(sicom_years)
            self.max_data_year = max(sicom_years)
        elif all_years:
            self.min_data_year = min(all_years)
            self.max_data_year = max(all_years)

        self.populated_tables_count = sum(1 for m in self.tables.values() if m.is_populated)
        self.empty_tables_count = sum(1 for m in self.tables.values() if not m.is_populated)

        best_month: Optional[int] = None
        orc_meta = self.tables.get("vw_gold_orcamento_previsto_vs_realizado")
        if orc_meta and orc_meta.is_populated:
            try:
                row = cur.execute(
                    "SELECT ano_referencia, GREATEST(COALESCE(ultimo_mes_despesa, 0), COALESCE(ultimo_mes_receita, 0)) "
                    "FROM vw_gold_orcamento_previsto_vs_realizado "
                    "WHERE COALESCE(despesa_empenhada, 0) > 0 OR COALESCE(receita_arrecadada_realizada, 0) > 0 "
                    "ORDER BY ano_referencia DESC LIMIT 1"
                ).fetchone()
                if row and row[0]:
                    self.max_data_year = max(self.max_data_year, int(row[0]))
                    if row[1] and 1 <= int(row[1]) <= 12:
                        best_month = int(row[1])
            except Exception:
                pass

        folha_meta = self.tables.get("vw_gold_folha_servidores")
        if folha_meta and folha_meta.is_populated:
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

        if not sicom_years:
            self.latest_remittance_month = 12
            self.latest_remittance_human = (
                f"exercício {self.max_data_year} nas bases federais oficiais (SICONFI/STN, IBGE, DATASUS e CGU; "
                f"pacotes SICOM/TCE-MG pendentes de ingestão)"
            )
            self.source_freshness_map = {
                "SICOM": "Pendente de upload do pacote mensal TCE-MG (.zip em inbox/)",
            }
        elif best_month and 1 <= best_month <= 12:
            self.latest_remittance_month = best_month
            m_name = months_pt[best_month]
            suffix = (
                f"exercício {self.max_data_year} em remessa parcial acumulada no SICOM/TCE-MG"
                if best_month < 12
                else f"exercício {self.max_data_year} encerrado (12 meses) no SICOM/TCE-MG"
            )
            self.latest_remittance_human = f"{m_name} de {self.max_data_year} ({suffix})"
            self.source_freshness_map = {
                "SICOM": f"{self.min_data_year} até {self.latest_remittance_human}",
            }
        else:
            self.latest_remittance_human = f"exercício {self.max_data_year} (última remessa consolidada no SICOM/TCE-MG)"
            self.source_freshness_map = {
                "SICOM": f"{self.min_data_year} até {self.latest_remittance_human}",
            }
        for fam in ("CGU", "SICONFI", "IBGE", "INEP", "DATASUS", "RECEITA_QSA"):
            fam_yrs: Set[int] = set()
            for t_low, meta in self.tables.items():
                if meta.is_populated and meta.source_family == fam:
                    fam_yrs.update(y for y in meta.years_available if 1990 <= y <= 2100)
            if fam_yrs:
                self.source_freshness_map[fam] = (
                    f"{min(fam_yrs)} a {max(fam_yrs)}" if min(fam_yrs) != max(fam_yrs) else f"{max(fam_yrs)}"
                )

        # Inspecionar cobertura real do acervo RAG de Diário Oficial (descartando datas futuras > date.today())
        today_iso = date.today().isoformat()
        rag_candidates: List[Path] = []
        if self.parquet_dir is not None:
            rag_candidates.append(self.parquet_dir / "rag_jornais_individuais.json")
            rag_candidates.append(self.parquet_dir.parent / self.tenant_id / "rag_jornais_individuais.json")
        rag_candidates.append(self.data_dir.parent / "parquet" / self.tenant_id / "rag_jornais_individuais.json")
        rag_candidates.append(self.data_dir.parent / f"atos_normativos_rag_{self.tenant_id}.json")
        if self.tenant_id in ("caete_mg_3110004", "caete-mg"):
            rag_candidates.append(self.data_dir.parent / "atos_normativos_rag.json")
        for rag_path in rag_candidates:
            if rag_path.exists():
                try:
                    raw_acts = json.loads(rag_path.read_text(encoding="utf-8"))
                    if isinstance(raw_acts, list) and raw_acts:
                        self.rag_acts_count = len(raw_acts)
                        iso_dates: List[str] = []
                        for item in raw_acts:
                            if not isinstance(item, dict):
                                continue
                            for d_key in ("data_publicacao", "data_ato", "data", "published_at"):
                                d_val = str(item.get(d_key) or "").strip()
                                iso_d: Optional[str] = None
                                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d_val):
                                    iso_d = d_val
                                elif re.fullmatch(r"\d{2}/\d{2}/\d{4}", d_val):
                                    dd, mm, yyyy = d_val.split("/")
                                    iso_d = f"{yyyy}-{mm}-{dd}"
                                if iso_d and "1990-01-01" <= iso_d <= today_iso:
                                    iso_dates.append(iso_d)
                        if iso_dates:
                            max_iso = max(iso_dates)
                            self.rag_latest_date_iso = max_iso
                            y_r, m_r, _ = max_iso.split("-")
                            m_label = months_pt.get(int(m_r), m_r)
                            self.source_freshness_map["DIARIO_OFICIAL"] = (
                                f"até {m_label} de {y_r} ({self.rag_acts_count} atos normativos indexados)"
                            )
                        else:
                            self.source_freshness_map["DIARIO_OFICIAL"] = (
                                f"{self.rag_acts_count} atos normativos indexados"
                            )
                        break
                except Exception:
                    pass

    def get_effective_freshness_human(self, table_names: List[str]) -> str:
        """
        Retorna o horizonte temporal real das tabelas efetivamente consultadas numa query SQL,
        evitando anunciar a competência global máxima do município quando a consulta utiliza
        tabelas granulares cuja última remessa é anterior (ex.: empenhos/frota até agosto/2025
        enquanto folha/orçamento já possuem dados de 2026).
        """
        months_pt = {
            1: "janeiro", 2: "fevereiro", 3: "março", 4: "abril",
            5: "maio", 6: "junho", 7: "julho", 8: "agosto",
            9: "setembro", 10: "outubro", 11: "novembro", 12: "dezembro",
        }
        matched_metas: List[TableMetadata] = []
        for t in table_names or []:
            if not t:
                continue
            m = self.tables.get(t.strip().lower())
            if m and m.is_populated:
                matched_metas.append(m)
        if not matched_metas:
            return self.latest_remittance_human

        eff_years = sorted({y for m in matched_metas for y in m.years_available if 1990 <= y <= 2100})
        if not eff_years:
            return self.latest_remittance_human

        eff_max_yr = max(eff_years)
        fams = {m.source_family for m in matched_metas}
        if fams == {"INEP"}:
            return f"edição {eff_max_yr} (série oficial bienal INEP/IDEB de {min(eff_years)} a {eff_max_yr})"
        if fams == {"IBGE"}:
            return f"exercício {eff_max_yr} (série demográfica e PIB oficial IBGE de {min(eff_years)} a {eff_max_yr})"
        if fams == {"SICONFI"}:
            return f"exercício {eff_max_yr} (demonstrativos fiscais homologados no SICONFI/STN de {min(eff_years)} a {eff_max_yr})"
        if fams == {"DATASUS"}:
            return f"exercício {eff_max_yr} (série oficial SIOPS/DATASUS de {min(eff_years)} a {eff_max_yr})"
        if fams == {"CGU"}:
            return f"exercício {eff_max_yr} (Portal da Transparência Federal / CGU)"

        # Descobrir mês efetivo nas tabelas consultadas que atingem eff_max_yr
        candidate_months: List[int] = []
        for m in matched_metas:
            if eff_max_yr in m.years_available:
                mo = self._table_latest_month.get(m.table_name.lower())
                if mo and 1 <= mo <= 12:
                    candidate_months.append(mo)

        # Fallback de mês para Gold Views agregadas sem coluna mensal própria (ex.: vw_gold_execucao_despesa_credor)
        if not candidate_months:
            t_lows = {m.table_name.lower() for m in matched_metas}
            fallback_bases: List[str] = []
            if t_lows & {"vw_gold_execucao_despesa_credor", "vw_gold_fornecedores_qsa_360", "receita_qsa_fornecedores"}:
                fallback_bases.extend(["empenho_empenho", "despesa_pagamento", "despesa_liquidacao"])
            if "vw_gold_frota_consolidada" in t_lows:
                fallback_bases.append("frota_gastofrota")
            if "vw_gold_contratos_vigencia_aditivos" in t_lows:
                fallback_bases.append("contrato_contratos")
            for fb in fallback_bases:
                fb_m = self.tables.get(fb)
                if fb_m and fb_m.is_populated and eff_max_yr in fb_m.years_available:
                    mo = self._table_latest_month.get(fb)
                    if mo and 1 <= mo <= 12:
                        candidate_months.append(mo)

        eff_mo = max(candidate_months) if candidate_months else None
        if eff_max_yr < self.max_data_year:
            seg_labels: List[str] = []
            segs = {m.segment for m in matched_metas}
            if "execucao_despesa" in segs or "auditoria_fornecedores_qsa" in segs:
                seg_labels.append("empenhos/pagamentos")
            if "frota_veiculos" in segs:
                seg_labels.append("frota")
            if "licitacoes_compras" in segs:
                seg_labels.append("licitações")
            if "contratos_aditivos" in segs:
                seg_labels.append("contratos/aditivos")
            scope_desc = "/".join(seg_labels) if seg_labels else ", ".join(m.table_name for m in matched_metas[:2])
            if eff_mo and 1 <= eff_mo <= 12:
                return (
                    f"{months_pt[eff_mo]} de {eff_max_yr} "
                    f"(última remessa granular de {scope_desc} carregada no SICOM/TCE-MG)"
                )
            return f"exercício {eff_max_yr} (última remessa granular de {scope_desc} carregada no SICOM/TCE-MG)"

        if eff_mo and 1 <= eff_mo <= 12:
            m_name = months_pt[eff_mo]
            suffix = (
                f"exercício {eff_max_yr} em remessa parcial acumulada no SICOM/TCE-MG"
                if eff_mo < 12
                else f"exercício {eff_max_yr} encerrado (12 meses) no SICOM/TCE-MG"
            )
            return f"{m_name} de {eff_max_yr} ({suffix})"
        return self.latest_remittance_human

    def _compute_profile_cache_fingerprint(self) -> str:
        h = hashlib.sha256()
        h.update(b"sim_catalog_profile_v4.5.1|")
        h.update(self.tenant_id.encode("utf-8"))
        for t_low in sorted(self.tables.keys()):
            cols_sig = ",".join(f"{c.name}:{c.physical_type}" for c in self.tables[t_low].columns)
            h.update(f"|T:{t_low}:{cols_sig}".encode("utf-8"))
        if self.resolved_parquet_files:
            for fp in sorted(self.resolved_parquet_files, key=lambda p: p.name.lower()):
                try:
                    if fp.exists():
                        st = fp.stat()
                        h.update(f"|F:{fp.name.lower()}:{st.st_size}:{int(st.st_mtime)}".encode("utf-8"))
                except Exception:
                    pass
        else:
            scan_dir = self.parquet_dir if (self.parquet_dir and self.parquet_dir.exists()) else self.data_dir
            if scan_dir.exists():
                try:
                    for fp in sorted(scan_dir.iterdir()):
                        if fp.is_file() and fp.suffix.lower() in (".parquet", ".csv"):
                            st = fp.stat()
                            h.update(f"|F:{fp.name.lower()}:{st.st_size}:{int(st.st_mtime)}".encode("utf-8"))
                except Exception:
                    pass
        return h.hexdigest()[:24]

    def _get_profile_cache_path(self) -> Optional[Path]:
        target_dir = self.parquet_dir if (self.parquet_dir and self.parquet_dir.exists()) else None
        if target_dir is None:
            cand_pq = self.data_dir.parent / "parquet" / self.tenant_id
            if cand_pq.exists():
                target_dir = cand_pq
        if target_dir is None:
            return None
        if target_dir.resolve().name.lower() == "database":
            return None
        return target_dir / "_catalog_profile_v1.json"

    def _profile_categorical_columns(self) -> None:
        """
        Auto-Profiling no startup (contagem real `COUNT(*)`, `is_populated`, `years_available`
        em tabelas e Gold Views, mês mais recente por tabela e valores categóricos CHESS),
        com denylist estrita de PII/LGPD e cache persistente em `_catalog_profile_v1.json`.
        """
        if self.conn is None:
            return

        fingerprint = self._compute_profile_cache_fingerprint()
        cache_path = self._get_profile_cache_path()
        if cache_path and cache_path.exists():
            try:
                cached = json.loads(cache_path.read_text(encoding="utf-8"))
                if (
                    isinstance(cached, dict)
                    and cached.get("fingerprint") == fingerprint
                    and isinstance(cached.get("table_stats"), dict)
                ):
                    cached_stats = cached["table_stats"]
                    for t_low, st_entry in cached_stats.items():
                        meta = self.tables.get(t_low)
                        if not meta or not isinstance(st_entry, dict):
                            continue
                        rc = int(st_entry.get("row_count") or 0)
                        meta.row_count = rc
                        meta.is_populated = bool(st_entry.get("is_populated", rc > 0))
                        meta.years_available = (
                            [int(y) for y in (st_entry.get("years_available") or []) if 1990 <= int(y) <= 2100]
                            if meta.is_populated
                            else []
                        )
                        lm = st_entry.get("latest_month")
                        self._table_latest_month[t_low] = int(lm) if (lm and 1 <= int(lm) <= 12) else None

                    cached_tables = cached.get("tables", {})
                    for t_low, col_map in cached_tables.items():
                        meta = self.tables.get(t_low)
                        if not meta or not isinstance(col_map, dict):
                            continue
                        for col in meta.columns:
                            entry = col_map.get(col.name.lower())
                            if isinstance(entry, dict):
                                vals = [str(v) for v in entry.get("samples", []) if v]
                                col.sample_values = vals
                                col.distinct_count = int(entry.get("distinct_count") or len(vals))

                    # Tabelas PII denylistadas nunca são salvas em disco (_catalog_profile_v1.json);
                    # computa contagem/anos rápidos apenas em memória (<2ms).
                    cur_mem = self.conn.cursor()
                    for pii_t in PII_SENSITIVE_TABLES_NO_PROFILE:
                        p_meta = self.tables.get(pii_t)
                        if not p_meta:
                            continue
                        try:
                            p_rc = int((cur_mem.execute(f"SELECT COUNT(*) FROM {p_meta.table_name}").fetchone() or [0])[0] or 0)
                            p_meta.row_count = p_rc
                            p_meta.is_populated = p_rc > 0
                            if not p_meta.is_populated:
                                p_meta.years_available = []
                            else:
                                p_cols = {c.name.lower(): c.name for c in p_meta.columns}
                                p_yr = p_cols.get("ano_referencia") or p_cols.get("ano")
                                if p_yr:
                                    y_rows = cur_mem.execute(
                                        f'SELECT DISTINCT TRY_CAST("{p_yr}" AS INTEGER) FROM {p_meta.table_name} WHERE "{p_yr}" IS NOT NULL LIMIT 40'
                                    ).fetchall()
                                    p_meta.years_available = sorted({int(r[0]) for r in y_rows if r and r[0] and 1990 <= int(r[0]) <= 2100})
                                    if p_meta.years_available:
                                        self._table_latest_month[pii_t] = self._extract_table_latest_month(
                                            cur_mem, p_meta, p_cols, p_yr, max(p_meta.years_available)
                                        )
                        except Exception:
                            pass
                    return
            except Exception:
                pass

        cur = self.conn.cursor()
        serialized_stats: Dict[str, Dict[str, Any]] = {}

        # 1. Contagem real de linhas e cobertura temporal em TODAS as tabelas e Gold Views
        for t_low, meta in self.tables.items():
            rc = 0
            try:
                cnt_row = cur.execute(f"SELECT COUNT(*) FROM {meta.table_name}").fetchone()
                rc = int(cnt_row[0] or 0) if cnt_row else 0
            except Exception:
                rc = 0
            meta.row_count = rc
            if rc <= 0:
                meta.is_populated = False
                meta.years_available = []
                self._table_latest_month[t_low] = None
                if t_low not in PII_SENSITIVE_TABLES_NO_PROFILE:
                    serialized_stats[t_low] = {
                        "row_count": 0,
                        "is_populated": False,
                        "years_available": [],
                        "latest_month": None,
                    }
                continue

            meta.is_populated = True
            col_names = {c.name.lower(): c.name for c in meta.columns}
            yr_col = col_names.get("ano_referencia") or col_names.get("ano")
            real_yrs: List[int] = []
            if yr_col:
                try:
                    yr_rows = cur.execute(
                        f'SELECT DISTINCT TRY_CAST("{yr_col}" AS INTEGER) AS y '
                        f'FROM {meta.table_name} WHERE "{yr_col}" IS NOT NULL LIMIT 40'
                    ).fetchall()
                    real_yrs = sorted({int(r[0]) for r in yr_rows if r and r[0] and 1990 <= int(r[0]) <= 2100})
                except Exception:
                    real_yrs = []
            elif "primeiro_ano_pagamento" in col_names and "ultimo_ano_pagamento" in col_names:
                try:
                    mm_row = cur.execute(
                        f"SELECT MIN(TRY_CAST(primeiro_ano_pagamento AS INTEGER)), "
                        f"MAX(TRY_CAST(ultimo_ano_pagamento AS INTEGER)) FROM {meta.table_name}"
                    ).fetchone()
                    if mm_row and mm_row[0] and mm_row[1]:
                        y1, y2 = int(mm_row[0]), int(mm_row[1])
                        if 1990 <= y1 <= y2 <= 2100:
                            real_yrs = list(range(y1, y2 + 1))
                except Exception:
                    real_yrs = []
            elif "primeiro_ano_licitacao" in col_names and "ultimo_ano_licitacao" in col_names:
                try:
                    mm_row = cur.execute(
                        f"SELECT LEAST(COALESCE(MIN(TRY_CAST(primeiro_ano_licitacao AS INTEGER)), 9999), "
                        f"COALESCE(MIN(TRY_CAST(primeiro_ano_folha AS INTEGER)), 9999)), "
                        f"GREATEST(COALESCE(MAX(TRY_CAST(ultimo_ano_licitacao AS INTEGER)), 0), "
                        f"COALESCE(MAX(TRY_CAST(ultimo_ano_folha AS INTEGER)), 0)) "
                        f"FROM {meta.table_name}"
                    ).fetchone()
                    if mm_row and mm_row[0] and mm_row[1] and 1990 <= int(mm_row[0]) <= int(mm_row[1]) <= 2100:
                        real_yrs = list(range(int(mm_row[0]), int(mm_row[1]) + 1))
                except Exception:
                    real_yrs = []

            meta.years_available = real_yrs
            latest_mo = (
                self._extract_table_latest_month(cur, meta, col_names, yr_col, max(real_yrs))
                if real_yrs
                else None
            )
            self._table_latest_month[t_low] = latest_mo
            if t_low not in PII_SENSITIVE_TABLES_NO_PROFILE:
                serialized_stats[t_low] = {
                    "row_count": rc,
                    "is_populated": True,
                    "years_available": real_yrs,
                    "latest_month": latest_mo,
                }

        # 2. Auto-Profiling de valores categóricos apenas em tabelas populadas (e fora da denylist PII)
        serialized_tables: Dict[str, Dict[str, Any]] = {}
        for t_low in sorted(PROFILED_TABLES_PRIORITY):
            if t_low in PII_SENSITIVE_TABLES_NO_PROFILE:
                continue
            meta = self.tables.get(t_low)
            if not meta or not meta.is_populated:
                continue
            for col in meta.columns:
                c_low = col.name.lower()
                if c_low not in PROFILED_CATEGORICAL_COLUMNS:
                    continue
                if PII_SENSITIVE_COLUMN_RE.search(c_low):
                    continue
                is_code_col = c_low in {"cod_orgao", "cod_orgao_norm"}
                if "VARCHAR" not in col.physical_type.upper() and not is_code_col:
                    continue
                col_ident = f'"{col.name}"'
                limit_n = 20
                try:
                    d_row = cur.execute(
                        f"SELECT COUNT(DISTINCT TRIM(CAST({col_ident} AS VARCHAR))) "
                        f"FROM {meta.table_name} "
                        f"WHERE {col_ident} IS NOT NULL "
                        f"AND TRIM(CAST({col_ident} AS VARCHAR)) NOT IN ('', '-1', 'NULL')"
                    ).fetchone()
                    distinct_cnt = int(d_row[0] or 0) if d_row else 0
                    if distinct_cnt <= 0:
                        continue
                    fetch_n = min(limit_n, distinct_cnt) if distinct_cnt <= 20 else 5
                    rows = cur.execute(
                        f"SELECT TRIM(CAST({col_ident} AS VARCHAR)) AS val, COUNT(*) AS cnt "
                        f"FROM {meta.table_name} "
                        f"WHERE {col_ident} IS NOT NULL "
                        f"AND TRIM(CAST({col_ident} AS VARCHAR)) NOT IN ('', '-1', 'NULL') "
                        f"GROUP BY 1 ORDER BY cnt DESC LIMIT {fetch_n}"
                    ).fetchall()
                    samples: List[str] = []
                    for val, _cnt in rows:
                        if val is None or str(val).strip() == "":
                            continue
                        s_val = str(val).strip()
                        if _CPF_LIKE_SAMPLE_RE.search(s_val):
                            continue
                        if len(s_val) > 64:
                            s_val = s_val[:61] + "..."
                        samples.append(s_val)
                    if samples:
                        col.sample_values = samples
                        col.distinct_count = distinct_cnt
                        serialized_tables.setdefault(t_low, {})[c_low] = {
                            "samples": samples,
                            "distinct_count": distinct_cnt,
                        }
                except Exception:
                    continue

        if cache_path is not None:
            try:
                cache_payload = {
                    "version": "v4.5",
                    "tenant_id": self.tenant_id,
                    "fingerprint": fingerprint,
                    "table_stats": serialized_stats,
                    "tables": serialized_tables,
                }
                cache_path.write_text(
                    json.dumps(cache_payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            except Exception:
                pass

    def get_allowed_table_names(
        self,
        role: str = "citizen",
        allowed_sources: Optional[List[str]] = None,
        explicit_tables: Optional[List[str]] = None,
    ) -> Set[str]:
        base = set(self.tables.keys())
        if explicit_tables:
            return {t.lower() for t in explicit_tables if t.lower() in self.tables}
        if allowed_sources:
            raw_upper = {s.upper().strip() for s in allowed_sources if s and s.strip()}
            if not raw_upper:
                return base
            if raw_upper == {"DIARIO_OFICIAL"}:
                return set()
            norm_sources = raw_upper - {"DIARIO_OFICIAL"}
            return {
                t_low
                for t_low, meta in self.tables.items()
                if meta.source_family.upper() in norm_sources
            }
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

        # 3. Seleção semântica automática pela pergunta do usuário (com fronteira de palavra anti-falso-positivo)
        if question and not explicit_tables:
            if explicit_segments or norm_sources:
                mode = "source_filtered" if norm_sources else "hybrid"
            for seg_id, seg_info in SEGMENT_DEFINITIONS.items():
                for kw in seg_info["keywords"]:
                    kw_norm = _norm(kw)
                    if not kw_norm:
                        continue
                    if " " in kw_norm:
                        matched_kw = kw_norm in q_norm
                    elif len(kw_norm) <= 4:
                        matched_kw = bool(re.search(r"\b" + re.escape(kw_norm) + r"\b", q_norm))
                    else:
                        matched_kw = bool(re.search(r"\b" + re.escape(kw_norm), q_norm))
                    if matched_kw:
                        if seg_id not in matched_segments:
                            matched_segments.append(seg_id)
                        for t in seg_info["tables"]:
                            if t.lower() in self.tables:
                                selected_set.add(t.lower())
                        break

            generic_suffixes = {
                "municipio", "orgao", "despesa", "receita", "contratos",
                "licitacao", "frota", "educacao", "saude", "pagamento",
            }
            for t_low, meta in self.tables.items():
                short_suffix = t_low.split("_", 1)[-1]
                has_exact_table = t_low in q_norm
                has_specific_suffix = (
                    len(short_suffix) >= 6
                    and short_suffix not in generic_suffixes
                    and bool(re.search(r"\b" + re.escape(short_suffix) + r"\b", q_norm))
                )
                if has_exact_table or has_specific_suffix:
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

        if re.search(r"\b(social|ies|saude|educacao|ensino|fundeb|ideb|siops)\b", q_norm) and not explicit_tables:
            for t in SEGMENT_DEFINITIONS["educacao_saude"]["tables"]:
                if t.lower() in self.tables:
                    selected_set.add(t.lower())
            if "educacao_saude" not in matched_segments:
                matched_segments.append("educacao_saude")

        # 6. APLICAR RESTRIÇÃO ESTRITA DE FONTES (`allowed_sources`) OU TABELAS (`explicit_tables`)
        #    E EXCLUIR TABELAS VAZIAS (`not meta.is_populated`) DO ANÚNCIO AO LLM!
        if explicit_tables:
            exp_low = {
                t.lower()
                for t in explicit_tables
                if t.lower() in self.tables and self.tables[t.lower()].is_populated
            }
            if exp_low:
                selected_set = exp_low
                mode = "manual_tables"
            else:
                selected_set = set()
                mode = "manual_tables"
        elif norm_sources:
            mode = "source_filtered"
            filtered_by_source = {
                t_low
                for t_low in selected_set
                if t_low in self.tables
                and self.tables[t_low].is_populated
                and self.tables[t_low].source_family.upper() in norm_sources
            }
            if not filtered_by_source:
                # Se a pergunta não citou palavras-chave exatas daquela fonte, inclui todas as tabelas populadas da(s) fonte(s) selecionada(s) (até 18)
                all_in_sources = [
                    t_low
                    for t_low, meta in self.tables.items()
                    if meta.is_populated and meta.source_family.upper() in norm_sources
                ]
                filtered_by_source = set(all_in_sources[:18])
            selected_set = filtered_by_source
            matched_segments = list(
                dict.fromkeys(self.tables[t_low].segment for t_low in selected_set if t_low in self.tables)
            )
        else:
            selected_set = {
                t_low
                for t_low in selected_set
                if t_low in self.tables and self.tables[t_low].is_populated
            }
            if not selected_set:
                selected_set = {
                    t_low
                    for t_low, meta in self.tables.items()
                    if meta.is_populated
                }
                selected_set = set(sorted(selected_set)[:16])

        ordered_tables = sorted(
            self.tables[t_low].table_name
            for t_low in selected_set
            if t_low in self.tables and self.tables[t_low].is_populated
        )

        populated_total = sum(1 for m in self.tables.values() if m.is_populated)
        total_tables = populated_total if populated_total > 0 else len(self.tables)
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
            rem_meta = self.tables.get("remuneracao_municipio") or self.tables.get("vw_gold_folha_servidores")
            rem_yrs = sorted(rem_meta.years_available) if (rem_meta and rem_meta.years_available) else []
            if rem_yrs and min(rem_yrs) > self.min_data_year:
                notes.append(
                    f"ATENÇÃO: `remuneracao_municipio` e `vw_gold_folha_servidores` contêm dados apenas de {min(rem_yrs)} a {max(rem_yrs)} "
                    f"(lacuna de publicação na origem em {self.min_data_year}–{min(rem_yrs) - 1})."
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
        explicitando `grain_type`, `counting_rule` e `sample_values` auto-perfilados,
        excluindo estritamente tabelas vazias (`not meta.is_populated`).
        """
        populated_names = [
            t
            for t in table_names
            if t.lower() in self.tables and self.tables[t.lower()].is_populated
        ]
        gold_tables = [t for t in populated_names if t.lower().startswith("vw_gold_")]
        base_tables = [t for t in populated_names if not t.lower().startswith("vw_gold_")]
        prioritized = gold_tables + base_tables

        lines: List[str] = []
        if gold_tables:
            lines.append(
                "-- === CAMADA SEMÂNTICA PRIMÁRIA (GOLD VIEWS PRÉ-DESDUPLICADAS — USE PREFERENCIALMENTE!) ==="
            )

        emitted_base_header = False
        for tname in prioritized:
            meta = self.tables.get(tname.lower())
            if not meta or not meta.is_populated:
                continue
            if not meta.is_gold_view and gold_tables and not emitted_base_header:
                lines.append("-- === TABELAS BASE DETALHADAS (USE QUANDO PRECISAR DE GRANULARIDADE EXTRA) ===")
                emitted_base_header = True

            yrs_str = (
                (
                    f"{min(meta.years_available)}-{max(meta.years_available)}"
                    if min(meta.years_available) != max(meta.years_available)
                    else f"{max(meta.years_available)}"
                )
                if meta.years_available
                else "Consolidado"
            )
            priority_tag = " | PRIORIDADE MÁXIMA (GOLD VIEW)" if meta.is_gold_view else ""
            col_strs = []
            for c in meta.columns:
                cname_fmt = f'"{c.name}"' if (" " in c.name or "-" in c.name) else c.name
                if c.sample_values:
                    if c.distinct_count > 20:
                        vals_preview = ", ".join(f"'{v}'" for v in c.sample_values[:5])
                        col_strs.append(
                            f"{cname_fmt} ({c.semantic_type} | exemplos entre {c.distinct_count} distintos: [{vals_preview}])"
                        )
                    else:
                        vals_preview = ", ".join(f"'{v}'" for v in c.sample_values[:20])
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
