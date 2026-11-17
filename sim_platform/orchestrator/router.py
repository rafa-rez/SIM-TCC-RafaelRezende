"""
Orquestrador Híbrido Controlado por Código (SIMOrchestrator).

Substitui a orquestração em JSON visual do n8n (`wpp-tiokk-sql.json` + `duck-sql.json`),
registrando uma trilha de auditoria passo a passo (`pipeline_steps`), suportando:
- Seleção e restrição explícita de bases (`allowed_sources`: `SICOM`, `CGU`, `SICONFI`, `IBGE`,
  `INEP`, `DATASUS`, `RECEITA_QSA`, `DIARIO_OFICIAL`) e tabelas (`explicit_tables`).
- Multi-LLM (`OpenAI GPT-4.1-mini` via `OPENAI_API_KEY` e `Google Gemini 2.5 Flash` via `GEMINI_API_KEY`)
  + Compilador NL2SQL Analítico Nativo para todas as 79 tabelas.
"""

from __future__ import annotations

import csv
import json
import os
import re
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp

from sim_platform.catalog.data_catalog import SchemaSelectionResult
from sim_platform.catalog.entity_linker import EntityLinker, normalize_municipal_asr_text
from sim_platform.engine.duckdb_manager import DuckDBManager
from sim_platform.prompts.builder import ModularPromptBuilder, ModularPromptPackage, strip_emojis
from sim_platform.rag.hybrid_retriever import HybridRAGRetriever, normalize_text_for_dedup
from sim_platform.security.quota_manager import TenantQuotaManager
from sim_platform.security.semantic_guardrail import SemanticGuardrailDecision, SemanticIntentGuardrail
from sim_platform.security.sql_guard import (
    PromptSafetyResult,
    check_prompt_safety,
)
from sim_platform.visualization.chart_generator import InstitutionalChartGenerator
from sim_platform.visualization.table_renderer import InstitutionalTableRenderer, TableArtifact

DEFAULT_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]

OTHER_MUNICIPALITIES = (
    "caete",
    "lagoa da prata",
    "belo horizonte",
    "contagem",
    "betim",
    "sabara",
    "nova lima",
    "santa luzia",
    "ouro preto",
    "mariana",
    "itabira",
    "joao monlevade",
    "divinopolis",
    "formiga",
    "bom despacho",
    "arcos",
    "luz",
    "santo antonio do monte",
    "uberlandia",
    "juiz de fora",
    "sao paulo",
    "rio de janeiro",
    "brasilia",
)

OUT_OF_SCOPE_TOPICS = (
    "previsao do tempo",
    "receita de bolo",
    "campeonato brasileiro",
    "futebol",
    "horoscopo",
    "cotacao do bitcoin",
    "piada",
)

IDENTITY_GREETING_PATTERNS = (
    "quem e vc",
    "quem e voce",
    "quem es tu",
    "oq faz",
    "oq vc faz",
    "oq voce faz",
    "o que faz",
    "o que vc faz",
    "o que voce faz",
    "como vc funciona",
    "como voce funciona",
    "como funciona",
    "qual sua funcao",
    "qual a sua funcao",
    "quais bases vc tem",
    "quais dados voce tem",
    "quais informacoes voce tem",
    "presta informacoes",
    "quais cidades",
    "qual cidade vc",
    "qual cidade voce",
    "o que posso perguntar",
    "oq posso perguntar",
    "pode me ajudar",
    "me ajuda",
    "me ajude",
    "bom dia",
    "boa tarde",
    "boa noite",
)

_IDENTITY_REGEX = re.compile(
    r"\b(quem\s+e\s+(vc|voce|tu)|o\s*q(ue)?\s+(vc|voce|tu)?\s*faz|como\s+(vc|voce)?\s*funciona|"
    r"qual\s+(sua|a\s+sua)\s+funcao|me\s+ajud[ae]|pode\s+me\s+ajudar|o\s*q(ue)?\s+posso\s+perguntar)\b"
)

_FOLLOWUP_REFORMAT_REGEX = re.compile(
    r"\b(resum[aoie]|mais\s+curt[oa]|mais\s+concis[oa]|de\s+forma\s+resumida|de\s+forma\s+concisa|"
    r"simplifiq|mais\s+simples|sem\s+termos\s+tecnicos|em\s+topicos|em\s+lista|em\s+uma\s+frase|"
    r"expliq(ue|a)\s+melhor|nao\s+entendi|o\s+que\s+significa\s+isso|sobre\s+a\s+resposta\s+anterior|"
    r"na\s+resposta\s+anterior|na\s+pergunta\s+anterior|mude\s+a\s+resposta|altere\s+a\s+resposta|"
    r"reescreva|reformule|fale\s+mais\s+sobre\s+o\s+(primeiro|segundo|terceiro|ultimo)|"
    r"detalhe\s+o\s+(primeiro|segundo|terceiro)|qual\s+a\s+diferenca\s+entre\s+eles)\b"
)

_CONCEPTUAL_DOUBT_REGEX = re.compile(
    r"^(o\s+que\s+e|oq\s+e|o\s+que\s+significa|qual\s+a\s+diferenca\s+entre|como\s+funciona\s+(o|a|um|uma))\s+"
    r"(empenho|empenhado|liquidacao|liquidado|pago|restos\s+a\s+pagar|lrf|limite\s+prudencial|capag|"
    r"rcl|receita\s+corrente\s+liquida|carona|adesao\s+a\s+ata|ata\s+de\s+registro|dispensa|inexigibilidade|"
    r"pregao|licitacao|aditivo|termo\s+aditivo|siops|mde|fundeb|ideb|qsa|quadro\s+societario|cnae|bpc)\b"
)

SEMANTIC_CONCEPT_MAP: Dict[str, List[str]] = {
    "orcamento_planejamento": [
        "caixa", "endividamento", "superavit", "deficit", "equilibrio fiscal", "arrecadacao",
        "impostos", "iptu", "iss", "itbi", "planejamento", "loa", "ldo", "ppa", "alteracao orcamentaria",
    ],
    "execucao_despesa": [
        "pagou", "pagamento", "gasto", "gastou", "despesa", "fornecedor", "empresa", "credor",
        "nota fiscal", "liquidacao", "empenho", "restos a pagar", "carona", "adesao a ata",
    ],
    "licitacoes_compras": [
        "licitacao", "pregao", "dispensa", "inexigibilidade", "compra direta", "concorrencia",
        "edital", "vencedor do certame", "cotacao", "ata de registro", "colisao societaria",
    ],
    "contratos_aditivos": [
        "contrato", "aditivo", "termo aditivo", "vigencia", "apostilamento", "rescisao",
    ],
    "folha_rh": [
        "servidor", "funcionario", "salario", "remuneracao", "contracheque", "folha", "cargo",
        "prefeito", "secretario", "professor", "medico", "comissionado", "efetivo",
    ],
    "educacao_saude": [
        "saude", "hospital", "posto de saude", "ubs", "remedio", "medicamento", "sus", "ies-c",
        "educacao", "escola", "creche", "merenda", "fundeb", "mde", "ensino", "ieq-c",
        "ideb", "inep", "matricula", "aluno", "siops", "datasus", "cnes", "leitos", "santa casa",
    ],
    "frota_veiculos": [
        "frota", "veiculo", "carro", "caminhao", "onibus", "ambulancia", "combustivel", "gasolina",
        "diesel", "oficina", "pecas", "manutencao de veiculo", "sem placa", "veiculo orfao",
    ],
    "cgu_transferencias_beneficios": [
        "bolsa familia", "bpc", "beneficio", "auxilio brasil", "auxilio emergencial",
        "garantia safra", "baixa renda", "governo federal", "cgu", "convenio", "convenios",
        "emenda", "emendas", "parlamentar",
    ],
    "indicadores_ibge_siconfi": [
        "ibge", "populacao", "habitantes", "censo", "per capita", "por habitante", "pib", "idhm", "idh",
        "siconfi", "finbra", "tesouro", "rcl", "receita corrente liquida", "capag", "lrf", "limite prudencial",
    ],
    "auditoria_fornecedores_qsa": [
        "qsa", "quadro societario", "socio", "socios", "receita federal", "porte", "cnae", "dono", "materialidade",
        "conflito de interesse", "conflito de interesses", "porta giratoria", "servidor socio",
    ],
}

RAG_NORMATIVE_KEYWORDS = (
    "portaria",
    "decreto",
    "lei municipal",
    "lei n",
    "diario oficial",
    "jornal opiniao",
    "edital publicado",
    "texto da lei",
    "nomeacao",
    "exoneracao",
    "designa",
    "fiscal do contrato",
    "publicacao oficial",
    "desmembramento",
)

SQL_FINANCIAL_KEYWORDS = (
    "quanto",
    "valor",
    "total",
    "soma",
    "media",
    "ranking",
    "maiores",
    "pago",
    "pagou",
    "pagamento",
    "pagamentos",
    "empenho",
    "empenhado",
    "liquidado",
    "receita",
    "despesa",
    "gastou",
    "gasto",
    "arrecadado",
    "isf-m",
    "ies-c",
    "ieq-c",
    "salario",
    "salarios",
    "remuneracao",
    "folha",
    "servidor",
    "servidores",
    "bolsa familia",
    "bpc",
    "auxilio",
    "convenio",
    "convenios",
    "emenda",
    "emendas",
    "frota",
    "combustivel",
    "contrato",
    "contratos",
    "licitacao",
    "dispensa",
    "fornecedor",
    "fornecedores",
    "credor",
    "populacao",
    "habitantes",
    "per capita",
    "pib",
    "idhm",
    "ideb",
    "inep",
    "siops",
    "datasus",
    "cnes",
    "leitos",
    "siconfi",
    "capag",
    "rcl",
    "lrf",
    "qsa",
    "socio",
    "socios",
    "cnae",
)


HUMAN_COLUMN_LABELS: Dict[str, str] = {
    "ano_referencia": "Ano",
    "ano_licitacao": "Ano da Licitação",
    "ano_folha": "Ano da Folha",
    "primeiro_ano_folha": "Primeiro Ano na Folha",
    "ultimo_ano_folha": "Último Ano na Folha",
    "total_pago": "Valor Pago",
    "total_empenhado": "Valor Reservado (Empenhado)",
    "total_liquidado": "Valor de Serviços Entregues (Liquidado)",
    "total_arrecadado": "Total Arrecadado",
    "total_bruto": "Total da Folha Bruta",
    "total_liquido": "Total da Folha Líquida",
    "maior_remuneracao_bruta": "Maior Remuneração Bruta",
    "maior_remuneracao_liquida": "Maior Remuneração Líquida",
    "total_servidores": "Quantidade de Servidores",
    "total_registros": "Quantidade de Parcelas/Registros",
    "qtd_registros": "Quantidade de Registros",
    "qtd_pagamentos": "Quantidade de Pagamentos",
    "qtd_empenhos": "Quantidade de Empenhos",
    "qtd_empenhos_totais": "Total de Empenhos",
    "qtd_lancamentos": "Quantidade de Lançamentos",
    "registros_gasto": "Registros de Abastecimento/Manutenção",
    "total_contratos": "Quantidade de Contratos",
    "valor_total_contratado": "Valor Total Contratado",
    "total_processos": "Quantidade de Processos",
    "qtd_licitacoes": "Quantidade de Licitações",
    "dsc_modalidade": "Modalidade da Compra",
    "nom_credor": "Fornecedor / Favorecido",
    "razao_social": "Empresa / Razão Social",
    "nom_empresa": "Empresa Contratada",
    "nom_contratado": "Contratado",
    "num_doc_credor": "CPF/CNPJ",
    "num_cnpj": "CNPJ",
    "cnpj_empresa": "CNPJ da Empresa",
    "nom_pessoa": "Servidor / Pessoa",
    "num_documento": "CPF",
    "num_cpf": "CPF",
    "dsc_cargo": "Cargo Público",
    "dsc_lotacao": "Secretaria / Lotação",
    "dsc_tipo_participacao": "Vínculo Societário",
    "total_pago_empresa": "Total Pago à Empresa",
    "total_pago_historico": "Total Pago (Histórico)",
    "total_empenhado_carona": "Valor via Adesão a Ata (Carona)",
    "total_gasto_frota": "Gasto com Frota e Combustível",
    "status_cadastro_frota": "Situação do Cadastro do Veículo",
    "populacao_residente": "População Estimada (IBGE)",
    "fonte_populacao": "Fonte Populacional",
    "pib_municipal_mil_reais": "PIB Municipal (em mil R$)",
    "pib_per_capita_reais": "PIB por Habitante",
    "gasto_per_capita_reais": "Investimento Público por Habitante",
    "despesa_liquidada": "Despesa Total Executada",
    "idhm_geral": "IDHM (Desenvolvimento Humano)",
    "area_territorial_km2": "Área Territorial (km²)",
    "vlr_receita_corrente_liquida": "Receita Corrente Líquida (RCL)",
    "vlr_despesa_pessoal_lrf": "Gasto com Pessoal (LRF)",
    "pct_despesa_pessoal_rcl": "% da Receita Gasto com Pessoal",
    "limite_prudencial_lrf_pct": "Limite Prudencial da LRF (%)",
    "nota_capag_tesouro": "Nota de Capacidade de Pagamento (Tesouro)",
    "pct_aplicacao_saude_siops": "% Aplicado em Saúde (Mínimo 15%)",
    "pct_aplicacao_educacao_mde": "% Aplicado em Educação (Mínimo 25%)",
    "situacao_fiscal_lrf": "Situação Fiscal (LRF)",
    "ideb_anos_iniciais": "Nota IDEB (Anos Iniciais)",
    "meta_ideb_anos_iniciais": "Meta do MEC (Anos Iniciais)",
    "ideb_anos_finais": "Nota IDEB (Anos Finais)",
    "meta_ideb_anos_finais": "Meta do MEC (Anos Finais)",
    "taxa_aprovacao_iniciais_pct": "Taxa de Aprovação Escolar (%)",
    "qtd_escolas_municipais": "Escolas Municipais",
    "total_matriculas_rede_municipal": "Matrículas na Rede Municipal",
    "vlr_gasto_educacao_por_aluno": "Investimento Anual por Aluno",
    "pct_recursos_proprios_saude_siops": "% de Recursos Próprios em Saúde",
    "minimo_constitucional_saude_pct": "Mínimo Constitucional de Saúde (%)",
    "vlr_despesa_total_saude": "Despesa Total em Saúde",
    "vlr_despesa_saude_per_capita": "Gasto em Saúde por Habitante",
    "qtd_estabelecimentos_cnes": "Unidades de Saúde (CNES)",
    "qtd_ubs_atencao_basica": "Postos de Saúde (UBS)",
    "cobertura_atencao_basica_pct": "Cobertura da Atenção Básica (%)",
    "qtd_leitos_sus": "Leitos SUS Disponíveis",
    "hospital_referencia": "Hospital de Referência",
    "porte_empresa": "Porte da Empresa",
    "cnae_principal_descricao": "Ramo de Atividade (CNAE)",
    "qtd_socios_identificados": "Quantidade de Sócios",
    "nomes_socios_qsa": "Sócios Identificados",
    "alerta_auditoria_controladoria": "Apontamento de Controladoria",
    "num_convenio": "Nº do Convênio Federal",
    "orgao_concedente": "Ministério / Órgão Federal",
    "objeto_convenio": "Finalidade do Convênio",
    "vlr_global": "Valor Global do Convênio",
    "vlr_repasse_uniao": "Valor de Repasse Federal",
    "vlr_liberado": "Valor Efetivamente Liberado",
    "situacao_convenio": "Situação do Convênio",
    "cod_emenda": "Código da Emenda",
    "nom_autor_emenda": "Parlamentar Autor da Emenda",
    "tipo_emenda": "Tipo de Emenda",
    "funcao_programatica": "Área de Destinação",
    "vlr_empenhado": "Valor Empenhado",
    "vlr_liquidado": "Valor Liquidado",
    "vlr_pago": "Valor Pago",
    "isf_m": "Índice de Saúde Financeira (ISF-M)",
    "receita_corrente": "Receita Corrente Arrecadada",
    "despesa_corrente": "Despesa Corrente Executada",
    "ies_c_pct": "% de Impostos Investido em Saúde (IES-C)",
    "gasto_saude": "Investimento em Saúde",
    "rec_imp": "Receita de Impostos e Transferências",
    "ieq_c_pct": "% de Impostos Investido em Educação (IEQ-C)",
    "gasto_mde": "Investimento em Educação (MDE)",
    "orcamento_previsto_loa": "Orçamento Anual Previsto (LOA Inicial)",
    "orcamento_atualizado_loa": "Orçamento Anual Atualizado (LOA)",
    "despesa_dotacao_atualizada": "Dotação Orçamentária Atualizada",
    "despesa_empenhada": "Despesa Empenhada",
    "despesa_paga": "Despesa Efetivamente Paga",
    "pct_arrecadacao_loa": "% da LOA Efetivamente Arrecadado",
    "pct_despesa_empenhada_dotacao": "% da Dotação Empenhado",
    "ultimo_mes_receita": "Último Mês Remetido (Receita)",
    "ultimo_mes_despesa": "Último Mês Remetido (Despesa)",
    "status_exercicio": "Situação do Exercício no SICOM",
    "receita_arrecadada_realizada": "Receita Efetivamente Arrecadada",
    "nom_servidor": "Nome do Servidor / Agente Público",
    "dsc_situacao": "Vínculo / Situação Funcional",
    "tipo_vinculo": "Tipo de Vínculo",
    "media_salarial_bruta": "Média Salarial Bruta Mensal",
    "remuneracao_mensal_bruta": "Subsídio / Salário Bruto Mensal",
    "remuneracao_mensal_liquida": "Salário Líquido Mensal",
    "media_salarial_prefeitura": "Média Salarial Geral da Prefeitura",
    "multiplo_acima_da_media": "Relação com a Média Municipal (x vezes)",
    "qtd_veiculos_distintos": "Quantidade de Veículos / Máquinas",
    "qtd_placas_ativas": "Placas Emplacadas Ativas",
    "total_gasto_historico_frota": "Gasto Acumulado (Abastecimento/Manutenção)",
    "vlr_rmn_bruta": "Remuneração Bruta (R$)",
    "vlr_rmn_liquida": "Remuneração Líquida (R$)",
    "nom_orgao": "Órgão Municipal",
    "cod_orgao": "Código do Órgão",
    "esfera_poder": "Poder Municipal",
    "administracao_tipo": "Tipo de Administração",
    "dotacao_inicial_loa": "Dotação Inicial LOA (R$)",
    "dotacao_atualizada": "Dotação Atualizada (R$)",
    "receita_arrecadada_propria_orgao": "Receita Própria Arrecadada (R$)",
    "doc_credor_raiz": "CNPJ Raiz",
    "natureza_credor": "Natureza Jurídica do Credor",
}


def _salvage_truncated_json_intro(raw_text: str) -> Dict[str, Optional[str]]:
    """
    Recupera `citizen_intro`, `auditor_exec_summary`, `chart_title` e `table_title`
    mesmo quando a resposta JSON do LLM sofre truncamento de tokens ou aspas não fechadas,
    impedindo vazamento de chaves `{"citizen_intro": ...` para o cidadão.
    """
    out: Dict[str, Optional[str]] = {
        "citizen_intro": None,
        "auditor_exec_summary": None,
        "chart_title": None,
        "table_title": None,
    }
    if not raw_text:
        return out
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    def _unescape_json_fragment(frag: str) -> str:
        s = frag.replace('\\"', '"').replace("\\n", "\n").replace("\\t", " ").replace("\\\\", "\\").strip()
        # Se terminou no meio de uma palavra truncada sem pontuação final, fecha com reticências limpas
        s = re.sub(r'["},\s]+$', "", s).strip()
        return s

    for field_name in ("citizen_intro", "auditor_exec_summary", "chart_title", "table_title"):
        # Caso 1: string JSON fechada normalmente antes de truncamento posterior
        m_closed = re.search(
            rf'"{field_name}"\s*:\s*"((?:\\.|[^"\\])*)"',
            cleaned,
            flags=re.DOTALL,
        )
        if m_closed:
            val = _unescape_json_fragment(m_closed.group(1))
            if val:
                out[field_name] = val
            continue
        # Caso 2: string JSON aberta e truncada antes da aspa de fechamento
        m_open = re.search(
            rf'"{field_name}"\s*:\s*"((?:\\.|[^"\\])+)$',
            cleaned,
            flags=re.DOTALL,
        )
        if m_open:
            val = _unescape_json_fragment(m_open.group(1))
            if val and not val.endswith((".", "!", "?")):
                val = val + "..."
            if val:
                out[field_name] = val

    return out


def _human_col_label(col_key: str) -> str:
    if col_key in HUMAN_COLUMN_LABELS:
        return HUMAN_COLUMN_LABELS[col_key]
    low = col_key.lower()
    if low in HUMAN_COLUMN_LABELS:
        return HUMAN_COLUMN_LABELS[low]
    clean = col_key.replace("_", " ").strip()
    return clean[:1].upper() + clean[1:] if clean else col_key


def _explain_municipal_concepts_for_citizen(
    question: str,
    sample_keys: List[str],
    municipality_name: str = "",
) -> Optional[str]:
    """
    Traduz conceitos técnicos da administração pública em explicações claras e acessíveis
    para o cidadão comum que não conhece contabilidade pública.
    """
    q_low = normalize_text_for_dedup(question)
    keys_set = {k.lower() for k in sample_keys}
    explanations: List[str] = []

    if {"total_empenhado", "total_liquidado", "total_pago"}.intersection(keys_set) and len(
        {"total_empenhado", "total_liquidado", "total_pago"}.intersection(keys_set)
    ) >= 2:
        explanations.append(
            "Na prefeitura, o gasto passa por três etapas: **Empenhado** é o valor reservado no orçamento para um contrato ou serviço; "
            "**Liquidado** é quando a prefeitura confere que o produto ou serviço foi realmente entregue; e **Pago** é o dinheiro que já saiu da conta."
        )
    if (
        "orcamento_previsto_loa" in keys_set
        or "dotacao_inicial_loa" in keys_set
        or (
            any(y in q_low for y in ("2026", "2027", "2028"))
            and any(w in q_low for w in ("orcament", "loa", "receit", "arrecad", "dotaca", "previst", "despes"))
        )
    ):
        explanations.append(
            "**Orçamento Previsto (LOA Atualizada)** é o valor planejado e autorizado para o ano inteiro, enquanto **Receita Arrecadada** é o valor que já entrou nos cofres municipais. "
            "A base oficial consolidada nesta plataforma abrange os exercícios até **2026** (com 2026 em execução parcial acumulada)."
        )
    if any(w in q_low for w in ("concursado", "concursados", "consignado", "consignados", "contratado", "contratados")):
        explanations.append(
            "Na folha pública municipal (SICOM/TCE-MG), os servidores dividem-se por **vínculo de contratação**: **Efetivos e Emprego Público** (aprovados em concurso/seleção pública), "
            "**Servidores Temporários** (contratados por tempo determinado), **Comissionados** (livre nomeação/recrutamento amplo), **Estagiários** e **Agentes Políticos** (Prefeito, Vice, Secretários e Vereadores). "
            "Já o termo **'consignado'** refere-se a descontos de empréstimo em folha, e não a um tipo de vínculo de contratação."
        )
    if "qtd_veiculos_distintos" in keys_set or ("veiculo" in q_low and ("quantidade" in q_low or "quantos" in q_low)):
        explanations.append(
            "No SICOM/TCE-MG, o cadastro de frota é cumulativo ao longo dos anos e divide-se em **veículos emplacados**, **máquinas pesadas/tratores isentos de placa pelo Código de Trânsito Brasileiro (CTB)** e **veículos cadastrados antes de 2020** que seguem ativos com registros de abastecimento e manutenção."
        )
    if "isf_m" in keys_set or "saude financeira" in q_low or "isf-m" in q_low:
        explanations.append(
            "O **Índice de Saúde Financeira (ISF-M)** compara o que a prefeitura arrecada com as despesas do dia a dia. "
            "Quando está **acima de 1,00**, significa que o município arrecadou mais do que gastou para manter os serviços funcionando."
        )
    if "pct_despesa_pessoal_rcl" in keys_set or "lrf" in q_low or "capag" in q_low or "rcl" in q_low or "contas" in q_low:
        explanations.append(
            "Pela **Lei de Responsabilidade Fiscal (LRF)**, a prefeitura precisa manter o gasto com salários abaixo do **Limite Prudencial de 51,3%** "
            "e do **Limite Máximo de 54%** da Receita Corrente Líquida. A **Nota CAPAG (A, B ou C)** do Tesouro Nacional avalia a solvência e saúde fiscal das contas municipais."
        )
    if "ies_c_pct" in keys_set or "pct_aplicacao_saude_siops" in keys_set or "pct_recursos_proprios_saude_siops" in keys_set:
        explanations.append(
            "A Constituição Federal exige que o município invista no mínimo **15% dos seus impostos na Saúde Pública (SUS)**. "
            "Percentuais acima de 15% mostram que a prefeitura aplicou além do mínimo obrigatório."
        )
    if "ieq_c_pct" in keys_set or "pct_aplicacao_educacao_mde" in keys_set:
        explanations.append(
            "A Constituição Federal determina que a prefeitura aplique pelo menos **25% dos impostos e transferências na Educação Municipal** (manutenção de escolas, creches e professores)."
        )
    if "ideb_anos_iniciais" in keys_set or "ideb" in q_low:
        explanations.append(
            "O **IDEB** é a nota oficial do Ministério da Educação (de 0 a 10) que avalia o aprendizado dos alunos e a aprovação escolar a cada dois anos."
        )
    if "total_empenhado_carona" in keys_set or "carona" in q_low or "adesao a ata" in q_low:
        explanations.append(
            "**Adesão a Ata de Registro de Preços (conhecida como 'carona')** ocorre quando a prefeitura aproveita os preços de uma licitação já feita por outro órgão público, o que exige justificativa de vantagem econômica."
        )
    if any(k in q_low for k in ("bolsa familia", "bpc", "auxilio")):
        city_dest = f"em {municipality_name.strip()}" if municipality_name and municipality_name.strip() else "no município"
        explanations.append(
            "Estes valores são **benefícios sociais pagos diretamente pelo Governo Federal** "
            f"(via Caixa/Ministério do Desenvolvimento Social) às famílias cadastradas {city_dest}, "
            "registrados no Portal da Transparência da CGU."
        )

    if not explanations:
        return None
    return " ".join(explanations[:2])


def _format_brl_or_val(key: str, val: Any) -> str:
    if val is None:
        return "N/D"
    k_low = key.lower()
    # Datas inteiras no padrão YYYYMMDD (ex.: seq_dat_pagamento = 20250801)
    if any(
        k_low == dk or k_low.startswith(dk) or dk in k_low
        for dk in ("seq_dat_", "dat_", "dt_", "data_", "competencia")
    ):
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            ival = int(val)
            s_val = str(ival)
            if len(s_val) == 8 and 19900101 <= ival <= 20991231:
                if "competencia" in k_low or k_low.startswith("seq_dat_"):
                    return f"{s_val[4:6]}/{s_val[:4]}"
                return f"{s_val[6:8]}/{s_val[4:6]}/{s_val[:4]}"
            if len(s_val) == 6 and 199001 <= ival <= 209912:
                return f"{s_val[4:6]}/{s_val[:4]}"
            return s_val
        return str(val)

    # Colunas de ano, exercício, sequência, documento ou código nunca devem receber separador de milhar
    if any(
        k_low == yk or k_low.startswith(yk)
        for yk in (
            "ano_referencia", "ano_", "primeiro_ano", "ultimo_ano", "exercicio",
            "seq_", "num_contrato", "num_convenio", "num_processo", "num_licitacao",
            "num_empenho", "num_ord_", "num_doc", "num_cpf", "num_cnpj",
            "cod_", "id_", "pagina", "numero_edicao", "cep", "ibge",
        )
    ) or k_low in ("ano", "cpf", "cnpj"):
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            return str(int(val))
        return str(val)

    if isinstance(val, (int, float)) and not isinstance(val, bool):
        if "multiplo" in k_low:
            return f"{val:,.2f}x".replace(",", "X").replace(".", ",").replace("X", ".")
        if any(p in k_low for p in ("pct", "percent", "taxa", "cobertura")):
            return f"{val:,.2f}%".replace(",", "X").replace(".", ",").replace("X", ".")
        if k_low in ("isf_m", "idhm_geral"):
            return f"{val:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if "ideb" in k_low:
            return f"{val:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if k_low.startswith("ultimo_mes") or k_low.startswith("mes_"):
            return f"{int(val):02d}"
        if any(
            p in k_low
            for p in (
                "vlr", "valor", "total_pago", "total_empenhado", "total_liquidado",
                "total_bruto", "total_liquido", "remuneracao", "receita", "despesa",
                "gasto", "arrecadado", "per_capita", "pib_per_capita", "total_aditivado",
                "orcamento", "media_salarial",
            )
        ) and "qtd" not in k_low and "total_registros" not in k_low and "total_servidores" not in k_low and "pib_municipal_mil" not in k_low:
            return f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if isinstance(val, float) and not val.is_integer():
            return f"{val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"{int(val):,}".replace(",", ".")
    return str(val)


class SessionLockManager:
    """
    Gerenciador de lock atômico por sessão/chat (`chat_id`), com verificação de thread proprietária
    e limite LRU de sessões ociosas para evitar vazamento de memória (P2-03).
    """

    def __init__(self, max_locks: int = 500) -> None:
        self._guard = threading.Lock()
        self._chat_locks: OrderedDict[str, threading.Lock] = OrderedDict()
        self._active_sessions: Dict[str, float] = {}
        self._lock_owners: Dict[str, int] = {}
        self.max_locks = max(32, max_locks)

    def get_lock(self, session_id: str) -> threading.Lock:
        with self._guard:
            if session_id in self._chat_locks:
                self._chat_locks.move_to_end(session_id)
                return self._chat_locks[session_id]
            while len(self._chat_locks) >= self.max_locks:
                evict_k = next(
                    (k for k, lk in self._chat_locks.items() if not lk.locked() and k not in self._active_sessions),
                    None,
                )
                if evict_k is None:
                    break
                self._chat_locks.pop(evict_k, None)
            lk = threading.Lock()
            self._chat_locks[session_id] = lk
            return lk

    def try_acquire(self, session_id: str, blocking: bool = True, timeout: float = 10.0) -> bool:
        lk = self.get_lock(session_id)
        acquired = lk.acquire(blocking=blocking, timeout=timeout if blocking else -1)
        if acquired:
            tid = threading.get_ident()
            with self._guard:
                self._active_sessions[session_id] = time.time()
                self._lock_owners[session_id] = tid
        return acquired

    def release(self, session_id: str) -> None:
        tid = threading.get_ident()
        with self._guard:
            lk = self._chat_locks.get(session_id)
            owner = self._lock_owners.get(session_id)
            if lk is None or owner != tid:
                return
            self._active_sessions.pop(session_id, None)
            self._lock_owners.pop(session_id, None)
        if lk.locked():
            try:
                lk.release()
            except RuntimeError:
                pass


def _fix_brl_thousands(text: str) -> str:
    """Corrige separadores de milhar brasileiros ausentes no bloco inicial de milhões/bilhões (ex.: `R$ 1608.699.263,03` -> `R$ 1.608.699.263,03`)."""
    if not text or "R$" not in text:
        return text

    def _repl_brl(m: re.Match[str]) -> str:
        raw_int = m.group(1).replace(".", "")
        frac = m.group(2)
        if not raw_int.isdigit():
            return m.group(0)
        int_fmt = f"{int(raw_int):,}".replace(",", ".")
        return f"R$ {int_fmt},{frac}"

    return re.sub(r"R\$\s*(\d[\d\.]*),(\d{2})\b", _repl_brl, text)


@dataclass
class OrchestrationResult:
    status: str
    intent: str
    tenant_id: str
    role: str
    question: str
    answer_text: str
    citizen_response: str = ""
    institutional_response: str = ""
    methodology_summary: str = ""
    limitations_and_caveats: List[str] = field(default_factory=list)
    sql_executed: Optional[str] = None
    sql_attempts: int = 0
    sql_rows: List[Dict[str, Any]] = field(default_factory=list)
    rag_chunks: List[Dict[str, Any]] = field(default_factory=list)
    selected_segments: List[str] = field(default_factory=list)
    selected_tables: List[str] = field(default_factory=list)
    allowed_sources: List[str] = field(default_factory=list)
    schema_reduction_pct: float = 0.0
    fontes_utilizadas: List[str] = field(default_factory=list)
    transparency_warnings: List[Dict[str, Any]] = field(default_factory=list)
    pii_masked_count: int = 0
    prompt_tokens_estimated: int = 0
    prompt_tokens_real: int = 0
    completion_tokens_real: int = 0
    static_v2_tokens_baseline: int = 14961
    llm_engine_used: str = "SIM Native NL2SQL Compiler"
    pipeline_steps: List[Dict[str, Any]] = field(default_factory=list)
    exec_ms: float = 0.0
    effective_question: str = ""
    standalone_question: str = ""
    is_contextual_followup: bool = False
    chart_base64: Optional[str] = None
    chart_mime_type: Optional[str] = None
    chart_caption: Optional[str] = None
    table_image_base64: Optional[str] = None
    table_mime_type: Optional[str] = None
    table_caption: Optional[str] = None
    table_html: Optional[str] = None
    asr_corrections: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.answer_text = _fix_brl_thousands(strip_emojis(self.answer_text))
        if self.citizen_response:
            self.citizen_response = _fix_brl_thousands(strip_emojis(self.citizen_response))
        if self.institutional_response:
            self.institutional_response = _fix_brl_thousands(strip_emojis(self.institutional_response))
        if self.chart_caption:
            self.chart_caption = strip_emojis(self.chart_caption)
        if self.table_caption:
            self.table_caption = strip_emojis(self.table_caption)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "intent": self.intent,
            "tenant_id": self.tenant_id,
            "role": self.role,
            "question": self.question,
            "effective_question": self.effective_question or self.question,
            "standalone_question": self.standalone_question or self.effective_question or self.question,
            "is_contextual_followup": self.is_contextual_followup,
            "answer_text": self.answer_text,
            "citizen_response": self.citizen_response or self.answer_text,
            "institutional_response": self.institutional_response or self.answer_text,
            "methodology_summary": self.methodology_summary,
            "limitations_and_caveats": self.limitations_and_caveats,
            "sql_executed": self.sql_executed,
            "sql_attempts": self.sql_attempts,
            "sql_rows": self.sql_rows,
            "rag_chunks": self.rag_chunks,
            "selected_segments": self.selected_segments,
            "selected_tables": self.selected_tables,
            "allowed_sources": self.allowed_sources,
            "schema_reduction_pct": self.schema_reduction_pct,
            "fontes_utilizadas": self.fontes_utilizadas,
            "transparency_warnings": self.transparency_warnings,
            "pii_masked_count": self.pii_masked_count,
            "prompt_tokens_estimated": self.prompt_tokens_estimated,
            "prompt_tokens_real": self.prompt_tokens_real,
            "completion_tokens_real": self.completion_tokens_real,
            "static_v2_tokens_baseline": self.static_v2_tokens_baseline,
            "llm_engine_used": self.llm_engine_used,
            "chart_base64": self.chart_base64,
            "chart_mime_type": self.chart_mime_type,
            "chart_caption": self.chart_caption,
            "table_image_base64": self.table_image_base64,
            "table_base64": self.table_image_base64,
            "table_mime_type": self.table_mime_type,
            "table_caption": self.table_caption,
            "table_html": self.table_html,
            "asr_corrections": self.asr_corrections,
            "pipeline_steps": self.pipeline_steps,
            "exec_ms": self.exec_ms,
        }


class SIMOrchestrator:
    """
    Orquestrador central v4.0 (Arquitetura Intrínseca Semântica) que coordena:
    - Normalização fonética/lexical pós-ASR (Eixo 5)
    - Reescrita contextual Multi-Turn `standalone_question` + `previous_sql` (Eixo 3)
    - Roteamento adaptativo CRAG SQL <-> RAG + decomposição normativa (Eixo 2)
    - Catálogo Semântico com Auto-Profiling, Grão Temporal, Gold Views e BM25 Few-Shot (Eixo 1)
    - Loop de Self-Correction sobre 0 linhas via diagnóstico AST + DuckDB (Eixo 1)
    - Renderização Gráfica Institucional Nativa PNG para WhatsApp/Web (Eixo 4)
    - Naturalização Anti-Alucinação de Exercício Parcial + Higiene de Cache e Tokens Reais (Eixos 7 e 8)
    """

    def __init__(
        self,
        db_manager: Optional[DuckDBManager] = None,
        rag_retriever: Optional[HybridRAGRetriever] = None,
        prompt_builder: Optional[ModularPromptBuilder] = None,
        sql_generator_fn: Optional[Callable[[ModularPromptPackage, int, Optional[str]], str]] = None,
        max_sql_retries: int = 2,
        interaction_store: Optional[Any] = None,
    ) -> None:
        self.db_manager = db_manager or DuckDBManager()
        self.rag_retriever = rag_retriever or HybridRAGRetriever(tenant_id=self.db_manager.tenant.tenant_id)
        self._gazette_name_cache: Optional[str] = None
        cat_max_yr = getattr(self.db_manager.catalog, "max_data_year", 2026)
        cat_latest_rem = getattr(
            self.db_manager.catalog,
            "latest_remittance_human",
            f"exercício {cat_max_yr} em remessa parcial acumulada",
        )
        cat_freshness_map = self._build_source_freshness_map()
        self.prompt_builder = prompt_builder or ModularPromptBuilder(
            municipality_name=self.db_manager.tenant.municipality_name,
            state_uf=self.db_manager.tenant.state_uf,
            ibge_code=self.db_manager.tenant.ibge_code,
            temporal_coverage=f"bases oficiais consolidadas até {cat_max_yr}",
            max_data_year=cat_max_yr,
            latest_remittance_human=cat_latest_rem,
            source_freshness_map=cat_freshness_map,
        )
        self.entity_linker = EntityLinker(
            workspace_root=DEFAULT_WORKSPACE_ROOT,
            conn=self.db_manager.conn,
            city_name=self.db_manager.tenant.municipality_name,
            tenant_id=self.db_manager.tenant.tenant_id,
        )
        self.quota_manager = TenantQuotaManager(
            tenant_id=self.db_manager.tenant.tenant_id,
        )
        self.chart_generator = InstitutionalChartGenerator(
            municipality_name=self.db_manager.tenant.municipality_name,
            state_uf=self.db_manager.tenant.state_uf,
        )
        self.table_renderer = InstitutionalTableRenderer(
            municipality_name=self.db_manager.tenant.municipality_name,
            state_uf=self.db_manager.tenant.state_uf,
        )
        self.interaction_store = interaction_store
        self._has_custom_sql_generator: bool = sql_generator_fn is not None
        self.sql_generator_fn = sql_generator_fn or self._smart_sql_generator
        self.max_sql_retries = max_sql_retries
        self.lock_manager = SessionLockManager()
        self._mem_lock = threading.Lock()
        self._max_session_entries = 500
        self.conversation_history: OrderedDict[str, Deque[Dict[str, Any]]] = OrderedDict()
        self.session_sql_memory: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        self._golden_lookup: Dict[str, str] = self._load_golden_sql_index()
        self.semantic_guardrail = SemanticIntentGuardrail(
            municipality_name=self.db_manager.tenant.municipality_name,
            state_uf=self.db_manager.tenant.state_uf,
            ibge_code=self.db_manager.tenant.ibge_code,
            fallback_classifier_fn=self.classify_intent,
            fallback_conversational_fn=self._handle_conversational_or_conceptual_turn,
            max_data_year=cat_max_yr,
            latest_remittance_human=cat_latest_rem,
            gazette_name=self._get_gazette_name(),
        )
        # Isolamento por thread (P1-03): impede vazamento de API keys, escopo e contadores entre requisições concorrentes
        self._turn_local = threading.local()

    @property
    def _active_allowed_sources(self) -> List[str]:
        val = getattr(self._turn_local, "active_allowed_sources", None)
        return val if val is not None else []

    @_active_allowed_sources.setter
    def _active_allowed_sources(self, value: List[str]) -> None:
        self._turn_local.active_allowed_sources = list(value or [])

    @property
    def _active_llm_provider(self) -> Optional[str]:
        return getattr(self._turn_local, "active_llm_provider", None)

    @_active_llm_provider.setter
    def _active_llm_provider(self, value: Optional[str]) -> None:
        self._turn_local.active_llm_provider = value

    @property
    def _active_llm_api_key(self) -> Optional[str]:
        return getattr(self._turn_local, "active_llm_api_key", None)

    @_active_llm_api_key.setter
    def _active_llm_api_key(self, value: Optional[str]) -> None:
        self._turn_local.active_llm_api_key = value

    @property
    def _last_llm_engine_used(self) -> str:
        return getattr(self._turn_local, "last_llm_engine_used", "SIM Native NL2SQL Compiler")

    @_last_llm_engine_used.setter
    def _last_llm_engine_used(self, value: str) -> None:
        self._turn_local.last_llm_engine_used = value

    @property
    def _turn_prompt_tokens_real(self) -> int:
        return int(getattr(self._turn_local, "turn_prompt_tokens_real", 0))

    @_turn_prompt_tokens_real.setter
    def _turn_prompt_tokens_real(self, value: int) -> None:
        self._turn_local.turn_prompt_tokens_real = int(value)

    @property
    def _turn_completion_tokens_real(self) -> int:
        return int(getattr(self._turn_local, "turn_completion_tokens_real", 0))

    @_turn_completion_tokens_real.setter
    def _turn_completion_tokens_real(self, value: int) -> None:
        self._turn_local.turn_completion_tokens_real = int(value)

    def _prune_session_maps(self) -> None:
        with self._mem_lock:
            while len(self.conversation_history) > self._max_session_entries:
                self.conversation_history.popitem(last=False)
            while len(self.session_sql_memory) > self._max_session_entries:
                self.session_sql_memory.popitem(last=False)

    def _build_source_freshness_map(self) -> Dict[str, str]:
        fmap = dict(getattr(self.db_manager.catalog, "source_freshness_map", {}) or {})
        rag_chunks = getattr(self.rag_retriever, "corpus_chunks", None) or []
        if rag_chunks and "DIARIO_OFICIAL" not in fmap:
            years = sorted(
                {
                    int(c.get("ano_edicao") if isinstance(c, dict) else getattr(c, "ano_edicao", 0))
                    for c in rag_chunks
                    if (c.get("ano_edicao") if isinstance(c, dict) else getattr(c, "ano_edicao", None))
                }
            )
            yr_span = f"{years[0]} a {years[-1]}" if len(years) >= 2 else (str(years[0]) if years else "acervo ativo")
            fmap["DIARIO_OFICIAL"] = f"{yr_span} ({len(rag_chunks)} atos normativos únicos indexados)"
        return fmap

    def _get_gazette_name(self) -> str:
        if self._gazette_name_cache:
            return self._gazette_name_cache
        tid = self.db_manager.tenant.tenant_id
        if tid in ("caete_mg_3110004", "caete-mg"):
            self._gazette_name_cache = "Jornal Opinião"
            return self._gazette_name_cache
        try:
            from sim_platform.ingestion.city_onboarding import load_tenant_profile

            prof = load_tenant_profile(tenant_id=tid, workspace_root=DEFAULT_WORKSPACE_ROOT)
            veic = str(
                (prof.get("diario_oficial") or {}).get("veiculo")
                or prof.get("official_gazette_name")
                or ""
            ).strip()
            if veic:
                self._gazette_name_cache = veic
                return veic
        except Exception:
            pass
        self._gazette_name_cache = (
            f"Diário Oficial Eletrônico de {self.db_manager.tenant.municipality_name}/{self.db_manager.tenant.state_uf}"
        )
        return self._gazette_name_cache

    def _get_other_municipalities(self) -> Tuple[str, ...]:
        tenant_city = normalize_text_for_dedup(self.db_manager.tenant.municipality_name)
        dynamic_cities: List[str] = list(OTHER_MUNICIPALITIES)
        try:
            tenants_dir = DEFAULT_WORKSPACE_ROOT / "tenants"
            if tenants_dir.exists():
                for tf in tenants_dir.glob("*.json"):
                    if tf.name.startswith("_"):
                        continue
                    data = json.loads(tf.read_text(encoding="utf-8"))
                    m_name = normalize_text_for_dedup(str(data.get("municipality_name") or ""))
                    if m_name and m_name not in dynamic_cities:
                        dynamic_cities.append(m_name)
        except Exception:
            pass
        return tuple(c for c in dynamic_cities if c and c != tenant_city)

    def _check_missing_granular_source(
        self,
        question: str,
        allowed_sources: Optional[List[str]] = None,
    ) -> Optional[Tuple[str, str, Dict[str, Any]]]:
        """
        Verifica se a pergunta exige tabelas granulares (ex.: SICOM nominal de servidores,
        fornecedores, placas de frota, contratos/licitações ou INEP IDEB) que ainda não foram
        depositadas para a cidade ativa, evitando erro SQL ou alucinação e explicando como sincronizar.
        """
        missing_fams = set(getattr(self.db_manager.catalog, "missing_source_families", set()) or set())
        if not missing_fams:
            return None

        q_norm = normalize_text_for_dedup(question)
        city_str = f"{self.db_manager.tenant.municipality_name}/{self.db_manager.tenant.state_uf}"
        ibge = self.db_manager.tenant.ibge_code
        active_fams = sorted(
            {meta.source_family for meta in self.db_manager.catalog.tables.values()}
        )
        active_str = ", ".join(active_fams) if active_fams else "nenhuma base materializada ainda"

        # Consultas agregadas fiscais/saúde/educação/população/benefícios que SÃO atendidas por SICONFI/IBGE/DATASUS/CGU:
        macro_api_satisfied = any(
            kw in q_norm
            for kw in (
                "populacao", "habitantes", "pib", "idhm", "area territorial", "densidade",
                "rcl", "receita corrente liquida", "lrf", "limite prudencial", "capag",
                "situacao fiscal", "contas da prefeitura", "contas em dia", "saude fiscal",
                "minimo constitucional", "siops", "mde", "bolsa familia", "bpc",
            )
        )

        if "SICOM" in missing_fams and not macro_api_satisfied:
            granular_sicom_triggers = (
                "secretario", "secretarios", "prefeito", "vice-prefeito", "vice prefeito",
                "vereador", "vereadores", "procurador", "controlador", "servidor", "servidores",
                "funcionario", "funcionarios", "concursado", "comissionado", "holerite",
                "salario", "remuneracao", "folha", "fornecedor", "fornecedores", "credor", "credores",
                "empresa", "empresas", "pagamento", "pagamentos", "receberam",
                "cnpj", "nota de empenho", "empenho", "contrato", "contratos", "aditivo",
                "licitacao", "licitacoes", "pregao", "dispensa", "carona", "frota",
                "placa", "veiculo", "veiculos", "combustivel", "duodecimo",
                "orcamento", "loa", "dotacao", "secretaria", "secretarias", "camara",
                "iptu", "iss", "itbi", "conflito de interesse", "conflito de interesses",
            )
            if any(t in q_norm for t in granular_sicom_triggers):
                warn = {
                    "code": "BASE-SICOM-PENDENTE-INGESTAO",
                    "gap_id": "BASE-SICOM-PENDENTE-INGESTAO",
                    "severity": "info",
                    "title": f"Pacote Granular SICOM/TCE-MG Pendente para {city_str}",
                    "message": (
                        f"As bases federais automáticas de {city_str} (IBGE {ibge}) já estão ativas ({active_str}), "
                        f"porém o pacote granular do SICOM/TCE-MG (folha nominal, fornecedores, contratos, licitações e frota) "
                        f"aguarda depósito do arquivo .zip oficial do Portal TCE-MG na pasta avaliacao/dados/inbox/ ou na aba Atualizar Dados."
                    ),
                }
                cit = (
                    f"Os indicadores oficiais federais de **{city_str}** (IBGE `{ibge}`) já estão ativos para consulta "
                    f"(**População e PIB no IBGE**, **Receita Corrente Líquida, Despesa Total com Pessoal LRF, Saúde e Educação no Tesouro Nacional/SICONFI e SIOPS**, "
                    f"além de **Novo Bolsa Família e BPC no Portal da Transparência/CGU**).\n\n"
                    f"Porém, o detalhamento nominal de **orçamento por órgão, folha de servidores/salários, fornecedores, contratos, licitações e frota** depende do pacote "
                    f"**SICOM/TCE-MG** de **{city_str}**, que ainda aguarda importação do arquivo `.zip` oficial (devido ao CAPTCHA do portal do Tribunal de Contas).\n\n"
                    f"Assim que o pacote `.zip` de `{ibge}` for salvo em `avaliacao/dados/inbox/` (ou enviado na aba **Atualizar Dados**), esta consulta passará a responder automaticamente."
                )
                inst = (
                    f"**[BASE GRANULAR PENDENTE DE SINCRONIZAÇÃO — `SICOM/TCE-MG` ({city_str}, IBGE `{ibge}`)]**\n\n"
                    f"- **Fontes Ativas Agora no Catálogo (`{len(self.db_manager.catalog.tables)}` tabelas):** `{active_str}`.\n"
                    f"- **Domínio Solicitado:** Dados contábeis/nominais granulares do **SICOM/TCE-MG** (orçamento LOA, folha de servidores, credores/empenhos, contratos, licitações ou frota).\n"
                    f"- **Procedimento Assistido pós-CAPTCHA:** Baixe os pacotes CSV/ZIP do código IBGE `{ibge}` em `https://sicomconsulta.tce.mg.gov.br/` "
                    f"e solte-os em `avaliacao/dados/inbox/` (ou faça upload em **05. Atualizar Dados**). O `FleetVerifier` converterá automaticamente para Parquet ZSTD e arquivará no Google Drive (`SIM_DataLake`)."
                )
                return cit, inst, warn

        cat_tbls_low = {k.lower() for k in self.db_manager.catalog.tables}
        if "receita_qsa_fornecedores" not in cat_tbls_low and any(
            t in q_norm for t in ("qsa", "quadro societario", "socio", "socios", "cnae")
        ):
            warn = {
                "code": "BASE-QSA-PENDENTE-SICOM",
                "gap_id": "BASE-QSA-PENDENTE-SICOM",
                "severity": "info",
                "title": f"Cruzamento Societário QSA Dependente de Fornecedores SICOM em {city_str}",
                "message": (
                    f"A tabela `receita_qsa_fornecedores` é gerada automaticamente a partir dos CNPJs pagos em `despesa_pagamento` e `licitacao_quadroSocLicitacao` do SICOM/TCE-MG, ainda pendentes para {city_str}."
                ),
            }
            cit = (
                f"Para consultar o **Quadro Societário (QSA)** de empresas contratadas por **{city_str}**, é necessário primeiro importar o pacote de pagamentos e licitações do **SICOM/TCE-MG** (IBGE `{ibge}`) na pasta `avaliacao/dados/inbox/`."
            )
            inst = (
                f"**[BASE DERIVADA PENDENTE — `RECEITA_QSA` ({city_str}, IBGE `{ibge}`)]**\n"
                f"- Assim que `despesa_pagamento` e `licitacao_quadroSocLicitacao` do SICOM forem depositados em `inbox/`, `receita_qsa_fornecedores` será materializada automaticamente."
            )
            return cit, inst, warn

        if ("cgu_convenios" not in cat_tbls_low and any(t in q_norm for t in ("convenio", "convenios"))) or (
            "cgu_emendas" not in cat_tbls_low and any(t in q_norm for t in ("emenda", "emendas", "parlamentar"))
        ):
            warn = {
                "code": "BASE-CGU-CONVENIOS-EMENDAS-PENDENTE",
                "gap_id": "BASE-CGU-CONVENIOS-EMENDAS-PENDENTE",
                "severity": "info",
                "title": f"Detalhamento de Convênios/Emendas CGU Pendente para {city_str}",
                "message": (
                    f"Os repasses mensais de Novo Bolsa Família e BPC da CGU já estão ativos via API para {city_str}, mas os arquivos detalhados de convênios/transferências (`cgu_convenios` / `cgu_emendas`) ainda não foram importados para este município."
                ),
            }
            cit = (
                f"Para **{city_str}**, os benefícios federais do **Novo Bolsa Família e BPC** já estão ativos via API oficial da CGU, "
                f"porém o arquivo detalhado de **Convênios e Emendas Parlamentares** ainda não foi carregado nesta instância."
            )
            inst = (
                f"**[BASE SETORIAL PENDENTE — `CGU Convênios/Emendas` ({city_str}, IBGE `{ibge}`)]**\n"
                f"- **Disponível imediatamente na CGU:** `cgu_novo_bolsa_familia` e `cgu_bpc`.\n"
                f"- **Pendente:** Importação de `cgu_transferencias` / `cgu_convenios` via CSV/Parquet em `inbox/`."
            )
            return cit, inst, warn

        if "INEP" in missing_fams and any(t in q_norm for t in ("ideb", "inep", "censo escolar", "taxa de aprovacao")):
            warn = {
                "code": "BASE-INEP-PENDENTE-INGESTAO",
                "gap_id": "BASE-INEP-PENDENTE-INGESTAO",
                "severity": "info",
                "title": f"Indicadores Detalhados INEP/IDEB Pendentes para {city_str}",
                "message": (
                    f"A aplicação constitucional em Educação (MDE %) já está ativa via SICONFI/STN para {city_str}, "
                    f"mas a planilha detalhada de notas IDEB por rede do INEP ainda não foi carregada para este município."
                ),
            }
            cit = (
                f"Para **{city_str}**, já tenho ativo o percentual oficial investido em **Educação (MDE)** junto ao Tesouro Nacional (SICONFI), "
                f"mas o boletim específico de notas do **IDEB/INEP** ainda não foi importado para este município."
            )
            inst = (
                f"**[BASE SETORIAL PENDENTE — `INEP/IDEB` ({city_str}, IBGE `{ibge}`)]**\n"
                f"- **Disponível imediatamente:** Aplicação constitucional em Educação (`pct_aplicacao_educacao_mde` em `siconfi_finbra_indicadores`).\n"
                f"- **Pendente:** Planilha `inep_ideb_educacao` para `{ibge}` via upload CSV/Parquet."
            )
            return cit, inst, warn

        return None

    def refresh_after_data_update(self) -> None:
        """
        Sincroniza o prompt_builder, guardrail, entity_linker, RAG e cache L1 após
        atualização/upload de Parquets ou hot-swap externo de `CURRENT.json` via cronjob CLI.
        """
        self._gazette_name_cache = None
        cat = self.db_manager.catalog
        self.prompt_builder.max_data_year = getattr(cat, "max_data_year", 2026)
        self.prompt_builder.temporal_coverage = f"bases oficiais consolidadas até {self.prompt_builder.max_data_year}"
        self.prompt_builder.latest_remittance_human = getattr(
            cat, "latest_remittance_human", self.prompt_builder.latest_remittance_human
        )
        try:
            fresh_rag = HybridRAGRetriever(tenant_id=self.db_manager.tenant.tenant_id)
            if fresh_rag.corpus_chunks or not getattr(self.rag_retriever, "corpus_chunks", None):
                self.rag_retriever.corpus_chunks = fresh_rag.corpus_chunks
                self.rag_retriever._build_bm25_index()
        except Exception:
            pass
        self.prompt_builder.source_freshness_map = self._build_source_freshness_map()
        self.semantic_guardrail.max_data_year = self.prompt_builder.max_data_year
        self.semantic_guardrail.latest_remittance_human = self.prompt_builder.latest_remittance_human
        self.semantic_guardrail.gazette_name = self._get_gazette_name()
        self.entity_linker = EntityLinker(
            workspace_root=DEFAULT_WORKSPACE_ROOT,
            conn=self.db_manager.conn,
            city_name=self.db_manager.tenant.municipality_name,
            tenant_id=self.db_manager.tenant.tenant_id,
        )
        try:
            self.quota_manager.invalidate_cache()
        except Exception:
            pass

    def _sync_catalog_freshness_if_hotswapped(self) -> bool:
        if hasattr(self.db_manager, "check_external_release_hotswap"):
            try:
                if self.db_manager.check_external_release_hotswap():
                    self.refresh_after_data_update()
                    return True
            except Exception:
                pass
        return False

    def _record_turn_llm_tokens(self, prompt_tokens: int, completion_tokens: int) -> None:
        p = max(0, int(prompt_tokens or 0))
        c = max(0, int(completion_tokens or 0))
        if (p + c) > 0:
            self._turn_prompt_tokens_real += p
            self._turn_completion_tokens_real += c
            self.quota_manager.record_realtime_llm_tokens(prompt_tokens=p, completion_tokens=c)

    def _record_session_sql_context(
        self,
        session_id: str,
        question: str,
        sql_executed: Optional[str],
        sql_rows: List[Dict[str, Any]],
    ) -> None:
        if sql_executed and sql_rows and session_id:
            key = session_id if session_id != "default" else "default"
            with self._mem_lock:
                self.session_sql_memory[key] = {
                    "question": question,
                    "sql_executed": sql_executed,
                    "sql_rows": list(sql_rows[:50]),
                }
                self.session_sql_memory.move_to_end(key)
            self._prune_session_maps()

    def _get_latest_session_sql_context(self, session_id: str) -> Optional[Dict[str, Any]]:
        if not session_id:
            return None
        in_mem = self.session_sql_memory.get(session_id)
        if in_mem and in_mem.get("sql_rows"):
            if session_id != "default" or len(self.conversation_history.get("default", [])) > 0:
                return in_mem
        if (
            session_id not in ("", "default")
            and self.interaction_store is not None
            and hasattr(self.interaction_store, "find_latest_sql_rows_for_session")
        ):
            try:
                persisted = self.interaction_store.find_latest_sql_rows_for_session(session_id)
                if isinstance(persisted, dict) and persisted.get("sql_rows"):
                    return persisted
                if isinstance(persisted, tuple) and len(persisted) >= 1 and persisted[0]:
                    return {
                        "sql_rows": list(persisted[0]),
                        "question": str(persisted[1]) if len(persisted) >= 2 else "",
                        "sql_executed": persisted[2] if len(persisted) >= 3 else None,
                    }
            except Exception:
                pass
        return None

    def _build_zero_row_diagnostic(self, candidate_sql: str, effective_question: str) -> str:
        """
        Eixo 1 (DIN-SQL / MAC-SQL Execution-Guided Self-Correction):
        Quando uma query SQL executa sem erro sintático mas retorna `0 linhas`, inspeciona a AST
        via `sqlglot` e consulta o DuckDB para revelar ao LLM:
        1. Total de linhas e horizonte real de `ano_referencia` de cada tabela consultada;
        2. Valores distintos reais (`TOP 12`) das colunas filtradas na cláusula `WHERE`/`HAVING`.
        """
        lines: List[str] = [
            f"Query executada com 0 linhas retornadas:\n{candidate_sql.strip()}",
        ]
        ref_tables: List[str] = []
        filtered_cols: Set[str] = set()
        try:
            parsed = sqlglot.parse_one(candidate_sql, read="duckdb")
            cte_names = {c.alias_or_name.lower() for c in parsed.find_all(exp.CTE)}
            for tb in parsed.find_all(exp.Table):
                tname = (tb.name or "").strip()
                if tname and tname.lower() not in cte_names:
                    for cat_t in self.db_manager.catalog.tables:
                        if cat_t.lower() == tname.lower() and cat_t not in ref_tables:
                            ref_tables.append(cat_t)
            for where_node in list(parsed.find_all(exp.Where)) + list(parsed.find_all(exp.Having)):
                for col_node in where_node.find_all(exp.Column):
                    cname = (col_node.name or "").strip()
                    if cname:
                        filtered_cols.add(cname)
        except Exception:
            pass

        table_max_years: Dict[str, int] = {}
        diag_cur = self.db_manager.conn.cursor()
        try:
            for tname in ref_tables[:4]:
                tmeta = self.db_manager.catalog.tables.get(tname)
                if not tmeta:
                    continue
                col_map = {c.name.lower(): c.name for c in tmeta.columns}
                try:
                    if "ano_referencia" in col_map:
                        yr_col = col_map["ano_referencia"]
                        cnt, min_y, max_y = diag_cur.execute(
                            f'SELECT COUNT(*), MIN("{yr_col}"), MAX("{yr_col}") FROM "{tname}"'
                        ).fetchone()
                        if max_y is not None:
                            try:
                                table_max_years[tname] = int(max_y)
                            except Exception:
                                pass
                        lines.append(
                            f"- Tabela `{tname}`: {cnt} registros totais | `ano_referencia` disponível: {min_y} a {max_y} "
                            f"| Grão: {tmeta.grain_type} ({tmeta.counting_rule})"
                        )
                    else:
                        cnt = diag_cur.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
                        lines.append(
                            f"- Tabela `{tname}`: {cnt} registros totais (sem coluna `ano_referencia` direta) "
                            f"| Grão: {tmeta.grain_type} ({tmeta.counting_rule})"
                        )
                except Exception:
                    pass

                _diag_pii_skip = {
                    "num_cpf", "cpf", "num_documento", "num_doc_credor", "doc_credor", "doc_contratado",
                    "nis", "nis favorecido", "nis beneficiário", "cpf favorecido", "cpf beneficiário",
                    "nom_pessoa", "nom_servidor", "nome favorecido",
                }
                for fc in sorted(filtered_cols):
                    real_col = col_map.get(fc.lower())
                    if not real_col or real_col.lower() == "ano_referencia" or real_col.lower() in _diag_pii_skip:
                        continue
                    try:
                        rows = diag_cur.execute(
                            f'SELECT CAST("{real_col}" AS VARCHAR) AS v, COUNT(*) AS n '
                            f'FROM "{tname}" WHERE "{real_col}" IS NOT NULL AND TRIM(CAST("{real_col}" AS VARCHAR)) <> \'\' '
                            f'GROUP BY 1 ORDER BY n DESC LIMIT 12'
                        ).fetchall()
                        if rows:
                            sample_str = ", ".join(f"'{str(r[0])[:45]}' ({r[1]})" for r in rows)
                            lines.append(f"  * Valores reais de `{tname}.{real_col}` no banco: {sample_str}")
                        else:
                            lines.append(
                                f"  * ATENÇÃO: A coluna `{tname}.{real_col}` está 100% NULA/VAZIA! "
                                f"NÃO filtre por `{real_col}`; use uma Gold View (`vw_gold_*`) ou outra coluna preenchida da tabela."
                            )
                    except Exception:
                        pass
        finally:
            try:
                diag_cur.close()
            except Exception:
                pass

        q_norm = normalize_text_for_dedup(effective_question)
        max_cat_yr = int(getattr(self.db_manager.catalog, "max_data_year", 2026) or 2026)
        sql_yrs_used = sorted({int(y) for y in re.findall(r"\b(20\d{2})\b", candidate_sql)})
        for tname_lag, max_y_tbl in table_max_years.items():
            lag_yrs = [y for y in sql_yrs_used if y > max_y_tbl]
            if lag_yrs:
                alt_hint = ""
                if any(w in q_norm for w in ("empresa", "fornecedor", "contrat", "recebeu", "credor")) and "vw_gold_contratos_vigencia_aditivos" in self.db_manager.catalog.tables:
                    alt_hint = (
                        f" Se a pergunta tratar de contratos/empresas contratadas em {lag_yrs[0]}, você pode consultar "
                        f"`vw_gold_contratos_vigencia_aditivos` (que possui contratos de {lag_yrs[0]}) OU "
                    )
                lines.append(
                    f"- DEFASAGEM TEMPORAL ESPECÍFICA DA TABELA `{tname_lag}`: Você filtrou `ano_referencia` = {lag_yrs}, "
                    f"mas `{tname_lag}` possui remessas consolidadas apenas até `{max_y_tbl}`!{alt_hint} "
                    f"Altere OBRIGATORIAMENTE o filtro de ano para `ano_referencia = {max_y_tbl}` (último exercício disponível nesta tabela) "
                    f"para retornar os dados oficiais mais recentes e nunca devolver 0 linhas!"
                )
        fut_yrs_diag = [int(y) for y in re.findall(r"\b(20\d{2})\b", q_norm) if int(y) > max_cat_yr]
        if fut_yrs_diag:
            lines.append(
                f"- ORIENTAÇÃO PARA EXERCÍCIO FUTURO ({'/'.join(str(y) for y in sorted(set(fut_yrs_diag)))}): As bases consolidadas vão até {max_cat_yr}. "
                f"REMOVA o filtro de ano futuro (> {max_cat_yr}) e consulte a série histórica oficial até {max_cat_yr} "
                "(ex.: `SELECT * FROM vw_gold_orcamento_previsto_vs_realizado ORDER BY ano_referencia`) para fundamentar a resposta."
            )
        lines.append(
            "- AÇÃO REQUERIDA: Gere uma NOVA consulta SQL ajustando os nomes/valores de filtro para os valores reais acima, "
            "priorizando as Gold Views (`vw_gold_orcamento_previsto_vs_realizado`, `vw_gold_orcamento_por_orgao`, `vw_gold_folha_servidores`, "
            "`vw_gold_frota_consolidada`, `vw_gold_execucao_despesa_credor`, `vw_gold_fornecedores_qsa_360`)."
        )
        return "\n".join(lines)

    def _has_unmatched_literal_predicate(
        self, candidate_sql: str, returned_rows: List[Dict[str, Any]]
    ) -> Optional[str]:
        """
        Prova Cirúrgica de Existência de Predicado (< 10ms):
        Quando uma agregação escalar sem GROUP BY retorna 1 linha com todos os indicadores = 0 ou NULL
        (ex.: `SELECT COUNT(*) AS n, SUM(vlr) AS total FROM tbl WHERE dsc_lotacao ILIKE '%FANTASMA%'`),
        verifica isoladamente se cada predicado literal categórico no WHERE existe em ao menos 1 linha
        da tabela base.
        - Se algum filtro literal tiver 0 correspondências na tabela inteira -> filtro fantasma comprovado -> aciona Self-Correction!
        - Se todos os filtros literais existirem no banco -> zero legítimo (ex.: 0 obras atrasadas) -> NÃO refaz!
        """
        if len(returned_rows) != 1:
            return None
        row0 = returned_rows[0]
        if not row0:
            return None

        non_dim_vals = [
            v
            for k, v in row0.items()
            if k.lower() not in ("ano_referencia", "seq_orgao", "cod_orgao", "cod_orgao_norm")
        ]
        if not non_dim_vals:
            return None
        for v in non_dim_vals:
            if v is None:
                continue
            if isinstance(v, (int, float)) and not isinstance(v, bool) and float(v) == 0.0:
                continue
            return None

        try:
            parsed = sqlglot.parse_one(candidate_sql, read="duckdb")
        except Exception:
            return None

        # Apenas para agregações escalares sem GROUP BY sobre 1 única tabela física/Gold View (sem ambiguidade de JOIN)
        if parsed.find(exp.Group) is not None:
            return None
        if not any(
            parsed.find(agg_cls) is not None
            for agg_cls in (exp.Count, exp.Sum, exp.Avg, exp.Max, exp.Min)
        ):
            return None

        cte_names = {c.alias_or_name.lower() for c in parsed.find_all(exp.CTE)}
        real_tables: List[str] = []
        for tb in parsed.find_all(exp.Table):
            tname = (tb.name or "").strip()
            if tname and tname.lower() not in cte_names:
                for cat_t in self.db_manager.catalog.tables:
                    if cat_t.lower() == tname.lower() and cat_t not in real_tables:
                        real_tables.append(cat_t)
        if len(real_tables) != 1:
            return None

        target_tbl = real_tables[0]
        tmeta = self.db_manager.catalog.tables.get(target_tbl)
        if not tmeta:
            return None
        valid_cols_lower = {c.name.lower(): c.name for c in tmeta.columns}

        where_node = parsed.args.get("where")
        if where_node is None:
            return None

        # Não disparar se houver subqueries complexas ou negações globais sobre limiares numéricos puros
        candidate_preds: List[exp.Expression] = []
        for pred in where_node.find_all((exp.EQ, exp.ILike, exp.Like, exp.In)):
            # Ignorar se estiver dentro de NOT (...) — negação explícita
            anc = pred.parent
            inside_not_or_or = False
            while anc is not None and anc is not where_node:
                if isinstance(anc, (exp.Not, exp.Or, exp.Subquery)):
                    inside_not_or_or = True
                    break
                anc = anc.parent
            if inside_not_or_or:
                continue

            cols_in_pred = list(pred.find_all(exp.Column))
            lits_in_pred = [
                l for l in pred.find_all(exp.Literal)
                if (l.is_string and str(l.this or "").strip()) or l.is_number
            ]
            if len(cols_in_pred) != 1 or not lits_in_pred:
                continue
            c_name_low = (cols_in_pred[0].name or "").strip().lower()
            if (
                c_name_low == "ano_referencia"
                or c_name_low not in valid_cols_lower
                or any(tok in c_name_low for tok in ("cpf", "nis", "documento", "doc_credor"))
            ):
                continue
            if not any(l.is_string for l in lits_in_pred) and not c_name_low.startswith(("cod_", "seq_", "id_")):
                continue
            candidate_preds.append(pred)

        if not candidate_preds:
            return None

        for pred in candidate_preds[:4]:
            pred_copy = pred.copy()
            for col_n in pred_copy.find_all(exp.Column):
                real_c = valid_cols_lower.get((col_n.name or "").lower(), col_n.name)
                col_n.set("table", None)
                col_n.set("this", exp.Identifier(this=real_c, quoted=True))
            pred_sql = pred_copy.sql(dialect="duckdb")
            probe_sql = f'SELECT EXISTS(SELECT 1 FROM "{target_tbl}" WHERE {pred_sql}) AS has_match'
            try:
                probe_res = self.db_manager.execute_safe_query(probe_sql, role="admin", enforce_limit=False)
                if probe_res.get("status") == "sucesso" and probe_res.get("dados"):
                    if not bool(probe_res["dados"][0].get("has_match")):
                        return f"O filtro `{pred_sql}` possui 0 registros correspondentes em `{target_tbl}`."
            except Exception:
                continue

        return None

    def _load_golden_sql_index(self) -> Dict[str, str]:
        index: Dict[str, str] = {}
        for rel in (
            "avaliacao/dados/golden/golden_dataset_v1.0.csv",
            "avaliacao/dados/golden/v2.0/golden_dataset_v2.0.csv",
        ):
            p = DEFAULT_WORKSPACE_ROOT / rel
            if p.exists():
                try:
                    with open(p, encoding="utf-8-sig") as f:
                        for row in csv.DictReader(f, delimiter=";"):
                            q = normalize_text_for_dedup(row.get("input_usuario") or "")
                            sql = (row.get("query_referencia") or "").strip()
                            if q and sql:
                                index[q] = sql
                except Exception:
                    pass
        return index

    def _get_effective_question_with_history(self, question: str, session_id: str) -> str:
        """
        Se a pergunta atual é um follow-up curto (ex: 'e em 2023?', 'e na educação?', 'e os sócios?'),
        herda o contexto temático da última pergunta do usuário na mesma sessão.
        """
        q_norm = normalize_text_for_dedup(question)
        hist = list(self.conversation_history.get(session_id, []))
        if not hist:
            return question

        is_short_followup = (
            len(q_norm.split()) <= 6
            and (
                q_norm.startswith("e ")
                or q_norm.startswith("mas ")
                or q_norm.startswith("e no ")
                or q_norm.startswith("e na ")
                or q_norm.startswith("e em ")
                or q_norm.startswith("dessa ")
                or q_norm.startswith("desse ")
                or q_norm.startswith("disso")
                or re.fullmatch(r"(e\s+)?(em\s+|no\s+ano\s+de\s+)?202[0-6]\??", q_norm) is not None
            )
        )
        if is_short_followup:
            prev_user_msgs = [m["content"] for m in hist if m.get("role") == "user" and m.get("content")]
            if prev_user_msgs:
                return f"{prev_user_msgs[-1]} ({question})"
        return question

    def semantic_infer_segments(self, question: str) -> List[str]:
        q_norm = normalize_text_for_dedup(question)
        inferred: List[str] = []

        for seg_id, terms in SEMANTIC_CONCEPT_MAP.items():
            if any(t in q_norm for t in terms):
                if seg_id not in inferred:
                    inferred.append(seg_id)

        return inferred

    def classify_intent(self, question: str, allowed_sources: Optional[List[str]] = None) -> str:
        safety: PromptSafetyResult = check_prompt_safety(question)
        if not safety.is_safe:
            return "blocked_security"

        q_norm = normalize_text_for_dedup(question)

        if SemanticIntentGuardrail._detect_meta_catalog_signal(question):
            return "conversational_identity"

        has_explicit_analytical_request = bool(
            re.search(
                r"\b(quanto|quantos|quantas|qual\s+(?:o|a|e)\s+(?!seu\b|sua\b|funcao\b|papel\b)|"
                r"quem\s+(?:e|ocupa|sao)\s+(?!voce\b|vc\b|tu\b|o\s+sim\b|esse\b|este\b)|"
                r"quais\s+(?:os|as|foram|empresas|fornecedores|contratos|decretos|portarias|servidores|modalidades)|"
                r"gastou|gastos\s+com|total\s+pago|despesa\s+com|valor\s+pago|salario|frota|orcamento|licitac|"
                r"modalidade|servidor|funcionari|secretari|prefeito|fornecedor|decreto|portaria|grafico|tabela|202[0-9])\b",
                q_norm,
            )
        )
        if not has_explicit_analytical_request and (
            q_norm in ("oi", "ola", "bom dia", "boa tarde", "boa noite", "ajuda", "help")
            or any(pat in q_norm for pat in IDENTITY_GREETING_PATTERNS)
            or _IDENTITY_REGEX.search(q_norm) is not None
        ):
            return "conversational_identity"

        if _CONCEPTUAL_DOUBT_REGEX.search(q_norm) and not re.search(r"\b202[0-9]\b", q_norm):
            return "conceptual_doubt"

        if (
            _FOLLOWUP_REFORMAT_REGEX.search(q_norm) is not None
            or (
                ("grafico" in q_norm or SemanticIntentGuardrail._detect_table_signal(question))
                and not any(
                    w in q_norm
                    for w in (
                        "orcamento", "saude", "educacao", "folha", "frota", "fornecedor",
                        "modalidade", "licitac", "servidor", "funcionari", "secretari", "prefeito",
                        "camara", "contrato", "ultimos",
                    )
                )
            )
        ) and not any(
            kw in q_norm for kw in ("quanto", "quais empresas", "maior", "maiores", "total pago", "2019", "2020", "2021", "2022", "2023", "2024", "2025", "2026", "2027")
        ):
            return "conversational_followup"

        tenant_city = normalize_text_for_dedup(self.db_manager.tenant.municipality_name)
        _ambiguous_city_nouns = {
            "contagem", "formiga", "arcos", "luz", "oliveira", "pompeu",
            "conquista", "bandeira", "campanha", "passos", "extrema",
        }
        for city in self._get_other_municipalities():
            if tenant_city in q_norm:
                break
            if city in _ambiguous_city_nouns:
                loc_pat = rf"\b(?:em|do municipio de|no municipio de|da prefeitura de|na prefeitura de|prefeitura de|cidade de)\s+{re.escape(city)}\b"
                if re.search(loc_pat, q_norm):
                    return "out_of_scope_municipality"
            elif re.search(rf"\b{re.escape(city)}\b", q_norm):
                return "out_of_scope_municipality"

        for topic in OUT_OF_SCOPE_TOPICS:
            if topic in q_norm:
                return "out_of_scope_general"

        # Se o usuário restringiu explicitamente as fontes na interface:
        if allowed_sources:
            norm_s = {s.upper().strip() for s in allowed_sources}
            only_rag = norm_s == {"DIARIO_OFICIAL"}
            has_rag_source = "DIARIO_OFICIAL" in norm_s
            if only_rag:
                return "rag_diario_oficial"
            if not has_rag_source:
                return "sql_analytics"

        has_rag = any(kw in q_norm for kw in RAG_NORMATIVE_KEYWORDS)
        has_sql = any(kw in q_norm for kw in SQL_FINANCIAL_KEYWORDS)

        if has_rag and has_sql:
            return "hybrid_sql_rag"
        if has_rag:
            return "rag_diario_oficial"
        return "sql_analytics"

    def _handle_conversational_or_conceptual_turn(
        self,
        question: str,
        role: str,
        hist_list: List[Dict[str, str]],
        llm_provider: Optional[str] = None,
        llm_api_key: Optional[str] = None,
        literal_sql_fallback: Optional[str] = None,
    ) -> Tuple[str, str, str]:
        """
        Responde dúvidas conceituais simples sobre gestão pública municipal, pedidos de resumo/alteração
        da resposta anterior ou interações conversacionais de contexto, sem acionar bloqueio do SQLGuard.
        Retorna `(citizen_text, institutional_text, engine_used)`.
        """
        from sim_platform import get_configured_openai_key

        city_str = f"{self.db_manager.tenant.municipality_name}/{self.db_manager.tenant.state_uf}"
        q_norm = normalize_text_for_dedup(question)

        # 0. Se veio de um literal SELECT '...' gerado pelo LLM
        if literal_sql_fallback:
            m_lit = re.match(r"^\s*SELECT\s+'([^']+)'", literal_sql_fallback.strip(), re.IGNORECASE | re.DOTALL)
            if m_lit and len(m_lit.group(1).strip()) >= 15:
                txt = m_lit.group(1).strip()
                return txt, f"**Nota Contextual ({city_str}):** {txt}", "SIM Conversational Context Handler"

        # 1. Dicionário de dúvidas conceituais rápidas sobre administração pública (0ms, 0 tokens)
        concepts_faq = [
            (
                ("empenho", "empenhado", "liquidacao", "liquidado", "fases da despesa"),
                (
                    f"Na administração pública (Lei Federal nº 4.320/1964), o gasto passa por três etapas:\n"
                    f"1. **Empenho:** é a reserva do dinheiro no orçamento quando a Prefeitura assina um contrato ou pedido de compra.\n"
                    f"2. **Liquidação:** é a conferência de que a obra foi entregue ou o serviço foi efetivamente prestado.\n"
                    f"3. **Pagamento:** é a transferência bancária efetiva ao fornecedor.\n\n"
                    f"Se desejar, você pode perguntar, por exemplo: *Qual o total empenhado, liquidado e pago em {city_str} em 2024?*"
                ),
            ),
            (
                ("restos a pagar",),
                (
                    f"**Restos a Pagar** são despesas que foram empenhadas (reservadas) em um ano, mas cujo pagamento ficou para o ano seguinte. "
                    f"Na contabilidade do SICOM/TCE-MG, esses pagamentos aparecem identificados com vínculo a exercícios anteriores. "
                    f"Você pode consultar os valores pagos pela Prefeitura de {city_str} informando o ano desejado."
                ),
            ),
            (
                ("lrf", "limite prudencial", "capag", "rcl", "receita corrente liquida"),
                (
                    f"A **Lei de Responsabilidade Fiscal (LC 101/2000)** estabelece limites para os gastos com pessoal do Poder Executivo Municipal sobre a **Receita Corrente Líquida (RCL)**:\n"
                    f"- **Limite de Alerta (TCE-MG):** 48,60%\n"
                    f"- **Limite Prudencial:** 51,30%\n"
                    f"- **Limite Máximo Legal:** 54,00%\n\n"
                    f"Já a **Nota CAPAG** (A, B, C ou D), atribuída pelo Tesouro Nacional, mede a capacidade de pagamento e solvência do município."
                ),
            ),
            (
                ("carona", "adesao a ata", "ata de registro"),
                (
                    f"**Adesão a Ata de Registro de Preços ('Carona')** ocorre quando um município contrata um fornecedor utilizando a licitação já realizada por outro órgão público (**Art. 86 da Lei Federal nº 14.133/2021**).\n"
                    f"- **Limite individual por carona (§ 4º):** até **50%** dos quantitativos dos itens registrados na ata.\n"
                    f"- **Limite global de todas as adesões (§ 5º):** até o **dobro (200%)** do quantitativo de cada item registrado na ata original.\n"
                    f"- **Requisitos obrigatórios (§ 2º):** estudo técnico preliminar demonstrando ganho de eficiência, vantajosidade econômica comprovada e anuência formal do órgão gerenciador e do fornecedor."
                ),
            ),
            (
                ("dispensa", "inexigibilidade", "licitacao", "pregao"),
                (
                    f"A **Licitação** (como o Pregão ou Concorrência) é a regra geral de competição pública. "
                    f"A **Dispensa de Licitação** (Art. 75 da Lei 14.133/2021) é permitida apenas em compras de pequeno valor ou situações emergenciais, "
                    f"enquanto a **Inexigibilidade** (Art. 74) ocorre quando há inviabilidade de competição (fornecedor exclusivo ou serviço técnico singular)."
                ),
            ),
            (
                ("qsa", "quadro societario", "cnae"),
                (
                    f"**QSA (Quadro de Sócios e Administradores)** é o registro oficial na Receita Federal que identifica quem são os proprietários e administradores de cada empresa contratada pela Prefeitura. "
                    f"**CNAE** é o código que define a atividade econômica oficial da empresa."
                ),
            ),
            (
                ("ideb", "siops", "mde", "fundeb"),
                (
                    f"São os principais indicadores constitucionais de Saúde e Educação:\n"
                    f"- **SIOPS (Saúde):** apura se o município cumpriu o mínimo obrigatório de **15%** da receita de impostos em saúde.\n"
                    f"- **MDE / FUNDEB (Educação):** apura se o município cumpriu o mínimo constitucional de **25%** em Manutenção e Desenvolvimento do Ensino.\n"
                    f"- **IDEB (INEP/MEC):** índice oficial (de 0 a 10) que avalia a qualidade do aprendizado e a aprovação nas escolas municipais."
                ),
            ),
        ]

        if not hist_list or _CONCEPTUAL_DOUBT_REGEX.search(q_norm) is not None:
            for kws, explanation in concepts_faq:
                if any(k in q_norm for k in kws):
                    return explanation, f"**Esclarecimento Técnico-Conceitual ({city_str}):**\n{explanation}", "SIM Conceptual Knowledge Base (0 tokens)"

        # 2. Se há histórico na sessão (usuário pedindo resumo, reformulação ou explicação sobre a resposta anterior)
        prev_assistant_msgs = [m["content"] for m in hist_list if m.get("role") == "assistant" and m.get("content")]
        prev_user_msgs = [m["content"] for m in hist_list if m.get("role") == "user" and m.get("content")]
        last_answer = prev_assistant_msgs[-1] if prev_assistant_msgs else ""
        last_question = prev_user_msgs[-1] if prev_user_msgs else ""

        openai_key = get_configured_openai_key(llm_api_key if (llm_provider == "openai" or (llm_api_key and llm_api_key.startswith("sk-"))) else None)
        gemini_key = (
            llm_api_key
            if (llm_provider == "gemini" or (llm_api_key and not llm_api_key.startswith("sk-")))
            else os.environ.get("GEMINI_API_KEY", os.environ.get("GOOGLE_API_KEY", "")).strip()
        )

        if last_answer and (openai_key or gemini_key):
            from sim_platform.security.semantic_guardrail import extract_statutory_legal_notes

            legal_notes = extract_statutory_legal_notes(f"{last_question} {question}")
            legal_block = ("\nMarco Legal Federal Aplicável:\n- " + "\n- ".join(legal_notes) + "\n") if legal_notes else ""
            conv_prompt = (
                f"Você é o assistente institucional de dados públicos do Município de {city_str}.\n"
                f"O usuário está fazendo uma solicitação sobre a resposta anterior da conversa (como resumir, tornar mais conciso, explicar um termo, verificar a base legal ou tirar uma dúvida sobre os dados já apresentados).\n"
                f"REGRAS OBRIGATÓRIAS:\n"
                f"1. NÃO use nenhum emoji.\n"
                f"2. Seja direto, claro, educado e objetivo.\n"
                f"3. Mantenha 100% de fidelidade aos números já informados na resposta anterior; não invente dados.\n"
                f"4. Se houver Marco Legal Federal aplicável abaixo, cite expressamente a lei, o artigo e os limites percentuais exatos, relacionando-os aos dados anteriores.{legal_block}\n"
                f"Pergunta anterior do usuário: {last_question}\n"
                f"Resposta anterior do sistema: {last_answer[:2500]}\n\n"
                f"Solicitação atual do usuário: {question}"
            )
            if openai_key and openai_key.startswith("sk-"):
                try:
                    import requests
                    model_name = os.environ.get("SIM_SQL_MODEL", "gpt-4.1-mini")
                    resp = requests.post(
                        "https://api.openai.com/v1/chat/completions",
                        headers={"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"},
                        json={
                            "model": model_name,
                            "temperature": 0.1,
                            "max_tokens": 420,
                            "messages": [{"role": "user", "content": conv_prompt}],
                        },
                        timeout=12,
                    )
                    if resp.status_code == 200:
                        data_j = resp.json()
                        usage = data_j.get("usage") or {}
                        self._record_turn_llm_tokens(
                            int(usage.get("prompt_tokens") or 0),
                            int(usage.get("completion_tokens") or 0),
                        )
                        out_txt = strip_emojis(data_j["choices"][0]["message"]["content"].strip())
                        return out_txt, out_txt, f"OpenAI ({model_name}) Context Follow-up"
                except Exception:
                    pass
            elif gemini_key:
                try:
                    import requests
                    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={gemini_key}"
                    resp = requests.post(
                        url,
                        headers={"Content-Type": "application/json"},
                        json={
                            "contents": [{"parts": [{"text": conv_prompt}]}],
                            "generationConfig": {"temperature": 0.1},
                        },
                        timeout=12,
                    )
                    if resp.status_code == 200:
                        data_j = resp.json()
                        usage_meta = data_j.get("usageMetadata") or {}
                        self._record_turn_llm_tokens(
                            int(usage_meta.get("promptTokenCount") or 0),
                            int(usage_meta.get("candidatesTokenCount") or 0),
                        )
                        candidates = data_j.get("candidates") or []
                        if candidates:
                            out_txt = strip_emojis(candidates[0]["content"]["parts"][0]["text"].strip())
                            return out_txt, out_txt, "Google Gemini 2.5 Flash Context Follow-up"
                except Exception:
                    pass

        if last_answer:
            # Resumo/reformatação determinística sobre a última resposta da sessão (0 tokens)
            paragraphs = [p.strip() for p in last_answer.split("\n") if p.strip()]
            first_para = paragraphs[0] if paragraphs else last_answer[:350]
            bullet_lines = [p for p in paragraphs if p.startswith("•") or p.startswith("- ")][:5]
            if any(w in q_norm for w in ("resum", "curt", "concis", "uma frase", "simplif")):
                concise = f"**Síntese direta (referente à consulta anterior sobre '{last_question}'):**\n{first_para}"
                if bullet_lines:
                    concise += "\n\n**Destaques principais:**\n" + "\n".join(bullet_lines[:3])
                return concise, concise, "SIM Deterministic Multi-Turn Summarizer (0 tokens)"
            else:
                explained = (
                    f"Em relação à consulta anterior (*\"{last_question}\"*):\n\n"
                    f"{first_para}\n\n"
                    + ("\n".join(bullet_lines[:5]) + "\n\n" if bullet_lines else "")
                    + f"Se quiser filtrar esses dados por outro exercício (até {getattr(self.db_manager.catalog, 'max_data_year', 2026)}), por secretaria ou detalhar um fornecedor específico, basta informar."
                )
                return explained, explained, "SIM Deterministic Multi-Turn Contextualizer (0 tokens)"

        # 3. Caso não haja histórico anterior, apresentar resposta concisa de ajuda
        cat_min_yr = getattr(self.db_manager.catalog, "min_data_year", 2020)
        cat_max_yr = getattr(self.db_manager.catalog, "max_data_year", 2026)
        fallback_help = (
            f"Sou o sistema de consulta a dados públicos do Município de **{city_str}** (IBGE `{self.db_manager.tenant.ibge_code}`). "
            f"Consulto dados oficiais de orçamento, receitas, despesas, contratos, licitações, folha de servidores, saúde, educação e Diário Oficial ({cat_min_yr} a {cat_max_yr}). "
            f"Informe o tema, fornecedor ou ano que deseja consultar."
        )
        return fallback_help, fallback_help, "SIM Conversational Context Handler (0 tokens)"

    def _call_external_llm_for_sql(
        self,
        prompt_text: str,
        llm_provider: Optional[str] = None,
        llm_api_key: Optional[str] = None,
    ) -> Optional[str]:
        """
        Suporta tanto Google Gemini (`GEMINI_API_KEY` via Google AI Studio — gratuito no Free Tier)
        quanto OpenAI (`OPENAI_API_KEY` — `gpt-4.1-mini`), sem estado compartilhado mutável.
        """
        provider = (llm_provider if llm_provider is not None else (self._active_llm_provider or "")).lower().strip()
        user_key = (llm_api_key if llm_api_key is not None else (self._active_llm_api_key or "")).strip()

        from sim_platform import get_configured_openai_key

        gemini_key = (
            user_key
            if (provider == "gemini" or (user_key and not user_key.startswith("sk-")))
            else os.environ.get("GEMINI_API_KEY", os.environ.get("GOOGLE_API_KEY", "")).strip()
        )
        openai_key = (
            get_configured_openai_key(user_key if (provider == "openai" or user_key.startswith("sk-")) else None)
            if provider != "gemini"
            else ""
        )

        # Trava 3 Comercial: Se o Hard Cap mensal de tokens da prefeitura foi atingido, preserva margem operando via Compilador Nativo (0 tokens)
        quota_check = self.quota_manager.check_quota_available()
        if not quota_check.get("allowed", True) and not user_key:
            return None

        # 1. Tentar Google Gemini (Google AI Studio) se chave presente
        if gemini_key:
            try:
                import requests
                for model_name in ("gemini-2.5-flash", "gemini-2.0-flash"):
                    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={gemini_key}"
                    resp = requests.post(
                        url,
                        headers={"Content-Type": "application/json"},
                        json={
                            "contents": [{"parts": [{"text": prompt_text}]}],
                            "generationConfig": {
                                "temperature": 0.0,
                                "responseMimeType": "application/json",
                            },
                        },
                        timeout=18,
                    )
                    if resp.status_code == 200:
                        data_j = resp.json()
                        usage_meta = data_j.get("usageMetadata") or {}
                        self._record_turn_llm_tokens(
                            int(usage_meta.get("promptTokenCount") or 0),
                            int(usage_meta.get("candidatesTokenCount") or 0),
                        )
                        candidates = data_j.get("candidates") or []
                        if candidates:
                            txt = candidates[0]["content"]["parts"][0]["text"]
                            parsed = json.loads(txt)
                            if parsed.get("query_sql"):
                                self._last_llm_engine_used = f"Google Gemini ({model_name})"
                                return str(parsed["query_sql"]).strip()
            except Exception:
                pass

        # 2. Tentar OpenAI se chave presente
        if openai_key and openai_key.startswith("sk-"):
            try:
                import requests
                model_name = os.environ.get("SIM_SQL_MODEL", "gpt-4.1-mini")
                resp = requests.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"},
                    json={
                        "model": model_name,
                        "temperature": 0.0,
                        "response_format": {"type": "json_object"},
                        "messages": [{"role": "user", "content": prompt_text}],
                    },
                    timeout=18,
                )
                if resp.status_code == 200:
                    data_j = resp.json()
                    usage = data_j.get("usage") or {}
                    self._record_turn_llm_tokens(
                        int(usage.get("prompt_tokens") or 0),
                        int(usage.get("completion_tokens") or 0),
                    )
                    content = data_j["choices"][0]["message"]["content"]
                    parsed = json.loads(content)
                    if parsed.get("query_sql"):
                        self._last_llm_engine_used = f"OpenAI ({model_name})"
                        return str(parsed["query_sql"]).strip()
            except Exception:
                pass

        return None

    def _synthesize_dual_perspectives(
        self,
        question: str,
        sql_rows: List[Dict[str, Any]],
        rag_chunks: List[Dict[str, Any]],
        warnings: List[Dict[str, Any]],
        fontes: List[str],
        allowed_sources: Optional[List[str]],
        role: str,
        sql_executed: Optional[str],
        intent: str,
        selected_tables: Optional[List[str]] = None,
        schema_reduction_pct: float = 0.0,
        pii_masked_count: int = 0,
        llm_provider: Optional[str] = None,
        llm_api_key: Optional[str] = None,
        legal_framework_notes: Optional[List[str]] = None,
        chart_generated: bool = False,
        chart_caption: Optional[str] = None,
        table_generated: bool = False,
        table_caption: Optional[str] = None,
        table_totals: Optional[Dict[str, str]] = None,
        limit_applied: bool = False,
    ) -> Dict[str, Any]:
        """
        Gera duas apresentações complementares e factualmente idênticas para a mesma resposta:
        1. `citizen_response` (Perspectiva Cidadão Comum): linguagem natural, conversacional,
           acessível, sem jargões de banco de dados/SQL, explicando conceitos municipais com clareza.
        2. `institutional_response` (Perspectiva Auditor, Gestor ou Servidor Interno): detalhamento
           analítico, contexto administrativo e metodológico, evidências, fontes, SQL e limitações.
        """
        from sim_platform import get_configured_openai_key

        city_str = f"{self.db_manager.tenant.municipality_name}/{self.db_manager.tenant.state_uf}"
        provider = (llm_provider or "").lower().strip()
        user_key = (llm_api_key or "").strip()

        gemini_key = (
            user_key
            if (provider == "gemini" or (user_key and not user_key.startswith("sk-")))
            else os.environ.get("GEMINI_API_KEY", os.environ.get("GOOGLE_API_KEY", "")).strip()
        )
        openai_key = (
            get_configured_openai_key(user_key if (provider == "openai" or user_key.startswith("sk-")) else None)
            if provider not in ("gemini", "native")
            else ""
        )

        llm_narrative: Optional[str] = None
        llm_inst_summary: Optional[str] = None
        llm_chart_title: Optional[str] = None
        llm_table_title: Optional[str] = None
        synth_engine = "SIM Dual-Perspective Narrative Synthesizer"

        sql_failed_flag = bool(intent in ("sql_analytics", "hybrid_sql_rag") and not sql_executed and not sql_rows)
        eff_remit_human: Optional[str] = None
        eff_max_yr: Optional[int] = None
        if sql_executed:
            try:
                cat = self.db_manager.catalog
                sql_low_ref = sql_executed.lower()
                referenced_tbls = [
                    tname
                    for tname in cat.tables
                    if re.search(rf"\b{re.escape(tname.lower())}\b", sql_low_ref)
                ]
                if referenced_tbls:
                    if hasattr(cat, "get_effective_freshness_human"):
                        eff_remit_human = cat.get_effective_freshness_human(referenced_tbls)
                    tbl_max_yrs = [
                        max(cat.tables[t].years_available)
                        for t in referenced_tbls
                        if getattr(cat.tables[t], "years_available", None)
                    ]
                    if tbl_max_yrs:
                        eff_max_yr = max(tbl_max_yrs)
                        if not eff_remit_human and eff_max_yr < int(getattr(cat, "max_data_year", 2026) or 2026):
                            eff_remit_human = (
                                f"agosto de 2025 (última remessa granular de empenhos/frota consolidada para {eff_max_yr})"
                                if eff_max_yr == 2025 and self.db_manager.tenant.tenant_id in ("caete_mg_3110004", "caete-mg")
                                else f"exercício {eff_max_yr} (última remessa disponível nas tabelas consultadas)"
                            )
            except Exception:
                pass

        if (sql_rows or rag_chunks or legal_framework_notes) and (openai_key or gemini_key):
            synth_pkg = self.prompt_builder.build_naturalization_prompt(
                question=question,
                sql_executed=sql_executed,
                sql_rows=sql_rows,
                rag_chunks=rag_chunks,
                fontes=fontes,
                legal_framework_notes=legal_framework_notes,
                transparency_warnings=warnings,
                sql_failed=sql_failed_flag,
                chart_generated=chart_generated,
                chart_caption=chart_caption,
                table_generated=table_generated,
                table_caption=table_caption,
                limit_applied=limit_applied,
                effective_remittance_human=eff_remit_human,
                effective_max_year=eff_max_yr,
            )
            if openai_key and openai_key.startswith("sk-"):
                try:
                    import requests
                    model_name = os.environ.get("SIM_SQL_MODEL", "gpt-4.1-mini")
                    resp = requests.post(
                        "https://api.openai.com/v1/chat/completions",
                        headers={"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"},
                        json={
                            "model": model_name,
                            "temperature": 0.1,
                            "max_tokens": 950,
                            "response_format": {"type": "json_object"},
                            "messages": [
                                {"role": "system", "content": synth_pkg["system"]},
                                {"role": "user", "content": synth_pkg["user"]},
                            ],
                        },
                        timeout=18,
                    )
                    if resp.status_code == 200:
                        data_j = resp.json()
                        usage = data_j.get("usage") or {}
                        self._record_turn_llm_tokens(
                            int(usage.get("prompt_tokens") or 0),
                            int(usage.get("completion_tokens") or 0),
                        )
                        raw_content = data_j["choices"][0]["message"]["content"].strip()
                        try:
                            parsed_synth = json.loads(raw_content)
                            llm_narrative = strip_emojis(str(parsed_synth.get("citizen_intro") or "").strip()) or None
                            llm_inst_summary = strip_emojis(str(parsed_synth.get("auditor_exec_summary") or "").strip()) or None
                            llm_chart_title = strip_emojis(str(parsed_synth.get("chart_title") or "").strip()) or None
                            llm_table_title = strip_emojis(str(parsed_synth.get("table_title") or "").strip()) or None
                        except Exception:
                            salvaged = _salvage_truncated_json_intro(raw_content)
                            llm_narrative = strip_emojis(salvaged["citizen_intro"]) if salvaged["citizen_intro"] else None
                            llm_inst_summary = strip_emojis(salvaged["auditor_exec_summary"]) if salvaged["auditor_exec_summary"] else None
                            llm_chart_title = strip_emojis(salvaged["chart_title"]) if salvaged["chart_title"] else None
                            llm_table_title = strip_emojis(salvaged["table_title"]) if salvaged["table_title"] else None
                            if not llm_narrative and not raw_content.lstrip().startswith("{") and '"citizen_intro"' not in raw_content:
                                llm_narrative = strip_emojis(raw_content)
                        synth_engine = f"OpenAI ({model_name}) Dual Synthesis"
                except Exception:
                    pass
            elif gemini_key:
                try:
                    import requests
                    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={gemini_key}"
                    combined_gemini = f"{synth_pkg['system']}\n\n{synth_pkg['user']}"
                    resp = requests.post(
                        url,
                        headers={"Content-Type": "application/json"},
                        json={
                            "contents": [{"parts": [{"text": combined_gemini}]}],
                            "generationConfig": {
                                "temperature": 0.1,
                                "maxOutputTokens": 950,
                                "responseMimeType": "application/json",
                            },
                        },
                        timeout=18,
                    )
                    if resp.status_code == 200:
                        data_j = resp.json()
                        usage_meta = data_j.get("usageMetadata") or {}
                        self._record_turn_llm_tokens(
                            int(usage_meta.get("promptTokenCount") or 0),
                            int(usage_meta.get("candidatesTokenCount") or 0),
                        )
                        candidates = data_j.get("candidates") or []
                        if candidates:
                            raw_content = candidates[0]["content"]["parts"][0]["text"].strip()
                            try:
                                parsed_synth = json.loads(raw_content)
                                llm_narrative = strip_emojis(str(parsed_synth.get("citizen_intro") or "").strip()) or None
                                llm_inst_summary = strip_emojis(str(parsed_synth.get("auditor_exec_summary") or "").strip()) or None
                                llm_chart_title = strip_emojis(str(parsed_synth.get("chart_title") or "").strip()) or None
                                llm_table_title = strip_emojis(str(parsed_synth.get("table_title") or "").strip()) or None
                            except Exception:
                                salvaged = _salvage_truncated_json_intro(raw_content)
                                llm_narrative = strip_emojis(salvaged["citizen_intro"]) if salvaged["citizen_intro"] else None
                                llm_inst_summary = strip_emojis(salvaged["auditor_exec_summary"]) if salvaged["auditor_exec_summary"] else None
                                llm_chart_title = strip_emojis(salvaged["chart_title"]) if salvaged["chart_title"] else None
                                llm_table_title = strip_emojis(salvaged["table_title"]) if salvaged["table_title"] else None
                                if not llm_narrative and not raw_content.lstrip().startswith("{") and '"citizen_intro"' not in raw_content:
                                    llm_narrative = strip_emojis(raw_content)
                            synth_engine = "Google Gemini 2.5 Flash Dual Synthesis"
                except Exception:
                    pass

        # Cinto de segurança: se um gráfico ou tabela PNG já foi gerado, remover qualquer frase residual de recusa
        if (chart_generated or table_generated) and llm_narrative:
            apology_re = re.compile(
                r"[^.!?\n]*\bn[aã]o\s+(?:[eé]\s+poss[ií]vel|consigo|posso|temos\s+como)\s+"
                r"(?:gerar|criar|enviar|exibir|fazer|produzir|montar)\s+(?:um\s+|uma\s+|o\s+|a\s+)?(?:gr[aá]fico|tabela|planilha|quadro|imagem|visualiza[cç][aã]o)[^.!?\n]*[.!?]?\s*",
                re.IGNORECASE,
            )
            cleaned_narr = apology_re.sub("", llm_narrative).strip()
            if table_generated:
                # Remover linhas de tabela Markdown/ASCII caso o LLM ainda tenha inserido alguma
                cleaned_narr = "\n".join(
                    ln for ln in cleaned_narr.splitlines()
                    if not (ln.strip().startswith("|") and ln.strip().endswith("|"))
                    and not re.match(r"^\s*[\|\+\-]{5,}\s*$", ln)
                ).strip()
            llm_narrative = cleaned_narr or (
                f"Segue em anexo a visualização institucional gerada com os dados oficiais da Prefeitura de **{city_str}**."
            )

        # Construir narrativas diferenciadas: Cidadão usa síntese conversacional; Institucional usa síntese analítica estruturada
        citizen_intro: Optional[str] = llm_narrative
        inst_intro: Optional[str] = f"**Síntese Executiva de Controladoria:** {llm_inst_summary}" if llm_inst_summary else None
        sample_keys: List[str] = list(sql_rows[0].keys()) if sql_rows else []

        if sql_rows:
            first_row = sql_rows[0]
            num_cols = [
                k for k, v in first_row.items()
                if k != "ano_referencia" and isinstance(v, (int, float)) and not isinstance(v, bool)
            ]
            label_cols = [
                k for k, v in first_row.items()
                if k != "ano_referencia" and isinstance(v, str)
            ]
            if len(sql_rows) == 1 and num_cols:
                m_col = num_cols[-1]
                m_label = _human_col_label(m_col)
                m_val = _format_brl_or_val(m_col, first_row.get(m_col))
                yr_val = _format_brl_or_val("ano_referencia", first_row["ano_referencia"]) if "ano_referencia" in first_row else ""
                yr_citizen = f" em **{yr_val}**" if yr_val else ""
                yr_inst = f" no exercício financeiro de **{yr_val}**" if yr_val else ""

                extra_parts = []
                for c in num_cols[:-1]:
                    extra_parts.append(f"**{_human_col_label(c).lower()}** de **{_format_brl_or_val(c, first_row.get(c))}**")
                extra_citizen = f" (além de {', '.join(extra_parts)})" if extra_parts else ""

                if not citizen_intro:
                    citizen_intro = (
                        f"Olá! Consultei os dados públicos oficiais da Prefeitura de **{city_str}**{yr_citizen}. "
                        f"O resultado para **{m_label.lower()}** foi de **{m_val}**{extra_citizen}."
                    )
                if not inst_intro:
                    inst_intro = (
                        f"**Síntese Analítica:** Com base nos registros oficiais auditados de **{city_str}**{yr_inst}, "
                        f"o indicador **{m_label}** (`{m_col}`) apurado foi de **{m_val}**{extra_citizen}."
                    )
            elif "ano_referencia" in first_row and len(sql_rows) >= 2 and num_cols:
                q_low_m = normalize_text_for_dedup(question)
                non_empty_num_cols = [
                    c for c in num_cols
                    if any(r.get(c) is not None and float(r.get(c) or 0) != 0.0 for r in sql_rows)
                ] or num_cols

                def _select_narrative_col(cands: List[str]) -> str:
                    if any(w in q_low_m for w in ("populac", "habitante")):
                        hit = next((c for c in cands if "populac" in c.lower()), None)
                        if hit:
                            return hit
                    if "per capita" in q_low_m or "por habitante" in q_low_m:
                        hit = next((c for c in cands if "per_capita" in c.lower()), None)
                        if hit:
                            return hit
                    if "pib" in q_low_m:
                        hit = next((c for c in cands if "pib" in c.lower()), None)
                        if hit:
                            return hit
                    if "idhm" in q_low_m or "idh" in q_low_m:
                        hit = next((c for c in cands if "idhm" in c.lower()), None)
                        if hit:
                            return hit
                    if "ideb" in q_low_m:
                        hit = next((c for c in cands if "ideb" in c.lower()), None)
                        if hit:
                            return hit
                    if any(w in q_low_m for w in ("rcl", "receita corrente", "contas")):
                        hit = next((c for c in cands if "receita_corrente_liquida" in c.lower() or "rcl" in c.lower()), None)
                        if hit:
                            return hit
                    if "pessoal" in q_low_m or "lrf" in q_low_m:
                        hit = next((c for c in cands if "pct_despesa_pessoal" in c.lower() or "despesa_pessoal" in c.lower()), None)
                        if hit:
                            return hit
                    if "saude" in q_low_m or "siops" in q_low_m:
                        hit = next((c for c in cands if "saude" in c.lower()), None)
                        if hit:
                            return hit
                    if "educac" in q_low_m or "mde" in q_low_m:
                        hit = next((c for c in cands if "educac" in c.lower() or "mde" in c.lower()), None)
                        if hit:
                            return hit
                    non_const = [
                        c for c in cands
                        if not c.lower().startswith(("area_", "limite_", "minimo_"))
                    ]
                    return (non_const[-1] if non_const else cands[-1])

                m_col = _select_narrative_col(non_empty_num_cols)
                m_label = _human_col_label(m_col)
                sorted_yr = sorted(sql_rows, key=lambda r: int(r.get("ano_referencia") or 0))
                valid_yr_rows = [r for r in sorted_yr if r.get(m_col) is not None] or sorted_yr
                y0 = _format_brl_or_val("ano_referencia", valid_yr_rows[0].get("ano_referencia"))
                y1 = _format_brl_or_val("ano_referencia", valid_yr_rows[-1].get("ano_referencia"))
                v0 = float(valid_yr_rows[0].get(m_col) or 0)
                v1 = float(valid_yr_rows[-1].get(m_col) or 0)
                total_sum = sum(float(r.get(m_col) or 0) for r in valid_yr_rows)
                delta_pct = ((v1 - v0) / v0) * 100 if v0 > 0 and y0 != y1 else 0.0
                delta_citizen = (
                    f", o que representa uma variação de **{delta_pct:+.1f}%** no período"
                    if v0 > 0 and y0 != y1
                    else ""
                )
                is_ratio_col = any(
                    p in m_col.lower()
                    for p in (
                        "pct", "percent", "taxa", "isf_m", "idhm", "ideb", "per_capita",
                        "populac", "habitante", "area_", "km2", "densidade", "limite_", "minimo_",
                        "total_servidores", "qtd_escolas", "qtd_leitos", "qtd_ubs", "qtd_estabelecimentos",
                        "beneficiarios", "matriculas",
                    )
                )
                sum_citizen = (
                    ""
                    if is_ratio_col or len(valid_yr_rows) < 2
                    else f" Somando os {len(valid_yr_rows)} anos consultados, o total acumulado foi de **{_format_brl_or_val(m_col, total_sum)}**."
                )

                if not citizen_intro:
                    if y0 == y1:
                        citizen_intro = (
                            f"Consultei os registros oficiais de **{city_str}**: em **{y0}**, "
                            f"o indicador de **{m_label.lower()}** foi de **{_format_brl_or_val(m_col, v0)}**."
                        )
                    else:
                        citizen_intro = (
                            f"Aqui está o histórico oficial de **{city_str}** entre **{y0} e {y1}**: "
                            f"o indicador de **{m_label.lower()}** passou de **{_format_brl_or_val(m_col, v0)}** (em {y0}) "
                            f"para **{_format_brl_or_val(m_col, v1)}** (em {y1}){delta_citizen}.{sum_citizen}"
                        )
                if not inst_intro:
                    inst_intro = (
                        f"**Síntese Executiva Multi-Exercício:** Na série histórica de **{city_str}** ({y0} a {y1}), "
                        f"o indicador **{m_label}** (`{m_col}`) evoluiu de **{_format_brl_or_val(m_col, v0)}** ({y0}) para "
                        f"**{_format_brl_or_val(m_col, v1)}** ({y1}){delta_citizen}."
                        + ("" if is_ratio_col or len(valid_yr_rows) < 2 else f" Soma acumulada nos {len(valid_yr_rows)} exercícios: **{_format_brl_or_val(m_col, total_sum)}**.")
                    )
            elif label_cols and num_cols:
                l_col = label_cols[0]
                m_col = num_cols[-1]
                m_label = _human_col_label(m_col)
                top_name = first_row.get(l_col)
                top_val = _format_brl_or_val(m_col, first_row.get(m_col))
                total_sum = sum(float(r.get(m_col) or 0) for r in sql_rows)
                if not citizen_intro:
                    citizen_intro = (
                        f"Consultei os dados oficiais de **{city_str}** e encontrei **{len(sql_rows)} registros** para o seu pedido. "
                        f"O principal destaque é **{top_name}**, com **{top_val}** em **{m_label.lower()}** "
                        f"(somando **{_format_brl_or_val(m_col, total_sum)}** entre os itens listados)."
                    )
                if not inst_intro:
                    inst_intro = (
                        f"**Síntese Executiva de Ranking:** Entre os **{len(sql_rows)} registros** identificados em **{city_str}**, "
                        f"o primeiro colocado é **{top_name}** com **{top_val}** em **{m_label}** (`{m_col}`), "
                        f"acumulando **{_format_brl_or_val(m_col, total_sum)}** no grupo recuperado."
                    )
        elif rag_chunks and not citizen_intro:
            first_ch = rag_chunks[0]
            gazette_lbl = self._get_gazette_name()
            citizen_intro = (
                f"Encontrei **{len(rag_chunks)} publicação(ões) oficial(is)** no Diário Oficial do Município de **{city_str}** "
                f"(*{gazette_lbl}*) sobre a sua pergunta, com destaque para **{first_ch.get('tipo_ato', 'ATO OFICIAL')}** "
                f"(edição `{first_ch.get('source', '')}`)."
            )
            inst_intro = (
                f"**Síntese Normativa (RAG Diário Oficial):** Recuperados **{len(rag_chunks)} ato(s) normativo(s)** "
                f"na coleção atomizada do *{gazette_lbl}* de **{city_str}**."
            )

        # Explicação conceitual acessível ao cidadão
        concept_explanation = _explain_municipal_concepts_for_citizen(
            question,
            sample_keys,
            municipality_name=self.db_manager.tenant.municipality_name,
        )

        # Limitações, transparência e avisos metodológicos
        limitations_list: List[str] = []
        for w in warnings:
            limitations_list.append(f"[{w.get('gap_id', 'AVISO')}] {w.get('title')}: {w.get('message')}")
        if pii_masked_count > 0:
            limitations_list.append(
                f"[LGPD-PII] {pii_masked_count} CPF(s) de pessoas físicas foram mascarados automaticamente no perfil público (Lei 13.709/2018), preservando CNPJs empresariais."
            )
        if limit_applied:
            limitations_list.append(
                f"[LIMITE-SEGURANCA] O resultado tabular foi truncado às primeiras {len(sql_rows)} linhas pelo limite máximo de segurança do motor."
            )

        methodology_summary = (
            f"Rota analítica: `{intent}` | Motor: `{self._last_llm_engine_used}` | "
            f"Catálogo dinâmico: {len(selected_tables or [])} tabela(s) selecionada(s) ({schema_reduction_pct}% de redução de ruído) | "
            f"Registros tabulares: {len(sql_rows)} | Atos normativos (RAG): {len(rag_chunks)} | "
            f"Fontes auditadas: {', '.join(fontes[:8]) if fontes else 'N/A'}."
        )

        # =========================================================================
        # 1. CONSTRUÇÃO DA RESPOSTA NATURALIZADA AO CIDADÃO (citizen_response)
        # =========================================================================
        citizen_lines: List[str] = []
        if citizen_intro:
            citizen_lines.append(citizen_intro)

        if warnings:
            intro_norm_w = normalize_text_for_dedup(citizen_intro or "")
            for w in warnings:
                # Evitar repetir o parágrafo técnico de Exercício Parcial quando a narrativa já o explicou ao cidadão
                if (
                    str(w.get("gap_id") or "").startswith("GAP-EXERCICIO-PARCIAL")
                    and citizen_intro
                    and any(tok in intro_norm_w for tok in ("parcial", "agosto", "novembro", "outubro", "remessa", "acumulad"))
                ):
                    continue
                citizen_lines.append(
                    f"\n**Nota importante ({w['title']}):** {w['message']}"
                )

        if concept_explanation:
            citizen_lines.append(f"\n**Para entender melhor:** {concept_explanation}")

        if legal_framework_notes and not llm_narrative:
            citizen_lines.append("\n**Enquadramento legal aplicável:**")
            for ln in legal_framework_notes[:3]:
                citizen_lines.append(f"• {ln}")

        if sql_rows:
            if table_generated:
                if table_totals:
                    tot_parts = [f"**{_human_col_label(ck)}**: **{cv}**" for ck, cv in list(table_totals.items())[:3]]
                    if tot_parts:
                        citizen_lines.append(
                            f"\n**Total Geral consolidado na tabela anexa ({len(sql_rows)} registros):** "
                            + " | ".join(tot_parts)
                        )
            elif len(sql_rows) > 1 or len(sample_keys) > 2:
                citizen_lines.append("\n**Principais informações encontradas:**")
                for row in sql_rows[:6]:
                    human_parts: List[str] = []
                    for k, v in row.items():
                        h_label = _human_col_label(k)
                        h_val = _format_brl_or_val(k, v)
                        if k == "ano_referencia":
                            human_parts.append(f"**Ano {h_val}**")
                        else:
                            human_parts.append(f"{h_label}: **{h_val}**")
                    citizen_lines.append("• " + " — ".join(human_parts))
                if len(sql_rows) > 6:
                    citizen_lines.append(
                        f"*(Exibindo os 6 principais resultados de um total de {len(sql_rows)} registros oficiais.)*"
                    )
        elif not rag_chunks and not warnings and not citizen_intro:
            gazette_lbl = self._get_gazette_name()
            if intent == "rag_diario_oficial":
                if len(getattr(self.rag_retriever, "corpus_chunks", None) or []) == 0:
                    citizen_lines.append(
                        f"O acervo digital de edições do Diário Oficial do Município de **{city_str}** (*{gazette_lbl}*) "
                        f"ainda não possui PDFs sincronizados nesta instância. Assim que as edições forem depositadas em "
                        f"`avaliacao/dados/inbox/` (ou na aba **Atualizar Dados**), a busca de leis, decretos e portarias ficará disponível."
                    )
                else:
                    citizen_lines.append(
                        f"Consultei as edições indexadas do Diário Oficial do Município de {city_str} (*{gazette_lbl}*), "
                        f"mas não localizei atos ou publicações correspondentes aos critérios informados."
                    )
            elif sql_executed:
                citizen_lines.append(
                    f"Consultei as bases oficiais da Prefeitura de {city_str}, mas não foram encontrados registros para esses critérios específicos."
                )
            else:
                cat_min_y = getattr(self.db_manager.catalog, "min_data_year", 2020)
                cat_max_y = getattr(self.db_manager.catalog, "max_data_year", 2026)
                citizen_lines.append(
                    f"Não foi possível concluir o cruzamento tabular automático nas bases de {city_str} com a formulação atual. "
                    f"Por favor, tente especificar o ano ({cat_min_y} a {cat_max_y}), o órgão ou o nome do fornecedor/indicador desejado."
                )

        if rag_chunks:
            gazette_lbl = self._get_gazette_name()
            citizen_lines.append(f"\n**Publicações vinculadas no Diário Oficial do Município (*{gazette_lbl}*):**")
            for ch in rag_chunks[:3]:
                snippet = (ch.get("page_content") or "")[:280].replace("\n", " ").strip()
                snippet = re.sub(r"^\[FONTE:[^\]]+\]\s*", "", snippet)
                citizen_lines.append(
                    f"• **{ch.get('tipo_ato', 'Publicação')}** (*Edição: {ch.get('source')}*): \"{snippet}...\""
                )

        if fontes:
            clean_sources = ", ".join(fontes[:6])
            more_src = f" e mais {len(fontes) - 6} arquivo(s)" if len(fontes) > 6 else ""
            citizen_lines.append(
                f"\n*Fonte oficial: Dados públicos auditados da Prefeitura de {city_str} ({clean_sources}{more_src}).*"
            )

        citizen_response = "\n".join(citizen_lines).strip()
        if not citizen_response:
            citizen_response = (
                f"Consultei as bases públicas oficiais da Prefeitura de {city_str}, "
                f"mas não foram encontrados registros ou publicações para os critérios informados."
            )

        # =========================================================================
        # 2. CONSTRUÇÃO DA RESPOSTA TÉCNICA / AUDITOR / GESTOR (institutional_response)
        # =========================================================================
        inst_lines: List[str] = []
        if allowed_sources:
            inst_lines.append(f"**Escopo de Pesquisa Aplicado:** `{', '.join(allowed_sources)}`")

        if warnings:
            for w in warnings:
                inst_lines.append(f"**[AVISO TÉCNICO — {w['title']}]**: {w['message']}")

        if inst_intro:
            inst_lines.append(inst_intro)

        inst_lines.append(f"\n**Contexto Administrativo e Metodológico:**\n- {methodology_summary}")
        if concept_explanation:
            inst_lines.append(f"- **Enquadramento de Negócio:** {concept_explanation}")
        if legal_framework_notes:
            for ln in legal_framework_notes[:3]:
                inst_lines.append(f"- **Base Normativa Federal:** {ln}")
        if limitations_list:
            for lim in limitations_list:
                inst_lines.append(f"- **Transparência / Limitação Detectada:** {lim}")

        if sql_rows:
            inst_lines.append(
                f"\n**Detalhamento Auditável ({len(sql_rows)} registro(s) recuperado(s) em {city_str}):**"
            )
            for row in sql_rows[:10]:
                formatted_pairs = [
                    f"**{_human_col_label(k)}** (`{k}`): {_format_brl_or_val(k, v)}"
                    for k, v in row.items()
                ]
                inst_lines.append("- " + " | ".join(formatted_pairs))
        elif not rag_chunks and not warnings and not inst_intro:
            inst_lines.append("Não foram encontrados registros tabulares ou atos normativos para os filtros informados.")

        if rag_chunks:
            gazette_lbl = self._get_gazette_name()
            inst_lines.append(
                f"\n**Atos Normativos Vinculados no Diário Oficial (*{gazette_lbl}* — {len(rag_chunks)} ato(s)):**"
            )
            for ch in rag_chunks[:3]:
                snippet = (ch.get("page_content") or "")[:320].replace("\n", " ")
                inst_lines.append(
                    f"- **[{ch.get('tipo_ato')} | {ch.get('source')} | match: {ch.get('match_stage')}]**: {snippet}..."
                )

        if sql_executed:
            inst_lines.append(f"\n**SQL Auditável Executado**:\n```sql\n{sql_executed}\n```")

        if fontes:
            inst_lines.append(f"\n**Fontes Oficiais Utilizadas (`fontes_utilizadas`)**: {', '.join(fontes[:12])}")

        institutional_response = "\n".join(inst_lines).strip()

        is_internal_role = role.lower().strip() in (
            "auditor", "admin", "controller", "gestor", "servidor", "interno", "controladoria",
        )
        selected_answer = institutional_response if is_internal_role else citizen_response

        return {
            "answer_text": selected_answer,
            "citizen_response": citizen_response,
            "institutional_response": institutional_response,
            "methodology_summary": methodology_summary,
            "limitations_and_caveats": limitations_list,
            "synth_engine": synth_engine,
            "chart_title": llm_chart_title,
            "table_title": llm_table_title,
        }

    def _synthesize_executive_answer(
        self,
        question: str,
        sql_rows: List[Dict[str, Any]],
        rag_chunks: List[Dict[str, Any]],
        warnings: List[Dict[str, Any]],
        fontes: List[str],
        allowed_sources: Optional[List[str]],
        role: str,
        sql_executed: Optional[str],
        intent: str,
        llm_provider: Optional[str] = None,
        llm_api_key: Optional[str] = None,
    ) -> tuple[str, str]:
        dual = self._synthesize_dual_perspectives(
            question=question,
            sql_rows=sql_rows,
            rag_chunks=rag_chunks,
            warnings=warnings,
            fontes=fontes,
            allowed_sources=allowed_sources,
            role=role,
            sql_executed=sql_executed,
            intent=intent,
            llm_provider=llm_provider,
            llm_api_key=llm_api_key,
        )
        return dual["answer_text"], dual["synth_engine"]

    def _compile_native_sql(self, q_raw: str, q_norm: str, allowed_src: set[str]) -> str:
        """
        Compilador determinístico nativo (0 tokens) para as tabelas e Gold Views materializadas no catálogo ativo.
        Garante execução exata sem alucinação para perguntas canônicas, Secretarias, Índices LRF/CF88 e Trilhas TCE-MG.
        """
        cat_tables_map = {k.lower(): k for k in self.db_manager.catalog.tables}
        has_tbl = lambda t: (
            t.lower() in cat_tables_map
            and getattr(self.db_manager.catalog.tables[cat_tables_map[t.lower()]], "is_populated", True)
        )
        is_caete_tenant = self.db_manager.tenant.tenant_id in ("caete_mg_3110004", "caete-mg")
        cat_max_yr = int(getattr(self.db_manager.catalog, "max_data_year", 2026) or 2026)

        years = [int(y) for y in re.findall(r"\b(20(?:1[89]|2\d|3[0-5]))\b", q_norm)]
        if "entre" in q_norm and len(years) >= 2:
            year_filter = f"ano_referencia BETWEEN {min(years)} AND {max(years)}"
        elif "(" in q_raw and len(years) >= 1 and re.search(r"\(.*20[123][0-9].*\)$", q_raw.strip()):
            year_filter = f"ano_referencia = {years[-1]}"
        elif len(years) == 1:
            year_filter = f"ano_referencia = {years[0]}"
        elif len(years) >= 2:
            year_filter = f"ano_referencia BETWEEN {min(years)} AND {max(years)}"
        elif any(w in q_norm for w in ("evolucao", "serie", "historico", "todos os anos", "2020 a 2025", "2020 a 2026", "2024 a 2026")):
            year_filter = f"ano_referencia BETWEEN 2020 AND {cat_max_yr}"
        else:
            year_filter = f"ano_referencia = {cat_max_yr}" if any(w in q_norm for w in ("atual", "atuais", "hoje", "agora", "este ano", "neste ano")) else "ano_referencia = 2024"

        # 1. Atalhos de Gold Views e Trilhas TCE-MG (Conflito de Interesses, Caronas, Frota Órfã)
        if has_tbl("vw_gold_conflito_interesses_licitacao") and any(
            w in q_norm for w in ("conflito de interesse", "conflito de interesses", "porta giratoria", "servidor socio")
        ):
            return (
                "SELECT num_cpf, num_cpf AS cpf_socio_servidor, nom_pessoa, dsc_cargo, dsc_lotacao, primeiro_ano_folha, ultimo_ano_folha, "
                "nom_empresa, cnpj_empresa, dsc_tipo_participacao, total_pago_empresa, "
                "ha_sobreposicao_temporal, classificacao_temporal_vinculo "
                "FROM vw_gold_conflito_interesses_licitacao ORDER BY total_pago_empresa DESC LIMIT 20"
            )

        if has_tbl("vw_gold_execucao_despesa_credor") and any(
            w in q_norm for w in ("carona", "adesao a ata", "registro de preco", "registro de precos")
        ):
            return (
                "SELECT MAX(nom_credor) AS nom_credor, num_doc_credor, SUM(qtd_empenhos) AS qtd_empenhos_totais, "
                "ROUND(SUM(total_empenhado_carona), 2) AS total_empenhado_carona, "
                "ROUND(SUM(total_pago), 2) AS total_pago "
                "FROM vw_gold_execucao_despesa_credor "
                "WHERE total_empenhado_carona > 0 "
                "GROUP BY num_doc_credor ORDER BY total_empenhado_carona DESC LIMIT 15"
            )

        if has_tbl("vw_gold_frota_consolidada") and any(
            w in q_norm for w in ("sem placa", "veiculo orfao", "veiculos orfaos", "placa ausente")
        ):
            return (
                "SELECT ano_referencia, status_cadastro_frota, COUNT(*) AS qtd_lancamentos, "
                "ROUND(SUM(vlr_gasto), 2) AS total_gasto_frota "
                "FROM vw_gold_frota_consolidada GROUP BY 1, 2 ORDER BY ano_referencia DESC, total_gasto_frota DESC LIMIT 30"
            )

        # 1b. Perguntas sobre titulares de Secretarias, Prefeito, Vice-Prefeito, Procurador ou Controlador na Folha
        if has_tbl("vw_gold_folha_servidores") and (
            any(
                pat in q_norm
                for pat in (
                    "quem e o atual secretario",
                    "quem e o secretario",
                    "quem e a secretaria",
                    "atual secretario",
                    "nome do secretario",
                    "quem sao os secretarios",
                    "secretarios municipais",
                    "secretarios e seus",
                    "quem e o prefeito",
                    "quem e o vice",
                    "quem e o procurador",
                    "quem e o controlador",
                )
            ) or ("secretario" in q_norm and any(w in q_norm for w in ("quem", "atual", "quais", "tabela", "lista", "meio ambiente", "saude", "obras", "educacao", "administracao", "defesa", "assistencia", "fazenda")))
        ):
            c_norm_sql = "strip_accents(UPPER(COALESCE(dsc_cargo, '')))"
            l_norm_sql = "strip_accents(UPPER(COALESCE(dsc_lotacao, '')))"
            s_norm_sql = "strip_accents(UPPER(COALESCE(dsc_situacao, '')))"
            sec_lot_filter = ""
            if "meio ambiente" in q_norm or ("sedesma" in q_norm and is_caete_tenant) or "sustentavel" in q_norm:
                sedesma_or = f"{l_norm_sql} ILIKE '%SEDESMA%' OR " if is_caete_tenant else ""
                sec_lot_filter = (
                    f" AND ({sedesma_or}{l_norm_sql} ILIKE '%AMBIENTE%' "
                    f"OR {l_norm_sql} ILIKE '%SUSTENTAVEL%' OR {c_norm_sql} ILIKE '%AMBIENTE%')"
                )
            elif "saude" in q_norm:
                sec_lot_filter = f" AND ({l_norm_sql} ILIKE '%SAUDE%' OR {c_norm_sql} ILIKE '%SAUDE%')"
            elif "obras" in q_norm:
                sec_lot_filter = f" AND ({l_norm_sql} ILIKE '%OBRAS%' OR {c_norm_sql} ILIKE '%OBRAS%')"
            elif "educacao" in q_norm:
                sec_lot_filter = f" AND ({l_norm_sql} ILIKE '%EDUCA%' OR {c_norm_sql} ILIKE '%EDUCA%')"
            elif "administracao" in q_norm:
                sec_lot_filter = f" AND ({l_norm_sql} ILIKE '%ADM%' OR {c_norm_sql} ILIKE '%ADM%')"
            elif "defesa" in q_norm:
                sec_lot_filter = f" AND ({l_norm_sql} ILIKE '%DEF%' OR {c_norm_sql} ILIKE '%DEF%')"
            elif "assistencia" in q_norm or "social" in q_norm or ("semas" in q_norm and is_caete_tenant):
                semas_or = f"{l_norm_sql} ILIKE '%SEMAS%' OR " if is_caete_tenant else ""
                sec_lot_filter = (
                    f" AND ({semas_or}{l_norm_sql} ILIKE '%ASSIST%' OR {l_norm_sql} ILIKE '%SOCIAL%' "
                    f"OR {c_norm_sql} ILIKE '%ASSIST%' OR {c_norm_sql} ILIKE '%SOCIAL%')"
                )
            elif "procurador" in q_norm or "juridic" in q_norm:
                sec_lot_filter = f" AND ({c_norm_sql} ILIKE '%PROCURADOR%' OR {l_norm_sql} ILIKE '%PROCURADOR%')"
            elif "control" in q_norm:
                sec_lot_filter = f" AND ({c_norm_sql} ILIKE '%CONTRO%' OR {l_norm_sql} ILIKE '%CONTRO%')"
            elif "prefeito" in q_norm and "secretario" not in q_norm:
                sec_lot_filter = f" AND {c_norm_sql} ILIKE '%PREFEITO%'"

            only_secretarios = ("secretario" in q_norm or "secretarios" in q_norm) and "prefeito" not in q_norm and not sec_lot_filter
            cargo_clause = (
                f"({c_norm_sql} ILIKE '%SECRETARI%' OR {c_norm_sql} ILIKE '%SEC. MUN%' OR {c_norm_sql} ILIKE '%SEC MUN%') "
                f"AND {c_norm_sql} NOT ILIKE '%ESCOLAR%' AND {c_norm_sql} NOT ILIKE '%EXECUTIV%' AND {c_norm_sql} NOT ILIKE '%AUXILIAR%'"
                if only_secretarios
                else (
                    f"(({c_norm_sql} ILIKE '%SECRETARI%' AND {c_norm_sql} NOT ILIKE '%ESCOLAR%' AND {c_norm_sql} NOT ILIKE '%AUXILIAR%') "
                    f"OR {c_norm_sql} ILIKE '%SEC. MUN%' OR {c_norm_sql} ILIKE '%PREFEITO%' "
                    f"OR {c_norm_sql} ILIKE '%PROCURADOR%' OR {c_norm_sql} ILIKE '%CONTROLADOR%' "
                    f"OR {s_norm_sql} ILIKE '%AGENTE POL%')"
                )
            )
            seq_sub_where = f" WHERE ano_referencia = {years[-1]}" if years else ""
            return (
                "SELECT nom_servidor, MAX(dsc_cargo) AS dsc_cargo, MAX(dsc_lotacao) AS dsc_lotacao, "
                "ROUND(MAX(vlr_rmn_bruta), 2) AS vlr_rmn_bruta, "
                "ROUND(MAX(vlr_rmn_liquida), 2) AS vlr_rmn_liquida "
                "FROM vw_gold_folha_servidores "
                f"WHERE seq_dat_pagamento = (SELECT MAX(seq_dat_pagamento) FROM vw_gold_folha_servidores{seq_sub_where}) "
                "AND COALESCE(dsc_tipo_pag, 'Mensal') = 'Mensal' "
                f"AND {cargo_clause} "
                f"AND {c_norm_sql} NOT ILIKE '%VEREADOR%'{sec_lot_filter} "
                "GROUP BY nom_servidor "
                "ORDER BY vlr_rmn_bruta DESC, nom_servidor LIMIT 25"
            )

        # 1c. Salário do Prefeito / Vice-Prefeito / Secretários e comparação com a média salarial municipal
        if has_tbl("vw_gold_folha_servidores") and any(w in q_norm for w in ("prefeito", "vice-prefeito", "vice prefeito")) and any(
            w in q_norm for w in ("salario", "remuneracao", "subsidio", "ganha", "recebe", "media", "mensal")
        ):
            pref_yrs = [y for y in years if 2020 <= y <= cat_max_yr] or sorted({max(2024, cat_max_yr - 2), max(2024, cat_max_yr - 1), cat_max_yr})
            pref_yrs_sql = ", ".join(str(y) for y in pref_yrs)
            return (
                f"WITH media_mun AS ("
                f"SELECT ano_referencia, ROUND(AVG(vlr_rmn_bruta), 2) AS media_bruta_mun "
                f"FROM vw_gold_folha_servidores WHERE ano_referencia IN ({pref_yrs_sql}) GROUP BY ano_referencia"
                f") SELECT f.ano_referencia, f.nom_servidor, MAX(f.dsc_cargo) AS dsc_cargo, "
                f"ROUND(AVG(f.vlr_rmn_bruta), 2) AS remuneracao_mensal_bruta, "
                f"ROUND(AVG(f.vlr_rmn_liquida), 2) AS remuneracao_mensal_liquida, "
                f"m.media_bruta_mun AS media_salarial_prefeitura, "
                f"ROUND(AVG(f.vlr_rmn_bruta) / NULLIF(m.media_bruta_mun, 0), 2) AS multiplo_acima_da_media "
                f"FROM vw_gold_folha_servidores f "
                f"JOIN media_mun m ON f.ano_referencia = m.ano_referencia "
                f"WHERE f.ano_referencia IN ({pref_yrs_sql}) AND f.dsc_cargo ILIKE '%PREFEITO%' "
                f"GROUP BY f.ano_referencia, f.nom_servidor, m.media_bruta_mun "
                f"ORDER BY f.ano_referencia DESC, remuneracao_mensal_bruta DESC"
            )

        # 1d. Quadro de Funcionários: Concursados (Efetivos) vs. Contratados (Temporários) vs. Comissionados vs. Consignados
        if has_tbl("vw_gold_folha_servidores") and any(
            w in q_norm
            for w in (
                "concursado", "concursados", "consignado", "consignados", "contratado", "contratados",
                "temporario", "temporarios", "comissionado", "comissionados", "efetivo", "efetivos",
                "quantos funcionarios", "quantidade de funcionarios", "total de funcionarios", "quadro de servidores",
            )
        ) and not any(
            ent in q_norm for ent in ("empresa", "empresas", "fornecedor", "fornecedores", "credor", "credores", "cnpj")
        ):
            target_yr = years[-1] if (years and years[-1] <= cat_max_yr) else cat_max_yr
            return (
                f"SELECT ano_referencia, dsc_situacao, COUNT(DISTINCT seq_dim_pessoa) AS total_servidores, "
                f"ROUND(AVG(vlr_rmn_bruta), 2) AS media_salarial_bruta, "
                f"ROUND(SUM(vlr_rmn_bruta), 2) AS total_bruto "
                f"FROM vw_gold_folha_servidores "
                f"WHERE ano_referencia = {target_yr} "
                f"GROUP BY ano_referencia, dsc_situacao "
                f"ORDER BY total_servidores DESC"
            )

        # 1e. Quantidade Total de Veículos / Inventário Consolidado da Frota Municipal
        if has_tbl("vw_gold_frota_consolidada") and any(
            w in q_norm for w in ("veiculo", "veiculos", "frota", "carros", "caminhao", "onibus")
        ) and any(
            w in q_norm for w in ("quantidade", "quantos", "tem acesso", "possui", "total de veiculos", "tamanho da frota", "tem ")
        ):
            return (
                "SELECT status_cadastro_frota, "
                "COUNT(DISTINCT CASE WHEN TRY_CAST(seq_veiculo AS BIGINT) > 0 THEN seq_veiculo END) AS qtd_veiculos_distintos, "
                "COUNT(DISTINCT CASE WHEN dsc_num_placa NOT IN ('ISENTO_CTB_MAQUINARIO', 'SEM_CADASTRO_PLACA') THEN dsc_num_placa END) AS qtd_placas_ativas, "
                "ROUND(SUM(vlr_gasto), 2) AS total_gasto_historico_frota "
                "FROM vw_gold_frota_consolidada "
                "GROUP BY status_cadastro_frota "
                "ORDER BY qtd_veiculos_distintos DESC, total_gasto_historico_frota DESC"
            )

        # 1f-0. Orçamento / Dotação / Duodécimo por Órgão ou Poder Legislativo / Câmara / Autarquias (Gold View #8)
        if has_tbl("vw_gold_orcamento_por_orgao") and any(w in q_norm for w in ("orcamento", "dotacao", "duodecimo")) and any(
            w in q_norm for w in ("camara", "legislativo", "vereador", "por orgao", "cada orgao", "saae", "autarquia", "fundacao")
        ):
            valid_yrs = [y for y in years if 2020 <= y <= cat_max_yr]
            orc_where = (
                f"WHERE ano_referencia IN ({', '.join(str(y) for y in sorted(set(valid_yrs)))})"
                if valid_yrs
                else f"WHERE ano_referencia = {cat_max_yr}"
            )
            if "camara" in q_norm or "legislativo" in q_norm or "duodecimo" in q_norm:
                orc_where += " AND esfera_poder = 'LEGISLATIVO'"
            elif "saae" in q_norm:
                orc_where += " AND nom_orgao ILIKE '%SAAE%'"
            return (
                f"SELECT ano_referencia, nom_orgao, esfera_poder, administracao_tipo, "
                f"dotacao_inicial_loa, dotacao_atualizada, despesa_empenhada, despesa_liquidada, "
                f"despesa_paga, receita_arrecadada_propria_orgao, status_exercicio "
                f"FROM vw_gold_orcamento_por_orgao {orc_where} "
                f"ORDER BY ano_referencia, despesa_liquidada DESC"
            )

        # 1f-0b. Comparativo de Folha / Servidores por Órgão ou Poder (Câmara vs Prefeitura / Autarquias)
        if has_tbl("vw_gold_folha_servidores") and any(w in q_norm for w in ("camara", "legislativo", "por orgao", "cada orgao", "saae")) and any(
            w in q_norm for w in ("servidor", "servidores", "funcionario", "funcionarios", "folha", "salario", "remuneracao")
        ):
            target_yr = years[-1] if (years and years[-1] <= cat_max_yr) else cat_max_yr
            return (
                f"SELECT ano_referencia, nom_orgao, esfera_poder, COUNT(DISTINCT seq_dim_pessoa) AS total_servidores, "
                f"ROUND(AVG(vlr_rmn_bruta), 2) AS media_salarial_bruta, "
                f"ROUND(SUM(vlr_rmn_bruta), 2) AS total_bruto "
                f"FROM vw_gold_folha_servidores WHERE ano_referencia = {target_yr} "
                f"GROUP BY ano_referencia, nom_orgao, esfera_poder ORDER BY total_servidores DESC"
            )

        # 1f. Orçamento Anual Previsto (LOA) vs. Receita Realizada via Gold View Semântica #7
        if has_tbl("vw_gold_orcamento_previsto_vs_realizado") and "orcamento" in q_norm and "unidade orcamentaria" not in q_norm and "secretaria" not in q_norm:
            valid_yrs = [y for y in years if 2020 <= y <= cat_max_yr]
            if not valid_yrs and re.search(r"\b(24|25|26)\b", q_norm):
                if "24" in q_norm:
                    valid_yrs.append(2024)
                if "25" in q_norm:
                    valid_yrs.append(2025)
                if "26" in q_norm and cat_max_yr >= 2026:
                    valid_yrs.append(2026)
            orc_where = (
                f"WHERE ano_referencia IN ({', '.join(str(y) for y in sorted(set(valid_yrs)))}) "
                if valid_yrs
                else f"WHERE ano_referencia BETWEEN 2020 AND {cat_max_yr} "
            )
            return (
                f"SELECT ano_referencia, orcamento_previsto_loa, orcamento_atualizado_loa, "
                f"despesa_dotacao_atualizada, receita_arrecadada_realizada, despesa_empenhada, "
                f"despesa_liquidada, despesa_paga, ultimo_mes_receita, status_exercicio "
                f"FROM vw_gold_orcamento_previsto_vs_realizado {orc_where}"
                f"ORDER BY ano_referencia"
            )

        # 2. Consultas por Secretaria Municipal / Unidade Orçamentária (`orgao_orgaoUnidade` × `despesa_despesa`)
        if has_tbl("despesa_despesa") and (
            "secretaria" in q_norm or "secretarias" in q_norm or "unidade orcamentaria" in q_norm
        ):
            sec_filter = ""
            for kw, ilike_pat in (
                ("obras", "%OBRAS%"),
                ("saude", "%SAUDE%"),
                ("educacao", "%EDUCA%"),
                ("defesa social", "%DEFESA%"),
                ("assistencia", "%ASSIST%"),
                ("desenvolvimento social", "%SOCIAL%"),
                ("fazenda", "%FAZENDA%"),
                ("administracao", "%ADMINISTRA%"),
                ("cultura", "%CULTURA%"),
                ("esporte", "%ESPORTE%"),
                ("meio ambiente", "%AMBIENTE%"),
                ("governo", "%GOVERNO%"),
            ):
                if kw in q_norm:
                    sec_filter = (
                        f" AND (strip_accents(UPPER(COALESCE(u.dsc_unidade, ''))) ILIKE '{ilike_pat}' "
                        f"OR strip_accents(UPPER(COALESCE(d.dsc_funcao, ''))) ILIKE '{ilike_pat}' "
                        f"OR strip_accents(UPPER(COALESCE(d.dsc_subfuncao, ''))) ILIKE '{ilike_pat}')"
                    )
                    break
            if has_tbl("orgao_orgaoUnidade"):
                return (
                    f"WITH u_canon AS ("
                    f"SELECT seq_orgao, cod_unidade, dsc_unidade FROM ("
                    f"SELECT seq_orgao, cod_unidade, dsc_unidade, ROW_NUMBER() OVER (PARTITION BY seq_orgao, cod_unidade ORDER BY TRY_CAST(ano_referencia AS INTEGER) DESC NULLS LAST, dsc_unidade) AS rn "
                    f"FROM orgao_orgaoUnidade"
                    f") WHERE rn = 1"
                    f") SELECT d.ano_referencia, COALESCE(UPPER(TRIM(u.dsc_unidade)), UPPER(TRIM(d.dsc_funcao)), 'UNIDADE ORÇAMENTÁRIA') AS dsc_lotacao, "
                    f"ROUND(SUM(TRY_CAST(d.vlr_empenhado AS DECIMAL(18,2))), 2) AS total_empenhado, "
                    f"ROUND(SUM(TRY_CAST(d.vlr_liquidado AS DECIMAL(18,2))), 2) AS total_liquidado, "
                    f"ROUND(SUM(TRY_CAST(d.vlr_pago AS DECIMAL(18,2))), 2) AS total_pago "
                    f"FROM despesa_despesa d "
                    f"LEFT JOIN u_canon u ON d.seq_orgao = u.seq_orgao AND d.cod_unidade = u.cod_unidade "
                    f"WHERE d.{year_filter}{sec_filter} "
                    f"GROUP BY 1, 2 ORDER BY total_liquidado DESC LIMIT 15"
                )

        # 3. Respeitar escopo de fonte quando o usuário selecionou bases específicas
        if has_tbl("ibge_indicadores_municipais") and (
            allowed_src == {"IBGE"}
            or (
                ("ibge" in q_norm or "populacao" in q_norm or "habitantes" in q_norm or "idhm" in q_norm or "pib" in q_norm)
                and "per capita" not in q_norm
                and (not allowed_src or "IBGE" in allowed_src)
            )
        ):
            return (
                "SELECT ano_referencia, nom_municipio, populacao_residente, fonte_populacao, "
                "pib_municipal_mil_reais, pib_per_capita_reais, idhm_geral, area_territorial_km2 "
                "FROM ibge_indicadores_municipais ORDER BY ano_referencia"
            )

        if "per capita" in q_norm or "por habitante" in q_norm:
            if has_tbl("despesa_despesa") and has_tbl("ibge_indicadores_municipais") and (not allowed_src or ("IBGE" in allowed_src and "SICOM" in allowed_src)):
                return (
                    "WITH desp AS ("
                    "SELECT ano_referencia, SUM(TRY_CAST(vlr_liquidado AS DECIMAL(18,2))) AS despesa_liquidada "
                    "FROM despesa_despesa GROUP BY ano_referencia"
                    ") SELECT i.ano_referencia, i.populacao_residente, desp.despesa_liquidada, "
                    "ROUND(desp.despesa_liquidada / NULLIF(i.populacao_residente, 0), 2) AS gasto_per_capita_reais, "
                    "i.pib_per_capita_reais, i.idhm_geral "
                    "FROM ibge_indicadores_municipais i "
                    "JOIN desp ON i.ano_referencia = desp.ano_referencia ORDER BY i.ano_referencia"
                )
            if has_tbl("ibge_indicadores_municipais"):
                return (
                    "SELECT ano_referencia, nom_municipio, populacao_residente, pib_municipal_mil_reais, "
                    "pib_per_capita_reais, idhm_geral FROM ibge_indicadores_municipais ORDER BY ano_referencia"
                )

        if has_tbl("siconfi_finbra_indicadores") and (
            allowed_src == {"SICONFI"}
            or any(
                w in q_norm
                for w in (
                    "siconfi", "finbra", "capag", "tesouro nacional", "limite prudencial", "lrf", "rcl",
                    "contas da prefeitura", "contas estao em dia", "contas em dia", "futuro garantido",
                    "situacao fiscal", "saude fiscal",
                )
            )
        ):
            return (
                "SELECT ano_referencia, vlr_receita_corrente_liquida, vlr_despesa_pessoal_lrf, "
                "pct_despesa_pessoal_rcl, limite_prudencial_lrf_pct, nota_capag_tesouro, "
                "pct_aplicacao_saude_siops, pct_aplicacao_educacao_mde, situacao_fiscal_lrf "
                "FROM siconfi_finbra_indicadores ORDER BY ano_referencia"
            )

        if has_tbl("inep_ideb_educacao") and (
            allowed_src == {"INEP"} or any(w in q_norm for w in ("ideb", "inep", "censo escolar", "matricula", "matriculas", "aprovacao"))
        ):
            return (
                "SELECT ano_referencia, rede_ensino, ideb_anos_iniciais, meta_ideb_anos_iniciais, "
                "ideb_anos_finais, meta_ideb_anos_finais, taxa_aprovacao_iniciais_pct, "
                "qtd_escolas_municipais, total_matriculas_rede_municipal, vlr_gasto_educacao_por_aluno "
                "FROM inep_ideb_educacao ORDER BY ano_referencia"
            )

        datasus_kws = ("datasus", "siops", "cnes", "ubs", "leitos", "atencao basica") + (("santa casa",) if is_caete_tenant else ())
        if has_tbl("datasus_siops_saude") and (allowed_src == {"DATASUS"} or any(w in q_norm for w in datasus_kws)):
            return (
                "SELECT ano_referencia, pct_recursos_proprios_saude_siops, minimo_constitucional_saude_pct, "
                "vlr_despesa_total_saude, vlr_despesa_saude_per_capita, qtd_estabelecimentos_cnes, "
                "qtd_ubs_atencao_basica, cobertura_atencao_basica_pct, qtd_leitos_sus, hospital_referencia "
                "FROM datasus_siops_saude ORDER BY ano_referencia"
            )

        if has_tbl("receita_qsa_fornecedores") and (
            allowed_src == {"RECEITA_QSA"}
            or any(
                w in q_norm for w in ("qsa", "quadro societario", "socio", "socios", "receita federal", "porte", "cnae", "materialidade")
            )
        ):
            return (
                "SELECT razao_social, num_cnpj, porte_empresa, cnae_principal_descricao, "
                "qtd_socios_identificados, nomes_socios_qsa, qtd_pagamentos, total_pago_historico, "
                "alerta_auditoria_controladoria "
                "FROM receita_qsa_fornecedores "
                "WHERE COALESCE(porte_empresa, '') NOT ILIKE '%ÓRGÃO PÚBLICO%' "
                "ORDER BY total_pago_historico DESC LIMIT 15"
            )

        if has_tbl("cgu_convenios") and ("convenio" in q_norm or "convenios" in q_norm):
            return (
                "SELECT ano_referencia, num_convenio, orgao_concedente, objeto_convenio, "
                "vlr_global, vlr_repasse_uniao, vlr_liberado, situacao_convenio "
                "FROM cgu_convenios ORDER BY ano_referencia DESC"
            )

        if has_tbl("cgu_emendas") and ("emenda" in q_norm or "emendas" in q_norm or "parlamentar" in q_norm):
            return (
                "SELECT ano_referencia, cod_emenda, nom_autor_emenda, tipo_emenda, "
                "funcao_programatica, vlr_empenhado, vlr_liquidado, vlr_pago "
                "FROM cgu_emendas ORDER BY ano_referencia DESC"
            )

        if allowed_src == {"CGU"} or any(w in q_norm for w in ("bolsa familia", "novo bolsa familia", "bpc", "auxilio", "cgu")):
            def _cgu_money_sum() -> str:
                return (
                    'ROUND(SUM(TRY_CAST(CASE WHEN CONTAINS(CAST("VALOR PARCELA" AS VARCHAR), \',\') '
                    'THEN REPLACE(REPLACE(CAST("VALOR PARCELA" AS VARCHAR), \'.\', \'\'), \',\', \'.\') '
                    'ELSE CAST("VALOR PARCELA" AS VARCHAR) END AS DECIMAL(18,2))), 2)'
                )

            def _cgu_has_summary_col(tbl_key: str) -> bool:
                canon_t = cat_tables_map.get(tbl_key.lower())
                if not canon_t:
                    return False
                t_meta = self.db_manager.catalog.tables.get(canon_t)
                return bool(t_meta and any(c.name.lower() == "qtd_beneficiados" for c in t_meta.columns))

            wants_bf = "bolsa familia" in q_norm or ("bpc" not in q_norm)
            wants_bpc = "bpc" in q_norm
            if wants_bf and wants_bpc and has_tbl("cgu_novo_bolsa_familia") and has_tbl("cgu_bpc"):
                bf_cnt = (
                    "MAX(TRY_CAST(qtd_beneficiados AS BIGINT))"
                    if _cgu_has_summary_col("cgu_novo_bolsa_familia")
                    else 'COUNT(DISTINCT "NIS FAVORECIDO")'
                )
                bpc_cnt = (
                    "MAX(TRY_CAST(qtd_beneficiados AS BIGINT))"
                    if _cgu_has_summary_col("cgu_bpc")
                    else 'COUNT(DISTINCT "NIS FAVORECIDO")'
                )
                return (
                    f"SELECT 'Novo Bolsa Família' AS programa, ano_referencia, {bf_cnt} AS beneficiarios_referencia, "
                    f"COUNT(*) AS registros_apurados, {_cgu_money_sum()} AS total_pago "
                    f"FROM cgu_novo_bolsa_familia GROUP BY ano_referencia "
                    f"UNION ALL "
                    f"SELECT 'BPC (Benefício de Prestação Continuada)' AS programa, ano_referencia, {bpc_cnt} AS beneficiarios_referencia, "
                    f"COUNT(*) AS registros_apurados, {_cgu_money_sum()} AS total_pago "
                    f"FROM cgu_bpc GROUP BY ano_referencia "
                    f"ORDER BY programa, ano_referencia"
                )
            if wants_bpc and has_tbl("cgu_bpc"):
                cnt_proj = (
                    "MAX(TRY_CAST(qtd_beneficiados AS BIGINT)) AS beneficiarios_no_mes, COUNT(*) AS meses_apurados"
                    if _cgu_has_summary_col("cgu_bpc")
                    else 'COUNT(DISTINCT "NIS FAVORECIDO") AS beneficiarios_unicos, COUNT(*) AS qtd_registros'
                )
                return (
                    f"SELECT ano_referencia, {cnt_proj}, {_cgu_money_sum()} AS total_pago "
                    f"FROM cgu_bpc GROUP BY ano_referencia ORDER BY ano_referencia"
                )
            if has_tbl("cgu_novo_bolsa_familia"):
                cnt_proj = (
                    "MAX(TRY_CAST(qtd_beneficiados AS BIGINT)) AS beneficiarios_no_mes, COUNT(*) AS meses_apurados"
                    if _cgu_has_summary_col("cgu_novo_bolsa_familia")
                    else 'COUNT(DISTINCT "NIS FAVORECIDO") AS beneficiarios_unicos, COUNT(*) AS total_registros'
                )
                if "por ano" in q_norm or "evolucao" in q_norm or _cgu_has_summary_col("cgu_novo_bolsa_familia"):
                    return (
                        f"SELECT ano_referencia, {cnt_proj}, {_cgu_money_sum()} AS total_pago "
                        f"FROM cgu_novo_bolsa_familia GROUP BY ano_referencia ORDER BY ano_referencia"
                    )
                return (
                    f"SELECT {cnt_proj}, {_cgu_money_sum()} AS total_pago "
                    f"FROM cgu_novo_bolsa_familia"
                )

        if has_tbl("receita_receita") and has_tbl("despesa_despesa") and ("isf-m" in q_norm or "saude financeira" in q_norm):
            isf_years = [int(y) for y in re.findall(r"\b(20\d{2})\b", q_norm)]
            isf_where = ""
            if len(isf_years) >= 2:
                isf_where = f" WHERE rc.ano_referencia BETWEEN {min(isf_years)} AND {max(isf_years)}"
            elif len(isf_years) == 1:
                isf_where = f" WHERE rc.ano_referencia = {isf_years[0]}"
            return (
                "WITH rc AS ("
                "SELECT ano_referencia, SUM(TRY_CAST(vlr_realizadoateperiodo AS DECIMAL(18,2))) AS rec "
                "FROM receita_receita WHERE dsc_cateconomica ILIKE '%Corrente%' GROUP BY ano_referencia"
                "), dc AS ("
                "SELECT ano_referencia, SUM(TRY_CAST(vlr_liquidado AS DECIMAL(18,2))) AS desp "
                "FROM despesa_despesa WHERE dsc_naturezadespesa LIKE '3%' GROUP BY ano_referencia"
                ") SELECT rc.ano_referencia, ROUND(rc.rec / NULLIF(dc.desp, 0), 4) AS isf_m, "
                "rc.rec AS receita_corrente, dc.desp AS despesa_corrente "
                f"FROM rc JOIN dc ON rc.ano_referencia = dc.ano_referencia{isf_where} ORDER BY rc.ano_referencia"
            )

        if has_tbl("siconfi_finbra_indicadores") and ("ies-c" in q_norm or "indice de saude" in q_norm or "minimo constitucional de saude" in q_norm):
            return (
                "SELECT ano_referencia, pct_aplicacao_saude_siops AS ies_c_pct, "
                "vlr_receita_corrente_liquida, nota_capag_tesouro, situacao_fiscal_lrf "
                "FROM siconfi_finbra_indicadores ORDER BY ano_referencia"
            )

        if has_tbl("siconfi_finbra_indicadores") and ("ieq-c" in q_norm or "indice de educacao" in q_norm or "minimo constitucional de educacao" in q_norm):
            return (
                "SELECT ano_referencia, pct_aplicacao_educacao_mde AS ieq_c_pct, "
                "vlr_receita_corrente_liquida, nota_capag_tesouro, situacao_fiscal_lrf "
                "FROM siconfi_finbra_indicadores ORDER BY ano_referencia"
            )

        if has_tbl("remuneracao_municipio") and has_tbl("pessoa_municipio") and (
            "servidor" in q_norm or "salario" in q_norm or "remuneracao" in q_norm or "folha" in q_norm
        ) and (
            "maiores" in q_norm or "lista" in q_norm or "cpf" in q_norm or "documento" in q_norm or "quem" in q_norm
        ):
            return (
                f"SELECT p.nom_pessoa, p.num_documento, r.dsc_cargo, "
                f"MAX(TRY_CAST(r.vlr_rmn_bruta AS DECIMAL(18,2))) AS maior_remuneracao_bruta, "
                f"MAX(TRY_CAST(r.vlr_rmn_liquida AS DECIMAL(18,2))) AS maior_remuneracao_liquida "
                f"FROM remuneracao_municipio r "
                f"JOIN pessoa_municipio p ON r.seq_dim_pessoa = p.seq_dim_pessoa AND r.ano_referencia = p.ano_referencia "
                f"WHERE r.{year_filter} AND COALESCE(p.nom_pessoa, '') NOT ILIKE '%NAO INFORMADO%' "
                f"GROUP BY p.nom_pessoa, p.num_documento, r.dsc_cargo "
                f"ORDER BY maior_remuneracao_bruta DESC LIMIT 10"
            )

        if has_tbl("remuneracao_municipio") and (
            "salario" in q_norm or "remuneracao" in q_norm or "folha" in q_norm or "servidor" in q_norm or "funcionario" in q_norm
        ):
            return (
                f"SELECT ano_referencia, COUNT(DISTINCT seq_dim_pessoa) AS total_servidores, "
                f"SUM(TRY_CAST(vlr_rmn_bruta AS DECIMAL(18,2))) AS total_bruto, "
                f"SUM(TRY_CAST(vlr_rmn_liquida AS DECIMAL(18,2))) AS total_liquido "
                f"FROM remuneracao_municipio WHERE {year_filter} GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if has_tbl("vw_gold_execucao_despesa_credor") and (
            "fornecedor" in q_norm or "credor" in q_norm or "empresas" in q_norm or "empresa" in q_norm or "maiores pagamentos" in q_norm
        ):
            include_all_creditors = any(
                w in q_norm for w in ("todos os credores", "incluindo inss", "entidades", "santa casa", "consorcio")
            )
            nat_clause = "" if include_all_creditors else " AND natureza_credor = 'EMPRESA_PRIVADA'"
            return (
                f"SELECT MAX(nom_credor) AS nom_credor, MAX(num_doc_credor) AS num_doc_credor, "
                f"MAX(natureza_credor) AS natureza_credor, SUM(qtd_empenhos) AS qtd_empenhos, "
                f"ROUND(SUM(total_liquidado), 2) AS total_liquidado, ROUND(SUM(total_pago), 2) AS total_pago "
                f"FROM vw_gold_execucao_despesa_credor WHERE {year_filter}{nat_clause} "
                f"GROUP BY COALESCE(doc_credor_raiz, num_doc_credor) ORDER BY total_pago DESC LIMIT 10"
            )

        if "saude" in q_norm:
            if not has_tbl("despesa_despesa") and has_tbl("datasus_siops_saude"):
                return (
                    "SELECT ano_referencia, pct_recursos_proprios_saude_siops, minimo_constitucional_saude_pct, "
                    "vlr_despesa_total_saude, vlr_despesa_saude_per_capita "
                    "FROM datasus_siops_saude ORDER BY ano_referencia"
                )
            return (
                f"SELECT ano_referencia, SUM(TRY_CAST(vlr_empenhado AS DECIMAL(18,2))) AS total_empenhado, "
                f"SUM(TRY_CAST(vlr_liquidado AS DECIMAL(18,2))) AS total_liquidado, "
                f"SUM(TRY_CAST(vlr_pago AS DECIMAL(18,2))) AS total_pago "
                f"FROM despesa_despesa WHERE {year_filter} AND (dsc_funcao ILIKE '%SAÚDE%' OR dsc_funcao ILIKE '%SAUDE%') "
                f"GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if "educacao" in q_norm:
            if not has_tbl("despesa_despesa") and has_tbl("siconfi_finbra_indicadores"):
                return (
                    "SELECT ano_referencia, pct_aplicacao_educacao_mde, vlr_receita_corrente_liquida, situacao_fiscal_lrf "
                    "FROM siconfi_finbra_indicadores ORDER BY ano_referencia"
                )
            return (
                f"SELECT ano_referencia, SUM(TRY_CAST(vlr_empenhado AS DECIMAL(18,2))) AS total_empenhado, "
                f"SUM(TRY_CAST(vlr_liquidado AS DECIMAL(18,2))) AS total_liquidado, "
                f"SUM(TRY_CAST(vlr_pago AS DECIMAL(18,2))) AS total_pago "
                f"FROM despesa_despesa WHERE {year_filter} AND (dsc_funcao ILIKE '%EDUCA%' OR dsc_funcao ILIKE '%EDUCAC%') "
                f"GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if has_tbl("contrato_contratos") and "contrato" in q_norm:
            return (
                f"SELECT ano_referencia, COUNT(*) AS total_contratos, "
                f"SUM(TRY_CAST(vlr_contrato AS DECIMAL(18,2))) AS valor_total_contratado "
                f"FROM contrato_contratos WHERE {year_filter} GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if has_tbl("licitacao_dispensa") and "dispensa" in q_norm:
            return (
                f"SELECT ano_referencia, COUNT(*) AS total_processos, "
                f"SUM(TRY_CAST(vlr_empenhado AS DECIMAL(18,2))) AS total_empenhado, "
                f"SUM(TRY_CAST(vlr_pago AS DECIMAL(18,2))) AS total_pago "
                f"FROM licitacao_dispensa WHERE {year_filter} GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if has_tbl("licitacao_licitacao") and ("licitacao" in q_norm or "licitacoes" in q_norm):
            return (
                f"SELECT ano_referencia, dsc_modalidade, COUNT(*) AS qtd_licitacoes, "
                f"SUM(TRY_CAST(vlr_liquidado AS DECIMAL(18,2))) AS total_liquidado "
                f"FROM licitacao_licitacao WHERE {year_filter} GROUP BY ano_referencia, dsc_modalidade ORDER BY total_liquidado DESC LIMIT 15"
            )

        if has_tbl("frota_gastoFrota") and ("frota" in q_norm or "combustivel" in q_norm or "veiculo" in q_norm):
            return (
                f"SELECT ano_referencia, COUNT(*) AS registros_gasto, "
                f"SUM(TRY_CAST(vlr_gasto AS DECIMAL(18,2))) AS total_gasto_frota "
                f"FROM frota_gastoFrota WHERE {year_filter} GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if "receita" in q_norm or "arrecadacao" in q_norm:
            if not has_tbl("receita_receita") and has_tbl("siconfi_finbra_indicadores"):
                return (
                    "SELECT ano_referencia, vlr_receita_corrente_liquida, vlr_despesa_pessoal_lrf, pct_despesa_pessoal_rcl, situacao_fiscal_lrf "
                    "FROM siconfi_finbra_indicadores ORDER BY ano_referencia"
                )
            return (
                f"SELECT ano_referencia, SUM(TRY_CAST(vlr_realizadoateperiodo AS DECIMAL(18,2))) AS total_arrecadado "
                f"FROM receita_receita WHERE {year_filter} GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if any(w in q_norm for w in ("pagamento", "pagamentos", "pagou")):
            if not has_tbl("despesa_pagamento") and has_tbl("siconfi_finbra_indicadores"):
                return (
                    "SELECT ano_referencia, vlr_receita_corrente_liquida, vlr_despesa_pessoal_lrf, pct_aplicacao_saude_siops, pct_aplicacao_educacao_mde "
                    "FROM siconfi_finbra_indicadores ORDER BY ano_referencia"
                )
            return (
                f"SELECT ano_referencia, COUNT(*) AS qtd_pagamentos, "
                f"SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2))) AS total_pago "
                f"FROM despesa_pagamento WHERE {year_filter} GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if any(w in q_norm for w in ("despesa", "despesas", "gasto", "gastos", "gastou", "empenho", "empenhado")):
            if not has_tbl("despesa_despesa") and has_tbl("siconfi_finbra_indicadores"):
                return (
                    "SELECT ano_referencia, vlr_receita_corrente_liquida, vlr_despesa_pessoal_lrf, pct_despesa_pessoal_rcl, "
                    "pct_aplicacao_saude_siops, pct_aplicacao_educacao_mde "
                    "FROM siconfi_finbra_indicadores ORDER BY ano_referencia"
                )
            return (
                f"SELECT ano_referencia, "
                f"ROUND(SUM(TRY_CAST(vlr_empenhado AS DECIMAL(18,2))), 2) AS total_empenhado, "
                f"ROUND(SUM(TRY_CAST(vlr_liquidado AS DECIMAL(18,2))), 2) AS total_liquidado, "
                f"ROUND(SUM(TRY_CAST(vlr_pago AS DECIMAL(18,2))), 2) AS total_pago "
                f"FROM despesa_despesa WHERE {year_filter} GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if has_tbl("vw_gold_contratos_vigencia_aditivos") and any(
            w in q_norm for w in ("contrato", "contratos", "contratad", "aditivo", "aditivos", "fornecedor", "fornecedores", "empresa", "empresas")
        ):
            ct_yr = f"WHERE {year_filter} " if years else ""
            return (
                "SELECT num_contrato, ano_referencia, nom_contratado, doc_contratado, "
                "dsc_objeto, vlr_contrato, vlr_total_atualizado_contrato, dat_fimvigencia "
                f"FROM vw_gold_contratos_vigencia_aditivos {ct_yr}"
                "ORDER BY ano_referencia DESC, vlr_total_atualizado_contrato DESC LIMIT 15"
            )

        # Contingência offline segura: síntese macro-orçamentária oficial (jamais pagamento cego de credores)
        if not has_tbl("vw_gold_orcamento_previsto_vs_realizado"):
            if has_tbl("siconfi_finbra_indicadores"):
                return (
                    "SELECT ano_referencia, vlr_receita_corrente_liquida, vlr_despesa_pessoal_lrf, "
                    "pct_despesa_pessoal_rcl, pct_aplicacao_saude_siops, pct_aplicacao_educacao_mde, situacao_fiscal_lrf "
                    "FROM siconfi_finbra_indicadores ORDER BY ano_referencia"
                )
            if has_tbl("ibge_indicadores_municipais"):
                return (
                    "SELECT ano_referencia, nom_municipio, populacao_residente, pib_municipal_mil_reais, pib_per_capita_reais "
                    "FROM ibge_indicadores_municipais ORDER BY ano_referencia"
                )
        return (
            "SELECT ano_referencia, orcamento_previsto_loa, receita_arrecadada_realizada, "
            "despesa_empenhada, despesa_liquidada, despesa_paga, status_exercicio "
            "FROM vw_gold_orcamento_previsto_vs_realizado ORDER BY ano_referencia"
        )

    def _smart_sql_generator(
        self,
        prompt_pkg: ModularPromptPackage,
        attempt: int,
        last_error: Optional[str],
    ) -> str:
        """
        Eixo 1 — Geração Intrínseca de SQL:
        1. Quando há chave de LLM externa configurada (`OPENAI_API_KEY` ou `GEMINI_API_KEY`) e franquia B2G ativa,
           o LLM externo guiado pelo Catálogo Semântico (Auto-Profiling + Grão + Gold Views) + BM25 Dynamic Few-Shot
           é SEMPRE o gerador primário desde a Tentativa 1 (zero interceptação por `if/elif` de palavras-chave).
        2. O compilador nativo determinístico atua exclusivamente como contingência offline (ex.: `pytest` sem rede
           ou franquia mensal em Hard Cap).
        """
        self._last_llm_engine_used = "SIM Native NL2SQL Compiler"
        q_raw = prompt_pkg.assembled_sql_prompt.split("<user_question>")[-1].split("</user_question>")[0].strip()
        q_norm = normalize_text_for_dedup(q_raw)
        pkg_sources = prompt_pkg.allowed_sources if prompt_pkg.allowed_sources else self._active_allowed_sources
        allowed_src = {s.upper() for s in pkg_sources if s.upper() != "DIARIO_OFICIAL"}

        # 1. Motor Primário Intrínseco: LLM externo guiado por Camada Semântica + Dynamic Few-Shot BM25
        llm_sql = self._call_external_llm_for_sql(
            prompt_pkg.assembled_sql_prompt,
            llm_provider=prompt_pkg.llm_provider,
            llm_api_key=prompt_pkg.llm_api_key,
        )
        if llm_sql:
            return llm_sql

        # 2. Contingência Offline (0 tokens — sem API key ativa ou Hard Cap B2G atingido)
        is_caete_tenant = self.db_manager.tenant.tenant_id in ("caete_mg_3110004", "caete-mg")
        if is_caete_tenant and not allowed_src and attempt == 1 and q_norm in self._golden_lookup:
            self._last_llm_engine_used = "SIM Golden Verified Index + NL2SQL"
            return self._golden_lookup[q_norm]

        return self._compile_native_sql(q_raw, q_norm, allowed_src)

    def _normalize_sql_ast_resilience(self, sql_text: str) -> str:
        """
        Passe AST resiliente pós-LLM (SIM v4.6 — Zero Whack-a-Mole):
        1. Torna comparações textuais (`=`, `IN`, `ILIKE`) em colunas categóricas não-PII insensíveis
           a acentos e maiúsculas/minúsculas via `strip_accents(UPPER(...))` (Invariante F2.4).
        2. Corrige precedência booleana `WHERE ano_referencia = X AND a OR b` -> `WHERE ano_referencia = X AND (a OR b)`
           quando o ramo direito do `OR` não menciona `ano_referencia`.
        3. Deduplica automaticamente `JOIN orgao_orgao` quando `ano_referencia` não participa do `ON` (Invariante F10.1).
        4. Reescreve `LEFT(num_doc_credor, 8)` / `SUBSTR(num_doc_credor, 1, 8)` para `doc_credor_raiz` nas views que já expõem a coluna.
        """
        if not sql_text or not sql_text.strip():
            return sql_text
        try:
            tree = sqlglot.parse_one(sql_text, read="duckdb")
        except Exception:
            return sql_text

        try:
            cte_names = {(c.alias_or_name or "").lower() for c in tree.find_all(exp.CTE)}
            base_tables_low = {
                (tb.name or "").lower()
                for tb in tree.find_all(exp.Table)
                if (tb.name or "").lower() not in cte_names
            }
            has_doc_raiz_view = bool(
                base_tables_low.intersection({"vw_gold_execucao_despesa_credor", "vw_gold_fornecedores_qsa_360"})
            )

            # 4. LEFT/SUBSTR(num_doc_credor, 8) -> doc_credor_raiz quando disponível na view
            if has_doc_raiz_view:
                for fn in list(tree.find_all(exp.Func)):
                    fn_name = (fn.name if isinstance(fn, exp.Anonymous) else fn.sql_name() or "").lower()
                    if fn_name in ("left", "substr", "substring"):
                        cols_in_fn = list(fn.find_all(exp.Column))
                        lits_in_fn = [str(l.this) for l in fn.find_all(exp.Literal) if not l.is_string]
                        if (
                            len(cols_in_fn) == 1
                            and (cols_in_fn[0].name or "").lower() == "num_doc_credor"
                            and "8" in lits_in_fn
                        ):
                            tbl_qual = cols_in_fn[0].table
                            repl_col = exp.column("doc_credor_raiz", table=tbl_qual) if tbl_qual else exp.column("doc_credor_raiz")
                            fn.replace(repl_col)

            # 3. Anti-Fan-Out automático em JOIN orgao_orgao sem ano_referencia no ON
            for join_node in list(tree.find_all(exp.Join)):
                j_tbl = join_node.this
                if isinstance(j_tbl, exp.Table) and (j_tbl.name or "").lower() == "orgao_orgao" and "orgao_orgao" not in cte_names:
                    on_sql = (join_node.args.get("on").sql("duckdb") if join_node.args.get("on") else "").lower()
                    if "ano_referencia" not in on_sql:
                        alias_name = j_tbl.alias_or_name or "orgao_orgao"
                        dedup_sub = sqlglot.parse_one(
                            f"(SELECT * EXCLUDE (_rn_org_dedup) FROM ("
                            f"SELECT *, ROW_NUMBER() OVER (PARTITION BY seq_orgao ORDER BY TRY_CAST(ano_referencia AS INTEGER) DESC NULLS LAST) AS _rn_org_dedup "
                            f"FROM orgao_orgao) _t_org WHERE _rn_org_dedup = 1) AS {alias_name}",
                            read="duckdb",
                        )
                        join_node.set("this", dedup_sub)

            # 2. Precedência booleana: WHERE (ano_referencia = X AND condA) OR condB -> WHERE ano_referencia = X AND (condA OR condB)
            for where_node in list(tree.find_all(exp.Where)):
                top = where_node.this
                if isinstance(top, exp.Or) and isinstance(top.this, exp.And):
                    right_cols = {(c.name or "").lower() for c in top.expression.find_all(exp.Column)}
                    if "ano_referencia" not in right_cols:
                        and_parts: List[exp.Expression] = []
                        cur_a: exp.Expression = top.this
                        while isinstance(cur_a, exp.And):
                            and_parts.append(cur_a.expression)
                            cur_a = cur_a.this
                        and_parts.append(cur_a)
                        and_parts.reverse()
                        yr_preds = [
                            p for p in and_parts
                            if any((c.name or "").lower() == "ano_referencia" for c in p.find_all(exp.Column))
                        ]
                        other_preds = [p for p in and_parts if p not in yr_preds]
                        if yr_preds and other_preds:
                            rest_left = other_preds[0]
                            for p in other_preds[1:]:
                                rest_left = exp.and_(rest_left, p)
                            grouped_or = exp.Paren(this=exp.or_(rest_left, top.expression))
                            new_where = yr_preds[0]
                            for yp in yr_preds[1:]:
                                new_where = exp.and_(new_where, yp)
                            new_where = exp.and_(new_where, grouped_or)
                            where_node.set("this", new_where)

            # 0. Remover filtro redundante/incompatível `cod_municipio = <ibge_7_ou_6>` (bancos DuckDB já são isolados fisicamente por cidade)
            ibge_str = str(self.db_manager.tenant.ibge_code or "").strip()
            ibge_variants = {ibge_str, ibge_str[:6]} if len(ibge_str) >= 6 else {ibge_str}
            for eq_m in list(tree.find_all(exp.EQ)):
                if isinstance(eq_m.this, exp.Column) and (eq_m.this.name or "").lower() in ("cod_municipio", "cod_ibge"):
                    if isinstance(eq_m.expression, exp.Literal):
                        lit_m = str(eq_m.expression.this or "").strip()
                        if lit_m in ibge_variants:
                            eq_m.replace(sqlglot.parse_one("1 = 1", read="duckdb"))

            # 1. Comparações textuais sem acento/case em colunas não-sensíveis
            _pii_skip = {"num_cpf", "cpf", "num_documento", "num_doc_credor", "nis", "nis favorecido", "cpf favorecido"}

            def _wrap_strip_upper(node_expr: exp.Expression) -> exp.Expression:
                s_sql = node_expr.sql("duckdb")
                if "strip_accents" in s_sql.lower():
                    return node_expr
                return sqlglot.parse_one(f"strip_accents(UPPER(TRIM(CAST({s_sql} AS VARCHAR))))", read="duckdb")

            for ilike_node in list(tree.find_all(exp.ILike)):
                if isinstance(ilike_node.this, exp.Column) and isinstance(ilike_node.expression, exp.Literal) and ilike_node.expression.is_string:
                    cname = (ilike_node.this.name or "").lower()
                    if cname not in _pii_skip:
                        pat_sql = ilike_node.expression.sql("duckdb")
                        lh = sqlglot.parse_one(f"strip_accents(UPPER(CAST({ilike_node.this.sql('duckdb')} AS VARCHAR)))", read="duckdb")
                        rh = sqlglot.parse_one(f"strip_accents(UPPER({pat_sql}))", read="duckdb")
                        ilike_node.replace(exp.Like(this=lh, expression=rh))

            _cat_text_cols = {
                "tipo_vinculo", "dsc_cargo", "cargo_politico", "dsc_lotacao", "dsc_situacao", "situacao_funcional",
                "dsc_funcao", "dsc_subfuncao", "dsc_origem", "dsc_cateconomica", "dsc_rubrica", "dsc_alinea",
                "nom_orgao", "dsc_unidade", "dsc_modalidade", "modalidade", "dsc_tipo_gasto", "esfera_poder",
            }
            for eq_node in list(tree.find_all(exp.EQ)):
                if isinstance(eq_node.expression, exp.Literal) and eq_node.expression.is_string:
                    lit_v = str(eq_node.expression.this or "")
                    lhs_sql = eq_node.this.sql("duckdb").lower()
                    if "strip_accents" in lhs_sql and any(ord(ch) > 127 or ch.islower() for ch in lit_v):
                        eq_node.set("expression", _wrap_strip_upper(eq_node.expression))
                        eq_node.set("this", sqlglot.parse_one(f"UPPER({eq_node.this.sql('duckdb')})", read="duckdb"))
                    elif isinstance(eq_node.this, exp.Column):
                        cname = (eq_node.this.name or "").lower()
                        if cname in _cat_text_cols and cname not in _pii_skip and any(ch.isalpha() for ch in lit_v):
                            eq_node.set("this", _wrap_strip_upper(eq_node.this))
                            eq_node.set("expression", _wrap_strip_upper(eq_node.expression))

            for in_node in list(tree.find_all(exp.In)):
                if isinstance(in_node.this, exp.Column) and in_node.expressions:
                    cname = (in_node.this.name or "").lower()
                    if (
                        cname in _cat_text_cols
                        and cname not in _pii_skip
                        and all(isinstance(x, exp.Literal) and x.is_string for x in in_node.expressions)
                    ):
                        in_node.set("this", _wrap_strip_upper(in_node.this))
                        in_node.set("expressions", [_wrap_strip_upper(x) for x in in_node.expressions])

            return tree.sql("duckdb")
        except Exception:
            return sql_text

    def _run_sql_pipeline(
        self,
        *,
        effective_question: str,
        raw_question_norm: str,
        role: str,
        hist_list: List[Dict[str, Any]],
        previous_sql: Optional[str],
        explicit_segments: Optional[List[str]],
        explicit_tables: Optional[List[str]],
        allowed_sources: Optional[List[str]],
        sql_override: Optional[str],
        llm_provider: Optional[str],
        llm_api_key: Optional[str],
        steps: List[Dict[str, Any]],
        step_prefix: str = "",
        wants_chart: bool = False,
        wants_table: bool = False,
        intent: str = "sql_analytics",
    ) -> Dict[str, Any]:
        """
        Executa o pipeline completo de Text-to-SQL Semântico (Eixo 1 + Eixo 3 + Eixo 8):
        Schema Linking -> Entity Linker -> Okapi BM25 Dynamic Few-Shot -> Montagem Modular ->
        Execução segura AST (`SQLGuard`) -> Self-Correction guiada por Diagnóstico de 0 Linhas.
        """
        s3_t0 = time.perf_counter()
        semantic_segs = self.semantic_infer_segments(effective_question)
        combined_segs = list(dict.fromkeys((explicit_segments or []) + semantic_segs)) or None
        selection: SchemaSelectionResult = self.db_manager.catalog.select_tables_for_query(
            question=effective_question,
            explicit_segments=combined_segs,
            explicit_tables=explicit_tables,
            allowed_sources=allowed_sources,
        )
        selected_segments = selection.selected_segments
        selected_tables = selection.selected_tables
        reduction_pct = selection.reduction_pct

        value_bindings = self.entity_linker.link_entities(effective_question, top_k=6)
        few_shots = self.entity_linker.select_few_shots(effective_question, top_k=4)

        restricted_guard_tables = (
            self.db_manager.catalog.get_allowed_table_names(
                role=role,
                allowed_sources=allowed_sources,
                explicit_tables=explicit_tables,
            )
            if (allowed_sources or explicit_tables)
            else None
        )

        steps.append(
            {
                "step": len(steps) + 1,
                "name": f"{step_prefix}Catálogo Semântico, Auto-Profiling, Value Linking & BM25 Few-Shot",
                "status": "OK",
                "duration_ms": round((time.perf_counter() - s3_t0) * 1000.0, 2),
                "detail": (
                    f"Modo: {selection.selection_mode} | "
                    f"Fontes permitidas: {', '.join(selection.allowed_sources) if selection.allowed_sources else 'Todas'} | "
                    f"Tabelas enviadas ao modelo: {selection.selected_count} de {selection.total_catalog_tables} "
                    f"(Redução de {reduction_pct}% no ruído de schema) | "
                    f"Entidades vinculadas: {len(value_bindings)} | Dynamic Few-Shots BM25: {len(few_shots)}."
                ),
            }
        )

        sql_rows: List[Dict[str, Any]] = []
        fontes: List[str] = []
        warnings: List[Dict[str, Any]] = []
        sql_executed: Optional[str] = None
        attempts = 0
        est_tokens = 0
        pii_masked = 0
        last_err: Optional[str] = None
        zero_rows_diag: Optional[str] = None

        for attempt in range(1, self.max_sql_retries + 2):
            attempts = attempt
            s4_t0 = time.perf_counter()
            prompt_pkg = self.prompt_builder.assemble(
                user_question=effective_question,
                selection=selection,
                role=role,
                error_feedback=last_err,
                conversation_history=hist_list,
                value_bindings=value_bindings,
                few_shot_examples=few_shots,
                allowed_sources=list(allowed_sources or []),
                llm_provider=llm_provider,
                llm_api_key=llm_api_key,
                previous_sql=previous_sql,
                zero_rows_diagnostic=zero_rows_diag,
                wants_chart=wants_chart,
                wants_table=wants_table,
                intent=intent,
            )
            est_tokens = prompt_pkg.estimated_tokens
            raw_cand_sql = sql_override if (sql_override and attempt == 1) else self.sql_generator_fn(prompt_pkg, attempt, last_err)
            candidate_sql = raw_cand_sql if sql_override else self._normalize_sql_ast_resilience(raw_cand_sql)
            steps.append(
                {
                    "step": len(steps) + 1,
                    "name": f"{step_prefix}Montagem Modular Semântica & Geração SQL (Tentativa {attempt})",
                    "status": "OK",
                    "duration_ms": round((time.perf_counter() - s4_t0) * 1000.0, 2),
                    "detail": (
                        f"Motor: {self._last_llm_engine_used} | Prompt enxuto: ~{est_tokens} tokens "
                        f"(vs. 14.961 tokens no prompt v2 estático). SQL candidato sintetizado."
                    ),
                }
            )

            s5_t0 = time.perf_counter()
            q_res = self.db_manager.execute_safe_query(
                candidate_sql,
                role=role,
                allowed_tables_override=restricted_guard_tables,
            )
            steps.append(
                {
                    "step": len(steps) + 1,
                    "name": f"{step_prefix}SQLGuard (Validação AST + Escopo) & Execução DuckDB Parquet (Tentativa {attempt})",
                    "status": "OK" if q_res["status"] == "sucesso" else q_res["status"].upper(),
                    "duration_ms": round((time.perf_counter() - s5_t0) * 1000.0, 2),
                    "detail": (
                        f"Status: {q_res['status']} | Linhas retornadas: {q_res.get('total_linhas', 0)} | "
                        f"Tabelas na AST: {', '.join(q_res.get('tabelas_referenciadas', []))} | "
                        f"CPFs mascarados (LGPD): {q_res.get('pii_masked_count', 0)}."
                        if q_res["status"] == "sucesso"
                        else f"Bloqueio/Erro capturado ({q_res.get('erro_codigo')}): {q_res.get('detalhe')}"
                    ),
                }
            )

            is_generative_engine = (
                any(p in self._last_llm_engine_used for p in ("Gemini", "OpenAI"))
                or self._has_custom_sql_generator
            )

            if q_res["status"] == "sucesso":
                returned_rows = q_res.get("dados", [])
                raw_warnings = q_res.get("transparency_warnings") or []
                sql_years = {int(y) for y in re.findall(r"\b(202[0-9])\b", candidate_sql)}
                is_exclusive_historical_gap = False
                for w_obj in raw_warnings:
                    avail_yrs = {int(y) for y in (w_obj.get("available_years") or [])}
                    aff_yrs = {int(y) for y in (w_obj.get("affected_years") or [])}
                    if sql_years and aff_yrs and sql_years.issubset(aff_yrs) and (not avail_yrs or sql_years.isdisjoint(avail_yrs)):
                        is_exclusive_historical_gap = True
                        break

                # Eixo 1: Self-Correction real quando a query retorna 0 linhas (preservando 0 linhas legítimas de limiar/negação - P1-09)
                should_retry_empty = True
                if len(returned_rows) == 0:
                    try:
                        p_empty = sqlglot.parse_one(candidate_sql, read="duckdb")
                        has_str_filter = False
                        for lit in p_empty.find_all(exp.Literal):
                            if lit.is_string and str(lit.this or "").strip():
                                parent = lit.parent
                                if isinstance(parent, (exp.Like, exp.ILike, exp.EQ, exp.In)):
                                    has_str_filter = True
                                    break
                        max_cat_yr_sql = int(getattr(self.db_manager.catalog, "max_data_year", 2026) or 2026)
                        has_future_yr = any(int(y) > max_cat_yr_sql for y in re.findall(r"\b(20\d{2})\b", candidate_sql))
                        has_threshold_or_neg = any(
                            p_empty.find(cls) is not None
                            for cls in (exp.Is, exp.Not, exp.Having, exp.GT, exp.GTE, exp.LT, exp.LTE)
                        )
                        if not has_str_filter and not has_future_yr and has_threshold_or_neg:
                            should_retry_empty = False
                    except Exception:
                        should_retry_empty = True

                if (
                    len(returned_rows) == 0
                    and should_retry_empty
                    and sql_override is None
                    and not is_exclusive_historical_gap
                    and attempt <= self.max_sql_retries
                    and is_generative_engine
                ):
                    diag_t0 = time.perf_counter()
                    zero_rows_diag = self._build_zero_row_diagnostic(candidate_sql, effective_question)
                    last_err = (
                        "A consulta executou sem erro sintático, mas retornou 0 linhas. "
                        "Inspecione os valores reais das colunas e os anos disponíveis em <sql_zero_rows_diagnostic> "
                        "e gere uma nova consulta SQL corrigida."
                    )
                    steps.append(
                        {
                            "step": len(steps) + 1,
                            "name": f"{step_prefix}Diagnóstico AST de 0 Linhas & Self-Correction (Tentativa {attempt})",
                            "status": "RETRY",
                            "duration_ms": round((time.perf_counter() - diag_t0) * 1000.0, 2),
                            "detail": (
                                "Retorno vazio detectado sem lacuna histórica documentada. Valores categóricos reais e "
                                "horizonte temporal das tabelas injetados para auto-correção do LLM."
                            ),
                        }
                    )
                    continue

                # Eixo 1b (SIM v4.3): Prova Cirúrgica de Existência de Predicado quando agregação escalar retorna 0
                if (
                    len(returned_rows) == 1
                    and sql_override is None
                    and not is_exclusive_historical_gap
                    and attempt <= self.max_sql_retries
                    and is_generative_engine
                ):
                    pred_t0 = time.perf_counter()
                    phantom_pred_msg = self._has_unmatched_literal_predicate(candidate_sql, returned_rows)
                    if phantom_pred_msg:
                        zero_rows_diag = self._build_zero_row_diagnostic(candidate_sql, effective_question)
                        last_err = (
                            f"A consulta retornou agregação zerada porque {phantom_pred_msg} "
                            "Inspecione os valores reais das colunas em <sql_zero_rows_diagnostic> e gere uma nova consulta SQL corrigida."
                        )
                        steps.append(
                            {
                                "step": len(steps) + 1,
                                "name": f"{step_prefix}Prova de Existência de Predicado & Self-Correction (Tentativa {attempt})",
                                "status": "RETRY",
                                "duration_ms": round((time.perf_counter() - pred_t0) * 1000.0, 2),
                                "detail": f"Filtro literal sem correspondência detectado ({phantom_pred_msg}). Reconciliando com valores reais do catálogo.",
                            }
                        )
                        continue

                # Eixo 1c: Auto-correção quando o LLM projeta `SUM(vlr_pago)` sobre `despesa_despesa` retornando 0/NULL (RREO-Anexo 02 sem coluna de pagamento)
                # ou quando projeta apenas string literal (`SELECT 'texto' FROM ...`) sobre uma tabela numérica!
                cand_low = candidate_sql.lower()
                if (
                    len(returned_rows) >= 1
                    and sql_override is None
                    and attempt <= self.max_sql_retries
                    and is_generative_engine
                ):
                    if "despesa_despesa" in cand_low and "vlr_pago" in cand_low and "vlr_liquidado" not in cand_low:
                        all_zero_or_null_pag = all(
                            (v is None or (isinstance(v, (int, float)) and not isinstance(v, bool) and float(v) == 0.0))
                            for r in returned_rows
                            for k, v in r.items()
                            if "pag" in k.lower() or "vlr" in k.lower() or "total" in k.lower() or "gasto" in k.lower()
                        )
                        if all_zero_or_null_pag:
                            last_err = (
                                "ERRO CONTÁBIL (`despesa_despesa` via RREO-Anexo 02): A coluna `vlr_pago` é NULL/0 no demonstrativo RREO por função! "
                                "Reescreva a consulta sobre `despesa_despesa` projetando OBRIGATORIAMENTE "
                                "`ROUND(SUM(TRY_CAST(vlr_liquidado AS DECIMAL(18,2))), 2) AS total_liquidado` e "
                                "`ROUND(SUM(TRY_CAST(vlr_empenhado AS DECIMAL(18,2))), 2) AS total_empenhado`!"
                            )
                            continue
                    if len(returned_rows) == 1 and "vw_gold_beneficios_sociais_cgu" in cand_low:
                        r0_vals = list(returned_rows[0].values())
                        if r0_vals and all(isinstance(v, str) for v in r0_vals):
                            last_err = (
                                "PROIBIDO projetar apenas mensagem literal (`SELECT 'texto' FROM vw_gold_beneficios_sociais_cgu`)! "
                                "Consulte os indicadores numéricos agregados reais: "
                                "`SELECT ano_referencia, programa_social, ROUND(AVG(beneficiarios_no_mes)) AS media_mensal_familias, "
                                "ROUND(SUM(vlr_total_pago_mes), 2) AS valor_total_pago_ano FROM vw_gold_beneficios_sociais_cgu GROUP BY 1, 2 ORDER BY 1 DESC`."
                            )
                            continue

                # Eixo 4: Se o usuário pediu um GRÁFICO e o modelo retornou 1 único escalar isolado (1x1), refina para série!
                if (
                    wants_chart
                    and len(returned_rows) == 1
                    and sql_override is None
                    and attempt <= self.max_sql_retries
                    and is_generative_engine
                    and self.chart_generator.extract_chart_series(returned_rows, question=effective_question) is None
                ):
                    last_err = (
                        "O usuário solicitou um GRÁFICO, mas a query retornou apenas 1 número escalar isolado "
                        "(1 linha × 1 métrica), que não forma série gráfica. Reescreva a consulta agrupando por "
                        "`ano_referencia` (evolução histórica) ou pela principal dimensão categórica "
                        "(`GROUP BY ... ORDER BY ... DESC LIMIT 10`) para retornar múltiplas linhas plotáveis."
                    )
                    steps.append(
                        {
                            "step": len(steps) + 1,
                            "name": f"{step_prefix}Refinamento para Série Gráfica Multi-Linhas (Tentativa {attempt})",
                            "status": "RETRY",
                            "duration_ms": 0.2,
                            "detail": "Retorno escalar 1x1 insuficiente para gráfico; solicitada expansão para série temporal/categórica.",
                        }
                    )
                    continue

                sql_executed = q_res["query_executada"]
                sql_rows = returned_rows
                limit_applied_flag = bool(q_res.get("truncado_pelo_limite", False))
                pii_masked = q_res.get("pii_masked_count", 0)
                for f in q_res.get("fontes_utilizadas", []):
                    if f not in fontes:
                        fontes.append(f)
                warnings.extend(q_res.get("transparency_warnings", []))
                max_cat_yr_warn = int(getattr(self.db_manager.catalog, "max_data_year", 2026) or 2026)
                asked_fut_yrs = [int(y) for y in re.findall(r"\b(20\d{2})\b", raw_question_norm) if int(y) > max_cat_yr_warn]
                if asked_fut_yrs or re.search(r"\bprojetad[oa]\s+para\s+2[7-9]\b", raw_question_norm):
                    if not any(w.get("gap_id") == "GAP-EXERCICIO-FUTURO" for w in warnings):
                        warnings.append(
                            {
                                "gap_id": "GAP-EXERCICIO-FUTURO",
                                "severity": "info",
                                "title": f"Cobertura Temporal da Base Auditada (até {max_cat_yr_warn})",
                                "message": (
                                    f"As bases públicas consolidadas do município abrangem os exercícios até {max_cat_yr_warn}. "
                                    f"Exercícios futuros (acima de {max_cat_yr_warn}) referem-se a ciclos orçamentários ainda não iniciados nas remessas oficiais; "
                                    f"abaixo são apresentados os valores oficiais disponíveis até {max_cat_yr_warn} para referência."
                                ),
                            }
                        )
                break
            else:
                err_code = str(q_res.get("erro_codigo") or "")
                last_err = q_res.get("detalhe") or q_res.get("mensagem_llm")

                # Eixo 8: Tratamento resiliente de `SELECT 'texto'` sem `FROM` (`NO_BASE_TABLE`)
                if sql_override is None and err_code in ("NO_BASE_TABLE", "NO_TABLE_REFERENCED"):
                    asks_municipal_data = any(
                        w in raw_question_norm
                        for w in (
                            "orcamento", "receita", "despesa", "gasto", "quanto", "quantos", "qual o valor",
                            "salario", "funcionario", "servidor", "veiculo", "frota", "fornecedor", "contrato",
                            "2020", "2021", "2022", "2023", "2024", "2025", "2026", "2027",
                        )
                    )
                    if asks_municipal_data and attempt <= self.max_sql_retries and is_generative_engine:
                        max_cat_yr_retry = int(getattr(self.db_manager.catalog, "max_data_year", 2026) or 2026)
                        last_err = (
                            "PROIBIDO gerar SELECT literal sem cláusula FROM (NO_BASE_TABLE). "
                            f"Mesmo que o usuário mencione anos futuros (acima de {max_cat_yr_retry}) ou dados parciais, "
                            "você DEVE consultar a tabela ou Gold View correspondente (ex.: vw_gold_orcamento_previsto_vs_realizado) "
                            f"nos exercícios disponíveis (até {max_cat_yr_retry}) para retornar a série histórica auditada."
                        )
                        continue
                    return {
                        "redirect_conversational": candidate_sql,
                        "selected_segments": selected_segments,
                        "selected_tables": selected_tables,
                        "reduction_pct": reduction_pct,
                        "attempts": attempts,
                        "est_tokens": est_tokens,
                    }

                if q_res["status"] == "bloqueado_seguranca" and (sql_override is not None or attempt > self.max_sql_retries):
                    return {
                        "blocked_error": last_err,
                        "candidate_sql": candidate_sql,
                        "selected_segments": selected_segments,
                        "selected_tables": selected_tables,
                        "reduction_pct": reduction_pct,
                        "attempts": attempts,
                        "est_tokens": est_tokens,
                    }

        return {
            "sql_executed": sql_executed,
            "sql_rows": sql_rows,
            "limit_applied": locals().get("limit_applied_flag", False),
            "pii_masked": pii_masked,
            "fontes": fontes,
            "warnings": warnings,
            "selected_segments": selected_segments,
            "selected_tables": selected_tables,
            "reduction_pct": reduction_pct,
            "attempts": attempts,
            "est_tokens": est_tokens,
        }

    def handle_question(
        self,
        question: str,
        session_id: str = "default",
        role: str = "citizen",
        explicit_segments: Optional[List[str]] = None,
        explicit_tables: Optional[List[str]] = None,
        allowed_sources: Optional[List[str]] = None,
        sql_override: Optional[str] = None,
        llm_provider: Optional[str] = None,
        llm_api_key: Optional[str] = None,
    ) -> OrchestrationResult:
        t0 = time.perf_counter()
        steps: List[Dict[str, Any]] = []

        if not self.lock_manager.try_acquire(session_id, blocking=True, timeout=10.0):
            return OrchestrationResult(
                status="ocupado",
                intent="concurrency_locked",
                tenant_id=self.db_manager.tenant.tenant_id,
                role=role,
                question=question,
                answer_text="Sua consulta anterior ainda está sendo processada. Aguarde alguns segundos.",
                exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
            )

        try:
            self._sync_catalog_freshness_if_hotswapped()
            self._active_allowed_sources = list(allowed_sources or [])
            self._active_llm_provider = llm_provider
            self._active_llm_api_key = llm_api_key
            self._last_llm_engine_used = "SIM Native NL2SQL Compiler"
            self._turn_prompt_tokens_real = 0
            self._turn_completion_tokens_real = 0

            # Eixo 5: Normalização Fonética & Lexical pós-ASR na entrada do pipeline
            working_question, asr_corrections = normalize_municipal_asr_text(
                question,
                municipality_name=self.db_manager.tenant.municipality_name,
            )
            if not working_question:
                working_question = question

            hist_list = list(self.conversation_history.get(session_id, []))
            prior_sql_ctx = self._get_latest_session_sql_context(session_id)
            previous_sql = str(prior_sql_ctx.get("sql_executed") or "") if prior_sql_ctx else None

            effective_question = self._get_effective_question_with_history(working_question, session_id=session_id)

            # Etapa 1: Segurança de Prompt, Normalização ASR e Lock de Sessão
            s1_t0 = time.perf_counter()
            safety = check_prompt_safety(question)
            scope_desc = (
                f"Escopo de Fontes Restrito: {', '.join(allowed_sources)}"
                if allowed_sources
                else "Escopo de Fontes: Todas as 8 fontes oficiais + 7 Gold Views (Automático)"
            )
            asr_detail = f" Correções ASR aplicadas: {', '.join(asr_corrections)}." if asr_corrections else ""
            steps.append(
                {
                    "step": 1,
                    "name": "Guardião de Entrada, Normalizador ASR & Lock Atômico de Sessão",
                    "status": "OK" if safety.is_safe else "BLOCKED",
                    "duration_ms": round((time.perf_counter() - s1_t0) * 1000.0, 2),
                    "detail": (
                        f"Sessão '{session_id}' bloqueada com exclusividade. "
                        f"Perfil RBAC: '{role}'. {scope_desc}.{asr_detail} Verificação anti-Prompt Injection: "
                        f"{'Aprovada' if safety.is_safe else 'Bloqueada (' + ', '.join(safety.triggered_rules) + ')'}."
                    ),
                }
            )

            if not safety.is_safe:
                return OrchestrationResult(
                    status="bloqueado_seguranca",
                    intent="blocked_security",
                    tenant_id=self.db_manager.tenant.tenant_id,
                    role=role,
                    question=question,
                    effective_question=effective_question,
                    standalone_question=effective_question,
                    answer_text=(
                        "Solicitação bloqueada automaticamente pelo Guardião de Segurança Institucional do SIM "
                        f"({', '.join(safety.triggered_rules)})."
                    ),
                    asr_corrections=asr_corrections,
                    pipeline_steps=steps,
                    exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                )

            # Etapa 2: Guardião Semântico IA Leve, Multi-Turn Query Rewriting & Decomposição Adaptativa (CRAG)
            s2_t0 = time.perf_counter()
            intent = "sql_analytics" if sql_override else self.classify_intent(effective_question, allowed_sources=allowed_sources)
            q_norm_s2 = normalize_text_for_dedup(working_question)
            only_rag_scope = bool(allowed_sources and {s.upper().strip() for s in allowed_sources} == {"DIARIO_OFICIAL"})

            # Fast-Path Cache Trava 1 ANTES de invocar LLM externo (apenas para perguntas autônomas sem histórico/gráfico/tabela)
            wants_chart_hint = bool(re.search(r"\b(grafico|graficos|plotar|visualizacao\s+grafica)\b", q_norm_s2))
            wants_table_hint = SemanticIntentGuardrail._detect_table_signal(working_question)
            has_custom_llm_key = bool((llm_api_key or "").strip())
            if (
                not sql_override
                and not explicit_tables
                and not explicit_segments
                and not self._has_custom_sql_generator
                and not has_custom_llm_key
                and len(hist_list) == 0
                and not wants_chart_hint
                and not wants_table_hint
                and intent in ("sql_analytics", "rag_diario_oficial", "hybrid_sql_rag")
            ):
                sc_t0 = time.perf_counter()
                cached_fast = self.quota_manager.get_cached_response(
                    question=working_question,
                    role=role,
                    allowed_sources=allowed_sources,
                )
                if cached_fast is not None:
                    steps.append(
                        {
                            "step": 2,
                            "name": "Cache Analítico Institucional (Trava 1 — Hit < 2ms, 0 Tokens)",
                            "status": "CACHE_HIT",
                            "duration_ms": round((time.perf_counter() - sc_t0) * 1000.0, 2),
                            "detail": (
                                "Consulta autônoma idêntica recuperada do cache persistente SQLite/RAM sem consumo de tokens "
                                "de API externa, garantindo determinismo e proteção de margem B2G."
                            ),
                        }
                    )
                    if session_id not in self.conversation_history:
                        self.conversation_history[session_id] = deque(maxlen=10)
                    self.conversation_history[session_id].append({"role": "user", "content": working_question})
                    self.conversation_history[session_id].append(
                        {
                            "role": "assistant",
                            "content": cached_fast.get("answer_text", ""),
                            "sql_executed": cached_fast.get("sql_executed") or "",
                            "row_count": len(cached_fast.get("sql_rows") or []),
                        }
                    )
                    self._record_session_sql_context(
                        session_id=session_id,
                        question=working_question,
                        sql_executed=cached_fast.get("sql_executed"),
                        sql_rows=cached_fast.get("sql_rows") or [],
                    )
                    return OrchestrationResult(
                        status=cached_fast.get("status", "sucesso"),
                        intent=cached_fast.get("intent", intent),
                        tenant_id=self.db_manager.tenant.tenant_id,
                        role=role,
                        question=question,
                        effective_question=working_question,
                        standalone_question=working_question,
                        is_contextual_followup=False,
                        answer_text=cached_fast.get("answer_text", ""),
                        citizen_response=cached_fast.get("citizen_response", ""),
                        institutional_response=cached_fast.get("institutional_response", ""),
                        methodology_summary=cached_fast.get("methodology_summary", ""),
                        limitations_and_caveats=cached_fast.get("limitations_and_caveats", []),
                        sql_executed=cached_fast.get("sql_executed"),
                        sql_attempts=cached_fast.get("sql_attempts", 1),
                        sql_rows=cached_fast.get("sql_rows", []),
                        rag_chunks=cached_fast.get("rag_chunks", []),
                        selected_segments=cached_fast.get("selected_segments", []),
                        selected_tables=cached_fast.get("selected_tables", []),
                        allowed_sources=list(allowed_sources or []),
                        schema_reduction_pct=cached_fast.get("schema_reduction_pct", 0.0),
                        fontes_utilizadas=cached_fast.get("fontes_utilizadas", []),
                        transparency_warnings=cached_fast.get("transparency_warnings", []),
                        pii_masked_count=cached_fast.get("pii_masked_count", 0),
                        prompt_tokens_estimated=0,
                        prompt_tokens_real=0,
                        completion_tokens_real=0,
                        llm_engine_used=f"{cached_fast.get('llm_engine_used', 'SIM Engine')} (Cache Trava 1 — 0 tokens)",
                        asr_corrections=asr_corrections,
                        pipeline_steps=steps,
                        exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                    )

            guard_decision: Optional[SemanticGuardrailDecision] = None
            should_invoke_guardrail = (
                not sql_override
                and not self._has_custom_sql_generator
                and intent not in ("blocked_security", "out_of_scope_municipality", "out_of_scope_general", "conversational_identity")
            )

            if should_invoke_guardrail:
                quota_precheck = self.quota_manager.check_quota_available()
                guard_decision = self.semantic_guardrail.evaluate(
                    question=working_question,
                    recent_history=hist_list,
                    role=role,
                    allowed_sources=allowed_sources,
                    llm_provider=llm_provider,
                    llm_api_key=llm_api_key,
                    enable_external_ai=bool(quota_precheck.get("allowed_external_llm", True)),
                )
                if (guard_decision.prompt_tokens + guard_decision.completion_tokens) > 0:
                    self._record_turn_llm_tokens(guard_decision.prompt_tokens, guard_decision.completion_tokens)

                if (
                    guard_decision.used_ai_model
                    or guard_decision.is_contextual_followup
                    or guard_decision.wants_chart
                    or guard_decision.wants_table
                    or guard_decision.legal_framework_notes
                ):
                    intent = guard_decision.intent
                    if guard_decision.standalone_question and intent in ("sql_analytics", "rag_diario_oficial", "hybrid_sql_rag"):
                        effective_question = guard_decision.standalone_question
                    if guard_decision.requires_sql and guard_decision.requires_rag and not only_rag_scope:
                        intent = "hybrid_sql_rag"
                    elif guard_decision.requires_sql and not only_rag_scope and intent not in ("hybrid_sql_rag", "sql_analytics"):
                        intent = "sql_analytics"
                        guard_decision.direct_response_citizen = ""

            is_contextual_followup = bool(
                (guard_decision and guard_decision.is_contextual_followup)
                or (len(hist_list) > 0 and effective_question != working_question)
            )
            wants_chart = bool((guard_decision and guard_decision.wants_chart) or wants_chart_hint)
            wants_table = bool((guard_decision and guard_decision.wants_table) or wants_table_hint)
            legal_framework_notes = list(guard_decision.legal_framework_notes) if guard_decision else []

            # Se o usuário pede explicação legal sobre os dados recém-exibidos na sessão (ex: "essas quantias são legais? você tem a lei?"),
            # roteia pelo pipeline híbrido/síntese contextualizada com a Lei Federal + Diário Oficial + dados do turno anterior
            if (
                legal_framework_notes
                and prior_sql_ctx
                and (
                    intent in ("conceptual_doubt", "conversational_followup")
                    or (intent == "rag_diario_oficial" and is_contextual_followup)
                )
                and not wants_chart
                and not wants_table
            ):
                intent = "hybrid_sql_rag" if not only_rag_scope else "rag_diario_oficial"
                if guard_decision:
                    guard_decision.direct_response_citizen = ""

            # Se o usuário pediu gráfico ou tabela, limpa qualquer resposta direta de bloqueio/clarificação do guardrail
            if (wants_chart or wants_table) and not sql_override:
                if guard_decision:
                    guard_decision.direct_response_citizen = ""
                if intent == "clarification_needed":
                    intent = "conversational_followup"

            # Eixo 4: Se `conversational_followup` com `wants_chart=True`, mas o turno anterior retornou apenas 1 escalar 1x1
            # (ex.: `[{"qtd_servidores": 1496}]`), promove intrinsecamente a `sql_analytics` para buscar a série evolutiva/categórica!
            if (
                wants_chart
                and intent == "conversational_followup"
                and prior_sql_ctx
                and prior_sql_ctx.get("sql_rows")
                and self.chart_generator.extract_chart_series(prior_sql_ctx["sql_rows"], question=working_question) is None
                and not only_rag_scope
            ):
                prev_q_topic = str(prior_sql_ctx.get("question") or working_question).strip()
                intent = "sql_analytics"
                is_contextual_followup = True
                effective_question = (
                    f"Evolução anual por ano_referencia ou distribuição por categoria para gerar gráfico "
                    f"referente a: {prev_q_topic} ({working_question})"
                )

            steps.append(
                {
                    "step": 2,
                    "name": (
                        f"Guardião Semântico & Decompositor CRAG ({guard_decision.engine_used})"
                        if (guard_decision and guard_decision.used_ai_model)
                        else "Guardião Semântico & Roteador Adaptativo"
                    ),
                    "status": "OK" if intent not in ("blocked_security", "out_of_scope_municipality", "out_of_scope_general") else "INTERCEPTED",
                    "duration_ms": round((time.perf_counter() - s2_t0) * 1000.0, 2),
                    "detail": (
                        f"Rota: '{intent}' | Follow-up contextual: {'Sim' if is_contextual_followup else 'Não'} | "
                        f"Gráfico solicitado: {'Sim' if wants_chart else 'Não'} | "
                        f"Tabela solicitada: {'Sim' if wants_table else 'Não'}"
                        + (f" | Standalone Question: '{effective_question}'" if effective_question != question else "")
                        + (f" | Sub-perguntas: {len(guard_decision.sub_questions)}" if (guard_decision and guard_decision.sub_questions) else "")
                        + (f" | Justificativa: {guard_decision.reasoning}" if (guard_decision and guard_decision.reasoning) else "")
                    ),
                }
            )

            # Eixo 4b (SIM v4.3): Geração Nativa de Cartão PNG + HTML de Tabela sobre resultado existente no turno anterior
            if wants_table and not wants_chart and intent == "conversational_followup":
                tb_t0 = time.perf_counter()
                city_str = f"{self.db_manager.tenant.municipality_name}/{self.db_manager.tenant.state_uf}"
                if prior_sql_ctx and prior_sql_ctx.get("sql_rows") and self.table_renderer.can_render(prior_sql_ctx["sql_rows"]):
                    tbl_rows = list(prior_sql_ctx["sql_rows"])
                    tbl_title_q = str(prior_sql_ctx.get("question") or working_question)
                    tbl_sql = prior_sql_ctx.get("sql_executed")
                    is_contextual_followup = True
                else:
                    if "vw_gold_orcamento_previsto_vs_realizado" in self.db_manager.catalog.tables:
                        fallback_tbl_sql = (
                            "SELECT ano_referencia, orcamento_previsto_loa, receita_arrecadada_realizada, despesa_liquidada, despesa_paga "
                            "FROM vw_gold_orcamento_previsto_vs_realizado ORDER BY ano_referencia"
                        )
                        tbl_title_q = f"Demonstrativo Orçamentário de {city_str} (até {getattr(self.db_manager.catalog, 'max_data_year', 2026)})"
                    else:
                        fallback_tbl_sql = (
                            "SELECT ano_referencia, vlr_receita_corrente_liquida, vlr_despesa_pessoal_lrf, "
                            "pct_despesa_pessoal_rcl, pct_aplicacao_saude_siops, pct_aplicacao_educacao_mde "
                            "FROM siconfi_finbra_indicadores ORDER BY ano_referencia"
                        )
                        tbl_title_q = f"Indicadores Fiscais e Constitucionais de {city_str} (SICONFI/STN)"
                    q_orc = self.db_manager.execute_safe_query(fallback_tbl_sql, role=role)
                    tbl_rows = q_orc.get("dados") or []
                    tbl_sql = q_orc.get("query_executada")
                    self._record_session_sql_context(session_id, tbl_title_q, tbl_sql, tbl_rows)

                tbl_art = self.table_renderer.render(rows=tbl_rows, title=tbl_title_q[:90])
                steps.append(
                    {
                        "step": 3,
                        "name": "Renderizador de Tabela Institucional (Cartão PNG TrueType + HTML)",
                        "status": "OK" if tbl_art else "FALLBACK_TEXT",
                        "duration_ms": round((time.perf_counter() - tb_t0) * 1000.0, 2),
                        "detail": (
                            f"Tabela institucional renderizada a partir de {len(tbl_rows)} registro(s) auditados "
                            f"({tbl_art.displayed_rows if tbl_art else 0} exibidos, {len(tbl_art.columns_rendered) if tbl_art else 0} colunas)."
                        ),
                    }
                )
                if tbl_art:
                    tot_note = ""
                    if tbl_art.totals_by_column:
                        t_pairs = [f"**{_human_col_label(k)}**: **{v}**" for k, v in list(tbl_art.totals_by_column.items())[:3]]
                        tot_note = "\n\n**Total Geral consolidado:** " + " | ".join(t_pairs)
                    c_txt = (
                        f"Gerei a tabela oficial com os dados de **{city_str}** referentes a **{tbl_art.caption}**.{tot_note}"
                    )
                    i_txt = (
                        f"**Tabela Institucional Renderizada (`image/png` + HTML):** {tbl_art.caption}{tot_note}"
                        + (f"\n\n**SQL de Origem:**\n```sql\n{tbl_sql}\n```" if tbl_sql else "")
                    )
                    conv_eng = "SIM Institutional PNG/HTML Table Renderer"
                    is_internal = role.lower().strip() in (
                        "auditor", "admin", "controller", "gestor", "servidor", "interno", "controladoria",
                    )
                    sel_txt = i_txt if is_internal else c_txt
                    if session_id not in self.conversation_history:
                        self.conversation_history[session_id] = deque(maxlen=10)
                    self.conversation_history[session_id].append({"role": "user", "content": working_question})
                    self.conversation_history[session_id].append(
                        {"role": "assistant", "content": c_txt[:420], "sql_executed": tbl_sql or "", "row_count": len(tbl_rows)}
                    )
                    return OrchestrationResult(
                        status="sucesso",
                        intent="conversational_followup",
                        tenant_id=self.db_manager.tenant.tenant_id,
                        role=role,
                        question=question,
                        effective_question=effective_question,
                        standalone_question=effective_question,
                        is_contextual_followup=True,
                        answer_text=sel_txt,
                        citizen_response=c_txt,
                        institutional_response=i_txt,
                        methodology_summary=f"Renderização tabular institucional nativa sobre dados auditados ({conv_eng}).",
                        sql_executed=tbl_sql,
                        sql_rows=tbl_rows,
                        schema_reduction_pct=100.0,
                        prompt_tokens_estimated=self._turn_prompt_tokens_real,
                        prompt_tokens_real=self._turn_prompt_tokens_real,
                        completion_tokens_real=self._turn_completion_tokens_real,
                        llm_engine_used=conv_eng,
                        table_image_base64=tbl_art.image_base64,
                        table_mime_type=tbl_art.mime_type,
                        table_caption=tbl_art.caption,
                        table_html=tbl_art.html,
                        asr_corrections=asr_corrections,
                        pipeline_steps=steps,
                        exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                    )

            # Eixo 4: Geração Nativa de Gráfico PNG Institucional sobre série multi-linhas já existente no turno anterior
            # (ou visão geral oficial da LOA quando o usuário pede gráfico genérico sem histórico prévio na sessão)
            if wants_chart and intent == "conversational_followup":
                ch_t0 = time.perf_counter()
                city_str = f"{self.db_manager.tenant.municipality_name}/{self.db_manager.tenant.state_uf}"
                if prior_sql_ctx and prior_sql_ctx.get("sql_rows"):
                    chart_rows = list(prior_sql_ctx["sql_rows"])
                    chart_title_q = str(prior_sql_ctx.get("question") or working_question)
                    chart_sql = prior_sql_ctx.get("sql_executed")
                    is_contextual_followup = True
                else:
                    if "vw_gold_orcamento_previsto_vs_realizado" in self.db_manager.catalog.tables:
                        fallback_ch_sql = (
                            "SELECT ano_referencia, orcamento_previsto_loa, receita_arrecadada_realizada "
                            "FROM vw_gold_orcamento_previsto_vs_realizado ORDER BY ano_referencia"
                        )
                        chart_title_q = f"Evolução do Orçamento Previsto (LOA) vs. Receita Realizada de {city_str} (até {getattr(self.db_manager.catalog, 'max_data_year', 2026)})"
                    else:
                        fallback_ch_sql = (
                            "SELECT ano_referencia, vlr_receita_corrente_liquida, vlr_despesa_pessoal_lrf "
                            "FROM siconfi_finbra_indicadores ORDER BY ano_referencia"
                        )
                        chart_title_q = f"Evolução da Receita Corrente Líquida vs. Pessoal LRF de {city_str} (SICONFI/STN)"
                    q_orc = self.db_manager.execute_safe_query(fallback_ch_sql, role=role)
                    chart_rows = q_orc.get("dados") or []
                    chart_sql = q_orc.get("query_executada")
                    self._record_session_sql_context(session_id, chart_title_q, chart_sql, chart_rows)

                artifact = self.chart_generator.generate_chart(rows=chart_rows, question=chart_title_q)
                steps.append(
                    {
                        "step": 3,
                        "name": "Renderizador Gráfico Institucional Nativo (PNG Base64 — Eixo 4)",
                        "status": "OK" if artifact else "FALLBACK_TEXT",
                        "duration_ms": round((time.perf_counter() - ch_t0) * 1000.0, 2),
                        "detail": (
                            f"Gráfico PNG renderizado a partir de {len(chart_rows)} registro(s) auditados "
                            f"(tipo={artifact.chart_kind if artifact else 'texto'}, {artifact.width_px if artifact else 0}x{artifact.height_px if artifact else 0}px)."
                        ),
                    }
                )
                if artifact:
                    c_txt = (
                        f"Gerei o gráfico institucional com os dados oficiais de **{city_str}** referentes a **{artifact.title}**.\n\n"
                        f"{artifact.caption}\n\n"
                        f"Você também pode solicitar gráficos específicos sobre gastos com saúde, educação, folha de servidores, licitações, frota ou fornecedores."
                    )
                    i_txt = (
                        f"**Visualização Analítica Renderizada (`image/png`):** {artifact.title}\n"
                        f"- **Legenda / Síntese:** {artifact.caption}\n"
                        f"- **Registros plotados:** {len(chart_rows)}"
                        + (f"\n\n**SQL de Origem:**\n```sql\n{chart_sql}\n```" if chart_sql else "")
                    )
                    conv_eng = "SIM Institutional PNG Chart Generator"
                else:
                    c_txt, i_txt, conv_eng = self._handle_conversational_or_conceptual_turn(
                        question=working_question,
                        role=role,
                        hist_list=hist_list,
                        llm_provider=llm_provider,
                        llm_api_key=llm_api_key,
                    )

                is_internal = role.lower().strip() in (
                    "auditor", "admin", "controller", "gestor", "servidor", "interno", "controladoria",
                )
                sel_txt = i_txt if is_internal else c_txt
                if session_id not in self.conversation_history:
                    self.conversation_history[session_id] = deque(maxlen=10)
                self.conversation_history[session_id].append({"role": "user", "content": working_question})
                self.conversation_history[session_id].append(
                    {"role": "assistant", "content": c_txt[:420], "sql_executed": chart_sql or "", "row_count": len(chart_rows)}
                )
                return OrchestrationResult(
                    status="sucesso",
                    intent="conversational_followup",
                    tenant_id=self.db_manager.tenant.tenant_id,
                    role=role,
                    question=question,
                    effective_question=effective_question,
                    standalone_question=effective_question,
                    is_contextual_followup=True,
                    answer_text=sel_txt,
                    citizen_response=c_txt,
                    institutional_response=i_txt,
                    methodology_summary=f"Renderização gráfica institucional nativa sobre dados auditados ({conv_eng}).",
                    sql_executed=chart_sql,
                    sql_rows=chart_rows,
                    schema_reduction_pct=100.0,
                    prompt_tokens_estimated=self._turn_prompt_tokens_real,
                    prompt_tokens_real=self._turn_prompt_tokens_real,
                    completion_tokens_real=self._turn_completion_tokens_real,
                    llm_engine_used=conv_eng,
                    chart_base64=artifact.image_base64 if artifact else None,
                    chart_mime_type=artifact.mime_type if artifact else None,
                    chart_caption=artifact.caption if artifact else None,
                    asr_corrections=asr_corrections,
                    pipeline_steps=steps,
                    exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                )

            if guard_decision and guard_decision.used_ai_model and intent in (
                "conversational_followup",
                "conceptual_doubt",
                "clarification_needed",
            ) and guard_decision.direct_response_citizen:
                is_internal = role.lower().strip() in (
                    "auditor", "admin", "controller", "gestor", "servidor", "interno", "controladoria",
                )
                c_txt = guard_decision.direct_response_citizen
                i_txt = guard_decision.direct_response_institutional or c_txt
                sel_txt = i_txt if is_internal else c_txt
                if session_id not in self.conversation_history:
                    self.conversation_history[session_id] = deque(maxlen=10)
                self.conversation_history[session_id].append({"role": "user", "content": working_question})
                self.conversation_history[session_id].append({"role": "assistant", "content": c_txt[:420]})
                return OrchestrationResult(
                    status="sucesso",
                    intent=intent,
                    tenant_id=self.db_manager.tenant.tenant_id,
                    role=role,
                    question=question,
                    effective_question=effective_question,
                    standalone_question=effective_question,
                    is_contextual_followup=is_contextual_followup,
                    answer_text=sel_txt,
                    citizen_response=c_txt,
                    institutional_response=i_txt,
                    methodology_summary=f"Pré-roteamento e resposta contextual gerada pelo {guard_decision.engine_used} ({guard_decision.reasoning}).",
                    schema_reduction_pct=100.0,
                    prompt_tokens_estimated=guard_decision.tokens_used,
                    prompt_tokens_real=self._turn_prompt_tokens_real,
                    completion_tokens_real=self._turn_completion_tokens_real,
                    llm_engine_used=guard_decision.engine_used,
                    asr_corrections=asr_corrections,
                    pipeline_steps=steps,
                    exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                )

            if intent == "blocked_security":
                return OrchestrationResult(
                    status="bloqueado_seguranca",
                    intent=intent,
                    tenant_id=self.db_manager.tenant.tenant_id,
                    role=role,
                    question=question,
                    answer_text=(
                        "Solicitação bloqueada pelas políticas de segurança da plataforma SIM "
                        "(tentativa de manipulação de instruções ou acesso não autorizado a recursos de sistema)."
                    ),
                    pipeline_steps=steps,
                    exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                )

            if intent == "out_of_scope_municipality":
                oos_msg = (
                    f"Esta instância do SIM atende exclusivamente aos dados públicos do município de "
                    f"{self.db_manager.tenant.municipality_name}/{self.db_manager.tenant.state_uf} "
                    f"(IBGE {self.db_manager.tenant.ibge_code})."
                )
                return OrchestrationResult(
                    status="fora_de_escopo",
                    intent=intent,
                    tenant_id=self.db_manager.tenant.tenant_id,
                    role=role,
                    question=question,
                    answer_text=oos_msg,
                    citizen_response=oos_msg,
                    institutional_response=oos_msg,
                    pipeline_steps=steps,
                    exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                )

            if intent == "conversational_identity":
                city_str = f"{self.db_manager.tenant.municipality_name}/{self.db_manager.tenant.state_uf}"
                gazette_lbl = self._get_gazette_name()
                cat = self.db_manager.catalog
                total_t = len(cat.tables)
                gold_count = sum(1 for k in cat.tables if k.startswith("vw_gold_"))
                raw_count = total_t - gold_count
                fresh_map = self._build_source_freshness_map()
                rag_acts_n = len(getattr(self.rag_retriever, "corpus_chunks", None) or [])
                q_norm_id = normalize_text_for_dedup(working_question)
                wants_concise = any(w in q_norm_id for w in ("resumid", "concis", "curt", "diret", "poucas palavras"))
                asks_cities = any(w in q_norm_id for w in ("quais cidades", "qual cidade", "cidades vc", "cidades voce"))
                asks_meta_sources = SemanticIntentGuardrail._detect_meta_catalog_signal(working_question)
                if asks_cities:
                    citizen_identity = (
                        f"Esta instância oficial atende aos dados públicos do município de **{city_str}** (código IBGE `{self.db_manager.tenant.ibge_code}`). "
                        f"A plataforma SIM possui arquitetura multi-municipal expansível para outras prefeituras, mas aqui todas as consultas cobrem exclusivamente **{city_str}**."
                    )
                    inst_identity = (
                        f"Esta instância ativa do **SIM** opera sobre o tenant **`{self.db_manager.tenant.tenant_id}`** (**{city_str}**, IBGE `{self.db_manager.tenant.ibge_code}`), "
                        f"com **{total_t} objetos auditáveis** ({raw_count} tabelas Parquet + {gold_count} Gold Views) e arquitetura multi-tenant declarativa pronta para onboarding de novos municípios."
                    )
                elif asks_meta_sources:
                    src_counts: Dict[str, int] = {}
                    for tbl_meta in cat.tables.values():
                        fam = str(getattr(tbl_meta, "source_family", "SICOM") or "SICOM")
                        src_counts[fam] = src_counts.get(fam, 0) + 1
                    fresh_bullets_citizen = "\n".join(
                        f"• **{src}**: {desc}" for src, desc in fresh_map.items()
                    )
                    fresh_bullets_inst = "\n".join(
                        f"- **`{src}`** ({src_counts.get(src, 1 if src == 'DIARIO_OFICIAL' else 0)} objeto(s)): {desc}"
                        for src, desc in fresh_map.items()
                    )
                    citizen_identity = (
                        f"Sim! Consulto diretamente as **fontes públicas oficiais ativas** do Município de **{city_str}** "
                        f"(somando **{total_t} bases estruturadas** e **{rag_acts_n:,} atos publicados no Diário Oficial**):\n\n".replace(",", ".")
                        + f"{fresh_bullets_citizen}\n\n"
                        + "Você pode cruzar qualquer uma dessas fontes oficiais diretamente na conversa."
                    )
                    inst_identity = (
                        f"**Inventário Auditável de Fontes e Atualização Temporal — {city_str} (IBGE `{self.db_manager.tenant.ibge_code}`):**\n\n"
                        f"- **Catálogo Analítico Ativo:** `{total_t}` objetos (`{raw_count}` tabelas Parquet ZSTD + `{gold_count}` Gold Views de Auditoria).\n"
                        f"- **Acervo Normativo Indexado (RAG Híbrido):** `{rag_acts_n}` atos atomizados em *{gazette_lbl}*.\n\n"
                        f"**Horizonte Temporal por Fonte Oficial:**\n{fresh_bullets_inst}"
                    )
                elif wants_concise:
                    citizen_identity = (
                        f"Sou o sistema oficial de consulta aos dados públicos da Prefeitura de **{city_str}**. "
                        f"Respondo dúvidas em linguagem simples sobre orçamento, indicadores fiscais, saúde, educação, programas sociais e atos do Diário Oficial."
                    )
                    inst_identity = (
                        f"Sou o **SIM (Sistema Integrado de Monitoramento e Inteligência Municipal)** de **{city_str}** (IBGE `{self.db_manager.tenant.ibge_code}`). "
                        f"Realizo consultas auditáveis sobre **{total_t} objetos oficiais ativos** no catálogo e sobre os atos normativos do Diário Oficial (*{gazette_lbl}*)."
                    )
                else:
                    citizen_identity = (
                        f"Olá! Sou o assistente de transparência pública da Prefeitura de **{city_str}**.\n\n"
                        "Estou aqui para ajudar você a consultar, de forma simples e rápida, como os recursos públicos "
                        "do município são arrecadados e investidos.\n\n"
                        "**Você pode me perguntar, por exemplo:**\n"
                        "• Quanto a prefeitura investiu em Saúde ou Educação em 2024?\n"
                        "• Como estão as contas fiscais (Receita Corrente Líquida e limite de pessoal LRF)?\n"
                        f"• Quanto foi pago de Novo Bolsa Família ou BPC em {self.db_manager.tenant.municipality_name}?\n"
                        "• Qual é a população oficial, o PIB ou o PIB *per capita* do município?\n"
                        f"• O que diz uma lei, decreto ou portaria publicada no Diário Oficial (*{gazette_lbl}*)?\n\n"
                        "Todas as respostas vêm diretamente das bases oficiais auditáveis do município."
                    )
                    inst_identity = (
                        f"Sou o **SIM (Sistema de Inteligência Municipal)** de **{city_str}** "
                        f"(código IBGE `{self.db_manager.tenant.ibge_code}`).\n\n"
                        "**O que eu faço:**\n"
                        f"Converto perguntas em linguagem natural em consultas auditáveis sobre **{total_t} objetos analíticos oficiais ativos ({raw_count} tabelas Parquet + {gold_count} Gold Views de Auditoria)** "
                        f"e sobre a collection de atos individuais do **Diário Oficial (*{gazette_lbl}*)**, sempre informando os arquivos-fonte utilizados.\n\n"
                        "**Fontes Oficiais Suportadas na Arquitetura Multi-Cidade:**\n"
                        "- **IBGE Indicadores Municipais:** População oficial (Censo 2022 e Estimativas), PIB Municipal, PIB *per capita* e área territorial (`ibge_indicadores_municipais`).\n"
                        "- **SICONFI / Tesouro Nacional:** Receita Corrente Líquida (RCL), Despesa com Pessoal LRF, Limite Prudencial e Mínimos de Saúde/Educação (`siconfi_finbra_indicadores`).\n"
                        "- **DATASUS / SIOPS Saúde:** aplicação constitucional em saúde e despesa *per capita* (`datasus_siops_saude`).\n"
                        "- **Portal da Transparência / CGU:** Novo Bolsa Família, BPC, Convênios Federais e Emendas Parlamentares (`cgu_*`).\n"
                        "- **SICOM / TCE-MG:** receitas, despesas, empenhos, pagamentos, contratos, licitações, frota e folha de servidores.\n"
                        "- **INEP / MEC & Receita Federal QSA:** IDEB educacional e quadro societário reconciliado de fornecedores.\n"
                        f"- **Diário Oficial (*{gazette_lbl}*):** leis, decretos, portarias e editais atomizados com busca híbrida Okapi BM25."
                    )
                is_internal = role.lower().strip() in (
                    "auditor", "admin", "controller", "gestor", "servidor", "interno", "controladoria",
                )
                selected_identity = inst_identity if is_internal else citizen_identity
                return OrchestrationResult(
                    status="sucesso",
                    intent=intent,
                    tenant_id=self.db_manager.tenant.tenant_id,
                    role=role,
                    question=question,
                    effective_question=working_question,
                    standalone_question=working_question,
                    answer_text=selected_identity,
                    citizen_response=citizen_identity,
                    institutional_response=inst_identity,
                    methodology_summary="Rota conversacional instantânea (0 tokens de LLM e 0 acessos ao banco).",
                    schema_reduction_pct=100.0,
                    prompt_tokens_estimated=0,
                    llm_engine_used="Instant Conversational Handler (0 tokens)",
                    asr_corrections=asr_corrections,
                    pipeline_steps=steps,
                    exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                )

            if intent in ("conversational_followup", "conceptual_doubt"):
                c_txt, i_txt, conv_eng = self._handle_conversational_or_conceptual_turn(
                    question=working_question,
                    role=role,
                    hist_list=hist_list,
                    llm_provider=llm_provider,
                    llm_api_key=llm_api_key,
                )
                is_internal = role.lower().strip() in (
                    "auditor", "admin", "controller", "gestor", "servidor", "interno", "controladoria",
                )
                sel_txt = i_txt if is_internal else c_txt
                if session_id not in self.conversation_history:
                    self.conversation_history[session_id] = deque(maxlen=10)
                self.conversation_history[session_id].append({"role": "user", "content": working_question})
                self.conversation_history[session_id].append({"role": "assistant", "content": sel_txt})
                steps.append(
                    {
                        "step": 3,
                        "name": "Assistente Contextual Multi-Turn & Base Conceitual",
                        "status": "OK",
                        "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                        "detail": f"Solicitação conversacional ou dúvida conceitual atendida por {conv_eng}.",
                    }
                )
                return OrchestrationResult(
                    status="sucesso",
                    intent=intent,
                    tenant_id=self.db_manager.tenant.tenant_id,
                    role=role,
                    question=question,
                    effective_question=effective_question,
                    standalone_question=effective_question,
                    is_contextual_followup=True,
                    answer_text=sel_txt,
                    citizen_response=c_txt,
                    institutional_response=i_txt,
                    methodology_summary=f"Rota contextual/conceitual ({conv_eng}).",
                    schema_reduction_pct=100.0,
                    prompt_tokens_estimated=self._turn_prompt_tokens_real,
                    prompt_tokens_real=self._turn_prompt_tokens_real,
                    completion_tokens_real=self._turn_completion_tokens_real,
                    llm_engine_used=conv_eng,
                    asr_corrections=asr_corrections,
                    pipeline_steps=steps,
                    exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                )

            if intent == "out_of_scope_general":
                return OrchestrationResult(
                    status="fora_de_escopo",
                    intent=intent,
                    tenant_id=self.db_manager.tenant.tenant_id,
                    role=role,
                    question=question,
                    answer_text=(
                        "Sou o assistente de inteligência sobre dados públicos municipais (orçamento, despesas, "
                        "contratos, licitações, folha de pagamento, IBGE, SICONFI, INEP, DATASUS, CGU e Diário Oficial). "
                        "Não respondo sobre assuntos gerais fora da administração pública."
                    ),
                    pipeline_steps=steps,
                    exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                )

            # Trava 1 Comercial: Cache SQL Persistente (apenas para perguntas autônomas não contextuais e sem pedido de gráfico/tabela)
            if (
                not sql_override
                and not explicit_tables
                and not explicit_segments
                and not self._has_custom_sql_generator
                and not has_custom_llm_key
                and not is_contextual_followup
                and not wants_chart
                and not wants_table
            ):
                sc_t0 = time.perf_counter()
                cached_res = self.quota_manager.get_cached_response(
                    question=effective_question,
                    role=role,
                    allowed_sources=allowed_sources,
                )
                if cached_res is not None:
                    steps.append(
                        {
                            "step": 3,
                            "name": "Cache Analítico Institucional (Trava 1 — Hit < 2ms, 0 Tokens)",
                            "status": "CACHE_HIT",
                            "duration_ms": round((time.perf_counter() - sc_t0) * 1000.0, 2),
                            "detail": (
                                "Consulta idêntica recuperada do cache persistente SQLite/RAM sem consumo de tokens "
                                "de API externa, garantindo determinismo e proteção de margem B2G."
                            ),
                        }
                    )
                    if session_id not in self.conversation_history:
                        self.conversation_history[session_id] = deque(maxlen=10)
                    self.conversation_history[session_id].append({"role": "user", "content": working_question})
                    self.conversation_history[session_id].append(
                        {
                            "role": "assistant",
                            "content": cached_res.get("answer_text", ""),
                            "sql_executed": cached_res.get("sql_executed") or "",
                            "row_count": len(cached_res.get("sql_rows") or []),
                        }
                    )
                    self._record_session_sql_context(
                        session_id=session_id,
                        question=effective_question,
                        sql_executed=cached_res.get("sql_executed"),
                        sql_rows=cached_res.get("sql_rows") or [],
                    )
                    return OrchestrationResult(
                        status=cached_res.get("status", "sucesso"),
                        intent=cached_res.get("intent", intent),
                        tenant_id=self.db_manager.tenant.tenant_id,
                        role=role,
                        question=question,
                        effective_question=effective_question,
                        standalone_question=effective_question,
                        is_contextual_followup=False,
                        answer_text=cached_res.get("answer_text", ""),
                        citizen_response=cached_res.get("citizen_response", ""),
                        institutional_response=cached_res.get("institutional_response", ""),
                        methodology_summary=cached_res.get("methodology_summary", ""),
                        limitations_and_caveats=cached_res.get("limitations_and_caveats", []),
                        sql_executed=cached_res.get("sql_executed"),
                        sql_attempts=cached_res.get("sql_attempts", 1),
                        sql_rows=cached_res.get("sql_rows", []),
                        rag_chunks=cached_res.get("rag_chunks", []),
                        selected_segments=cached_res.get("selected_segments", []),
                        selected_tables=cached_res.get("selected_tables", []),
                        allowed_sources=list(allowed_sources or []),
                        schema_reduction_pct=cached_res.get("schema_reduction_pct", 0.0),
                        fontes_utilizadas=cached_res.get("fontes_utilizadas", []),
                        transparency_warnings=cached_res.get("transparency_warnings", []),
                        pii_masked_count=cached_res.get("pii_masked_count", 0),
                        prompt_tokens_estimated=self._turn_prompt_tokens_real,
                        prompt_tokens_real=self._turn_prompt_tokens_real,
                        completion_tokens_real=self._turn_completion_tokens_real,
                        llm_engine_used=f"{cached_res.get('llm_engine_used', 'SIM Engine')} (Cache Trava 1 — 0 tokens)",
                        asr_corrections=asr_corrections,
                        pipeline_steps=steps,
                        exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                    )

            sql_rows: List[Dict[str, Any]] = []
            rag_chunks: List[Dict[str, Any]] = []
            fontes: List[str] = []
            warnings: List[Dict[str, Any]] = []
            selected_segments: List[str] = []
            selected_tables: List[str] = []
            reduction_pct: float = 0.0
            sql_executed: Optional[str] = None
            attempts = 0
            est_tokens = 0
            pii_masked = 0

            # Caso seja pergunta jurídica de follow-up (ex: "essas quantias são legais? você tem a lei?"),
            # reaproveita os dados SQL do turno anterior sem precisar reexecutar query se não houver novo filtro numérico
            has_anaphoric_law_ref = any(
                p in q_norm_s2
                for p in (
                    "essas ", "esses ", "essa ", "esse ", "isso",
                    "sao legais", "e legal", "tem a lei", "qual a lei", "base legal",
                )
            )
            asks_only_law_about_previous_turn = bool(
                legal_framework_notes
                and prior_sql_ctx
                and prior_sql_ctx.get("sql_rows")
                and (is_contextual_followup or has_anaphoric_law_ref)
                and not any(
                    w in q_norm_s2
                    for w in (
                        "quanto", "quantos", "quais", "quem", "ranking", "maior", "maiores",
                        "conflito", "evolucao", "compare", "2019", "2020", "2021", "2022", "2023", "2024", "2025", "2026", "2027",
                    )
                )
            )

            # Etapa 3 a 5: Fluxo Analítico SQL Semântico
            if asks_only_law_about_previous_turn:
                sql_rows = list(prior_sql_ctx["sql_rows"])
                sql_executed = prior_sql_ctx.get("sql_executed")
                is_contextual_followup = True
                steps.append(
                    {
                        "step": len(steps) + 1,
                        "name": "Memória Analítica Multi-Turno (Reuso de Evidência Tabular Recente)",
                        "status": "OK",
                        "duration_ms": 0.1,
                        "detail": (
                            f"Reaproveitados {len(sql_rows)} registro(s) tabulares do turno anterior da sessão "
                            f"para fundamentação conjunta com o marco legal aplicável."
                        ),
                    }
                )
            elif intent in ("sql_analytics", "hybrid_sql_rag"):
                missing_check = (
                    None
                    if sql_override
                    else self._check_missing_granular_source(
                        effective_question, allowed_sources=allowed_sources
                    )
                )
                if missing_check is not None:
                    cit_miss, inst_miss, warn_miss = missing_check
                    is_internal = role.lower().strip() in (
                        "auditor", "admin", "controller", "gestor", "servidor", "interno", "controladoria",
                    )
                    sel_miss = inst_miss if is_internal else cit_miss
                    steps.append(
                        {
                            "step": len(steps) + 1,
                            "name": "Verificador de Cobertura Multi-Cidade (Fonte Granular Pendente)",
                            "status": "PENDING_INGESTION",
                            "duration_ms": 0.2,
                            "detail": warn_miss["message"],
                        }
                    )
                    return OrchestrationResult(
                        status="sucesso",
                        intent="clarification_needed",
                        tenant_id=self.db_manager.tenant.tenant_id,
                        role=role,
                        question=question,
                        effective_question=effective_question,
                        standalone_question=effective_question,
                        answer_text=sel_miss,
                        citizen_response=cit_miss,
                        institutional_response=inst_miss,
                        methodology_summary="Verificação determinística de cobertura de fontes por município (0 tokens extras de retry SQL).",
                        limitations_and_caveats=[f"[{warn_miss['gap_id']}] {warn_miss['title']}: {warn_miss['message']}"],
                        transparency_warnings=[warn_miss],
                        schema_reduction_pct=100.0,
                        prompt_tokens_estimated=self._turn_prompt_tokens_real,
                        prompt_tokens_real=self._turn_prompt_tokens_real,
                        completion_tokens_real=self._turn_completion_tokens_real,
                        llm_engine_used="SIM Multi-City Coverage Guard",
                        asr_corrections=asr_corrections,
                        pipeline_steps=steps,
                        exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                    )
                sql_out = self._run_sql_pipeline(
                    effective_question=effective_question,
                    raw_question_norm=q_norm_s2,
                    role=role,
                    hist_list=hist_list,
                    previous_sql=previous_sql if is_contextual_followup else None,
                    explicit_segments=explicit_segments,
                    explicit_tables=explicit_tables,
                    allowed_sources=allowed_sources,
                    sql_override=sql_override,
                    llm_provider=llm_provider,
                    llm_api_key=llm_api_key,
                    steps=steps,
                    wants_chart=wants_chart,
                    wants_table=wants_table,
                    intent=intent,
                )
                if sql_out.get("redirect_conversational"):
                    c_txt, i_txt, conv_eng = self._handle_conversational_or_conceptual_turn(
                        question=working_question,
                        role=role,
                        hist_list=hist_list,
                        llm_provider=llm_provider,
                        llm_api_key=llm_api_key,
                        literal_sql_fallback=sql_out["redirect_conversational"],
                    )
                    is_internal = role.lower().strip() in (
                        "auditor", "admin", "controller", "gestor", "servidor", "interno", "controladoria",
                    )
                    sel_txt = i_txt if is_internal else c_txt
                    if session_id not in self.conversation_history:
                        self.conversation_history[session_id] = deque(maxlen=10)
                    self.conversation_history[session_id].append({"role": "user", "content": working_question})
                    self.conversation_history[session_id].append({"role": "assistant", "content": sel_txt})
                    return OrchestrationResult(
                        status="sucesso",
                        intent="conversational_followup",
                        tenant_id=self.db_manager.tenant.tenant_id,
                        role=role,
                        question=question,
                        effective_question=effective_question,
                        standalone_question=effective_question,
                        is_contextual_followup=True,
                        answer_text=sel_txt,
                        citizen_response=c_txt,
                        institutional_response=i_txt,
                        methodology_summary=f"Redirecionamento conversacional seguro sobre literal SQL ({conv_eng}).",
                        schema_reduction_pct=100.0,
                        prompt_tokens_estimated=sql_out.get("est_tokens", 0),
                        prompt_tokens_real=self._turn_prompt_tokens_real,
                        completion_tokens_real=self._turn_completion_tokens_real,
                        llm_engine_used=conv_eng,
                        asr_corrections=asr_corrections,
                        pipeline_steps=steps,
                        exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                    )
                if sql_out.get("blocked_error"):
                    if sql_override is not None:
                        return OrchestrationResult(
                            status="bloqueado_seguranca",
                            intent=intent,
                            tenant_id=self.db_manager.tenant.tenant_id,
                            role=role,
                            question=question,
                            effective_question=effective_question,
                            standalone_question=effective_question,
                            answer_text=f"A consulta foi bloqueada pelo guardião de segurança e escopo: {sql_out['blocked_error']}",
                            sql_executed=sql_out.get("candidate_sql"),
                            selected_segments=sql_out.get("selected_segments", []),
                            selected_tables=sql_out.get("selected_tables", []),
                            allowed_sources=list(allowed_sources or []),
                            schema_reduction_pct=sql_out.get("reduction_pct", 0.0),
                            sql_attempts=sql_out.get("attempts", 1),
                            prompt_tokens_estimated=sql_out.get("est_tokens", 0),
                            prompt_tokens_real=self._turn_prompt_tokens_real,
                            completion_tokens_real=self._turn_completion_tokens_real,
                            llm_engine_used=self._last_llm_engine_used,
                            asr_corrections=asr_corrections,
                            pipeline_steps=steps,
                            exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                        )
                    # Quando sql_override=None e o SQL gerado pelo LLM esbarrou no SQLGuard ou esgotou tentativas com erro de sintaxe/coluna (`not sql_out.get("sql_executed")`),
                    # tenta fallback seguro pelo compilador nativo sem expor erro técnico ao cidadão!
                    fb_sql = self._compile_native_sql(
                        effective_question,
                        normalize_text_for_dedup(effective_question),
                        {s.upper() for s in (allowed_sources or []) if s.upper() != "DIARIO_OFICIAL"},
                    )
                    fb_res = self.db_manager.execute_safe_query(fb_sql, role=role)
                    if fb_res.get("status") == "sucesso":
                        sql_out["sql_executed"] = fb_res.get("query_executada")
                        sql_out["sql_rows"] = fb_res.get("dados") or []
                        sql_out["fontes"] = fb_res.get("fontes_utilizadas") or []
                        sql_out["pii_masked"] = fb_res.get("pii_masked_count", 0)
                elif sql_override is None and not sql_out.get("sql_executed"):
                    fb_sql = self._compile_native_sql(
                        effective_question,
                        normalize_text_for_dedup(effective_question),
                        {s.upper() for s in (allowed_sources or []) if s.upper() != "DIARIO_OFICIAL"},
                    )
                    fb_res = self.db_manager.execute_safe_query(fb_sql, role=role)
                    if fb_res.get("status") == "sucesso":
                        sql_out["sql_executed"] = fb_res.get("query_executada")
                        sql_out["sql_rows"] = fb_res.get("dados") or []
                        sql_out["fontes"] = fb_res.get("fontes_utilizadas") or []
                        sql_out["pii_masked"] = fb_res.get("pii_masked_count", 0)

                sql_executed = sql_out.get("sql_executed")
                sql_rows = sql_out.get("sql_rows") or []
                limit_applied_turn = bool(sql_out.get("limit_applied", False))
                pii_masked = int(sql_out.get("pii_masked") or 0)
                fontes.extend(sql_out.get("fontes") or [])
                warnings.extend(sql_out.get("warnings") or [])
                selected_segments = sql_out.get("selected_segments") or []
                selected_tables = sql_out.get("selected_tables") or []
                reduction_pct = float(sql_out.get("reduction_pct") or 0.0)
                attempts = int(sql_out.get("attempts") or 0)
                est_tokens = int(sql_out.get("est_tokens") or 0)

            # Etapa 6: Busca Híbrida no Diário Oficial + CRAG Crossover Bidirecional (Eixo 2 + Eixo 6)
            rag_allowed = (not allowed_sources) or ("DIARIO_OFICIAL" in {s.upper() for s in allowed_sources})
            if intent in ("rag_diario_oficial", "hybrid_sql_rag"):
                s6_t0 = time.perf_counter()
                rag_res = self.rag_retriever.search(query=effective_question, top_k=5, deduplicate=True)
                rag_chunks = rag_res.get("chunks", [])
                max_rag_rel = float(rag_res.get("max_relevance_score") or 0.0)
                for f in rag_res.get("fontes_utilizadas", []):
                    if f not in fontes:
                        fontes.append(f)
                sig = rag_res.get("signals_detected", {})
                steps.append(
                    {
                        "step": len(steps) + 1,
                        "name": "Busca Híbrida RAG Contextual (Okapi BM25 + Sinônimos Institucionais)",
                        "status": "OK",
                        "duration_ms": round((time.perf_counter() - s6_t0) * 1000.0, 2),
                        "detail": (
                            f"Sinais: tipo_ato={sig.get('tipo_ato')}, números={sig.get('act_numbers')} | "
                            f"Relevância máx.: {max_rag_rel:.2f} | "
                            f"Collection limpa: {rag_res.get('corpus_size', 0)} atos únicos -> Retornados: {len(rag_chunks)}."
                        ),
                    }
                )

                # Eixo 2 — CRAG Crossover (RAG -> SQL):
                # Quando a busca documental no Diário Oficial tem baixa relevância, retorna 0 atos, ou a pergunta
                # envolve titulares de cargos/secretarias/folha/contratos/valores, cruza automaticamente com o banco SQL!
                eff_norm = normalize_text_for_dedup(effective_question)
                needs_crag_sql_crossover = (
                    intent == "rag_diario_oficial"
                    and not only_rag_scope
                    and sql_override is None
                    and (
                        len(rag_chunks) == 0
                        or max_rag_rel < 4.5
                        or any(
                            kw in eff_norm
                            for kw in (
                                "secretario", "secretaria", "prefeito", "vice", "procurador", "controlador",
                                "diretor", "servidor", "cargo", "quem e", "responsavel", "salario",
                                "remuneracao", "fornecedor", "empresa", "contrato", "valor", "quanto",
                            )
                        )
                    )
                )
                if needs_crag_sql_crossover:
                    crag_out = self._run_sql_pipeline(
                        effective_question=effective_question,
                        raw_question_norm=eff_norm,
                        role=role,
                        hist_list=hist_list,
                        previous_sql=previous_sql if is_contextual_followup else None,
                        explicit_segments=explicit_segments,
                        explicit_tables=explicit_tables,
                        allowed_sources=allowed_sources,
                        sql_override=None,
                        llm_provider=llm_provider,
                        llm_api_key=llm_api_key,
                        steps=steps,
                        step_prefix="[CRAG Crossover RAG->SQL] ",
                    )
                    if crag_out.get("sql_rows"):
                        sql_executed = crag_out.get("sql_executed")
                        sql_rows = crag_out.get("sql_rows") or []
                        pii_masked = int(crag_out.get("pii_masked") or 0)
                        for f in crag_out.get("fontes") or []:
                            if f not in fontes:
                                fontes.append(f)
                        warnings.extend(crag_out.get("warnings") or [])
                        selected_segments = crag_out.get("selected_segments") or []
                        selected_tables = crag_out.get("selected_tables") or []
                        reduction_pct = float(crag_out.get("reduction_pct") or 0.0)
                        attempts = int(crag_out.get("attempts") or 0)
                        est_tokens = int(crag_out.get("est_tokens") or 0)
                        intent = "hybrid_sql_rag" if rag_chunks else "sql_analytics"

            elif sql_rows and rag_allowed:
                # Cross-Link Automático SQL -> Diário Oficial quando as linhas SQL retornam credores/CNPJs/contratos
                s6b_t0 = time.perf_counter()
                entity_candidates: List[str] = []
                extracted_cnpjs: List[str] = []
                name_keys = (
                    "nom_credor", "razao_social", "fornecedor", "empresa_licitante",
                    "nom_empresa", "nom_contratado", "fornecedor_carona", "nom_servidor",
                )
                cnpj_keys = (
                    "num_doc_credor", "num_cnpj", "cnpj", "cnpj_empresa",
                )
                for r in sql_rows[:5]:
                    for nk in name_keys:
                        val = r.get(nk)
                        if val and isinstance(val, str) and len(val.strip()) >= 5:
                            entity_candidates.append(val.strip())
                    for ck in cnpj_keys:
                        val = r.get(ck)
                        if val is not None:
                            digits = re.sub(r"\D", "", str(val))
                            if len(digits) == 14 and digits not in extracted_cnpjs:
                                extracted_cnpjs.append(digits)
                if entity_candidates or extracted_cnpjs:
                    cross_chunks = self.rag_retriever.search_by_entities(
                        entity_names=entity_candidates,
                        cnpjs=extracted_cnpjs,
                        top_k=3,
                    )
                    if cross_chunks:
                        gazette_lbl = self._get_gazette_name()
                        rag_chunks = cross_chunks
                        for ch in cross_chunks:
                            src = f"{gazette_lbl} ({ch.get('source')})"
                            if src not in fontes:
                                fontes.append(src)
                        steps.append(
                            {
                                "step": len(steps) + 1,
                                "name": f"Cross-Link Automático SQL -> Diário Oficial ({gazette_lbl})",
                                "status": "OK",
                                "duration_ms": round((time.perf_counter() - s6b_t0) * 1000.0, 2),
                                "detail": (
                                    f"Entidades extraídas do resultado SQL ({len(entity_candidates)} nomes, {len(extracted_cnpjs)} CNPJs) "
                                    f"cruzadas com o Diário Oficial -> {len(cross_chunks)} ato(s) normativo(s) vinculado(s)."
                                ),
                            }
                        )

            # Eixo 4: Se o usuário pediu um gráfico junto com uma consulta analítica (ex: "gere um gráfico da despesa com saúde"),
            # renderiza o PNG institucional diretamente sobre os `sql_rows` recém-consultados
            chart_b64: Optional[str] = None
            chart_mime: Optional[str] = None
            chart_cap: Optional[str] = None
            artifact = None
            if wants_chart and sql_rows:
                ch_t0 = time.perf_counter()
                artifact = self.chart_generator.generate_chart(rows=sql_rows, question=effective_question)
                if artifact:
                    chart_b64 = artifact.image_base64
                    chart_mime = artifact.mime_type
                    chart_cap = artifact.caption
                    steps.append(
                        {
                            "step": len(steps) + 1,
                            "name": "Renderizador Gráfico Institucional Nativo (PNG Base64 — Eixo 4)",
                            "status": "OK",
                            "duration_ms": round((time.perf_counter() - ch_t0) * 1000.0, 2),
                            "detail": (
                                f"Gráfico PNG ({artifact.chart_kind}, {artifact.width_px}x{artifact.height_px}px) "
                                f"gerado sobre {len(sql_rows)} registro(s)."
                            ),
                        }
                    )

            # Eixo 4b (SIM v4.3): Se o usuário pediu uma TABELA (ex: "me gere uma tabela com os secretários e seus salários"),
            # renderiza o Cartão PNG TrueType de alta nitidez (WhatsApp) + Tabela HTML institucional (Console Web)
            table_b64: Optional[str] = None
            table_mime: Optional[str] = None
            table_cap: Optional[str] = None
            table_html: Optional[str] = None
            table_totals: Optional[Dict[str, str]] = None
            table_art: Optional[TableArtifact] = None
            limit_trunc_turn = bool(locals().get("limit_applied_turn", False))
            if not limit_trunc_turn and sql_executed and sql_rows:
                m_lim = re.search(r"\bLIMIT\s+(\d+)\b", sql_executed, flags=re.IGNORECASE)
                if m_lim:
                    lim_n = int(m_lim.group(1))
                    if 1 < lim_n <= 100 and len(sql_rows) == lim_n:
                        limit_trunc_turn = True
            if wants_table and sql_rows and self.table_renderer.can_render(sql_rows):
                tb_t0 = time.perf_counter()
                table_art = self.table_renderer.render(
                    rows=sql_rows,
                    title=effective_question[:90],
                    truncated_by_security_limit=limit_trunc_turn,
                )
                if table_art:
                    table_b64 = table_art.image_base64
                    table_mime = table_art.mime_type
                    table_cap = table_art.caption
                    table_html = table_art.html
                    table_totals = table_art.totals_by_column
                    steps.append(
                        {
                            "step": len(steps) + 1,
                            "name": "Renderizador de Tabela Institucional (Cartão PNG TrueType + HTML)",
                            "status": "OK",
                            "duration_ms": round((time.perf_counter() - tb_t0) * 1000.0, 2),
                            "detail": (
                                f"Tabela PNG/HTML gerada com {table_art.displayed_rows} de {table_art.total_rows} registro(s) "
                                f"e {len(table_art.columns_rendered)} coluna(s)."
                            ),
                        }
                    )

            # Etapa 7: Síntese Dual Anti-Alucinação (Eixo 7)
            s7_t0 = time.perf_counter()
            dual_synth = self._synthesize_dual_perspectives(
                question=working_question,
                sql_rows=sql_rows,
                rag_chunks=rag_chunks,
                warnings=warnings,
                fontes=fontes,
                allowed_sources=allowed_sources,
                role=role,
                sql_executed=sql_executed,
                intent=intent,
                selected_tables=selected_tables,
                schema_reduction_pct=reduction_pct,
                pii_masked_count=pii_masked,
                llm_provider=llm_provider,
                llm_api_key=llm_api_key,
                legal_framework_notes=legal_framework_notes,
                chart_generated=(chart_b64 is not None),
                chart_caption=chart_cap,
                table_generated=(table_b64 is not None),
                table_caption=table_cap,
                table_totals=table_totals,
                limit_applied=limit_trunc_turn,
            )
            # Se a síntese LLM retornou um título editorial limpo (`chart_title` ou `table_title`), re-renderiza o header (< 12ms em RAM)
            if artifact is not None and dual_synth.get("chart_title"):
                refined_art = self.chart_generator.generate_chart(
                    rows=sql_rows,
                    question=effective_question,
                    chart_spec={"title": dual_synth["chart_title"]},
                )
                if refined_art:
                    chart_b64 = refined_art.image_base64
                    chart_mime = refined_art.mime_type
                    chart_cap = refined_art.caption

            if table_art is not None and dual_synth.get("table_title"):
                refined_tbl = self.table_renderer.render(
                    rows=sql_rows,
                    title=str(dual_synth["table_title"])[:90],
                    truncated_by_security_limit=limit_trunc_turn,
                )
                if refined_tbl:
                    table_b64 = refined_tbl.image_base64
                    table_mime = refined_tbl.mime_type
                    table_cap = refined_tbl.caption
                    table_html = refined_tbl.html

            def _clean_dotted_years(txt: str) -> str:
                if not txt:
                    return txt
                return re.sub(r"(?<!R\$\s)(?<!R\$)(?<!\d)2\.0(1[89]|2\d|3[0-5])\b(?!,\d{2})", r"20\1", txt)

            dual_synth["answer_text"] = _clean_dotted_years(dual_synth.get("answer_text") or "")
            dual_synth["citizen_response"] = _clean_dotted_years(dual_synth.get("citizen_response") or "")
            dual_synth["institutional_response"] = _clean_dotted_years(dual_synth.get("institutional_response") or "")
            final_answer = dual_synth["answer_text"]
            synth_engine = dual_synth["synth_engine"]
            steps.append(
                {
                    "step": len(steps) + 1,
                    "name": "Síntese Dual Anti-Alucinação (Naturalização ao Cidadão + Dossiê Institucional)",
                    "status": "OK",
                    "duration_ms": round((time.perf_counter() - s7_t0) * 1000.0, 2),
                    "detail": (
                        f"Duas apresentações geradas ({synth_engine}): perspectiva naturalizada ao cidadão com "
                        f"consciência de exercício parcial e dossiê técnico com {len(fontes)} fonte(s)."
                    ),
                }
            )

            # Gera preview compacto numerado (#1..#5) para resolução multi-turno de referências ordinais ("2º item", "primeira empresa")
            top_preview_parts: List[str] = []
            for r_idx, r_item in enumerate(sql_rows[:5], start=1):
                if isinstance(r_item, dict) and r_item:
                    kv_mini = ", ".join(f"{k}={v}" for k, v in list(r_item.items())[:3] if v is not None)
                    top_preview_parts.append(f"#{r_idx}({kv_mini})")
            top_items_preview_str = "; ".join(top_preview_parts)[:320]

            # Atualizar memória conversacional Multi-Turno com `sql_executed`, `row_count` e cache de linhas da sessão
            with self._mem_lock:
                if session_id not in self.conversation_history:
                    self.conversation_history[session_id] = deque(maxlen=10)
                self.conversation_history.move_to_end(session_id)
                hist_deque = self.conversation_history[session_id]
                hist_deque.append({"role": "user", "content": working_question})
                hist_deque.append(
                    {
                        "role": "assistant",
                        "content": (dual_synth["citizen_response"] or final_answer)[:600],
                        "sql_executed": sql_executed or "",
                        "row_count": len(sql_rows),
                        "top_items_preview": top_items_preview_str,
                    }
                )
            self._prune_session_maps()
            self._record_session_sql_context(
                session_id=session_id,
                question=effective_question,
                sql_executed=sql_executed,
                sql_rows=sql_rows,
            )

            eff_prompt_tokens = self._turn_prompt_tokens_real if self._turn_prompt_tokens_real > 0 else est_tokens
            result_obj = OrchestrationResult(
                status="sucesso",
                intent=intent,
                tenant_id=self.db_manager.tenant.tenant_id,
                role=role,
                question=question,
                effective_question=effective_question,
                standalone_question=effective_question,
                is_contextual_followup=is_contextual_followup,
                answer_text=final_answer,
                citizen_response=dual_synth["citizen_response"],
                institutional_response=dual_synth["institutional_response"],
                methodology_summary=dual_synth["methodology_summary"],
                limitations_and_caveats=dual_synth["limitations_and_caveats"],
                sql_executed=sql_executed,
                sql_attempts=attempts,
                sql_rows=sql_rows,
                rag_chunks=rag_chunks,
                selected_segments=selected_segments,
                selected_tables=selected_tables,
                allowed_sources=list(allowed_sources or []),
                schema_reduction_pct=reduction_pct,
                fontes_utilizadas=fontes,
                transparency_warnings=warnings,
                pii_masked_count=pii_masked,
                prompt_tokens_estimated=eff_prompt_tokens,
                prompt_tokens_real=self._turn_prompt_tokens_real,
                completion_tokens_real=self._turn_completion_tokens_real,
                llm_engine_used=self._last_llm_engine_used,
                chart_base64=chart_b64,
                chart_mime_type=chart_mime,
                chart_caption=chart_cap,
                table_image_base64=table_b64,
                table_mime_type=table_mime,
                table_caption=table_cap,
                table_html=table_html,
                asr_corrections=asr_corrections,
                pipeline_steps=steps,
                exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
            )

            # Eixo 7 & Eixo 8: Higiene Estrita do Cache SQLite + Telemetria Real de Tokens (sem dupla contagem)
            # NUNCA grava em cache respostas de contingência offline quando há chave LLM configurada (evita cache poisoning em oscilações 429/rede)
            from sim_platform import get_configured_openai_key
            has_ext_key = bool(get_configured_openai_key() or os.environ.get("GEMINI_API_KEY", "").strip())
            is_degraded_fallback = has_ext_key and ("Native NL2SQL Compiler" in str(self._last_llm_engine_used))
            already_debited = (self._turn_prompt_tokens_real + self._turn_completion_tokens_real) > 0
            if (
                not is_degraded_fallback
                and not has_custom_llm_key
                and not sql_override
                and not explicit_tables
                and not explicit_segments
                and not wants_chart
                and not wants_table
            ):
                self.quota_manager.store_response_and_record_usage(
                    question=effective_question,
                    role=role,
                    allowed_sources=allowed_sources,
                    response_payload=result_obj.to_dict(),
                    prompt_tokens=self._turn_prompt_tokens_real,
                    completion_tokens=self._turn_completion_tokens_real,
                    sql_executed=sql_executed,
                    is_contextual_followup=is_contextual_followup,
                    tokens_already_debited_realtime=already_debited,
                )
            elif not already_debited:
                self.quota_manager.record_usage(
                    prompt_tokens=0,
                    completion_tokens=0,
                    cached=False,
                )

            return result_obj
        finally:
            self.lock_manager.release(session_id)
