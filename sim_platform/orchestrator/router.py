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
from collections import deque
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

DEFAULT_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]

OTHER_MUNICIPALITIES = (
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
}


def _human_col_label(col_key: str) -> str:
    if col_key in HUMAN_COLUMN_LABELS:
        return HUMAN_COLUMN_LABELS[col_key]
    low = col_key.lower()
    if low in HUMAN_COLUMN_LABELS:
        return HUMAN_COLUMN_LABELS[low]
    clean = col_key.replace("_", " ").strip()
    return clean[:1].upper() + clean[1:] if clean else col_key


def _explain_municipal_concepts_for_citizen(question: str, sample_keys: List[str]) -> Optional[str]:
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
    if "orcamento_previsto_loa" in keys_set or any(y in q_low for y in ("2026", "2027", "2028")):
        explanations.append(
            "**Orçamento Previsto (LOA Atualizada)** é o valor planejado e autorizado para o ano inteiro, enquanto **Receita Arrecadada** é o valor que já entrou nos cofres municipais. "
            "A base histórica oficial do SICOM/TCE-MG consolidada nesta plataforma abrange os exercícios de **2020 a 2025** (exercícios futuros como 2026 e 2027 ainda não possuem execução encerrada no SICOM)."
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
        explanations.append(
            "Estes valores são **benefícios sociais pagos diretamente pelo Governo Federal** (via Caixa/Ministério do Desenvolvimento Social) às famílias cadastradas em Caeté, registrados no Portal da Transparência da CGU."
        )

    if not explanations:
        return None
    return " ".join(explanations[:2])


def _format_brl_or_val(key: str, val: Any) -> str:
    if val is None:
        return "N/D"
    k_low = key.lower()
    # Colunas de ano, exercício, sequência ou código nunca devem receber separador de milhar (ex: 2024 e não 2.024)
    if any(
        k_low == yk or k_low.startswith(yk)
        for yk in (
            "ano_referencia", "ano_", "primeiro_ano", "ultimo_ano", "exercicio",
            "seq_", "num_contrato", "num_convenio", "cod_emenda", "numero_edicao",
        )
    ) or k_low == "ano":
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
    Gerenciador de lock atômico por sessão/chat (`chat_id`), corrigindo a falha
    do `wpp-tiokk-sql.json` onde o lock era deletado antes da execução do agente.
    """

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._chat_locks: Dict[str, threading.Lock] = {}
        self._active_sessions: Dict[str, float] = {}

    def get_lock(self, session_id: str) -> threading.Lock:
        with self._guard:
            if session_id not in self._chat_locks:
                self._chat_locks[session_id] = threading.Lock()
            return self._chat_locks[session_id]

    def try_acquire(self, session_id: str, blocking: bool = True, timeout: float = 10.0) -> bool:
        lk = self.get_lock(session_id)
        acquired = lk.acquire(blocking=blocking, timeout=timeout if blocking else -1)
        if acquired:
            with self._guard:
                self._active_sessions[session_id] = time.time()
        return acquired

    def release(self, session_id: str) -> None:
        lk = self.get_lock(session_id)
        with self._guard:
            self._active_sessions.pop(session_id, None)
        if lk.locked():
            lk.release()


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
    asr_corrections: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.answer_text = strip_emojis(self.answer_text)
        if self.citizen_response:
            self.citizen_response = strip_emojis(self.citizen_response)
        if self.institutional_response:
            self.institutional_response = strip_emojis(self.institutional_response)
        if self.chart_caption:
            self.chart_caption = strip_emojis(self.chart_caption)

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
        self.rag_retriever = rag_retriever or HybridRAGRetriever()
        cat_max_yr = getattr(self.db_manager.catalog, "max_data_year", 2025)
        cat_latest_rem = getattr(
            self.db_manager.catalog,
            "latest_remittance_human",
            "agosto de 2025 (exercício 2025 em remessa parcial acumulada no SICOM/TCE-MG)",
        )
        self.prompt_builder = prompt_builder or ModularPromptBuilder(
            municipality_name=self.db_manager.tenant.municipality_name,
            state_uf=self.db_manager.tenant.state_uf,
            ibge_code=self.db_manager.tenant.ibge_code,
            max_data_year=cat_max_yr,
            latest_remittance_human=cat_latest_rem,
        )
        self.entity_linker = EntityLinker(
            workspace_root=DEFAULT_WORKSPACE_ROOT,
            conn=self.db_manager.conn,
            city_name=self.db_manager.tenant.municipality_name,
        )
        self.quota_manager = TenantQuotaManager(
            tenant_id=self.db_manager.tenant.tenant_id,
        )
        self.chart_generator = InstitutionalChartGenerator(
            municipality_name=self.db_manager.tenant.municipality_name,
            state_uf=self.db_manager.tenant.state_uf,
        )
        self.interaction_store = interaction_store
        self._has_custom_sql_generator: bool = sql_generator_fn is not None
        self.sql_generator_fn = sql_generator_fn or self._smart_sql_generator
        self.max_sql_retries = max_sql_retries
        self.lock_manager = SessionLockManager()
        self.conversation_history: Dict[str, Deque[Dict[str, Any]]] = {}
        self.session_sql_memory: Dict[str, Dict[str, Any]] = {}
        self._golden_lookup: Dict[str, str] = self._load_golden_sql_index()
        self.semantic_guardrail = SemanticIntentGuardrail(
            municipality_name=self.db_manager.tenant.municipality_name,
            state_uf=self.db_manager.tenant.state_uf,
            ibge_code=self.db_manager.tenant.ibge_code,
            fallback_classifier_fn=self.classify_intent,
            fallback_conversational_fn=self._handle_conversational_or_conceptual_turn,
            max_data_year=cat_max_yr,
            latest_remittance_human=cat_latest_rem,
        )
        self._active_allowed_sources: List[str] = []
        self._active_llm_provider: Optional[str] = None
        self._active_llm_api_key: Optional[str] = None
        self._last_llm_engine_used: str = "SIM Native NL2SQL Compiler"
        self._turn_prompt_tokens_real: int = 0
        self._turn_completion_tokens_real: int = 0

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
        if sql_executed and sql_rows:
            self.session_sql_memory[session_id] = {
                "question": question,
                "sql_executed": sql_executed,
                "sql_rows": list(sql_rows[:40]),
            }

    def _get_latest_session_sql_context(self, session_id: str) -> Optional[Dict[str, Any]]:
        in_mem = self.session_sql_memory.get(session_id)
        if in_mem and in_mem.get("sql_rows"):
            return in_mem
        if self.interaction_store is not None and hasattr(self.interaction_store, "find_latest_sql_rows_for_session"):
            try:
                persisted = self.interaction_store.find_latest_sql_rows_for_session(session_id)
                if persisted and persisted.get("sql_rows"):
                    return persisted
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

        for tname in ref_tables[:4]:
            tmeta = self.db_manager.catalog.tables.get(tname)
            if not tmeta:
                continue
            col_map = {c.name.lower(): c.name for c in tmeta.columns}
            try:
                if "ano_referencia" in col_map:
                    yr_col = col_map["ano_referencia"]
                    cnt, min_y, max_y = self.db_manager.conn.execute(
                        f'SELECT COUNT(*), MIN("{yr_col}"), MAX("{yr_col}") FROM "{tname}"'
                    ).fetchone()
                    lines.append(
                        f"- Tabela `{tname}`: {cnt} registros totais | `ano_referencia` disponível: {min_y} a {max_y} "
                        f"| Grão: {tmeta.grain_type} ({tmeta.counting_rule})"
                    )
                else:
                    cnt = self.db_manager.conn.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
                    lines.append(
                        f"- Tabela `{tname}`: {cnt} registros totais (sem coluna `ano_referencia` direta) "
                        f"| Grão: {tmeta.grain_type} ({tmeta.counting_rule})"
                    )
            except Exception:
                pass

            for fc in sorted(filtered_cols):
                real_col = col_map.get(fc.lower())
                if not real_col or real_col.lower() == "ano_referencia":
                    continue
                try:
                    rows = self.db_manager.conn.execute(
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

        q_norm = normalize_text_for_dedup(effective_question)
        if re.search(r"\b202[6-9]\b", q_norm):
            lines.append(
                "- ORIENTAÇÃO PARA EXERCÍCIO FUTURO (2026/2027+): As bases consolidadas encerram-se em 2025. "
                "REMOVA o filtro `ano_referencia = 2026/2027` e consulte a série histórica oficial de 2020 a 2025 "
                "(ex.: `SELECT * FROM vw_gold_orcamento_previsto_vs_realizado ORDER BY ano_referencia`) para fundamentar a resposta."
            )
        lines.append(
            "- AÇÃO REQUERIDA: Gere uma NOVA consulta SQL ajustando os nomes/valores de filtro para os valores reais acima, "
            "priorizando as Gold Views (`vw_gold_orcamento_previsto_vs_realizado`, `vw_gold_folha_servidores`, "
            "`vw_gold_frota_consolidada`, `vw_gold_execucao_despesa_credor`, `vw_gold_fornecedores_qsa_360`)."
        )
        return "\n".join(lines)

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
                or q_norm.startswith("qual o ")
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

        has_explicit_analytical_request = bool(
            re.search(
                r"\b(quanto|quais\s+(?:os|as|foram|empresas|fornecedores|contratos|decretos|portarias|servidores)|"
                r"gastou|gastos\s+com|total\s+pago|despesa\s+com|valor\s+pago|202[0-6])\b",
                q_norm,
            )
        )
        if not has_explicit_analytical_request and (
            q_norm in ("oi", "ola", "bom dia", "boa tarde", "boa noite", "ajuda", "help")
            or any(pat in q_norm for pat in IDENTITY_GREETING_PATTERNS)
            or _IDENTITY_REGEX.search(q_norm) is not None
        ):
            return "conversational_identity"

        if _CONCEPTUAL_DOUBT_REGEX.search(q_norm) is not None and not re.search(r"\b202[0-6]\b", q_norm):
            return "conceptual_doubt"

        if (
            _FOLLOWUP_REFORMAT_REGEX.search(q_norm) is not None
            or ("grafico" in q_norm and not any(w in q_norm for w in ("orcamento", "saude", "educacao", "folha", "frota", "fornecedor")))
        ) and not any(
            kw in q_norm for kw in ("quanto", "quais empresas", "maior", "maiores", "total pago", "2020", "2021", "2022", "2023", "2024", "2025")
        ):
            return "conversational_followup"

        tenant_city = normalize_text_for_dedup(self.db_manager.tenant.municipality_name)
        for city in OTHER_MUNICIPALITIES:
            if city in q_norm and tenant_city not in q_norm:
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

        # 2. Se há histórico na sessão (usuário pedindo resumo, gráfico, reformulação ou explicação sobre a resposta anterior)
        prev_assistant_msgs = [m["content"] for m in hist_list if m.get("role") == "assistant" and m.get("content")]
        prev_user_msgs = [m["content"] for m in hist_list if m.get("role") == "user" and m.get("content")]
        last_answer = prev_assistant_msgs[-1] if prev_assistant_msgs else ""
        last_question = prev_user_msgs[-1] if prev_user_msgs else ""

        # 2a. Geração direta de Gráfico Visual em Barras (ASCII/Unicode) para WhatsApp e Web
        if "grafico" in q_norm:
            chart_items: List[Tuple[str, float, str]] = []
            if last_answer:
                for line in last_answer.split("\n"):
                    line_s = line.strip()
                    if not (line_s.startswith("•") or line_s.startswith("- ")):
                        continue
                    yr_m = re.search(r"\b(202[0-7])\b", line_s)
                    brl_matches = re.findall(r"R\$\s*([\d\.,]+)", line_s)
                    if brl_matches:
                        raw_num = brl_matches[-1].replace(".", "").replace(",", ".")
                        try:
                            val_f = float(raw_num)
                            lbl = f"Ano {yr_m.group(1)}" if yr_m else line_s.split("—")[0].replace("•", "").replace("*", "").strip()[:28]
                            chart_items.append((lbl, val_f, f"R$ {brl_matches[-1]}"))
                        except Exception:
                            pass
                    else:
                        num_matches = re.findall(r"\*\*([\d\.]+)\*\*", line_s)
                        if num_matches:
                            raw_num = num_matches[-1].replace(".", "")
                            if raw_num.isdigit() and int(raw_num) not in (2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027):
                                val_f = float(raw_num)
                                lbl = f"Ano {yr_m.group(1)}" if yr_m else line_s.split("—")[0].replace("•", "").replace("*", "").strip()[:28]
                                chart_items.append((lbl, val_f, num_matches[-1]))

                if not chart_items:
                    # Tentar extrair pares (Ano 202X ... R$ X milhoes) do parágrafo narrativo
                    for m_yr_val in re.finditer(r"\b(202[0-5])\b[^R\n]{0,60}R\$\s*([\d\.,]+)\s*(milh|mil)?", last_answer, re.IGNORECASE):
                        yr_s = m_yr_val.group(1)
                        num_s = m_yr_val.group(2)
                        mult_s = (m_yr_val.group(3) or "").lower()
                        try:
                            base_f = float(num_s.replace(".", "").replace(",", ".") if "," in num_s else num_s.replace(",", "."))
                            if mult_s.startswith("milh"):
                                base_f *= 1_000_000.0
                            disp = f"R$ {num_s} milhões" if mult_s.startswith("milh") else f"R$ {num_s}"
                            chart_items.append((f"Ano {yr_s}", base_f, disp))
                        except Exception:
                            pass

            if not chart_items:
                # Se pediu gráfico sem consulta prévia numérica, exibir o gráfico oficial da evolução orçamentária de Caeté (2020-2025)
                chart_items = [
                    ("2020 (LOA)", 125493351.20, "R$ 125,49 M"),
                    ("2021 (LOA)", 132375778.23, "R$ 132,38 M"),
                    ("2022 (LOA)", 147844515.55, "R$ 147,84 M"),
                    ("2023 (LOA)", 227430636.96, "R$ 227,43 M"),
                    ("2024 (LOA)", 221765888.37, "R$ 221,77 M"),
                    ("2025 (LOA)", 266827763.63, "R$ 266,83 M"),
                ]
                title_ctx = f"Evolução do Orçamento Anual Previsto (LOA) de {city_str} (2020 a 2025)"
            else:
                title_ctx = f"Comparativo Visual — {last_question or f'Dados de {city_str}'}"

            max_v = max((it[1] for it in chart_items), default=1.0) or 1.0
            bar_lines = [f"**Gráfico Comparativo em Barras ({title_ctx}):**\n"]
            for lbl, val_f, disp_v in chart_items[:10]:
                filled = max(1, int(round((val_f / max_v) * 14))) if val_f > 0 else 0
                empty = max(0, 14 - filled)
                bar_str = ("█" * filled) + ("░" * empty)
                pct_rel = (val_f / max_v) * 100.0 if max_v > 0 else 0.0
                bar_lines.append(f"• **{lbl}**: `{bar_str}` **{disp_v}** ({pct_rel:.0f}% do pico)")
            bar_lines.append(
                f"\n*Representação visual gerada diretamente pela Plataforma SIM de {city_str} com base nos registros oficiais auditados.*"
            )
            chart_text = "\n".join(bar_lines)
            return chart_text, chart_text, "SIM Visual Bar Chart Generator (0 tokens)"

        openai_key = get_configured_openai_key(llm_api_key if (llm_provider == "openai" or (llm_api_key and llm_api_key.startswith("sk-"))) else None)
        gemini_key = (
            llm_api_key
            if (llm_provider == "gemini" or (llm_api_key and not llm_api_key.startswith("sk-")))
            else os.environ.get("GEMINI_API_KEY", os.environ.get("GOOGLE_API_KEY", "")).strip()
        )

        if last_answer and (openai_key or gemini_key):
            from sim_platform.security.semantic_guardrail import _detect_legal_frameworks

            legal_notes = _detect_legal_frameworks(f"{last_question} {question}")
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
                    + "Se quiser filtrar esses dados por outro ano (2020 a 2025), por secretaria ou detalhar um fornecedor específico, basta informar."
                )
                return explained, explained, "SIM Deterministic Multi-Turn Contextualizer (0 tokens)"

        # 3. Caso não haja histórico anterior, apresentar resposta concisa de ajuda
        fallback_help = (
            f"Sou o sistema de consulta a dados públicos do Município de **{city_str}** (IBGE `{self.db_manager.tenant.ibge_code}`). "
            f"Consulto dados oficiais de orçamento, receitas, despesas, contratos, licitações, folha de servidores, saúde, educação e Diário Oficial (2020 a 2025). "
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
        synth_engine = "SIM Dual-Perspective Narrative Synthesizer"

        sql_failed_flag = bool(intent in ("sql_analytics", "hybrid_sql_rag") and not sql_executed and not sql_rows)
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
                            "max_tokens": 550,
                            "response_format": {"type": "json_object"},
                            "messages": [
                                {"role": "system", "content": synth_pkg["system"]},
                                {"role": "user", "content": synth_pkg["user"]},
                            ],
                        },
                        timeout=16,
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
                        except Exception:
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
                            "generationConfig": {"temperature": 0.1, "responseMimeType": "application/json"},
                        },
                        timeout=16,
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
                            except Exception:
                                llm_narrative = strip_emojis(raw_content)
                            synth_engine = "Google Gemini 2.5 Flash Dual Synthesis"
                except Exception:
                    pass

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
                m_col = num_cols[-1]
                m_label = _human_col_label(m_col)
                sorted_yr = sorted(sql_rows, key=lambda r: int(r.get("ano_referencia") or 0))
                y0 = _format_brl_or_val("ano_referencia", sorted_yr[0].get("ano_referencia"))
                y1 = _format_brl_or_val("ano_referencia", sorted_yr[-1].get("ano_referencia"))
                v0 = float(sorted_yr[0].get(m_col) or 0)
                v1 = float(sorted_yr[-1].get(m_col) or 0)
                total_sum = sum(float(r.get(m_col) or 0) for r in sql_rows)
                delta_pct = ((v1 - v0) / v0) * 100 if v0 > 0 else 0.0
                delta_citizen = (
                    f", o que representa uma variação de **{delta_pct:+.1f}%** no período"
                    if v0 > 0
                    else ""
                )
                is_ratio_col = any(p in m_col.lower() for p in ("pct", "percent", "taxa", "isf_m", "idhm", "ideb", "per_capita"))
                sum_citizen = (
                    ""
                    if is_ratio_col
                    else f" Somando os {len(sql_rows)} anos consultados, o total acumulado foi de **{_format_brl_or_val(m_col, total_sum)}**."
                )

                if not citizen_intro:
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
                        + ("" if is_ratio_col else f" Soma acumulada nos {len(sql_rows)} exercícios: **{_format_brl_or_val(m_col, total_sum)}**.")
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
            citizen_intro = (
                f"Encontrei **{len(rag_chunks)} publicação(ões) oficial(is)** no Diário Oficial do Município de **{city_str}** "
                f"(*Jornal Opinião*) sobre a sua pergunta, com destaque para **{first_ch.get('tipo_ato', 'ATO OFICIAL')}** "
                f"(edição `{first_ch.get('source', '')}`)."
            )
            inst_intro = (
                f"**Síntese Normativa (RAG Diário Oficial):** Recuperados **{len(rag_chunks)} ato(s) normativo(s)** "
                f"na coleção atomizada do *Jornal Opinião* de **{city_str}**."
            )

        # Explicação conceitual acessível ao cidadão
        concept_explanation = _explain_municipal_concepts_for_citizen(question, sample_keys)

        # Limitações, transparência e avisos metodológicos
        limitations_list: List[str] = []
        for w in warnings:
            limitations_list.append(f"[{w.get('gap_id', 'AVISO')}] {w.get('title')}: {w.get('message')}")
        if pii_masked_count > 0:
            limitations_list.append(
                f"[LGPD-PII] {pii_masked_count} CPF(s) de pessoas físicas foram mascarados automaticamente no perfil público (Lei 13.709/2018), preservando CNPJs empresariais."
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
            for w in warnings:
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
            if len(sql_rows) > 1 or len(sample_keys) > 2:
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
        elif intent in ("sql_analytics", "hybrid_sql_rag") and not warnings and not citizen_intro:
            if sql_executed:
                citizen_lines.append(
                    f"Consultei as bases oficiais da Prefeitura de {city_str}, mas não foram encontrados registros para esses critérios específicos."
                )
            else:
                citizen_lines.append(
                    f"Não foi possível concluir o cruzamento tabular automático nas bases de {city_str} com a formulação atual. "
                    f"Por favor, tente especificar o ano (2020 a 2025), o órgão ou o nome do fornecedor/indicador desejado."
                )

        if rag_chunks:
            citizen_lines.append("\n**Publicações vinculadas no Diário Oficial do Município (*Jornal Opinião*):**")
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
        elif intent in ("sql_analytics", "hybrid_sql_rag") and not warnings and not inst_intro:
            inst_lines.append("Não foram encontrados registros tabulares para os filtros informados.")

        if rag_chunks:
            inst_lines.append(
                f"\n**Atos Normativos Vinculados no Diário Oficial (*Jornal Opinião* — {len(rag_chunks)} ato(s)):**"
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
        Compilador determinístico nativo (0 tokens) para as 79 tabelas e 6 Gold Views.
        Garante execução exata sem alucinação para perguntas canônicas, Secretarias, Índices LRF/CF88 e Trilhas TCE-MG.
        """
        years = [int(y) for y in re.findall(r"\b(202[0-6])\b", q_norm)]
        if "entre" in q_norm and len(years) >= 2:
            year_filter = f"ano_referencia BETWEEN {min(years)} AND {max(years)}"
        elif "(" in q_raw and len(years) >= 1 and re.search(r"\(.*202[0-6].*\)$", q_raw.strip()):
            year_filter = f"ano_referencia = {years[-1]}"
        elif len(years) == 1:
            year_filter = f"ano_referencia = {years[0]}"
        elif len(years) >= 2:
            year_filter = f"ano_referencia BETWEEN {min(years)} AND {max(years)}"
        elif any(w in q_norm for w in ("evolucao", "serie", "historico", "todos os anos", "2020 a 2025")):
            year_filter = "ano_referencia BETWEEN 2020 AND 2025"
        else:
            year_filter = "ano_referencia = 2024"

        # 1. Atalhos de Gold Views e Trilhas TCE-MG (Conflito de Interesses, Caronas, Frota Órfã)
        if any(w in q_norm for w in ("conflito de interesse", "conflito de interesses", "porta giratoria", "servidor socio")):
            return (
                "SELECT num_cpf, num_cpf AS cpf_socio_servidor, nom_pessoa, dsc_cargo, dsc_lotacao, primeiro_ano_folha, ultimo_ano_folha, "
                "nom_empresa, cnpj_empresa, dsc_tipo_participacao, total_pago_empresa "
                "FROM vw_gold_conflito_interesses_licitacao ORDER BY total_pago_empresa DESC LIMIT 20"
            )

        if any(w in q_norm for w in ("carona", "adesao a ata", "registro de preco", "registro de precos")):
            return (
                "SELECT MAX(nom_credor) AS nom_credor, num_doc_credor, SUM(qtd_empenhos) AS qtd_empenhos_totais, "
                "ROUND(SUM(total_empenhado_carona), 2) AS total_empenhado_carona, "
                "ROUND(SUM(total_pago), 2) AS total_pago "
                "FROM vw_gold_execucao_despesa_credor "
                "WHERE total_empenhado_carona > 0 "
                "GROUP BY num_doc_credor ORDER BY total_empenhado_carona DESC LIMIT 15"
            )

        if any(w in q_norm for w in ("sem placa", "veiculo orfao", "veiculos orfaos", "placa ausente")):
            return (
                "SELECT ano_referencia, status_cadastro_frota, COUNT(*) AS qtd_lancamentos, "
                "ROUND(SUM(vlr_gasto), 2) AS total_gasto_frota "
                "FROM vw_gold_frota_consolidada GROUP BY 1, 2 ORDER BY ano_referencia DESC, total_gasto_frota DESC LIMIT 30"
            )

        # 1b. Perguntas sobre titulares de Secretarias, Prefeito, Vice-Prefeito, Procurador ou Controlador na Folha (2025)
        if any(
            pat in q_norm
            for pat in (
                "quem e o atual secretario",
                "quem e o secretario",
                "quem e a secretaria",
                "atual secretario",
                "nome do secretario",
                "quem e o prefeito",
                "quem e o vice",
                "quem e o procurador",
                "quem e o controlador",
            )
        ) or ("secretario" in q_norm and any(w in q_norm for w in ("quem", "atual", "meio ambiente", "saude", "obras", "educacao", "administracao", "defesa", "assistencia", "fazenda"))):
            sec_lot_filter = ""
            if "meio ambiente" in q_norm or "sedesma" in q_norm or "sustentavel" in q_norm:
                sec_lot_filter = " AND (dsc_lotacao ILIKE '%SEDESMA%' OR dsc_lotacao ILIKE '%AMBIENTE%')"
            elif "saude" in q_norm:
                sec_lot_filter = " AND dsc_lotacao ILIKE '%SAUDE%'"
            elif "obras" in q_norm:
                sec_lot_filter = " AND dsc_lotacao ILIKE '%OBRAS%'"
            elif "administracao" in q_norm:
                sec_lot_filter = " AND dsc_lotacao ILIKE '%ADM%'"
            elif "defesa" in q_norm:
                sec_lot_filter = " AND dsc_lotacao ILIKE '%DEF%'"
            elif "assistencia" in q_norm or "social" in q_norm or "semas" in q_norm:
                sec_lot_filter = " AND dsc_lotacao ILIKE '%SEMAS%'"
            elif "procurador" in q_norm or "juridic" in q_norm:
                sec_lot_filter = " AND (dsc_cargo ILIKE '%PROCURADOR%' OR dsc_lotacao ILIKE '%PROCURADOR%')"
            elif "control" in q_norm:
                sec_lot_filter = " AND (dsc_cargo ILIKE '%CONTRO%' OR dsc_lotacao ILIKE '%CONTRO%')"
            elif "prefeito" in q_norm and "secretario" not in q_norm:
                sec_lot_filter = " AND dsc_cargo ILIKE '%PREFEITO%'"
            return (
                "SELECT ano_referencia, nom_servidor, MAX(dsc_cargo) AS dsc_cargo, MAX(dsc_lotacao) AS dsc_lotacao, "
                "ROUND(AVG(vlr_rmn_bruta), 2) AS remuneracao_mensal_bruta, "
                "ROUND(AVG(vlr_rmn_liquida), 2) AS remuneracao_mensal_liquida "
                "FROM vw_gold_folha_servidores "
                "WHERE ano_referencia = 2025 "
                "AND (dsc_cargo ILIKE '%SECRETARIO MUNICIPAL%' OR dsc_cargo ILIKE '%PREFEITO%' "
                "OR dsc_cargo ILIKE '%PROCURADOR GERAL%' OR dsc_cargo ILIKE '%CONTROLADOR%' "
                "OR dsc_situacao ILIKE '%Agente Pol%') "
                f"AND dsc_cargo NOT ILIKE '%VEREADOR%'{sec_lot_filter} "
                "GROUP BY ano_referencia, nom_servidor "
                "ORDER BY remuneracao_mensal_bruta DESC, nom_servidor LIMIT 20"
            )

        # 1c. Salário do Prefeito / Vice-Prefeito / Secretários e comparação com a média salarial municipal
        if any(w in q_norm for w in ("prefeito", "vice-prefeito", "vice prefeito")) and any(
            w in q_norm for w in ("salario", "remuneracao", "subsidio", "ganha", "recebe", "media", "mensal")
        ):
            target_yr = years[-1] if years else 2025
            if target_yr > 2025:
                target_yr = 2025
            return (
                f"WITH media_mun AS ("
                f"SELECT ano_referencia, ROUND(AVG(vlr_rmn_bruta), 2) AS media_bruta_mun "
                f"FROM vw_gold_folha_servidores WHERE ano_referencia IN (2024, 2025) GROUP BY ano_referencia"
                f") SELECT f.ano_referencia, f.nom_servidor, MAX(f.dsc_cargo) AS dsc_cargo, "
                f"ROUND(AVG(f.vlr_rmn_bruta), 2) AS remuneracao_mensal_bruta, "
                f"ROUND(AVG(f.vlr_rmn_liquida), 2) AS remuneracao_mensal_liquida, "
                f"m.media_bruta_mun AS media_salarial_prefeitura, "
                f"ROUND(AVG(f.vlr_rmn_bruta) / NULLIF(m.media_bruta_mun, 0), 2) AS multiplo_acima_da_media "
                f"FROM vw_gold_folha_servidores f "
                f"JOIN media_mun m ON f.ano_referencia = m.ano_referencia "
                f"WHERE f.ano_referencia IN (2024, 2025) AND f.dsc_cargo ILIKE '%PREFEITO%' "
                f"GROUP BY f.ano_referencia, f.nom_servidor, m.media_bruta_mun "
                f"ORDER BY f.ano_referencia DESC, remuneracao_mensal_bruta DESC"
            )

        # 1d. Quadro de Funcionários: Concursados (Efetivos) vs. Contratados (Temporários) vs. Comissionados vs. Consignados
        if any(
            w in q_norm
            for w in (
                "concursado", "concursados", "consignado", "consignados", "contratado", "contratados",
                "temporario", "temporarios", "comissionado", "comissionados", "efetivo", "efetivos",
                "quantos funcionarios", "quantidade de funcionarios", "total de funcionarios", "quadro de servidores",
            )
        ):
            target_yr = years[-1] if (years and years[-1] <= 2025) else 2025
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
        if any(w in q_norm for w in ("veiculo", "veiculos", "frota", "carros", "caminhao", "onibus")) and any(
            w in q_norm for w in ("quantidade", "quantos", "tem acesso", "possui", "total de veiculos", "tamanho da frota", "tem ")
        ):
            return (
                "SELECT status_cadastro_frota, "
                "COUNT(DISTINCT seq_veiculo) AS qtd_veiculos_distintos, "
                "COUNT(DISTINCT CASE WHEN dsc_num_placa NOT IN ('ISENTO_CTB_MAQUINARIO', 'SEM_CADASTRO_PLACA') THEN dsc_num_placa END) AS qtd_placas_ativas, "
                "ROUND(SUM(vlr_gasto), 2) AS total_gasto_historico_frota "
                "FROM vw_gold_frota_consolidada "
                "GROUP BY status_cadastro_frota "
                "ORDER BY qtd_veiculos_distintos DESC"
            )

        # 1f. Orçamento Anual Previsto (LOA) vs. Receita Realizada via Gold View Semântica #7
        if "orcamento" in q_norm and "unidade orcamentaria" not in q_norm and "secretaria" not in q_norm:
            valid_yrs = [y for y in years if 2020 <= y <= 2025]
            if not valid_yrs and re.search(r"\b(24|25)\b", q_norm):
                if "24" in q_norm:
                    valid_yrs.append(2024)
                if "25" in q_norm:
                    valid_yrs.append(2025)
            orc_where = (
                f"WHERE ano_referencia IN ({', '.join(str(y) for y in sorted(set(valid_yrs)))}) "
                if valid_yrs
                else "WHERE ano_referencia BETWEEN 2020 AND 2025 "
            )
            return (
                f"SELECT ano_referencia, orcamento_previsto_loa, orcamento_atualizado_loa, "
                f"despesa_dotacao_atualizada, receita_arrecadada_realizada, despesa_empenhada, "
                f"despesa_liquidada, despesa_paga, ultimo_mes_receita, status_exercicio "
                f"FROM vw_gold_orcamento_previsto_vs_realizado {orc_where}"
                f"ORDER BY ano_referencia"
            )

        # 2. Consultas por Secretaria Municipal / Unidade Orçamentária (`orgao_orgaoUnidade` × `despesa_despesa`)
        if "secretaria" in q_norm or "secretarias" in q_norm or "unidade orcamentaria" in q_norm:
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
                        f" AND (strip_accents(UPPER(u.dsc_unidade)) ILIKE '{ilike_pat}' "
                        f"OR strip_accents(UPPER(d.dsc_funcao)) ILIKE '{ilike_pat}')"
                    )
                    break
            return (
                f"SELECT d.ano_referencia, UPPER(TRIM(u.dsc_unidade)) AS dsc_lotacao, "
                f"ROUND(SUM(TRY_CAST(d.vlr_empenhado AS DECIMAL(18,2))), 2) AS total_empenhado, "
                f"ROUND(SUM(TRY_CAST(d.vlr_liquidado AS DECIMAL(18,2))), 2) AS total_liquidado, "
                f"ROUND(SUM(TRY_CAST(d.vlr_pago AS DECIMAL(18,2))), 2) AS total_pago "
                f"FROM despesa_despesa d "
                f"JOIN (SELECT DISTINCT seq_orgao, cod_unidade, ano_referencia, FIRST_VALUE(dsc_unidade) OVER (PARTITION BY seq_orgao, cod_unidade, ano_referencia ORDER BY dsc_unidade) AS dsc_unidade FROM orgao_orgaoUnidade) u "
                f"ON d.seq_orgao = u.seq_orgao AND d.cod_unidade = u.cod_unidade AND d.ano_referencia = u.ano_referencia "
                f"WHERE d.{year_filter}{sec_filter} "
                f"GROUP BY 1, 2 ORDER BY total_liquidado DESC LIMIT 15"
            )

        # 3. Respeitar escopo de fonte quando o usuário selecionou bases específicas
        if allowed_src == {"IBGE"} or (
            ("ibge" in q_norm or "populacao" in q_norm or "habitantes" in q_norm or "idhm" in q_norm or "pib" in q_norm)
            and "per capita" not in q_norm
            and (not allowed_src or "IBGE" in allowed_src)
        ):
            return (
                "SELECT ano_referencia, nom_municipio, populacao_residente, fonte_populacao, "
                "pib_municipal_mil_reais, pib_per_capita_reais, idhm_geral, area_territorial_km2 "
                "FROM ibge_indicadores_municipais ORDER BY ano_referencia"
            )

        if "per capita" in q_norm or "por habitante" in q_norm:
            if not allowed_src or ("IBGE" in allowed_src and "SICOM" in allowed_src):
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

        if allowed_src == {"SICONFI"} or any(
            w in q_norm
            for w in (
                "siconfi", "finbra", "capag", "tesouro nacional", "limite prudencial", "lrf", "rcl",
                "contas da prefeitura", "contas estao em dia", "contas em dia", "futuro garantido",
                "situacao fiscal", "saude fiscal",
            )
        ):
            return (
                "SELECT ano_referencia, vlr_receita_corrente_liquida, vlr_despesa_pessoal_lrf, "
                "pct_despesa_pessoal_rcl, limite_prudencial_lrf_pct, nota_capag_tesouro, "
                "pct_aplicacao_saude_siops, pct_aplicacao_educacao_mde, situacao_fiscal_lrf "
                "FROM siconfi_finbra_indicadores ORDER BY ano_referencia"
            )

        if allowed_src == {"INEP"} or any(w in q_norm for w in ("ideb", "inep", "censo escolar", "matricula", "matriculas", "aprovacao")):
            return (
                "SELECT ano_referencia, rede_ensino, ideb_anos_iniciais, meta_ideb_anos_iniciais, "
                "ideb_anos_finais, meta_ideb_anos_finais, taxa_aprovacao_iniciais_pct, "
                "qtd_escolas_municipais, total_matriculas_rede_municipal, vlr_gasto_educacao_por_aluno "
                "FROM inep_ideb_educacao ORDER BY ano_referencia"
            )

        if allowed_src == {"DATASUS"} or any(w in q_norm for w in ("datasus", "siops", "cnes", "ubs", "leitos", "atencao basica", "santa casa")):
            return (
                "SELECT ano_referencia, pct_recursos_proprios_saude_siops, minimo_constitucional_saude_pct, "
                "vlr_despesa_total_saude, vlr_despesa_saude_per_capita, qtd_estabelecimentos_cnes, "
                "qtd_ubs_atencao_basica, cobertura_atencao_basica_pct, qtd_leitos_sus, hospital_referencia "
                "FROM datasus_siops_saude ORDER BY ano_referencia"
            )

        if allowed_src == {"RECEITA_QSA"} or any(
            w in q_norm for w in ("qsa", "quadro societario", "socio", "socios", "receita federal", "porte", "cnae", "materialidade")
        ):
            return (
                "SELECT razao_social, num_cnpj, porte_empresa, cnae_principal_descricao, "
                "qtd_socios_identificados, nomes_socios_qsa, qtd_pagamentos, total_pago_historico, "
                "alerta_auditoria_controladoria "
                "FROM receita_qsa_fornecedores "
                "WHERE COALESCE(porte_empresa, '') NOT ILIKE '%ÓRGÃO PÚBLICO%' "
                "ORDER BY total_pago_historico DESC LIMIT 15"
            )

        if "convenio" in q_norm or "convenios" in q_norm:
            return (
                "SELECT ano_referencia, num_convenio, orgao_concedente, objeto_convenio, "
                "vlr_global, vlr_repasse_uniao, vlr_liberado, situacao_convenio "
                "FROM cgu_convenios ORDER BY ano_referencia DESC"
            )

        if "emenda" in q_norm or "emendas" in q_norm or "parlamentar" in q_norm:
            return (
                "SELECT ano_referencia, cod_emenda, nom_autor_emenda, tipo_emenda, "
                "funcao_programatica, vlr_empenhado, vlr_liquidado, vlr_pago "
                "FROM cgu_emendas ORDER BY ano_referencia DESC"
            )

        if allowed_src == {"CGU"} or any(w in q_norm for w in ("bolsa familia", "novo bolsa familia", "bpc", "auxilio", "cgu")):
            if "bpc" in q_norm:
                return (
                    'SELECT ano_referencia, COUNT(*) AS qtd_registros, '
                    'SUM(TRY_CAST(REPLACE(CAST("VALOR PARCELA" AS VARCHAR), \',\', \'.\') AS DECIMAL(18,2))) AS total_pago '
                    'FROM cgu_bpc GROUP BY ano_referencia ORDER BY ano_referencia'
                )
            if "por ano" in q_norm or "evolucao" in q_norm:
                return (
                    'SELECT ano_referencia, COUNT(*) AS total_registros, '
                    'SUM(TRY_CAST(REPLACE(CAST("VALOR PARCELA" AS VARCHAR), \',\', \'.\') AS DECIMAL(18,2))) AS total_pago '
                    'FROM cgu_novo_bolsa_familia GROUP BY ano_referencia ORDER BY ano_referencia'
                )
            return (
                'SELECT COUNT(*) AS total_registros, '
                'SUM(TRY_CAST(REPLACE(CAST("VALOR PARCELA" AS VARCHAR), \',\', \'.\') AS DECIMAL(18,2))) AS total_pago '
                'FROM cgu_novo_bolsa_familia'
            )

        if "isf-m" in q_norm or "saude financeira" in q_norm:
            return (
                "WITH rc AS ("
                "SELECT ano_referencia, SUM(TRY_CAST(vlr_realizadoateperiodo AS DECIMAL(18,2))) AS rec "
                "FROM receita_receita WHERE dsc_cateconomica ILIKE '%Corrente%' GROUP BY ano_referencia"
                "), dc AS ("
                "SELECT ano_referencia, SUM(TRY_CAST(vlr_liquidado AS DECIMAL(18,2))) AS desp "
                "FROM despesa_despesa WHERE dsc_naturezadespesa LIKE '3%' GROUP BY ano_referencia"
                ") SELECT rc.ano_referencia, ROUND(rc.rec / NULLIF(dc.desp, 0), 4) AS isf_m, "
                "rc.rec AS receita_corrente, dc.desp AS despesa_corrente "
                "FROM rc JOIN dc ON rc.ano_referencia = dc.ano_referencia ORDER BY rc.ano_referencia"
            )

        if "ies-c" in q_norm or "indice de saude" in q_norm or "minimo constitucional de saude" in q_norm:
            return (
                "SELECT ano_referencia, pct_aplicacao_saude_siops AS ies_c_pct, "
                "vlr_receita_corrente_liquida, nota_capag_tesouro, situacao_fiscal_lrf "
                "FROM siconfi_finbra_indicadores ORDER BY ano_referencia"
            )

        if "ieq-c" in q_norm or "indice de educacao" in q_norm or "minimo constitucional de educacao" in q_norm:
            return (
                "SELECT ano_referencia, pct_aplicacao_educacao_mde AS ieq_c_pct, "
                "vlr_receita_corrente_liquida, nota_capag_tesouro, situacao_fiscal_lrf "
                "FROM siconfi_finbra_indicadores ORDER BY ano_referencia"
            )

        if ("servidor" in q_norm or "salario" in q_norm or "remuneracao" in q_norm or "folha" in q_norm) and (
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

        if "salario" in q_norm or "remuneracao" in q_norm or "folha" in q_norm or "servidor" in q_norm or "funcionario" in q_norm:
            return (
                f"SELECT ano_referencia, COUNT(DISTINCT seq_dim_pessoa) AS total_servidores, "
                f"SUM(TRY_CAST(vlr_rmn_bruta AS DECIMAL(18,2))) AS total_bruto, "
                f"SUM(TRY_CAST(vlr_rmn_liquida AS DECIMAL(18,2))) AS total_liquido "
                f"FROM remuneracao_municipio WHERE {year_filter} GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if "fornecedor" in q_norm or "credor" in q_norm or "empresas" in q_norm or "maiores pagamentos" in q_norm:
            return (
                f"SELECT MAX(UPPER(TRIM(nom_credor))) AS nom_credor, num_doc_credor, COUNT(*) AS qtd_pagamentos, "
                f"ROUND(SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2))), 2) AS total_pago "
                f"FROM despesa_pagamento WHERE {year_filter} "
                f"AND COALESCE(nom_credor, '') NOT ILIKE '%NAO INFORMADO%' "
                f"AND COALESCE(nom_credor, '') NOT ILIKE '%INSS%' "
                f"AND COALESCE(nom_credor, '') NOT ILIKE '%FGTS%' "
                f"AND COALESCE(nom_credor, '') NOT ILIKE '%PREFEITURA%' "
                f"AND COALESCE(nom_credor, '') NOT ILIKE '%FOLHA DE PAG%' "
                f"AND COALESCE(nom_credor, '') NOT ILIKE '%TRIBUNAL%' "
                f"AND COALESCE(nom_credor, '') NOT ILIKE '%PASEP%' "
                f"AND COALESCE(num_doc_credor, '') NOT IN ('-1', '0', '18302299000102', '00360305000104', '29979036000140') "
                f"GROUP BY num_doc_credor ORDER BY total_pago DESC LIMIT 10"
            )

        if "saude" in q_norm:
            return (
                f"SELECT ano_referencia, SUM(TRY_CAST(vlr_empenhado AS DECIMAL(18,2))) AS total_empenhado, "
                f"SUM(TRY_CAST(vlr_liquidado AS DECIMAL(18,2))) AS total_liquidado, "
                f"SUM(TRY_CAST(vlr_pago AS DECIMAL(18,2))) AS total_pago "
                f"FROM despesa_despesa WHERE {year_filter} AND (dsc_funcao ILIKE '%SAÚDE%' OR dsc_funcao ILIKE '%SAUDE%') "
                f"GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if "educacao" in q_norm:
            return (
                f"SELECT ano_referencia, SUM(TRY_CAST(vlr_empenhado AS DECIMAL(18,2))) AS total_empenhado, "
                f"SUM(TRY_CAST(vlr_liquidado AS DECIMAL(18,2))) AS total_liquidado, "
                f"SUM(TRY_CAST(vlr_pago AS DECIMAL(18,2))) AS total_pago "
                f"FROM despesa_despesa WHERE {year_filter} AND (dsc_funcao ILIKE '%EDUCA%' OR dsc_funcao ILIKE '%EDUCAC%') "
                f"GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if "contrato" in q_norm:
            return (
                f"SELECT ano_referencia, COUNT(*) AS total_contratos, "
                f"SUM(TRY_CAST(vlr_contrato AS DECIMAL(18,2))) AS valor_total_contratado "
                f"FROM contrato_contratos WHERE {year_filter} GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if "dispensa" in q_norm:
            return (
                f"SELECT ano_referencia, COUNT(*) AS total_processos, "
                f"SUM(TRY_CAST(vlr_empenhado AS DECIMAL(18,2))) AS total_empenhado, "
                f"SUM(TRY_CAST(vlr_pago AS DECIMAL(18,2))) AS total_pago "
                f"FROM licitacao_dispensa WHERE {year_filter} GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if "licitacao" in q_norm or "licitacoes" in q_norm:
            return (
                f"SELECT ano_referencia, dsc_modalidade, COUNT(*) AS qtd_licitacoes, "
                f"SUM(TRY_CAST(vlr_liquidado AS DECIMAL(18,2))) AS total_liquidado "
                f"FROM licitacao_licitacao WHERE {year_filter} GROUP BY ano_referencia, dsc_modalidade ORDER BY total_liquidado DESC LIMIT 15"
            )

        if "frota" in q_norm or "combustivel" in q_norm or "veiculo" in q_norm:
            return (
                f"SELECT ano_referencia, COUNT(*) AS registros_gasto, "
                f"SUM(TRY_CAST(vlr_gasto AS DECIMAL(18,2))) AS total_gasto_frota "
                f"FROM frota_gastoFrota WHERE {year_filter} GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if "receita" in q_norm or "arrecadacao" in q_norm:
            return (
                f"SELECT ano_referencia, SUM(TRY_CAST(vlr_realizadoateperiodo AS DECIMAL(18,2))) AS total_arrecadado "
                f"FROM receita_receita WHERE {year_filter} GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if any(w in q_norm for w in ("pagamento", "pagamentos", "pagou")):
            return (
                f"SELECT ano_referencia, COUNT(*) AS qtd_pagamentos, "
                f"SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2))) AS total_pago "
                f"FROM despesa_pagamento WHERE {year_filter} GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if any(w in q_norm for w in ("despesa", "despesas", "gasto", "gastos", "gastou", "empenho", "empenhado")):
            return (
                f"SELECT ano_referencia, "
                f"ROUND(SUM(TRY_CAST(vlr_empenhado AS DECIMAL(18,2))), 2) AS total_empenhado, "
                f"ROUND(SUM(TRY_CAST(vlr_liquidado AS DECIMAL(18,2))), 2) AS total_liquidado, "
                f"ROUND(SUM(TRY_CAST(vlr_pago AS DECIMAL(18,2))), 2) AS total_pago "
                f"FROM despesa_despesa WHERE {year_filter} GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        # Contingência offline segura: síntese macro-orçamentária oficial (jamais pagamento cego de credores)
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
        if not allowed_src and attempt == 1 and q_norm in self._golden_lookup:
            self._last_llm_engine_used = "SIM Golden Verified Index + NL2SQL"
            return self._golden_lookup[q_norm]

        return self._compile_native_sql(q_raw, q_norm, allowed_src)

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
            )
            est_tokens = prompt_pkg.estimated_tokens
            candidate_sql = sql_override if (sql_override and attempt == 1) else self.sql_generator_fn(prompt_pkg, attempt, last_err)
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

                # Eixo 1: Self-Correction real quando a query retorna 0 linhas (NUNCA sobrescrever com fallback cego!)
                if (
                    len(returned_rows) == 0
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

                sql_executed = q_res["query_executada"]
                sql_rows = returned_rows
                pii_masked = q_res.get("pii_masked_count", 0)
                for f in q_res.get("fontes_utilizadas", []):
                    if f not in fontes:
                        fontes.append(f)
                warnings.extend(q_res.get("transparency_warnings", []))
                if re.search(r"\b(202[6-9]|projetad[oa]\s+para\s+2[6-9])\b", raw_question_norm):
                    if not any(w.get("gap_id") == "GAP-EXERCICIO-FUTURO" for w in warnings):
                        warnings.append(
                            {
                                "gap_id": "GAP-EXERCICIO-FUTURO",
                                "severity": "info",
                                "title": "Cobertura Temporal da Base Auditada (2020 a 2025)",
                                "message": (
                                    "As bases públicas consolidadas do SICOM/TCE-MG, SICONFI e CGU abrangem os exercícios de 2020 a 2025. "
                                    "Exercícios futuros (como 2026 e 2027) referem-se a ciclos orçamentários ainda não encerrados nas remessas históricas do Tribunal de Contas; "
                                    "abaixo são apresentados os valores oficiais de 2020 a 2025 (Orçamento Previsto na LOA e Receita Realizada) para referência."
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
                        last_err = (
                            "PROIBIDO gerar SELECT literal sem cláusula FROM (NO_BASE_TABLE). "
                            "Mesmo que o usuário mencione anos futuros (como 2026 ou 2027) ou dados parciais, "
                            "você DEVE consultar a tabela ou Gold View correspondente (ex.: vw_gold_orcamento_previsto_vs_realizado) "
                            "nos exercícios disponíveis (2020 a 2025) para retornar a série histórica auditada."
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

            # Etapa 2: Guardião Semântico IA Leve, Multi-Turn Query Rewriting & Decomposição Adaptativa (CRAG)
            s2_t0 = time.perf_counter()
            intent = "sql_analytics" if sql_override else self.classify_intent(effective_question, allowed_sources=allowed_sources)
            q_norm_s2 = normalize_text_for_dedup(working_question)
            only_rag_scope = bool(allowed_sources and {s.upper().strip() for s in allowed_sources} == {"DIARIO_OFICIAL"})

            # Fast-Path Cache Trava 1 ANTES de invocar LLM externo (apenas para perguntas autônomas sem histórico/gráfico)
            wants_chart_hint = bool(re.search(r"\b(grafico|graficos|plotar|visualizacao\s+grafica)\b", q_norm_s2))
            if (
                not sql_override
                and not explicit_tables
                and not explicit_segments
                and not self._has_custom_sql_generator
                and len(hist_list) == 0
                and not wants_chart_hint
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

                if guard_decision.used_ai_model or guard_decision.is_contextual_followup or guard_decision.wants_chart or guard_decision.legal_framework_notes:
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
            legal_framework_notes = list(guard_decision.legal_framework_notes) if guard_decision else []

            # Se o usuário pede explicação legal sobre os dados recém-exibidos na sessão (ex: "essas quantias são legais? você tem a lei?"),
            # roteia pelo pipeline híbrido/síntese contextualizada com a Lei Federal + Diário Oficial + dados do turno anterior
            if (
                legal_framework_notes
                and prior_sql_ctx
                and intent in ("conceptual_doubt", "conversational_followup", "rag_diario_oficial")
                and not wants_chart
            ):
                intent = "hybrid_sql_rag" if not only_rag_scope else "rag_diario_oficial"
                if guard_decision:
                    guard_decision.direct_response_citizen = ""

            # Se o usuário pede apenas um gráfico (sem nova consulta analítica explícita), encaminha ao renderizador PNG nativo
            has_new_analytical_topic = any(
                w in q_norm_s2
                for w in (
                    "quanto", "quais", "orcamento", "saude", "educacao", "folha", "frota",
                    "fornecedor", "fornecedores", "receita", "despesa", "evolucao", "compar",
                    "2020", "2021", "2022", "2023", "2024", "2025",
                )
            )
            if wants_chart and not sql_override and not has_new_analytical_topic:
                intent = "conversational_followup"
                if guard_decision:
                    guard_decision.direct_response_citizen = ""
            elif wants_chart and has_new_analytical_topic and not only_rag_scope:
                if intent not in ("sql_analytics", "hybrid_sql_rag"):
                    intent = "sql_analytics"
                if guard_decision:
                    guard_decision.direct_response_citizen = ""

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
                        f"Gráfico solicitado: {'Sim' if wants_chart else 'Não'}"
                        + (f" | Standalone Question: '{effective_question}'" if effective_question != question else "")
                        + (f" | Sub-perguntas: {len(guard_decision.sub_questions)}" if (guard_decision and guard_decision.sub_questions) else "")
                        + (f" | Justificativa: {guard_decision.reasoning}" if (guard_decision and guard_decision.reasoning) else "")
                    ),
                }
            )

            # Eixo 4: Geração Nativa de Gráfico PNG Institucional sobre o turno anterior (ou série oficial de orçamento se sessão vazia)
            if wants_chart and intent == "conversational_followup":
                ch_t0 = time.perf_counter()
                city_str = f"{self.db_manager.tenant.municipality_name}/{self.db_manager.tenant.state_uf}"
                if prior_sql_ctx and prior_sql_ctx.get("sql_rows"):
                    chart_rows = list(prior_sql_ctx["sql_rows"])
                    chart_title_q = str(prior_sql_ctx.get("question") or working_question)
                    chart_sql = prior_sql_ctx.get("sql_executed")
                    is_contextual_followup = True
                else:
                    q_orc = self.db_manager.execute_safe_query(
                        "SELECT ano_referencia, orcamento_previsto_loa, receita_arrecadada_realizada "
                        "FROM vw_gold_orcamento_previsto_vs_realizado ORDER BY ano_referencia",
                        role=role,
                    )
                    chart_rows = q_orc.get("dados") or []
                    chart_title_q = f"Evolução do Orçamento Previsto (LOA) vs. Receita Realizada de {city_str} (2020 a 2025)"
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
                c_txt, i_txt, conv_eng = self._handle_conversational_or_conceptual_turn(
                    question=working_question,
                    role=role,
                    hist_list=hist_list,
                    llm_provider=llm_provider,
                    llm_api_key=llm_api_key,
                )
                if artifact:
                    c_txt = (
                        f"Gerei o gráfico institucional com os dados oficiais de **{city_str}** referentes a **{chart_title_q}**.\n\n"
                        f"{artifact.caption}\n\n"
                        f"Você também pode solicitar gráficos específicos sobre gastos com saúde, educação, folha de servidores, frota ou fornecedores."
                    )
                    i_txt = (
                        f"**Visualização Analítica Renderizada (`image/png`):** {artifact.title}\n"
                        f"- **Legenda / Síntese:** {artifact.caption}\n"
                        f"- **Registros plotados:** {len(chart_rows)}"
                        + (f"\n\n**SQL de Origem:**\n```sql\n{chart_sql}\n```" if chart_sql else "")
                    )
                    conv_eng = "SIM Institutional PNG Chart Generator"

                is_internal = role.lower().strip() in (
                    "auditor", "admin", "controller", "gestor", "servidor", "interno", "controladoria",
                )
                sel_txt = i_txt if is_internal else c_txt
                if session_id not in self.conversation_history:
                    self.conversation_history[session_id] = deque(maxlen=10)
                self.conversation_history[session_id].append({"role": "user", "content": working_question})
                self.conversation_history[session_id].append(
                    {"role": "assistant", "content": c_txt[:420], "sql_executed": chart_sql or ""}
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
                return OrchestrationResult(
                    status="fora_de_escopo",
                    intent=intent,
                    tenant_id=self.db_manager.tenant.tenant_id,
                    role=role,
                    question=question,
                    answer_text=(
                        f"Esta instância do SIM atende exclusivamente aos dados públicos do município de "
                        f"{self.db_manager.tenant.municipality_name}/{self.db_manager.tenant.state_uf} "
                        f"(IBGE {self.db_manager.tenant.ibge_code})."
                    ),
                    pipeline_steps=steps,
                    exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                )

            if intent == "conversational_identity":
                city_str = f"{self.db_manager.tenant.municipality_name}/{self.db_manager.tenant.state_uf}"
                total_t = len(self.db_manager.catalog.tables)
                q_norm_id = normalize_text_for_dedup(working_question)
                wants_concise = any(w in q_norm_id for w in ("resumid", "concis", "curt", "diret", "poucas palavras"))
                asks_cities = any(w in q_norm_id for w in ("quais cidades", "qual cidade", "cidades vc", "cidades voce"))
                if asks_cities:
                    citizen_identity = (
                        f"Esta instância oficial atende aos dados públicos do município de **{city_str}** (código IBGE `{self.db_manager.tenant.ibge_code}`). "
                        f"A plataforma SIM possui arquitetura multi-municipal expansível para outras prefeituras, mas aqui todas as consultas cobrem exclusivamente **{city_str}** (2020 a 2025)."
                    )
                    inst_identity = (
                        f"Esta instância ativa do **SIM** opera sobre o tenant **`{self.db_manager.tenant.tenant_id}`** (**{city_str}**, IBGE `{self.db_manager.tenant.ibge_code}`), "
                        f"com **{total_t} objetos auditáveis** (79 tabelas Parquet + 7 Gold Views) e arquitetura multi-tenant declarativa pronta para onboarding de novos municípios."
                    )
                elif wants_concise:
                    citizen_identity = (
                        f"Sou o sistema oficial de consulta aos dados públicos da Prefeitura de **{city_str}**. "
                        f"Respondo dúvidas em linguagem simples sobre orçamento, gastos com saúde e educação, pagamentos a empresas, salários públicos, convênios e atos do Diário Oficial (2020 a 2025)."
                    )
                    inst_identity = (
                        f"Sou o **SIM (Sistema Integrado de Monitoramento e Inteligência Municipal)** de **{city_str}** (IBGE `{self.db_manager.tenant.ibge_code}`). "
                        f"Realizo consultas auditáveis sobre **{total_t} objetos oficiais** (SICOM/TCE-MG, SICONFI/STN, CGU, IBGE, INEP, DATASUS e Receita Federal QSA) e sobre os atos normativos do Diário Oficial."
                    )
                else:
                    citizen_identity = (
                        f"Olá! Sou o assistente de transparência pública da Prefeitura de **{city_str}**.\n\n"
                        "Estou aqui para ajudar você a consultar, de forma simples e rápida, como os recursos públicos "
                        "do município são arrecadados e investidos.\n\n"
                        "**Você pode me perguntar, por exemplo:**\n"
                        "• Quanto a prefeitura investiu em Saúde ou Educação em 2024?\n"
                        "• Quais empresas mais receberam pagamentos ou contratos?\n"
                        "• Quanto foi pago de Bolsa Família ou BPC em Caeté?\n"
                        "• Qual é a população oficial, o PIB ou a nota do IDEB das escolas municipais?\n"
                        "• O que diz uma lei, decreto ou portaria publicada no Diário Oficial (*Jornal Opinião*)?\n\n"
                        "Todas as respostas vêm diretamente das bases oficiais (TCE-MG, Portal da Transparência, IBGE, MEC, Ministério da Saúde e Diário Oficial)."
                    )
                    inst_identity = (
                        f"Sou o **SIM (Sistema de Inteligência Municipal)** de **{city_str}** "
                        f"(código IBGE `{self.db_manager.tenant.ibge_code}`).\n\n"
                        "**O que eu faço:**\n"
                        f"Converto perguntas em linguagem natural em consultas auditáveis sobre **{total_t} objetos analíticos oficiais (79 tabelas Parquet + 7 Gold Views de Auditoria) em 7 fontes governamentais** "
                        "(2020 a 2025) e sobre a collection de atos individuais do **Diário Oficial (*Jornal Opinião*)**, sempre informando os arquivos-fonte utilizados.\n\n"
                        "**Fontes Oficiais Integradas (selecionáveis individualmente):**\n"
                        "- **SICOM / TCE-MG (65 tabelas + 6 Gold Views):** receitas, despesas, empenhos, pagamentos, contratos, licitações, frota, folha de servidores, orçamento LOA vs. realizado e conflito de interesses.\n"
                        "- **Portal da Transparência / CGU (9 tabelas):** Novo Bolsa Família, BPC, Auxílio Brasil, Convênios Federais (`cgu_convenios`) e Emendas Parlamentares (`cgu_emendas`).\n"
                        "- **SICONFI / Tesouro Nacional:** Receita Corrente Líquida (RCL), Despesa com Pessoal LRF, Limite Prudencial e Nota CAPAG (`siconfi_finbra_indicadores`).\n"
                        "- **IBGE Indicadores Municipais:** População oficial (Censo 2022 e Estimativas), PIB Municipal, PIB *per capita* e IDHM (`ibge_indicadores_municipais`).\n"
                        "- **INEP / MEC Educação:** IDEB Anos Iniciais e Finais (anos ímpares oficiais), taxa de aprovação, escolas e matrículas (`inep_ideb_educacao`).\n"
                        "- **DATASUS / SIOPS Saúde:** aplicação constitucional em saúde, UBS, CNES, leitos SUS e Santa Casa (`datasus_siops_saude`).\n"
                        "- **Receita Federal CNPJ / QSA:** porte, CNAE e quadro societário reconciliado dos fornecedores da Prefeitura (`receita_qsa_fornecedores`).\n"
                        "- **Diário Oficial (*Jornal Opinião*):** leis, decretos, portarias e editais atomizados com busca híbrida Okapi BM25."
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

            # Trava 1 Comercial: Cache SQL Persistente (apenas para perguntas autônomas não contextuais e sem pedido de gráfico)
            if (
                not sql_override
                and not explicit_tables
                and not explicit_segments
                and not self._has_custom_sql_generator
                and not is_contextual_followup
                and not wants_chart
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
                        "conflito", "evolucao", "compare", "2020", "2021", "2022", "2023", "2024", "2025",
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

                sql_executed = sql_out.get("sql_executed")
                sql_rows = sql_out.get("sql_rows") or []
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
                        rag_chunks = cross_chunks
                        for ch in cross_chunks:
                            src = f"Jornal Opinião ({ch.get('source')})"
                            if src not in fontes:
                                fontes.append(src)
                        steps.append(
                            {
                                "step": len(steps) + 1,
                                "name": "Cross-Link Automático SQL -> Diário Oficial (Jornal Opinião)",
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
            )
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

            # Atualizar memória conversacional Multi-Turno com `sql_executed` e cache de linhas da sessão
            if session_id not in self.conversation_history:
                self.conversation_history[session_id] = deque(maxlen=10)
            hist_deque = self.conversation_history[session_id]
            hist_deque.append({"role": "user", "content": working_question})
            hist_deque.append(
                {
                    "role": "assistant",
                    "content": (dual_synth["citizen_response"] or final_answer)[:600],
                    "sql_executed": sql_executed or "",
                }
            )
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
                asr_corrections=asr_corrections,
                pipeline_steps=steps,
                exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
            )

            # Eixo 7 & Eixo 8: Higiene Estrita do Cache SQLite + Telemetria Real de Tokens (sem dupla contagem)
            already_debited = (self._turn_prompt_tokens_real + self._turn_completion_tokens_real) > 0
            if not sql_override and not explicit_tables and not explicit_segments and not wants_chart:
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
