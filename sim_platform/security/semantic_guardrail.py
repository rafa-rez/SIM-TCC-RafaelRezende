"""
Guardião Semântico, Reformulador Contextual Multi-Turno e Roteador Adaptativo CRAG (`SemanticIntentGuardrail`).

Referências Arquiteturais:
- Yan et al. (2024) — *Corrective Retrieval Augmented Generation (CRAG)*
- Jeong et al. (NAACL 2024) — *Adaptive-RAG: Learning to Adapt Retrieval-Augmented LLMs through Question Complexity*
- Ma et al. (EMNLP 2023) — *Query Rewriting for Retrieval-Augmented Large Language Models*
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import unicodedata
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable, Dict, List, Optional, Tuple

from sim_platform.security.sql_guard import PromptSafetyResult, check_prompt_safety


_EMOJI_STRIP_RE = re.compile(
    "["
    "\U0001F300-\U0001F9FF"
    "\U0001FA00-\U0001FAFF"
    "\u2600-\u26FF"
    "\u2700-\u27BF"
    "\uFE0F"
    "]+",
    flags=re.UNICODE,
)


def strip_emojis(text: str) -> str:
    return _EMOJI_STRIP_RE.sub("", text or "").strip()


def _norm_ascii(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", (text or "").strip().lower())
    return re.sub(r"\s+", " ", "".join(c for c in nfkd if not unicodedata.combining(c)))


# Base normativa federal estruturada para consultas híbridas (Dados + Lei)
FEDERAL_LEGAL_FRAMEWORKS: List[Tuple[Tuple[str, ...], str]] = [
    (
        ("carona", "adesao", "ata de registro", "registro de precos", "arp"),
        "Lei Federal nº 14.133/2021 (Nova Lei de Licitações), Art. 86, §§ 2º a 5º (com redação da Lei nº 14.770/2023) — "
        "Regras e Limites Legais de Adesão à Ata de Registro de Preços ('Carona'): "
        "(1) Exige justificativa formal de vantajosidade econômica, compatibilidade de preços com o mercado e anuência prévia do órgão gerenciador e do fornecedor (Art. 86, § 2º); "
        "(2) Limite Individual por Órgão: cada adesão não pode exceder 50% (cinquenta por cento) dos quantitativos dos itens do instrumento convocatório registrados na ata (Art. 86, § 4º); "
        "(3) Limite Global de Adesões: o quantitativo decorrente de todas as adesões não pode exceder, na totalidade, ao dobro (200%) do quantitativo de cada item registrado na ata (Art. 86, § 5º); "
        "(4) Municípios podem aderir a atas federais, estaduais, distritais ou de outros municípios (desde que oriundas de licitação competitiva, Art. 86, § 3º, II). O pagamento de valores elevados via carona não é ilegal por si só, mas exige auditoria sobre o respeito aos tetos de 50%/200%, pesquisa de preços e existência do decreto municipal regulamentador.",
    ),
    (
        ("dispensa", "fracionamento", "compra direta", "inexigibilidade"),
        "Lei Federal nº 14.133/2021, Arts. 74 e 75 — Contratação Direta (Inexigibilidade e Dispensa de Licitação): "
        "O Art. 75, I e II fixa tetos anuais atualizados por Decreto Federal para dispensa por valor, e o § 1º proíbe o fracionamento indevido de despesa (soma dos gastos no exercício financeiro pela mesma unidade gestora com objetos de mesma natureza/ramo de atividade).",
    ),
    (
        ("aditivo", "termo aditivo", "acrescimo contratual", "25%"),
        "Lei Federal nº 14.133/2021, Arts. 124 e 125 — Alterações Contratuais e Limites de Aditivos: "
        "O contratado é obrigado a aceitar acréscimos ou supressões de até 25% do valor inicial atualizado do contrato para obras, serviços ou compras, e até 50% no caso de reforma de edifício ou de equipamento.",
    ),
    (
        ("conflito de interesse", "servidor socio", "quadro societario", "parente"),
        "Lei Federal nº 14.133/2021, Art. 9º, § 1º e Art. 14, IV — Impedimentos e Conflito de Interesses: "
        "Não pode disputar licitação nem participar da execução de contrato, direta ou indiretamente, agente público do órgão ou entidade contratante ou empresa da qual seja sócio, dirigente ou responsável técnico, devendo ser observada também a Súmula Vinculante nº 13 do STF (nepotismo).",
    ),
    (
        ("lrf", "responsabilidade fiscal", "limite prudencial", "despesa com pessoal", "contas da prefeitura", "contas estao em dia"),
        "Lei Complementar nº 101/2000 (Lei de Responsabilidade Fiscal — LRF), Arts. 19, 20 e 22: "
        "Para municípios, o limite máximo da Despesa Total com Pessoal é de 60% da Receita Corrente Líquida (RCL), sendo 54% para o Poder Executivo (limite de alerta TCE: 48,6%; limite prudencial: 51,3%) e 6% para o Poder Legislativo.",
    ),
    (
        ("minimo constitucional", "saude", "educacao", "mde", "asps", "siops", "25%", "15%"),
        "Constituição Federal de 1988, Art. 212 (aplicação mínima de 25% da receita resultante de impostos e transferências em Manutenção e Desenvolvimento do Ensino — MDE) e Art. 198, § 2º, III c/c LC nº 141/2012 (aplicação mínima de 15% em Ações e Serviços Públicos de Saúde — ASPS).",
    ),
]


def extract_statutory_legal_notes(text: str) -> List[str]:
    norm = _norm_ascii(text)
    notes: List[str] = []
    for triggers, legal_text in FEDERAL_LEGAL_FRAMEWORKS:
        if any(tr in norm for tr in triggers):
            notes.append(legal_text)
    return notes


@dataclass
class SemanticGuardrailDecision:
    makes_sense: bool
    is_safe: bool
    intent: str
    standalone_question: str
    requires_sql: bool = False
    requires_rag: bool = False
    sub_questions: List[str] = field(default_factory=list)
    is_contextual_followup: bool = False
    wants_chart: bool = False
    legal_framework_notes: List[str] = field(default_factory=list)
    direct_response_citizen: str = ""
    direct_response_institutional: str = ""
    reasoning: str = ""
    engine_used: str = "SIM Lightweight Semantic Guardrail"
    tokens_used: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    latency_ms: float = 0.0
    used_ai_model: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "makes_sense": self.makes_sense,
            "is_safe": self.is_safe,
            "intent": self.intent,
            "standalone_question": self.standalone_question,
            "requires_sql": self.requires_sql,
            "requires_rag": self.requires_rag,
            "sub_questions": self.sub_questions,
            "is_contextual_followup": self.is_contextual_followup,
            "wants_chart": self.wants_chart,
            "legal_framework_notes": self.legal_framework_notes,
            "direct_response_citizen": self.direct_response_citizen,
            "direct_response_institutional": self.direct_response_institutional,
            "reasoning": self.reasoning,
            "engine_used": self.engine_used,
            "tokens_used": self.tokens_used,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "latency_ms": self.latency_ms,
            "used_ai_model": self.used_ai_model,
        }


class SemanticIntentGuardrail:
    """
    Avaliador semântico leve que roda ANTES do pipeline analítico principal:
    - Bloqueia exploits determinísticos em <0.1ms (Camada 0);
    - Recebe os últimos 3 turnos (incluindo `sql_executed` e resumo da resposta) e gera obrigatoriamente
      `standalone_question` (Query Rewriting multi-turno);
    - Detecta perguntas compostas (`sub_questions`), intenção híbrida (`requires_sql` + `requires_rag`)
      e solicitações de geração visual de gráfico (`wants_chart`).
    """

    VALID_INTENTS = {
        "sql_analytics",
        "rag_diario_oficial",
        "hybrid_sql_rag",
        "conversational_identity",
        "conversational_followup",
        "conceptual_doubt",
        "clarification_needed",
        "out_of_scope_municipality",
        "out_of_scope_general",
        "blocked_security",
    }

    def __init__(
        self,
        municipality_name: str = "Caeté",
        state_uf: str = "MG",
        ibge_code: str = "3110004",
        fallback_classifier_fn: Optional[Callable[[str, Optional[List[str]]], str]] = None,
        fallback_conversational_fn: Optional[Callable[..., Any]] = None,
        max_cache_size: int = 512,
        max_data_year: int = 2025,
        latest_remittance_human: str = "agosto de 2025 (última remessa consolidada no SICOM/TCE-MG)",
    ) -> None:
        self.municipality_name = municipality_name
        self.state_uf = state_uf
        self.ibge_code = ibge_code
        self.fallback_classifier_fn = fallback_classifier_fn
        self.fallback_conversational_fn = fallback_conversational_fn
        self.max_cache_size = max_cache_size
        self.max_data_year = max_data_year
        self.latest_remittance_human = latest_remittance_human
        self._decision_cache: OrderedDict[str, SemanticGuardrailDecision] = OrderedDict()

    def _cache_key(
        self,
        question: str,
        recent_history: List[Dict[str, Any]],
        allowed_sources: Optional[List[str]],
    ) -> str:
        last_turn = ""
        if recent_history:
            last_turn = "|".join(
                f"{m.get('role')}:{str(m.get('content', ''))[:160]}:{str(m.get('sql_executed', ''))[:100]}"
                for m in recent_history[-3:]
            )
        srcs = ",".join(sorted(s.upper() for s in (allowed_sources or [])))
        raw = f"{question.strip().lower()}||{last_turn}||{srcs}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    @staticmethod
    def _detect_contextual_and_chart_signals(
        question: str,
        recent_history: List[Dict[str, Any]],
    ) -> Tuple[bool, bool]:
        q_norm = _norm_ascii(question)
        wants_chart = any(
            kw in q_norm
            for kw in (
                "grafico",
                "gráfico",
                "plotar",
                "visualizacao",
                "imagem dos dados",
                "desenhe um grafico",
                "gerar grafico",
                "gera um grafico",
            )
        )
        anaphora_markers = (
            "deste total",
            "desse total",
            "desses ",
            "dessas ",
            "dessa informacao",
            "dessa resposta",
            "esses valores",
            "essas quantias",
            "esses dados",
            "disso",
            "resposta anterior",
            "pergunta anterior",
            "parece que voce confundiu",
            "e em 202",
            "e no ano",
            "e quanto a",
            "gerar um grafico",
            "gera um grafico",
        )
        is_followup = bool(recent_history) and (
            any(m in q_norm for m in anaphora_markers)
            or (wants_chart and len(q_norm.split()) <= 10)
            or len(q_norm.split()) <= 5
        )
        return is_followup, wants_chart

    def _heuristic_rewrite_standalone(
        self,
        question: str,
        recent_history: List[Dict[str, Any]],
    ) -> str:
        if not recent_history:
            return question
        is_followup, wants_chart = self._detect_contextual_and_chart_signals(question, recent_history)
        if not is_followup:
            return question

        prev_user_qs = [
            str(m.get("content") or "").strip()
            for m in recent_history
            if m.get("role") == "user" and str(m.get("content") or "").strip()
        ]
        if not prev_user_qs:
            return question

        last_topic = prev_user_qs[-1]
        # Se a última pergunta do usuário também era pedido curto ("Consegue gerar um gráfico?"), recuar mais 1 turno
        if len(prev_user_qs) >= 2 and "grafico" in _norm_ascii(last_topic):
            last_topic = prev_user_qs[-2]

        q_norm = _norm_ascii(question)
        if wants_chart and len(q_norm.split()) <= 12:
            return f"Gerar gráfico referente a: {last_topic} ({question})"

        return f"{question} (Contexto da consulta imediatamente anterior em {self.municipality_name}/{self.state_uf}: {last_topic})"

    def evaluate(
        self,
        question: str,
        recent_history: Optional[List[Dict[str, Any]]] = None,
        role: str = "citizen",
        allowed_sources: Optional[List[str]] = None,
        llm_provider: Optional[str] = None,
        llm_api_key: Optional[str] = None,
        enable_external_ai: bool = True,
    ) -> SemanticGuardrailDecision:
        t0 = time.perf_counter()
        hist = list(recent_history or [])

        # 1. Camada 0: Verificação imediata de segurança determinística (Prompt Injection / DDL / Path Traversal)
        safety: PromptSafetyResult = check_prompt_safety(question)
        if not safety.is_safe:
            return SemanticGuardrailDecision(
                makes_sense=False,
                is_safe=False,
                intent="blocked_security",
                standalone_question=question,
                direct_response_citizen=(
                    "Solicitação bloqueada pelas políticas de segurança da plataforma SIM "
                    "(tentativa de manipulação de instruções ou acesso não autorizado a recursos de sistema)."
                ),
                direct_response_institutional=(
                    f"[BLOQUEIO DE SEGURANÇA] Solicitação interceptada pelo Guardião de Entrada. "
                    f"Regras acionadas: {', '.join(safety.triggered_rules)}."
                ),
                reasoning=f"Bloqueio determinístico anti-exploit: {', '.join(safety.triggered_rules)}",
                engine_used="SIM Deterministic Security Shield (0 tokens)",
                tokens_used=0,
                prompt_tokens=0,
                completion_tokens=0,
                latency_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                used_ai_model=False,
            )

        # 2. Verificar Cache LRU de decisões do Guardião Semântico
        ckey = self._cache_key(question, hist, allowed_sources)
        if ckey in self._decision_cache:
            cached = self._decision_cache[ckey]
            self._decision_cache.move_to_end(ckey)
            return SemanticGuardrailDecision(
                makes_sense=cached.makes_sense,
                is_safe=cached.is_safe,
                intent=cached.intent,
                standalone_question=cached.standalone_question,
                requires_sql=cached.requires_sql,
                requires_rag=cached.requires_rag,
                sub_questions=list(cached.sub_questions),
                is_contextual_followup=cached.is_contextual_followup,
                wants_chart=cached.wants_chart,
                legal_framework_notes=list(cached.legal_framework_notes),
                direct_response_citizen=cached.direct_response_citizen,
                direct_response_institutional=cached.direct_response_institutional,
                reasoning=cached.reasoning,
                engine_used=f"{cached.engine_used} (Cache L1)",
                tokens_used=0,
                prompt_tokens=0,
                completion_tokens=0,
                latency_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                used_ai_model=cached.used_ai_model,
            )

        # 3. Se o filtro de escopo na UI restringiu explicitamente a apenas DIARIO_OFICIAL, respeitar
        if allowed_sources:
            norm_s = {s.upper().strip() for s in allowed_sources}
            if norm_s == {"DIARIO_OFICIAL"}:
                rewritten = self._heuristic_rewrite_standalone(question, hist)
                return SemanticGuardrailDecision(
                    makes_sense=True,
                    is_safe=True,
                    intent="rag_diario_oficial",
                    standalone_question=rewritten,
                    requires_sql=False,
                    requires_rag=True,
                    legal_framework_notes=extract_statutory_legal_notes(rewritten),
                    reasoning="Escopo de fontes restrito exclusivamente ao Diário Oficial.",
                    engine_used="SIM Scope Filter Guard",
                    tokens_used=0,
                    latency_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                    used_ai_model=False,
                )

        # 4. Camada 1: Classificador Semântico, Query Rewriter & Decompositor IA Leve
        if enable_external_ai:
            ai_decision = self._call_lightweight_ai_guardrail(
                question=question,
                recent_history=hist,
                role=role,
                allowed_sources=allowed_sources,
                llm_provider=llm_provider,
                llm_api_key=llm_api_key,
                t0=t0,
            )
            if ai_decision is not None:
                if allowed_sources:
                    norm_s = {s.upper().strip() for s in allowed_sources}
                    if "DIARIO_OFICIAL" not in norm_s and ai_decision.intent in ("rag_diario_oficial", "hybrid_sql_rag"):
                        ai_decision.intent = "sql_analytics"
                        ai_decision.requires_rag = False
                        ai_decision.requires_sql = True
                self._store_cache(ckey, ai_decision)
                return ai_decision

        # 5. Fallback Heurístico Semântico (0 tokens — garante funcionamento offline e em suíte pytest)
        fallback_decision = self._evaluate_heuristic_fallback(
            question=question,
            recent_history=hist,
            role=role,
            allowed_sources=allowed_sources,
            t0=t0,
        )
        self._store_cache(ckey, fallback_decision)
        return fallback_decision

    def _store_cache(self, key: str, decision: SemanticGuardrailDecision) -> None:
        self._decision_cache[key] = decision
        if len(self._decision_cache) > self.max_cache_size:
            self._decision_cache.popitem(last=False)

    def _call_lightweight_ai_guardrail(
        self,
        question: str,
        recent_history: List[Dict[str, Any]],
        role: str,
        allowed_sources: Optional[List[str]],
        llm_provider: Optional[str],
        llm_api_key: Optional[str],
        t0: float,
    ) -> Optional[SemanticGuardrailDecision]:
        from sim_platform import get_configured_openai_key

        provider = (llm_provider or "").lower().strip()
        user_key = (llm_api_key or "").strip()

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

        if not openai_key and not gemini_key:
            return None

        city_str = f"{self.municipality_name}/{self.state_uf}"
        history_snippet = "Sem histórico anterior nesta sessão."
        if recent_history:
            turns = []
            # Últimos 3 turnos completos (até 6 mensagens user/assistant), incluindo sql_executed!
            for m in recent_history[-6:]:
                r_label = "Usuário" if m.get("role") == "user" else "SIM"
                c_txt = str(m.get("content") or "")[:420].replace("\n", " ")
                sql_prev = str(m.get("sql_executed") or "").strip()
                line = f"- {r_label}: {c_txt}"
                if sql_prev:
                    line += f" | [SQL Executado: {sql_prev[:260]}]"
                turns.append(line)
            history_snippet = "\n".join(turns)

        today_dt = date.today()
        today_pt = today_dt.strftime("%d/%m/%Y")
        system_prompt = (
            f"Você é o assistente de compreensão semântica da plataforma SIM "
            f"(Sistema de Inteligência Municipal) da Prefeitura de {city_str} (IBGE {self.ibge_code}).\n"
            f"Analise a mensagem atual do cidadão considerando os últimos turnos da conversa e retorne APENAS um JSON válido.\n\n"
            f"ÂNCORAS DE TEMPO REAIS:\n"
            f"- DATA DO CALENDÁRIO REAL HOJE: {today_pt} (ano civil atual: {today_dt.year}).\n"
            f"- ÚLTIMA ATUALIZAÇÃO OFICIAL CONSOLIDADA NA BASE: {self.latest_remittance_human}.\n\n"
            f"DOMÍNIOS DISPONÍVEIS NO BANCO SQL DE {city_str} (2020 a {self.max_data_year}):\n"
            f"- Orçamento LOA previsto vs. realizado, receitas, despesas, empenhos, liquidações e pagamentos (SICOM/TCE-MG)\n"
            f"- Fornecedores, quadro societário (QSA), licitações, dispensas, adesões a atas (caronas) e contratos/aditivos\n"
            f"- Folha de pagamento, vínculos (concursados/efetivos, temporários, comissionados, agentes políticos), salários e ocupantes de cargos/secretarias (2023 a {self.max_data_year})\n"
            f"- Frota municipal de veículos (cadastro patrimonial cumulativo e gastos de combustível/manutenção)\n"
            f"- Saúde (SIOPS/DATASUS), Educação (IDEB/INEP/FUNDEB), Demografia/PIB (IBGE), Indicadores Fiscais LRF/RCL/CAPAG (SICONFI), Convênios/Emendas/Bolsa Família/BPC (CGU)\n"
            f"- Atos normativos do Diário Oficial (*Jornal Opinião*: decretos, portarias, leis municipais, editais)\n\n"
            f"CLASSIFIQUE `intent` EM UMA DAS OPÇÕES:\n"
            f"1. `sql_analytics`: consulta quantitativa, cadastral, ocupantes de cargos/secretarias, frota, orçamento (inclusive anos futuros como 2026/2027 para comparar com a série histórica), indicadores fiscais ('as contas estão em dia?') ou follow-up numérico ('Deste total, quantos são concursados?', 'E em 2023?').\n"
            f"2. `rag_diario_oficial`: busca estritamente textual sobre decreto, lei municipal, portaria ou edital específico no Diário Oficial.\n"
            f"3. `hybrid_sql_rag`: pergunta composta que exige DADOS SQL + LEI/ATO NORMATIVO (ex.: 'Essas quantias de carona são legais? Qual lei trata disso?', 'Qual o valor pago em contratos e o que diz a Portaria X?').\n"
            f"4. `conversational_identity`: saudação, apresentação do sistema ('oi', 'quem é vc?', 'que é vc?', 'o que você faz?') ou sobre quais cidades/dados o SIM responde.\n"
            f"5. `conversational_followup`: pedido para resumir, colocar em tópicos ou gerar gráfico (`wants_chart: true`) dos dados JÁ consultados no turno anterior sem precisar de nova query SQL.\n"
            f"6. `conceptual_doubt`: dúvida puramente conceitual sobre contabilidade pública (ex.: 'O que significa empenho, liquidação e restos a pagar?') sem pedir dados nem julgar valores do turno anterior.\n"
            f"7. `clarification_needed`: mensagem incompreensível (ex.: 'teste 123') ou pedido vago sem contexto anterior na sessão.\n"
            f"8. `out_of_scope_municipality`: pergunta pedindo dados de outro município que não seja {self.municipality_name}.\n"
            f"9. `out_of_scope_general`: assunto sem relação com gestão pública (receita de bolo, previsão do tempo, futebol).\n\n"
            f"REGRAS CRÍTICAS:\n"
            f"- NUNCA use emojis e NUNCA exponha jargões internos de engenharia (como 'CRAG', 'roteador', 'SQL', 'RAG') em `direct_response`.\n"
            f"- CONCILIAÇÃO TEMPORAL ('HOJE / ATUAL' vs. TETO DA BASE): Hoje é {today_pt}, mas os dados oficiais vão até {self.latest_remittance_human}. Se o usuário pedir estoque acumulado ('qual a frota atual', 'quantos veículos a prefeitura tem'), NÃO adicione 'em {self.max_data_year}' na `standalone_question` (reescreva como 'Qual é a frota consolidada total de veículos da Prefeitura de {self.municipality_name} segundo a base oficial mais recente disponível?'). Se mudar totalmente de assunto em relação ao turno anterior (ex.: falava de funcionários e agora pergunta de veículos), marque `is_contextual_followup: false` e NÃO herde filtros de ano do assunto antigo!\n"
            f"- Preencha SEMPRE `standalone_question`: reescreva a pergunta de forma 100% explícita, completa e autônoma resolvendo pronomes/anáforas quando `is_contextual_followup` for `true`.\n"
            f"- Preencha `wants_chart: true` se o usuário pedir gráfico/visualização.\n"
            f"- Preencha `requires_sql` (bool), `requires_rag` (bool) e `sub_questions` (lista de sub-perguntas caso haja 2+ perguntas na mesma mensagem).\n"
            f"- Deixe `direct_response` vazio (\"\") para `conversational_identity`, `sql_analytics`, `rag_diario_oficial`, `hybrid_sql_rag` ou quando `wants_chart` for `true`.\n\n"
            f"FORMATO JSON OBRIGATÓRIO:\n"
            f'{{"makes_sense": true, "intent": "...", "standalone_question": "...", "requires_sql": true, "requires_rag": false, "sub_questions": [], "is_contextual_followup": false, "wants_chart": false, "direct_response": "...", "reasoning": "..."}}'
        )

        user_payload = (
            f"Histórico recente da sessão:\n{history_snippet}\n\n"
            f"Perfil do usuário: {role}\n"
            f"Mensagem atual do usuário: {question}"
        )

        try:
            import requests

            if openai_key and openai_key.startswith("sk-"):
                guard_model = os.environ.get("SIM_GUARDRAIL_MODEL", "gpt-4.1-mini")
                resp = requests.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"},
                    json={
                        "model": guard_model,
                        "temperature": 0.0,
                        "max_tokens": 340,
                        "response_format": {"type": "json_object"},
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_payload},
                        ],
                    },
                    timeout=6.0,
                )
                if resp.status_code == 200:
                    body = resp.json()
                    content = body["choices"][0]["message"]["content"]
                    usage = body.get("usage") or {}
                    p_tok = int(usage.get("prompt_tokens") or 0)
                    c_tok = int(usage.get("completion_tokens") or 0)
                    total_tok = int(usage.get("total_tokens") or (p_tok + c_tok) or 180)
                    parsed = json.loads(content)
                    return self._build_decision_from_json(
                        parsed=parsed,
                        original_question=question,
                        recent_history=recent_history,
                        engine_name=f"SemanticAI Guardrail ({guard_model})",
                        tokens_used=total_tok,
                        prompt_tokens=p_tok,
                        completion_tokens=c_tok,
                        t0=t0,
                    )

            if gemini_key:
                guard_model = "gemini-2.5-flash"
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{guard_model}:generateContent?key={gemini_key}"
                resp = requests.post(
                    url,
                    headers={"Content-Type": "application/json"},
                    json={
                        "contents": [{"parts": [{"text": f"{system_prompt}\n\n{user_payload}"}]}],
                        "generationConfig": {
                            "temperature": 0.0,
                            "maxOutputTokens": 340,
                            "responseMimeType": "application/json",
                        },
                    },
                    timeout=6.0,
                )
                if resp.status_code == 200:
                    r_json = resp.json()
                    candidates = r_json.get("candidates") or []
                    usage_meta = r_json.get("usageMetadata") or {}
                    p_tok = int(usage_meta.get("promptTokenCount") or 0)
                    c_tok = int(usage_meta.get("candidatesTokenCount") or 0)
                    total_tok = int(usage_meta.get("totalTokenCount") or (p_tok + c_tok) or 180)
                    if candidates:
                        txt = candidates[0]["content"]["parts"][0]["text"]
                        parsed = json.loads(txt)
                        return self._build_decision_from_json(
                            parsed=parsed,
                            original_question=question,
                            recent_history=recent_history,
                            engine_name=f"SemanticAI Guardrail ({guard_model})",
                            tokens_used=total_tok,
                            prompt_tokens=p_tok,
                            completion_tokens=c_tok,
                            t0=t0,
                        )
        except Exception:
            return None

        return None

    def _build_decision_from_json(
        self,
        parsed: Dict[str, Any],
        original_question: str,
        recent_history: List[Dict[str, Any]],
        engine_name: str,
        tokens_used: int,
        prompt_tokens: int,
        completion_tokens: int,
        t0: float,
    ) -> SemanticGuardrailDecision:
        intent = str(parsed.get("intent") or "sql_analytics").strip().lower()
        if intent not in self.VALID_INTENTS:
            intent = "sql_analytics"

        makes_sense = bool(parsed.get("makes_sense", True))
        standalone_q = strip_emojis(str(parsed.get("standalone_question") or original_question).strip()) or original_question
        direct_resp = strip_emojis(str(parsed.get("direct_response") or "").strip())
        reasoning = strip_emojis(str(parsed.get("reasoning") or "Classificação semântica concluída.").strip())

        det_followup, det_chart = self._detect_contextual_and_chart_signals(original_question, recent_history)
        is_followup = bool(parsed.get("is_contextual_followup", False)) or det_followup
        wants_chart = bool(parsed.get("wants_chart", False)) or det_chart

        if is_followup and standalone_q.strip().lower() == original_question.strip().lower():
            standalone_q = self._heuristic_rewrite_standalone(original_question, recent_history)

        raw_subs = parsed.get("sub_questions")
        sub_questions = [strip_emojis(str(s)) for s in raw_subs if str(s).strip()] if isinstance(raw_subs, list) else []

        legal_notes = extract_statutory_legal_notes(f"{original_question} {standalone_q}")
        q_norm = _norm_ascii(original_question)

        # Se o usuário questiona legalidade/lei de dados recém-apresentados, promover a hybrid_sql_rag
        if legal_notes and any(w in q_norm for w in ("lei", "legais", "legal", "ilegal", "contra isso", "permitido", "limite")):
            if intent in ("rag_diario_oficial", "conceptual_doubt", "conversational_followup"):
                intent = "hybrid_sql_rag"
                direct_resp = ""

        requires_sql = bool(parsed.get("requires_sql", intent in ("sql_analytics", "hybrid_sql_rag")))
        requires_rag = bool(parsed.get("requires_rag", intent in ("rag_diario_oficial", "hybrid_sql_rag")))
        if requires_sql and requires_rag and intent in ("sql_analytics", "rag_diario_oficial"):
            intent = "hybrid_sql_rag"
            direct_resp = ""

        # Se o usuário pediu um gráfico, nunca bloquear com texto de recusa no guardrail
        if wants_chart:
            direct_resp = ""
            if intent == "clarification_needed" and recent_history:
                intent = "conversational_followup"

        city_str = f"{self.municipality_name}/{self.state_uf}"
        if intent == "out_of_scope_municipality" and not direct_resp:
            direct_resp = (
                f"Esta instância do SIM atende exclusivamente aos dados públicos do município de "
                f"{city_str} (IBGE {self.ibge_code})."
            )
        elif intent == "out_of_scope_general" and not direct_resp:
            direct_resp = (
                f"Sou o assistente institucional de transparência da Prefeitura de {city_str}. "
                f"Respondo apenas dúvidas sobre dados públicos municipais (orçamento, receitas, despesas, contratos, "
                f"fornecedores, servidores, saúde, educação e Diário Oficial)."
            )
        elif intent == "clarification_needed" and not direct_resp:
            direct_resp = (
                f"Não consegui identificar qual dado público de {city_str} você deseja consultar. "
                f"Você pode informar o tema (ex.: orçamento, saúde, educação, fornecedores, contratos, salários ou decretos) e o ano desejado (2020 a 2025)?"
            )

        inst_resp = direct_resp
        if direct_resp and intent in ("conversational_followup", "conceptual_doubt", "clarification_needed"):
            inst_resp = f"**Nota Contextual ({city_str} — {engine_name}):**\n{direct_resp}"

        return SemanticGuardrailDecision(
            makes_sense=makes_sense,
            is_safe=(intent != "blocked_security"),
            intent=intent,
            standalone_question=standalone_q,
            requires_sql=requires_sql,
            requires_rag=requires_rag,
            sub_questions=sub_questions,
            is_contextual_followup=is_followup,
            wants_chart=wants_chart,
            legal_framework_notes=legal_notes,
            direct_response_citizen=direct_resp,
            direct_response_institutional=inst_resp,
            reasoning=reasoning,
            engine_used=engine_name,
            tokens_used=tokens_used,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            latency_ms=round((time.perf_counter() - t0) * 1000.0, 2),
            used_ai_model=True,
        )

    def _evaluate_heuristic_fallback(
        self,
        question: str,
        recent_history: List[Dict[str, Any]],
        role: str,
        allowed_sources: Optional[List[str]],
        t0: float,
    ) -> SemanticGuardrailDecision:
        q_norm = _norm_ascii(question)
        is_followup, wants_chart = self._detect_contextual_and_chart_signals(question, recent_history)
        standalone_q = self._heuristic_rewrite_standalone(question, recent_history)
        legal_notes = extract_statutory_legal_notes(standalone_q)

        intent = "sql_analytics"
        if self.fallback_classifier_fn is not None:
            intent = self.fallback_classifier_fn(question, allowed_sources)

        if wants_chart:
            has_data_topic = any(
                k in q_norm
                for k in ("saude", "educacao", "orcamento", "despesa", "receita", "fornecedor", "servidor", "funcionari", "frota", "carona", "contrato")
            )
            if not has_data_topic and recent_history:
                intent = "conversational_followup"
            elif not has_data_topic and not recent_history:
                intent = "clarification_needed"
            else:
                intent = "sql_analytics"
        elif intent == "sql_analytics":
            sql_data_signals = (
                "gasto", "despesa", "receita", "orcamento", "empenho", "pagamento", "liquidado", "fornecedor",
                "credor", "empresa", "socio", "qsa", "servidor", "funcionario", "salario", "remuneracao", "folha",
                "concursado", "comissionado", "secretario", "prefeito", "contrato", "aditivo", "licitacao",
                "dispensa", "carona", "frota", "veiculo", "placa", "saude", "siops", "educacao", "ideb",
                "escola", "matricula", "populacao", "pib", "idhm", "capag", "lrf", "rcl", "bolsa familia",
                "bpc", "convenio", "emenda", "contas",
            )
            has_sql_entity = any(s in q_norm for s in sql_data_signals)
            has_year = bool(re.search(r"\b202[0-9]\b", q_norm))

            identity_signals = (
                "pra que serve", "para que serve", "esse sistema", "essa plataforma", "esse assistente",
                "esse bot", "esse zap", "opa", "tudo bom", "tudo bem", "quem e", "oq vc", "o que vc",
                "o que voce", "como vc", "como voce", "me ajuda", "o que posso perguntar", "quais cidades",
                "presta informacoes sobre",
            )
            followup_signals = (
                "resposta anterior", "pergunta anterior", "mensagem anterior", "resuma", "resumir",
                "em topicos", "em 1 frase", "em uma frase", "simplifique", "fala mais facil",
                "explique melhor", "divida isso", "some esses",
            )
            conceptual_signals = (
                "o que significa", "qual a diferenca", "conceito de", "definicao de",
                "como funciona uma", "como funciona o",
            )

            if q_norm in ("teste", "teste 123", "alo", "oi?", "?"):
                intent = "clarification_needed"
            elif not has_sql_entity and not has_year:
                if any(s in q_norm for s in identity_signals):
                    intent = "conversational_identity"
                elif any(s in q_norm for s in followup_signals):
                    intent = "conversational_followup"
                elif any(s in q_norm for s in conceptual_signals):
                    intent = "conceptual_doubt"
            elif any(s in q_norm for s in followup_signals) and recent_history and not has_year:
                intent = "conversational_followup"

        if legal_notes and any(w in q_norm for w in ("lei", "legais", "legal", "ilegal", "contra isso")):
            if intent in ("rag_diario_oficial", "sql_analytics") and (recent_history or "carona" in _norm_ascii(standalone_q)):
                intent = "hybrid_sql_rag"

        cit_resp = ""
        inst_resp = ""
        city_str = f"{self.municipality_name}/{self.state_uf}"
        if intent == "conversational_identity":
            cit_resp = (
                f"Sou o sistema oficial de consulta aos dados públicos da Prefeitura de **{city_str}** (IBGE `{self.ibge_code}`). "
                f"Respondo dúvidas em linguagem simples sobre orçamento, despesas, saúde, educação, fornecedores, salários, frota, convênios e atos do Diário Oficial (2020 a 2025)."
            )
            inst_resp = cit_resp
        elif intent in ("conversational_followup", "conceptual_doubt") and not wants_chart and self.fallback_conversational_fn is not None:
            cit_resp, inst_resp, _ = self.fallback_conversational_fn(
                question=question,
                role=role,
                hist_list=recent_history,
            )
        elif intent == "clarification_needed":
            if wants_chart:
                cit_resp = (
                    f"Sim, gero gráficos oficiais de barras diretamente aqui! Informe qual indicador ou série de {city_str} "
                    f"você deseja visualizar em gráfico (por exemplo: orçamento anual 2020-2025, gastos com saúde ou maiores fornecedores)."
                )
            else:
                cit_resp = (
                    f"Olá! Para consultar os dados oficiais de {city_str}, informe o tema desejado "
                    f"(ex.: orçamento, gastos com saúde/educação, fornecedores, folha de servidores, frota ou Diário Oficial)."
                )
            inst_resp = cit_resp

        requires_sql = intent in ("sql_analytics", "hybrid_sql_rag")
        requires_rag = intent in ("rag_diario_oficial", "hybrid_sql_rag")

        return SemanticGuardrailDecision(
            makes_sense=intent not in ("out_of_scope_general", "out_of_scope_municipality", "blocked_security"),
            is_safe=(intent != "blocked_security"),
            intent=intent,
            standalone_question=standalone_q,
            requires_sql=requires_sql,
            requires_rag=requires_rag,
            sub_questions=[],
            is_contextual_followup=is_followup,
            wants_chart=wants_chart,
            legal_framework_notes=legal_notes,
            direct_response_citizen=strip_emojis(cit_resp),
            direct_response_institutional=strip_emojis(inst_resp),
            reasoning=f"Classificação semântica por sinais de intenção e contexto (rota: {intent}).",
            engine_used="SIM Semantic Feature Guardrail (0 tokens)",
            tokens_used=0,
            prompt_tokens=0,
            completion_tokens=0,
            latency_ms=round((time.perf_counter() - t0) * 1000.0, 2),
            used_ai_model=False,
        )
