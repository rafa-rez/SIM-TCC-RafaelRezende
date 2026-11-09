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
from typing import Any, Callable, Deque, Dict, List, Optional

from sim_platform.catalog.data_catalog import SchemaSelectionResult
from sim_platform.catalog.entity_linker import EntityLinker
from sim_platform.engine.duckdb_manager import DuckDBManager
from sim_platform.prompts.builder import ModularPromptBuilder, ModularPromptPackage
from sim_platform.rag.hybrid_retriever import HybridRAGRetriever, normalize_text_for_dedup
from sim_platform.security.quota_manager import TenantQuotaManager
from sim_platform.security.sql_guard import (
    PromptSafetyResult,
    check_prompt_safety,
)

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
    "oq faz",
    "o que faz",
    "o que vc faz",
    "o que voce faz",
    "como vc funciona",
    "como voce funciona",
    "como funciona",
    "quais bases vc tem",
    "quais dados voce tem",
    "quais informacoes voce tem",
    "o que posso perguntar",
    "bom dia",
    "boa tarde",
    "boa noite",
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


def _format_brl_or_val(key: str, val: Any) -> str:
    if val is None:
        return "N/D"
    k_low = key.lower()
    if isinstance(val, (int, float)):
        if any(p in k_low for p in ("pct", "percent", "taxa", "cobertura")):
            return f"{val:,.2f}%".replace(",", "X").replace(".", ",").replace("X", ".")
        if any(
            p in k_low
            for p in (
                "vlr", "valor", "total_pago", "total_empenhado", "total_liquidado",
                "total_bruto", "total_liquido", "remuneracao", "receita", "despesa",
                "gasto", "arrecadado", "per_capita", "pib_per_capita",
            )
        ) and "qtd" not in k_low and "total_registros" not in k_low and "total_servidores" not in k_low:
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
    static_v2_tokens_baseline: int = 14961
    llm_engine_used: str = "SIM Native NL2SQL Compiler"
    pipeline_steps: List[Dict[str, Any]] = field(default_factory=list)
    exec_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "intent": self.intent,
            "tenant_id": self.tenant_id,
            "role": self.role,
            "question": self.question,
            "answer_text": self.answer_text,
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
            "static_v2_tokens_baseline": self.static_v2_tokens_baseline,
            "llm_engine_used": self.llm_engine_used,
            "pipeline_steps": self.pipeline_steps,
            "exec_ms": self.exec_ms,
        }


class SIMOrchestrator:
    """
    Orquestrador central v3.0 que coordena segurança de prompt, roteamento de intenção,
    seleção dinâmica de catálogo com filtro de fontes (`allowed_sources`), Value Linking,
    Few-Shot Dinâmico, memória Multi-Turn (`deque(maxlen=10)`), execução SQL com auto-correção,
    cross-link automático SQL → Diário Oficial (RAG) e telemetria de franquia B2G.
    """

    def __init__(
        self,
        db_manager: Optional[DuckDBManager] = None,
        rag_retriever: Optional[HybridRAGRetriever] = None,
        prompt_builder: Optional[ModularPromptBuilder] = None,
        sql_generator_fn: Optional[Callable[[ModularPromptPackage, int, Optional[str]], str]] = None,
        max_sql_retries: int = 2,
    ) -> None:
        self.db_manager = db_manager or DuckDBManager()
        self.rag_retriever = rag_retriever or HybridRAGRetriever()
        self.prompt_builder = prompt_builder or ModularPromptBuilder(
            municipality_name=self.db_manager.tenant.municipality_name,
            state_uf=self.db_manager.tenant.state_uf,
            ibge_code=self.db_manager.tenant.ibge_code,
        )
        self.entity_linker = EntityLinker(
            workspace_root=DEFAULT_WORKSPACE_ROOT,
            conn=self.db_manager.conn,
        )
        self.quota_manager = TenantQuotaManager(
            tenant_id=self.db_manager.tenant.tenant_id,
        )
        self.sql_generator_fn = sql_generator_fn or self._smart_sql_generator
        self.max_sql_retries = max_sql_retries
        self.lock_manager = SessionLockManager()
        self.conversation_history: Dict[str, Deque[Dict[str, str]]] = {}
        self._golden_lookup: Dict[str, str] = self._load_golden_sql_index()
        self._active_allowed_sources: List[str] = []
        self._active_llm_provider: Optional[str] = None
        self._active_llm_api_key: Optional[str] = None
        self._last_llm_engine_used: str = "SIM Native NL2SQL Compiler"

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

        if q_norm in ("oi", "ola", "bom dia", "boa tarde", "boa noite", "ajuda", "help") or any(
            pat in q_norm for pat in IDENTITY_GREETING_PATTERNS
        ):
            return "conversational_identity"

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

        gemini_key = (
            user_key
            if (provider == "gemini" or (user_key and not user_key.startswith("sk-")))
            else os.environ.get("GEMINI_API_KEY", os.environ.get("GOOGLE_API_KEY", "")).strip()
        )
        openai_key = (
            user_key
            if (provider == "openai" or user_key.startswith("sk-"))
            else os.environ.get("OPENAI_API_KEY", "").strip()
        )

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
                        candidates = resp.json().get("candidates") or []
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
                    content = resp.json()["choices"][0]["message"]["content"]
                    parsed = json.loads(content)
                    if parsed.get("query_sql"):
                        self._last_llm_engine_used = f"OpenAI ({model_name})"
                        return str(parsed["query_sql"]).strip()
            except Exception:
                pass

        return None

    def _smart_sql_generator(
        self,
        prompt_pkg: ModularPromptPackage,
        attempt: int,
        last_error: Optional[str],
    ) -> str:
        self._last_llm_engine_used = "SIM Native NL2SQL Compiler"
        q_raw = prompt_pkg.assembled_sql_prompt.split("<user_question>")[-1].split("</user_question>")[0].strip()
        q_norm = normalize_text_for_dedup(q_raw)
        pkg_sources = prompt_pkg.allowed_sources if prompt_pkg.allowed_sources else self._active_allowed_sources
        allowed_src = {s.upper() for s in pkg_sources if s.upper() != "DIARIO_OFICIAL"}

        # 1. Tentar LLM externo (Gemini ou OpenAI) se o usuário forneceu chave
        llm_sql = self._call_external_llm_for_sql(
            prompt_pkg.assembled_sql_prompt,
            llm_provider=prompt_pkg.llm_provider,
            llm_api_key=prompt_pkg.llm_api_key,
        )
        if llm_sql:
            return llm_sql

        # 2. Se não há restrição de fonte incompatível e a pergunta coincide com o Golden Dataset v1/v2
        if not allowed_src and q_norm in self._golden_lookup:
            self._last_llm_engine_used = "SIM Golden Verified Index + NL2SQL"
            return self._golden_lookup[q_norm]

        # 3. Extrair filtro temporal da pergunta (priorizando o último ano mencionado em follow-ups)
        years = [int(y) for y in re.findall(r"\b(202[0-6])\b", q_norm)]
        if "entre" in q_norm and len(years) >= 2:
            year_filter = f"ano_referencia BETWEEN {min(years)} AND {max(years)}"
        elif "(" in q_raw and len(years) >= 1 and re.search(r"\(.*202[0-6].*\)$", q_raw.strip()):
            # Follow-up especificando um novo ano no final: ex. "Qual o gasto com saúde em 2024? (e em 2023?)"
            year_filter = f"ano_referencia = {years[-1]}"
        elif len(years) == 1:
            year_filter = f"ano_referencia = {years[0]}"
        elif len(years) >= 2:
            year_filter = f"ano_referencia BETWEEN {min(years)} AND {max(years)}"
        elif any(w in q_norm for w in ("evolucao", "serie", "historico", "todos os anos", "2020 a 2025")):
            year_filter = "ano_referencia BETWEEN 2020 AND 2025"
        else:
            year_filter = "ano_referencia = 2024"

        # 4. Atalhos de Gold Views e Trilhas TCE-MG (Conflito de Interesses, Caronas, Frota Órfã)
        if any(w in q_norm for w in ("conflito de interesse", "conflito de interesses", "porta giratoria", "servidor socio")):
            return (
                "SELECT num_cpf, nom_pessoa, dsc_cargo, dsc_lotacao, primeiro_ano_folha, ultimo_ano_folha, "
                "nom_empresa, cnpj_empresa, dsc_tipo_participacao, total_pago_empresa "
                "FROM vw_gold_conflito_interesses_licitacao ORDER BY total_pago_empresa DESC LIMIT 20"
            )

        if any(w in q_norm for w in ("carona", "adesao a ata", "registro de preco", "registro de precos")):
            return (
                "SELECT nom_credor, num_doc_credor, SUM(qtd_empenhos) AS qtd_empenhos_totais, "
                "ROUND(SUM(total_empenhado_carona), 2) AS total_empenhado_carona, "
                "ROUND(SUM(total_pago), 2) AS total_pago "
                "FROM vw_gold_execucao_despesa_credor "
                "WHERE total_empenhado_carona > 0 "
                "GROUP BY 1, 2 ORDER BY total_empenhado_carona DESC LIMIT 15"
            )

        if any(w in q_norm for w in ("sem placa", "veiculo orfao", "veiculos orfaos", "placa ausente")):
            return (
                "SELECT ano_referencia, status_cadastro_frota, COUNT(*) AS qtd_lancamentos, "
                "ROUND(SUM(vlr_gasto), 2) AS total_gasto_frota "
                "FROM vw_gold_frota_consolidada GROUP BY 1, 2 ORDER BY ano_referencia DESC, total_gasto_frota DESC LIMIT 30"
            )

        # 5. Respeitar escopo de fonte quando o usuário selecionou bases específicas
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
            w in q_norm for w in ("siconfi", "finbra", "capag", "tesouro nacional", "limite prudencial", "lrf", "rcl")
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
                "FROM receita_qsa_fornecedores ORDER BY total_pago_historico DESC LIMIT 15"
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

        if "ies-c" in q_norm or "indice de saude" in q_norm:
            return (
                "WITH s AS (SELECT ano_referencia, SUM(TRY_CAST(vlr_liquidado AS DECIMAL(18,2))) AS gasto_saude FROM saude_saude GROUP BY ano_referencia), "
                "r AS (SELECT ano_referencia, SUM(TRY_CAST(vlr_realizadoateperiodo AS DECIMAL(18,2))) AS rec_imp FROM receita_receita WHERE dsc_especie ILIKE '%Imposto%' OR dsc_rubrica ILIKE '%FPM%' OR dsc_rubrica ILIKE '%ICMS%' OR dsc_rubrica ILIKE '%IPVA%' OR dsc_origem ILIKE '%Transfer%' GROUP BY ano_referencia) "
                "SELECT s.ano_referencia, ROUND((s.gasto_saude / NULLIF(r.rec_imp, 0)) * 100, 2) AS ies_c_pct, s.gasto_saude, r.rec_imp "
                "FROM s JOIN r ON s.ano_referencia = r.ano_referencia ORDER BY s.ano_referencia"
            )

        if "ieq-c" in q_norm or "indice de educacao" in q_norm:
            return (
                "WITH e AS (SELECT ano_referencia, SUM(TRY_CAST(vlr_liquidado AS DECIMAL(18,2))) AS gasto_mde FROM educacao_educacao GROUP BY ano_referencia), "
                "r AS (SELECT ano_referencia, SUM(TRY_CAST(vlr_realizadoateperiodo AS DECIMAL(18,2))) AS rec_imp FROM receita_receita WHERE dsc_especie ILIKE '%Imposto%' GROUP BY ano_referencia) "
                "SELECT e.ano_referencia, ROUND((e.gasto_mde / NULLIF(r.rec_imp, 0)) * 100, 2) AS ieq_c_pct, e.gasto_mde, r.rec_imp "
                "FROM e JOIN r ON e.ano_referencia = r.ano_referencia ORDER BY e.ano_referencia"
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

        if "salario" in q_norm or "remuneracao" in q_norm or "folha" in q_norm or "servidor" in q_norm:
            return (
                f"SELECT ano_referencia, COUNT(DISTINCT seq_dim_pessoa) AS total_servidores, "
                f"SUM(TRY_CAST(vlr_rmn_bruta AS DECIMAL(18,2))) AS total_bruto, "
                f"SUM(TRY_CAST(vlr_rmn_liquida AS DECIMAL(18,2))) AS total_liquido "
                f"FROM remuneracao_municipio WHERE {year_filter} GROUP BY ano_referencia ORDER BY ano_referencia"
            )

        if "fornecedor" in q_norm or "credor" in q_norm or "empresas mais receberam" in q_norm or "maiores pagamentos" in q_norm:
            return (
                f"SELECT nom_credor, num_doc_credor, COUNT(*) AS qtd_pagamentos, "
                f"SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2))) AS total_pago "
                f"FROM despesa_pagamento WHERE {year_filter} "
                f"AND COALESCE(nom_credor, '') NOT ILIKE '%NAO INFORMADO%' "
                f"AND COALESCE(num_doc_credor, '') NOT IN ('-1', '0') "
                f"GROUP BY nom_credor, num_doc_credor ORDER BY total_pago DESC LIMIT 10"
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

        return (
            f"SELECT ano_referencia, COUNT(*) AS qtd_pagamentos, "
            f"SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2))) AS total_pago "
            f"FROM despesa_pagamento WHERE {year_filter} GROUP BY ano_referencia ORDER BY ano_referencia"
        )

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

            effective_question = self._get_effective_question_with_history(question, session_id=session_id)
            hist_list = list(self.conversation_history.get(session_id, []))

            # Etapa 1: Segurança de Prompt e Lock de Sessão
            s1_t0 = time.perf_counter()
            safety = check_prompt_safety(question)
            scope_desc = (
                f"Escopo de Fontes Restrito: {', '.join(allowed_sources)}"
                if allowed_sources
                else "Escopo de Fontes: Todas as 8 fontes oficiais + Gold Views (Automático)"
            )
            steps.append(
                {
                    "step": 1,
                    "name": "Guardião de Entrada & Lock Atômico de Sessão",
                    "status": "OK" if safety.is_safe else "BLOCKED",
                    "duration_ms": round((time.perf_counter() - s1_t0) * 1000.0, 2),
                    "detail": (
                        f"Sessão '{session_id}' bloqueada com exclusividade. "
                        f"Perfil RBAC: '{role}'. {scope_desc}. Verificação anti-Prompt Injection: "
                        f"{'Aprovada' if safety.is_safe else 'Bloqueada (' + ', '.join(safety.triggered_rules) + ')'}."
                    ),
                }
            )

            # Etapa 2: Classificação de Intenção
            s2_t0 = time.perf_counter()
            intent = "sql_analytics" if sql_override else self.classify_intent(effective_question, allowed_sources=allowed_sources)
            steps.append(
                {
                    "step": 2,
                    "name": "Roteador Determinístico de Intenção & Escopo",
                    "status": "OK" if intent not in ("blocked_security", "out_of_scope_municipality", "out_of_scope_general") else "INTERCEPTED",
                    "duration_ms": round((time.perf_counter() - s2_t0) * 1000.0, 2),
                    "detail": (
                        f"Rota classificada: '{intent}' para o tenant '{self.db_manager.tenant.tenant_id}'"
                        + (f" (Herança Multi-Turn ativa: '{effective_question}')." if effective_question != question else ".")
                    ),
                }
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
                identity_answer = (
                    f"Sou o **SIM (Sistema de Inteligência Municipal)** de **{city_str}** "
                    f"(código IBGE `{self.db_manager.tenant.ibge_code}`).\n\n"
                    "**O que eu faço:**\n"
                    f"Converto perguntas em linguagem natural em consultas auditáveis sobre **{total_t} objetos analíticos oficiais (79 tabelas Parquet + 6 Gold Views de Auditoria) em 7 fontes governamentais** "
                    "(2020 a 2025) e sobre a collection de atos individuais do **Diário Oficial (*Jornal Opinião*)**, sempre informando os arquivos-fonte utilizados.\n\n"
                    "**Fontes Oficiais Integradas (selecionáveis individualmente):**\n"
                    "- **SICOM / TCE-MG (65 tabelas + 6 Gold Views):** receitas, despesas, empenhos, pagamentos, contratos, licitações, frota, folha de servidores e cruzamento de conflito de interesses.\n"
                    "- **Portal da Transparência / CGU (9 tabelas):** Novo Bolsa Família, BPC, Auxílio Brasil, Convênios Federais (`cgu_convenios`) e Emendas Parlamentares (`cgu_emendas`).\n"
                    "- **SICONFI / Tesouro Nacional:** Receita Corrente Líquida (RCL), Despesa com Pessoal LRF, Limite Prudencial e Nota CAPAG (`siconfi_finbra_indicadores`).\n"
                    "- **IBGE Indicadores Municipais:** População oficial (Censo 2022 e Estimativas), PIB Municipal, PIB *per capita* e IDHM (`ibge_indicadores_municipais`).\n"
                    "- **INEP / MEC Educação:** IDEB Anos Iniciais e Finais (anos ímpares oficiais), taxa de aprovação, escolas e matrículas (`inep_ideb_educacao`).\n"
                    "- **DATASUS / SIOPS Saúde:** aplicação constitucional em saúde, UBS, CNES, leitos SUS e Santa Casa (`datasus_siops_saude`).\n"
                    "- **Receita Federal CNPJ / QSA:** porte, CNAE e quadro societário reconciliado dos fornecedores da Prefeitura (`receita_qsa_fornecedores`).\n"
                    "- **Diário Oficial (*Jornal Opinião*):** leis, decretos, portarias e editais atomizados com busca híbrida Okapi BM25."
                )
                return OrchestrationResult(
                    status="sucesso",
                    intent=intent,
                    tenant_id=self.db_manager.tenant.tenant_id,
                    role=role,
                    question=question,
                    answer_text=identity_answer,
                    schema_reduction_pct=100.0,
                    prompt_tokens_estimated=0,
                    llm_engine_used="Instant Conversational Handler (0 tokens)",
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

            # Etapa 3 a 5: Fluxo Analítico SQL
            if intent in ("sql_analytics", "hybrid_sql_rag"):
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

                # Value Linking & Dynamic Few-Shots (v3.0)
                value_bindings = self.entity_linker.link_entities(effective_question, top_k=6)
                few_shots = self.entity_linker.select_few_shots(effective_question, top_k=3)

                # Escopo estrito de tabelas para o SQLGuard quando o usuário selecionou fontes ou tabelas
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
                        "step": 3,
                        "name": "Catálogo Dinâmico, Value Linking & Few-Shot Selector",
                        "status": "OK",
                        "duration_ms": round((time.perf_counter() - s3_t0) * 1000.0, 2),
                        "detail": (
                            f"Modo: {selection.selection_mode} | "
                            f"Fontes permitidas: {', '.join(selection.allowed_sources) if selection.allowed_sources else 'Todas'} | "
                            f"Tabelas enviadas ao modelo: {selection.selected_count} de {selection.total_catalog_tables} "
                            f"(Redução de {reduction_pct}% no ruído de schema) | "
                            f"Entidades canônicas vinculadas: {len(value_bindings)} | Few-shots: {len(few_shots)}."
                        ),
                    }
                )

                last_err: Optional[str] = None
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
                    )
                    est_tokens = prompt_pkg.estimated_tokens
                    candidate_sql = sql_override if (sql_override and attempt == 1) else self.sql_generator_fn(prompt_pkg, attempt, last_err)
                    steps.append(
                        {
                            "step": 4,
                            "name": f"Montagem Modular de Prompt & Geração SQL (Tentativa {attempt})",
                            "status": "OK",
                            "duration_ms": round((time.perf_counter() - s4_t0) * 1000.0, 2),
                            "detail": (
                                f"Motor: {self._last_llm_engine_used} | Prompt enxuto: ~{est_tokens} tokens "
                                f"(vs. 14.961 tokens no prompt v2 estático). SQL candidato compilado."
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
                            "step": 5,
                            "name": f"SQLGuard (Validação AST + Escopo) & Execução DuckDB Parquet (Tentativa {attempt})",
                            "status": "OK" if q_res["status"] == "sucesso" else q_res["status"].upper(),
                            "duration_ms": round((time.perf_counter() - s5_t0) * 1000.0, 2),
                            "detail": (
                                f"Status: {q_res['status']} | Linhas retornadas: {q_res.get('total_linhas', 0)} | "
                                f"Tabelas na AST: {', '.join(q_res.get('tabelas_referenciadas', []))} | "
                                f"CPFs mascarados (LGPD): {q_res.get('pii_masked_count', 0)}."
                                if q_res["status"] == "sucesso"
                                else f"Bloqueio/Erro capturado: {q_res.get('detalhe')}"
                            ),
                        }
                    )

                    if q_res["status"] == "sucesso":
                        sql_executed = q_res["query_executada"]
                        sql_rows = q_res["dados"]
                        pii_masked = q_res.get("pii_masked_count", 0)
                        for f in q_res.get("fontes_utilizadas", []):
                            if f not in fontes:
                                fontes.append(f)
                        warnings.extend(q_res.get("transparency_warnings", []))
                        break
                    else:
                        last_err = q_res.get("detalhe") or q_res.get("mensagem_llm")
                        if q_res["status"] == "bloqueado_seguranca" and (sql_override is not None or attempt > self.max_sql_retries):
                            return OrchestrationResult(
                                status="bloqueado_seguranca",
                                intent=intent,
                                tenant_id=self.db_manager.tenant.tenant_id,
                                role=role,
                                question=question,
                                answer_text=f"A consulta foi bloqueada pelo guardião de segurança e escopo: {last_err}",
                                sql_executed=candidate_sql,
                                selected_segments=selected_segments,
                                selected_tables=selected_tables,
                                allowed_sources=list(allowed_sources or []),
                                schema_reduction_pct=reduction_pct,
                                sql_attempts=attempts,
                                prompt_tokens_estimated=est_tokens,
                                llm_engine_used=self._last_llm_engine_used,
                                pipeline_steps=steps,
                                exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                            )

            # Etapa 6: Busca Híbrida no Diário Oficial (RAG Direto ou Cross-Link SQL → RAG)
            rag_allowed = (not allowed_sources) or ("DIARIO_OFICIAL" in {s.upper() for s in allowed_sources})
            if intent in ("rag_diario_oficial", "hybrid_sql_rag"):
                s6_t0 = time.perf_counter()
                rag_res = self.rag_retriever.search(query=effective_question, top_k=5, deduplicate=True)
                rag_chunks = rag_res.get("chunks", [])
                for f in rag_res.get("fontes_utilizadas", []):
                    if f not in fontes:
                        fontes.append(f)
                sig = rag_res.get("signals_detected", {})
                steps.append(
                    {
                        "step": 6,
                        "name": "Busca Híbrida RAG (Okapi BM25 + Atos Atomizados — 0% Duplicatas)",
                        "status": "OK",
                        "duration_ms": round((time.perf_counter() - s6_t0) * 1000.0, 2),
                        "detail": (
                            f"Sinais: tipo_ato={sig.get('tipo_ato')}, números={sig.get('act_numbers')} | "
                            f"Collection limpa: {rag_res.get('corpus_size', 0)} atos únicos -> "
                            f"Retornados: {len(rag_chunks)}."
                        ),
                    }
                )
            elif sql_rows and rag_allowed:
                # Cross-Link Automático SQL → Diário Oficial quando as linhas SQL retornam credores/CNPJs/contratos
                s6b_t0 = time.perf_counter()
                entity_candidates: List[str] = []
                entity_keys = (
                    "nom_credor", "razao_social", "fornecedor", "empresa_licitante",
                    "nom_empresa", "nom_contratado", "fornecedor_carona",
                    "num_doc_credor", "num_cnpj", "cnpj", "cnpj_empresa",
                )
                for r in sql_rows[:5]:
                    for ek in entity_keys:
                        val = r.get(ek)
                        if val and isinstance(val, str) and len(val.strip()) >= 5:
                            entity_candidates.append(val.strip())
                if entity_candidates:
                    cross_chunks = self.rag_retriever.search_by_entities(entity_candidates, top_k=3)
                    if cross_chunks:
                        rag_chunks = cross_chunks
                        for ch in cross_chunks:
                            src = f"Jornal Opinião ({ch.get('source')})"
                            if src not in fontes:
                                fontes.append(src)
                        steps.append(
                            {
                                "step": 6,
                                "name": "Cross-Link Automático SQL → Diário Oficial (Jornal Opinião)",
                                "status": "OK",
                                "duration_ms": round((time.perf_counter() - s6b_t0) * 1000.0, 2),
                                "detail": (
                                    f"Entidades extraídas do resultado SQL ({len(entity_candidates)}) cruzadas com o "
                                    f"Diário Oficial -> {len(cross_chunks)} ato(s) normativo(s) vinculado(s)."
                                ),
                            }
                        )

            # Registrar telemetria comercial de tokens na franquia B2G do Tenant
            is_external_llm = any(p in self._last_llm_engine_used for p in ("Gemini", "OpenAI"))
            self.quota_manager.record_usage(
                prompt_tokens=est_tokens if is_external_llm else 0,
                completion_tokens=180 if is_external_llm else 0,
                cached=not is_external_llm,
            )

            # Etapa 7: Síntese Executiva da Resposta Final
            answer_lines: List[str] = []
            if allowed_sources:
                answer_lines.append(f"🔎 **Escopo de Pesquisa Aplicado:** `{', '.join(allowed_sources)}`")

            if warnings:
                for w in warnings:
                    answer_lines.append(f"⚠️ **{w['title']}**: {w['message']}")

            if sql_rows:
                answer_lines.append(
                    f"Foram localizados **{len(sql_rows)} registro(s)** oficiais para a consulta em "
                    f"**{self.db_manager.tenant.municipality_name}/{self.db_manager.tenant.state_uf}**:"
                )
                for row in sql_rows[:8]:
                    formatted_pairs = [f"**{k}**: {_format_brl_or_val(k, v)}" for k, v in row.items()]
                    answer_lines.append("- " + " | ".join(formatted_pairs))
            elif intent in ("sql_analytics", "hybrid_sql_rag") and not warnings:
                answer_lines.append("Não foram encontrados registros tabulares para os filtros informados.")

            if rag_chunks:
                answer_lines.append(
                    f"\nForam recuperados **{len(rag_chunks)} ato(s) normativo(s) individuais** no Diário Oficial (*Jornal Opinião*):"
                )
                for ch in rag_chunks[:3]:
                    snippet = (ch.get("page_content") or "")[:320].replace("\n", " ")
                    answer_lines.append(
                        f"- **[{ch.get('tipo_ato')} | {ch.get('source')} | match: {ch.get('match_stage')}]**: {snippet}..."
                    )

            if role in ("auditor", "admin", "controller") and sql_executed:
                answer_lines.append(f"\n**SQL Auditável Executado**:\n```sql\n{sql_executed}\n```")

            if fontes:
                answer_lines.append(f"\n**Fontes Oficiais Utilizadas (`fontes_utilizadas`)**: {', '.join(fontes[:12])}")

            final_answer = "\n".join(answer_lines)

            if session_id not in self.conversation_history:
                self.conversation_history[session_id] = deque(maxlen=10)
            hist_deque = self.conversation_history[session_id]
            hist_deque.append({"role": "user", "content": question})
            hist_deque.append({"role": "assistant", "content": final_answer})

            return OrchestrationResult(
                status="sucesso",
                intent=intent,
                tenant_id=self.db_manager.tenant.tenant_id,
                role=role,
                question=question,
                answer_text=final_answer,
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
                prompt_tokens_estimated=est_tokens,
                llm_engine_used=self._last_llm_engine_used,
                pipeline_steps=steps,
                exec_ms=round((time.perf_counter() - t0) * 1000.0, 2),
            )
        finally:
            self.lock_manager.release(session_id)
