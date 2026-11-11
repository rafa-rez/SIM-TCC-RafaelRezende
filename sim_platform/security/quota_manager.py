"""
Gerenciador Comercial de Franquia de Tokens, Cache Semântico/LRU Persistente e Rate Limiting Anti-Flood (`TenantQuotaManager`).

Materializa no backend as 3 Travas de Proteção Financeira e as Cláusulas Contratuais X.1 a X.4
definidas em `AUDIT/SIM_PLAYBOOK_COMERCIAL_E_TOKENS.html`:
1. **Trava 1 — Cache SQL Inteligente (Cláusula X.2)**:
   - Perguntas idênticas (normalizadas por texto + perfil RBAC + escopo de fontes) são respondidas em `< 2ms`
     a **custo R$ 0,00 de IA**, sem consumir a franquia mensal contratada.
   - Cache híbrido L1 (RAM `OrderedDict`) + L2 (SQLite persistente em disco).
2. **Trava 2 — Rate Limit Cidadão no WhatsApp (Cláusula X.3)**:
   - Limite de **20 consultas/dia por número de telefone** no canal público de WhatsApp.
   - Controle de notificação única diária (`should_send_whatsapp_notice`), garantindo que na 21ª mensagem
     o cidadão receba um aviso amigável via WhatsApp orientando-o a retornar no dia seguinte ou usar o portal web,
     e da 22ª em diante seja bloqueado silenciosamente (impedindo loops infinitos contra bots).
3. **Trava 3 — Ledger Contratual Mensal por Porte + Alerta em 85% (Cláusulas X.1 e X.4)**:
   - Suporta os 4 pacotes oficiais do Playbook Comercial:
     * `camara`: 10.000 consultas/mês (R$ 8.900/mês + Setup R$ 12.000)
     * `porte_1`: 15.000 consultas/mês (R$ 12.900/mês + Setup R$ 18.000)
     * `porte_2`: 30.000 consultas/mês (R$ 18.900/mês + Setup R$ 25.000 — Padrão Caeté/MG)
     * `porte_3`: 60.000 consultas/mês (R$ 26.900/mês + Setup R$ 38.000)
   - Persistência transacional em SQLite (`quota_ledger.sqlite`) sobrevivendo a reinicializações do container.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
import time
import unicodedata
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


COMMERCIAL_PLANS: Dict[str, Dict[str, Any]] = {
    "camara": {
        "plan_code": "camara",
        "plan_name": "SIM Câmara — Legislativo & Fiscalização (Até 15 Gabinetes)",
        "monthly_quota_requests": 10000,
        "monthly_price_brl": 8900.00,
        "setup_price_brl": 12000.00,
        "overage_lot_requests": 10000,
        "overage_lot_price_brl": 1500.00,
    },
    "porte_1": {
        "plan_code": "porte_1",
        "plan_name": "Prefeitura Porte I — Transparência & Gestão (Até 30 mil hab.)",
        "monthly_quota_requests": 15000,
        "monthly_price_brl": 12900.00,
        "setup_price_brl": 18000.00,
        "overage_lot_requests": 10000,
        "overage_lot_price_brl": 1500.00,
    },
    "porte_2": {
        "plan_code": "porte_2",
        "plan_name": "Prefeitura Porte II — Controladoria, Ouvidoria & Gestão 360º (30k a 100k hab.)",
        "monthly_quota_requests": 30000,
        "monthly_price_brl": 18900.00,
        "setup_price_brl": 25000.00,
        "overage_lot_requests": 10000,
        "overage_lot_price_brl": 1500.00,
    },
    "porte_3": {
        "plan_code": "porte_3",
        "plan_name": "Prefeitura Porte III — GovTech Full Enterprise (> 100 mil hab.)",
        "monthly_quota_requests": 60000,
        "monthly_price_brl": 26900.00,
        "setup_price_brl": 38000.00,
        "overage_lot_requests": 10000,
        "overage_lot_price_brl": 1500.00,
    },
}


def _normalize_cache_key(
    question: str,
    role: str = "citizen",
    allowed_sources: Optional[List[str]] = None,
    explicit_tables: Optional[List[str]] = None,
) -> str:
    nfkd = unicodedata.normalize("NFKD", (question or "").strip().lower())
    clean_q = re.sub(r"\s+", " ", "".join(c for c in nfkd if not unicodedata.combining(c)))
    r_low = (role or "citizen").lower().strip()
    if r_low in ("citizen", "cidadao", "public"):
        role_bucket = "citizen"
    elif r_low in ("auditor", "admin", "controller", "controladoria"):
        role_bucket = "institutional_unmasked"
    else:
        role_bucket = "institutional_masked"
    srcs = ",".join(sorted(s.upper().strip() for s in (allowed_sources or [])))
    tbls = ",".join(sorted(t.lower().strip() for t in (explicit_tables or [])))
    raw = f"{role_bucket}::{srcs}::{tbls}::{clean_q}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class TenantQuotaManager:
    """
    Controlador thread-safe e persistente (SQLite + L1 RAM) das 3 Travas do Playbook Comercial.
    """

    def __init__(
        self,
        tenant_id: str = "caete_mg_3110004",
        plan_code: str = "porte_2",
        plan_name: Optional[str] = None,
        monthly_quota_requests: Optional[int] = None,
        daily_user_limit_whatsapp: int = 20,
        cache_max_entries: int = 512,
        cache_ttl_sec: float = 3600.0,
        cost_per_1k_input_brl: float = 0.00232,
        cost_per_1k_output_brl: float = 0.00928,
        cost_per_audio_call_brl: float = 0.0174,
        sqlite_path: Optional[Path] = None,
        db_path: Optional[Path] = None,
    ) -> None:
        if sqlite_path is None and db_path is not None:
            sqlite_path = db_path
        plan_meta = COMMERCIAL_PLANS.get(plan_code, COMMERCIAL_PLANS["porte_2"])
        self.tenant_id = tenant_id
        self.plan_code = plan_meta["plan_code"]
        self.plan_name = plan_name or plan_meta["plan_name"]
        self.monthly_quota_requests = int(
            monthly_quota_requests if monthly_quota_requests is not None else plan_meta["monthly_quota_requests"]
        )
        self.monthly_price_brl = float(plan_meta["monthly_price_brl"])
        self.setup_price_brl = float(plan_meta["setup_price_brl"])
        self.daily_user_limit_whatsapp = daily_user_limit_whatsapp
        self.cache_max_entries = cache_max_entries
        self.cache_ttl_sec = cache_ttl_sec
        self.cost_per_1k_input_brl = cost_per_1k_input_brl
        self.cost_per_1k_output_brl = cost_per_1k_output_brl
        self.cost_per_audio_call_brl = cost_per_audio_call_brl

        self._lock = threading.Lock()
        self._cache: OrderedDict[str, Tuple[float, Dict[str, Any]]] = OrderedDict()
        # (today_str, count, notified_friendly_msg)
        self._daily_user_counts: Dict[str, Tuple[str, int, bool]] = {}

        self.current_month = datetime.now().strftime("%Y-%m")
        self.total_requests = 0
        self.cache_hits = 0
        self.golden_deterministic_hits = 0
        self.llm_calls = 0
        self.audio_transcriptions = 0
        self.rate_limited_blocks = 0
        self.estimated_input_tokens = 0
        self.estimated_output_tokens = 0

        default_db = (
            Path(__file__).resolve().parents[2]
            / "avaliacao"
            / "dados"
            / "parquet"
            / f"quota_ledger_{self.tenant_id}.sqlite"
        )
        self.sqlite_path = sqlite_path if sqlite_path is not None else default_db
        if self.sqlite_path is not None:
            self._init_sqlite()

    def _init_sqlite(self) -> None:
        if self.sqlite_path is None:
            return
        try:
            self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(str(self.sqlite_path), timeout=5.0) as con:
                con.execute(
                    """
                    CREATE TABLE IF NOT EXISTS monthly_ledger (
                        tenant_id TEXT,
                        month_ref TEXT,
                        total_requests INTEGER DEFAULT 0,
                        cache_hits INTEGER DEFAULT 0,
                        golden_hits INTEGER DEFAULT 0,
                        llm_calls INTEGER DEFAULT 0,
                        audio_transcriptions INTEGER DEFAULT 0,
                        rate_limited_blocks INTEGER DEFAULT 0,
                        input_tokens INTEGER DEFAULT 0,
                        output_tokens INTEGER DEFAULT 0,
                        PRIMARY KEY (tenant_id, month_ref)
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE IF NOT EXISTS whatsapp_daily_limits (
                        tenant_id TEXT,
                        user_id TEXT,
                        day_ref TEXT,
                        msg_count INTEGER DEFAULT 0,
                        notified INTEGER DEFAULT 0,
                        PRIMARY KEY (tenant_id, user_id, day_ref)
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE IF NOT EXISTS query_cache (
                        cache_key TEXT PRIMARY KEY,
                        created_ts REAL,
                        payload_json TEXT
                    )
                    """
                )
                row = con.execute(
                    """
                    SELECT total_requests, cache_hits, golden_hits, llm_calls,
                           audio_transcriptions, rate_limited_blocks, input_tokens, output_tokens
                    FROM monthly_ledger
                    WHERE tenant_id = ? AND month_ref = ?
                    """,
                    (self.tenant_id, self.current_month),
                ).fetchone()
                if row:
                    self.total_requests = int(row[0] or 0)
                    self.cache_hits = int(row[1] or 0)
                    self.golden_deterministic_hits = int(row[2] or 0)
                    self.llm_calls = int(row[3] or 0)
                    self.audio_transcriptions = int(row[4] or 0)
                    self.rate_limited_blocks = int(row[5] or 0)
                    self.estimated_input_tokens = int(row[6] or 0)
                    self.estimated_output_tokens = int(row[7] or 0)
        except Exception:
            pass

    def _flush_ledger_sqlite(self) -> None:
        if self.sqlite_path is None:
            return
        try:
            with sqlite3.connect(str(self.sqlite_path), timeout=3.0) as con:
                con.execute(
                    """
                    INSERT INTO monthly_ledger (
                        tenant_id, month_ref, total_requests, cache_hits, golden_hits,
                        llm_calls, audio_transcriptions, rate_limited_blocks, input_tokens, output_tokens
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(tenant_id, month_ref) DO UPDATE SET
                        total_requests = excluded.total_requests,
                        cache_hits = excluded.cache_hits,
                        golden_hits = excluded.golden_hits,
                        llm_calls = excluded.llm_calls,
                        audio_transcriptions = excluded.audio_transcriptions,
                        rate_limited_blocks = excluded.rate_limited_blocks,
                        input_tokens = excluded.input_tokens,
                        output_tokens = excluded.output_tokens
                    """,
                    (
                        self.tenant_id,
                        self.current_month,
                        self.total_requests,
                        self.cache_hits,
                        self.golden_deterministic_hits,
                        self.llm_calls,
                        self.audio_transcriptions,
                        self.rate_limited_blocks,
                        self.estimated_input_tokens,
                        self.estimated_output_tokens,
                    ),
                )
        except Exception:
            pass

    def _reset_month_if_needed(self) -> None:
        now_m = datetime.now().strftime("%Y-%m")
        if now_m != self.current_month:
            self.current_month = now_m
            self.total_requests = 0
            self.cache_hits = 0
            self.golden_deterministic_hits = 0
            self.llm_calls = 0
            self.audio_transcriptions = 0
            self.rate_limited_blocks = 0
            self.estimated_input_tokens = 0
            self.estimated_output_tokens = 0
            self._flush_ledger_sqlite()

    def check_rate_limit(
        self,
        user_identifier: Optional[str],
        channel: str = "web",
    ) -> Tuple[bool, Optional[str]]:
        """
        Compatibilidade direta com chamadas que esperam `(allowed, message)`.
        """
        allowed, msg, _ = self.check_whatsapp_rate_limit(user_identifier, channel=channel)
        return allowed, msg

    def check_whatsapp_rate_limit(
        self,
        user_identifier: Optional[str],
        channel: str = "whatsapp",
    ) -> Tuple[bool, Optional[str], bool]:
        """
        Verifica o limite diário de 20 consultas/dia por telefone no WhatsApp (Cláusula X.3).
        Retorna `(allowed, friendly_message, should_send_whatsapp_notice)`.
        Na 21ª tentativa do dia, `should_send_whatsapp_notice` é `True` para que o webhook
        envie uma única mensagem educada ao cidadão. Da 22ª em diante no mesmo dia, retorna
        `should_send_whatsapp_notice = False` para evitar loops com bots.
        """
        if not user_identifier or channel.lower() != "whatsapp":
            return True, None, False

        today = datetime.now().strftime("%Y-%m-%d")
        with self._lock:
            self._reset_month_if_needed()
            day_str, count, notified = self._daily_user_counts.get(user_identifier, (today, 0, False))
            if day_str != today:
                count = 0
                notified = False

            if count == 0 and not notified and self.sqlite_path is not None:
                try:
                    with sqlite3.connect(str(self.sqlite_path), timeout=2.0) as con:
                        row = con.execute(
                            "SELECT msg_count, notified FROM whatsapp_daily_limits WHERE tenant_id = ? AND user_id = ? AND day_ref = ?",
                            (self.tenant_id, user_identifier, today),
                        ).fetchone()
                        if row:
                            count = int(row[0] or 0)
                            notified = bool(row[1])
                except Exception:
                    pass

            new_count = count + 1
            if count >= self.daily_user_limit_whatsapp:
                self.rate_limited_blocks += 1
                should_notify = not notified
                new_notified = True
                allowed = False
                friendly_msg = (
                    f"*Ouvidoria e Transparência Municipal:* Olá! Você atingiu o limite diário gratuito de "
                    f"*{self.daily_user_limit_whatsapp} consultas por dia* neste número de WhatsApp (proteção contra uso automatizado — Cláusula X.3). "
                    f"Você poderá enviar novas perguntas por aqui amanhã, ou acessar o Portal Institucional da Transparência Municipal para consultas ilimitadas."
                )
            else:
                should_notify = False
                new_notified = notified
                allowed = True
                friendly_msg = None

            self._daily_user_counts[user_identifier] = (today, new_count, new_notified)
            if self.sqlite_path is not None:
                try:
                    with sqlite3.connect(str(self.sqlite_path), timeout=2.0) as con:
                        con.execute(
                            """
                            INSERT INTO whatsapp_daily_limits (tenant_id, user_id, day_ref, msg_count, notified)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(tenant_id, user_id, day_ref) DO UPDATE SET
                                msg_count = excluded.msg_count,
                                notified = excluded.notified
                            """,
                            (self.tenant_id, user_identifier, today, new_count, 1 if new_notified else 0),
                        )
                except Exception:
                    pass
            self._flush_ledger_sqlite()
            return allowed, friendly_msg, should_notify

    def record_audio_transcription(self) -> None:
        with self._lock:
            self._reset_month_if_needed()
            self.audio_transcriptions += 1
            self._flush_ledger_sqlite()

    @staticmethod
    def _is_payload_cache_hygienic(
        payload: Dict[str, Any],
        is_contextual_followup: bool = False,
    ) -> bool:
        """
        Eixo 7.2 — Higiene Estrita do Cache (`query_cache`):
        Jamais armazena ou reutiliza respostas de:
        - Follow-ups dependentes de contexto conversacional (`is_contextual_followup == True`);
        - Consultas analíticas (`sql_analytics` / `hybrid_sql_rag`) que retornaram `0 linhas`;
        - Fallbacks de erro, bloqueios de segurança ou pedidos de esclarecimento.
        """
        if is_contextual_followup or payload.get("is_contextual_followup"):
            return False
        if payload.get("status") != "sucesso":
            return False
        intent = str(payload.get("intent") or "")
        if intent in ("conversational_followup", "clarification_needed", "blocked_security"):
            return False
        if intent in ("sql_analytics", "hybrid_sql_rag"):
            rows = payload.get("sql_rows")
            if not isinstance(rows, list) or len(rows) == 0:
                return False
        return True

    def get_cached_response(
        self,
        question: str,
        role: str = "citizen",
        allowed_sources: Optional[List[str]] = None,
        explicit_tables: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Trava 1 (Cláusula X.2): Retorna resposta em cache (< 2ms) a custo R$ 0,00 de IA,
        validando a higiene estrita do payload armazenado.
        """
        key = _normalize_cache_key(question, role, allowed_sources, explicit_tables)
        now = time.time()
        with self._lock:
            self._reset_month_if_needed()
            item = self._cache.get(key)
            if item is not None:
                ts, payload = item
                if (now - ts) <= self.cache_ttl_sec and self._is_payload_cache_hygienic(payload):
                    self._cache.move_to_end(key)
                    self.total_requests += 1
                    self.cache_hits += 1
                    self._flush_ledger_sqlite()
                    cached_copy = dict(payload)
                    cached_copy["cache_hit"] = True
                    cached_copy["tokens_consumed"] = 0
                    return cached_copy
                else:
                    del self._cache[key]

            if self.sqlite_path is not None:
                try:
                    with sqlite3.connect(str(self.sqlite_path), timeout=2.0) as con:
                        row = con.execute(
                            "SELECT created_ts, payload_json FROM query_cache WHERE cache_key = ?",
                            (key,),
                        ).fetchone()
                        if row and (now - float(row[0])) <= self.cache_ttl_sec:
                            payload = json.loads(row[1])
                            if not self._is_payload_cache_hygienic(payload):
                                con.execute("DELETE FROM query_cache WHERE cache_key = ?", (key,))
                                return None
                            self._cache[key] = (float(row[0]), payload)
                            self.total_requests += 1
                            self.cache_hits += 1
                            self._flush_ledger_sqlite()
                            cached_copy = dict(payload)
                            cached_copy["cache_hit"] = True
                            cached_copy["tokens_consumed"] = 0
                            return cached_copy
                except Exception:
                    pass
            return None

    def record_realtime_llm_tokens(
        self,
        prompt_tokens: int,
        completion_tokens: int,
    ) -> None:
        """
        Eixo 8.1: Debita em tempo real os tokens reais consumidos (OpenAI `usage` / Gemini `usageMetadata`)
        em chamadas individuais de Guardrail, Text-to-SQL (incluindo tentativas de retry) e Síntese.
        """
        p_tok = max(0, int(prompt_tokens or 0))
        c_tok = max(0, int(completion_tokens or 0))
        if p_tok == 0 and c_tok == 0:
            return
        with self._lock:
            self._reset_month_if_needed()
            self.estimated_input_tokens += p_tok
            self.estimated_output_tokens += c_tok
            self._flush_ledger_sqlite()

    def store_response_and_record_usage(
        self,
        question: str,
        response_payload: Dict[str, Any],
        role: str = "citizen",
        allowed_sources: Optional[List[str]] = None,
        explicit_tables: Optional[List[str]] = None,
        input_tokens: int = 0,
        output_tokens: int = 0,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        sql_executed: Optional[str] = None,
        used_external_llm: bool = False,
        cacheable: bool = True,
        is_contextual_followup: bool = False,
        tokens_already_debited_realtime: bool = False,
        **kwargs: Any,
    ) -> None:
        eff_in = int(input_tokens or prompt_tokens or 0)
        eff_out = int(output_tokens or completion_tokens or 0)
        eff_ext = used_external_llm or (eff_in > 0) or (eff_out > 0)
        key = _normalize_cache_key(question, role, allowed_sources, explicit_tables)
        now = time.time()
        with self._lock:
            self._reset_month_if_needed()
            self.total_requests += 1
            if eff_ext:
                self.llm_calls += 1
                if not tokens_already_debited_realtime:
                    self.estimated_input_tokens += eff_in
                    self.estimated_output_tokens += eff_out
            else:
                self.golden_deterministic_hits += 1

            if cacheable and self._is_payload_cache_hygienic(
                response_payload, is_contextual_followup=is_contextual_followup
            ):
                snap = json.loads(json.dumps(response_payload, ensure_ascii=False, default=str))
                self._cache[key] = (now, snap)
                self._cache.move_to_end(key)
                while len(self._cache) > self.cache_max_entries:
                    self._cache.popitem(last=False)
                if self.sqlite_path is not None:
                    try:
                        with sqlite3.connect(str(self.sqlite_path), timeout=2.0) as con:
                            con.execute(
                                """
                                INSERT OR REPLACE INTO query_cache (cache_key, created_ts, payload_json)
                                VALUES (?, ?, ?)
                                """,
                                (key, now, json.dumps(snap, ensure_ascii=False, default=str)),
                            )
                    except Exception:
                        pass
            self._flush_ledger_sqlite()

    def record_usage(
        self,
        prompt_tokens: int = 1650,
        completion_tokens: int = 450,
        cached: bool = False,
    ) -> None:
        with self._lock:
            self._reset_month_if_needed()
            self.total_requests += 1
            if cached:
                self.cache_hits += 1
            else:
                self.llm_calls += 1
                self.estimated_input_tokens += max(100, prompt_tokens)
                self.estimated_output_tokens += max(50, completion_tokens)
            self._flush_ledger_sqlite()

    def record_audio_usage(self, seconds_estimated: int = 30) -> None:
        with self._lock:
            self._reset_month_if_needed()
            self.audio_transcriptions += 1
            self._flush_ledger_sqlite()

    def check_quota_available(self) -> Dict[str, Any]:
        """
        Verifica se o tenant ainda possui franquia mensal disponível para chamadas externas de LLM.
        """
        with self._lock:
            self._reset_month_if_needed()
            used_pct = round((self.llm_calls / max(1, self.monthly_quota_requests)) * 100.0, 2)
            allowed = self.llm_calls < self.monthly_quota_requests
            return {
                "allowed": allowed,
                "allowed_external_llm": allowed,
                "quota_used_pct": used_pct,
                "remaining_quota_requests": max(0, self.monthly_quota_requests - self.llm_calls),
            }

    def get_quota_status(self) -> Dict[str, Any]:
        with self._lock:
            self._reset_month_if_needed()
            used_pct = round((self.llm_calls / max(1, self.monthly_quota_requests)) * 100.0, 2)
            zero_cost_hits = self.cache_hits + self.golden_deterministic_hits
            cache_hit_rate_pct = round((zero_cost_hits / max(1, self.total_requests)) * 100.0, 1)
            cost_in = (self.estimated_input_tokens / 1000.0) * self.cost_per_1k_input_brl
            cost_out = (self.estimated_output_tokens / 1000.0) * self.cost_per_1k_output_brl
            cost_audio = self.audio_transcriptions * self.cost_per_audio_call_brl
            total_cost_brl = round(cost_in + cost_out + cost_audio, 2)
            saved_brl = round(
                zero_cost_hits * ((4.1 * self.cost_per_1k_input_brl) + (0.32 * self.cost_per_1k_output_brl)),
                2,
            )
            gross_margin_pct = round(
                ((self.monthly_price_brl - total_cost_brl) / max(1.0, self.monthly_price_brl)) * 100.0,
                2,
            )

            if used_pct >= 100.0:
                alert_level = "CRITICAL_OVERAGE_FALLBACK"
                alert_msg = (
                    "Franquia mensal atingiu 100%. Consultas adicionais operam em modo híbrido otimizado/cache "
                    "ou mediante lote suplementar de 10.000 requisições (R$ 1.500,00 — Cláusula X.4)."
                )
            elif used_pct >= 85.0:
                alert_level = "WARNING_85_PCT"
                alert_msg = (
                    f"Notificação Preventiva (Cláusula X.4): Município atingiu {used_pct}% da franquia mensal "
                    f"contratada ({self.monthly_quota_requests} interações IA)."
                )
            else:
                alert_level = "HEALTHY"
                alert_msg = "Consumo de tokens dentro da franquia mensal inclusa no contrato."

            return {
                "tenant_id": self.tenant_id,
                "plan_code": self.plan_code,
                "plan_name": self.plan_name,
                "monthly_price_brl": self.monthly_price_brl,
                "setup_price_brl": self.setup_price_brl,
                "gross_margin_pct": gross_margin_pct,
                "reference_month": self.current_month,
                "monthly_quota_requests": self.monthly_quota_requests,
                "total_requests": self.total_requests,
                "billable_llm_calls": self.llm_calls,
                "cache_hits": self.cache_hits,
                "golden_deterministic_hits": self.golden_deterministic_hits,
                "audio_transcriptions": self.audio_transcriptions,
                "cache_hit_rate_pct": cache_hit_rate_pct,
                "quota_used_pct": used_pct,
                "remaining_quota_requests": max(0, self.monthly_quota_requests - self.llm_calls),
                "estimated_input_tokens": self.estimated_input_tokens,
                "estimated_output_tokens": self.estimated_output_tokens,
                "estimated_cost_brl": total_cost_brl,
                "estimated_savings_from_cache_brl": saved_brl,
                "whatsapp_daily_limit_per_user": self.daily_user_limit_whatsapp,
                "rate_limited_blocks": self.rate_limited_blocks,
                "persistence_engine": "SQLite + L1 Memory LRU" if self.sqlite_path else "Memory LRU",
                "available_commercial_plans": COMMERCIAL_PLANS,
                "available_plans": COMMERCIAL_PLANS,
                "contracted_plan": {
                    "plan_key": self.plan_code,
                    "name": self.plan_name,
                    "monthly_price_brl": self.monthly_price_brl,
                    "monthly_queries_included": self.monthly_quota_requests,
                },
                "current_month_usage": {
                    "total_queries": self.total_requests,
                    "llm_External_calls": self.llm_calls,
                    "cached_queries_zero_cost": zero_cost_hits,
                    "audio_transcriptions": self.audio_transcriptions,
                    "queries_usage_pct": used_pct,
                    "estimated_api_cost_brl": total_cost_brl,
                    "gross_margin_estimated_pct": gross_margin_pct,
                },
                "semantic_cache": {
                    "cache_hits_saved_tokens": zero_cost_hits,
                    "persistence_engine": "SQLite + L1 Memory LRU" if self.sqlite_path else "Memory LRU",
                    "entries_cached": len(self._cache),
                },
                "alert_level": alert_level,
                "alert_message": alert_msg,
            }
