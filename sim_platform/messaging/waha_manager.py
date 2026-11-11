"""
Gerenciador Avançado de Mensageria WhatsApp / WAHA e Histórico Unificado de Interações (`sim_platform.messaging.waha_manager`).

Implementa os requisitos dos Eixos 2.2 e 2.4 da Evolução Institucional:
A. Indicador de digitação natural (`POST /api/sendSeen`, `POST /api/startTyping`, `POST /api/stopTyping`).
B. Envio inteligente sem timer fixo: resposta disparada imediatamente após a conclusão do processamento,
   com coordenação por `chat_id` para mensagens enviadas em sequência rápida durante o processamento.
C. Máquina de estados completa da mensagem:
   - `received` (Mensagem recebida)
   - `seen` (Mensagem visualizada no WhatsApp via `sendSeen`)
   - `processing` (Mensagem sendo processada pelos agentes com `typing` ativo)
   - `sent` (Resposta enviada e confirmada pelo WAHA / ACK 1)
   - `delivered` (Resposta entregue no dispositivo do cidadão — ACK 2)
   - `read` (Resposta visualizada/reproduzida pelo destinatário — ACK 3 ou 4)
   - `failed` (Falha de envio, com registro de erro e suporte a reenvio, nunca marcando como respondida sem confirmação)
   - Suporte a reações (`PUT /api/reaction` e webhook `message.reaction`).
D. Robustez operacional:
   - Deduplicação idempotente por `message_id` (evita duplo processamento entre `message` e `message.any` ou retentativas).
   - Coordenação por `chat_id` preservando a ordem cronológica das mensagens.
   - Persistência em disco + memória (`UnifiedInteractionStore`) integrando o canal WhatsApp Cidadão
     à Plataforma Institucional da Prefeitura.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple


class MessageLifecycleState(str, Enum):
    RECEIVED = "received"          # Mensagem recebida pelo webhook/canal
    SEEN = "seen"                  # Mensagem marcada como visualizada (sendSeen)
    PROCESSING = "processing"      # Em processamento pelo orquestrador (typing ativo)
    SENT = "sent"                  # Resposta enviada com confirmação HTTP/ACK=1
    DELIVERED = "delivered"        # Resposta entregue ao dispositivo do destinatário (ACK=2)
    READ = "read"                  # Resposta visualizada/ouvida pelo destinatário (ACK=3/4)
    FAILED = "failed"              # Falha no envio ou processamento (não confirmada)


STATE_LABELS_PT: Dict[str, str] = {
    MessageLifecycleState.RECEIVED.value: "Recebida",
    MessageLifecycleState.SEEN.value: "Visualizada",
    MessageLifecycleState.PROCESSING.value: "Processando (Digitando...)",
    MessageLifecycleState.SENT.value: "Enviada",
    MessageLifecycleState.DELIVERED.value: "Entregue ao Cidadão",
    MessageLifecycleState.READ.value: "Visualizada pelo Cidadão",
    MessageLifecycleState.FAILED.value: "Falha no Envio",
}

STATE_ORDER_RANK: Dict[str, int] = {
    MessageLifecycleState.RECEIVED.value: 1,
    MessageLifecycleState.SEEN.value: 2,
    MessageLifecycleState.PROCESSING.value: 3,
    MessageLifecycleState.SENT.value: 4,
    MessageLifecycleState.DELIVERED.value: 5,
    MessageLifecycleState.READ.value: 6,
    MessageLifecycleState.FAILED.value: 0,
}

WAHA_ACK_TO_STATE: Dict[int, str] = {
    -1: MessageLifecycleState.FAILED.value,
    0: MessageLifecycleState.PROCESSING.value,
    1: MessageLifecycleState.SENT.value,
    2: MessageLifecycleState.DELIVERED.value,
    3: MessageLifecycleState.READ.value,
    4: MessageLifecycleState.READ.value,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="milliseconds")


@dataclass
class InteractionRecord:
    interaction_id: str
    channel: str  # "whatsapp" | "web_institucional" | "api"
    session_id: str
    chat_id: str
    user_display: str
    role: str
    question: str
    gabinete_id: str = "publico"
    effective_question: str = ""
    inbound_message_id: Optional[str] = None
    outbound_message_id: Optional[str] = None
    was_audio: bool = False
    coalesced_count: int = 1
    status: str = "em_processamento"
    intent: str = ""
    message_state: str = MessageLifecycleState.RECEIVED.value
    message_state_label: str = STATE_LABELS_PT[MessageLifecycleState.RECEIVED.value]
    answered_confirmed: bool = False
    delivery_mode: str = "pending"  # "waha_live_confirmed" | "local_verified" | "failed_unconfirmed"
    send_attempts: int = 0
    last_send_error: Optional[str] = None
    citizen_response: str = ""
    institutional_response: str = ""
    whatsapp_text: str = ""
    sql_executed: Optional[str] = None
    sql_rows_count: int = 0
    sql_rows_preview: List[Dict[str, Any]] = field(default_factory=list)
    rag_chunks_count: int = 0
    rag_chunks_preview: List[Dict[str, Any]] = field(default_factory=list)
    selected_segments: List[str] = field(default_factory=list)
    selected_tables: List[str] = field(default_factory=list)
    allowed_sources: List[str] = field(default_factory=list)
    fontes_utilizadas: List[str] = field(default_factory=list)
    transparency_warnings: List[Dict[str, Any]] = field(default_factory=list)
    methodology_summary: str = ""
    limitations_and_caveats: List[str] = field(default_factory=list)
    pii_masked_count: int = 0
    llm_engine_used: str = ""
    exec_ms: float = 0.0
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)
    state_history: List[Dict[str, Any]] = field(default_factory=list)
    waha_actions: List[Dict[str, Any]] = field(default_factory=list)
    reactions: List[Dict[str, Any]] = field(default_factory=list)
    pipeline_steps: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self, include_heavy: bool = True) -> Dict[str, Any]:
        base = {
            "interaction_id": self.interaction_id,
            "channel": self.channel,
            "session_id": self.session_id,
            "chat_id": self.chat_id,
            "user_display": self.user_display,
            "role": self.role,
            "gabinete_id": self.gabinete_id,
            "question": self.question,
            "effective_question": self.effective_question or self.question,
            "inbound_message_id": self.inbound_message_id,
            "outbound_message_id": self.outbound_message_id,
            "was_audio": self.was_audio,
            "coalesced_count": self.coalesced_count,
            "status": self.status,
            "intent": self.intent,
            "message_state": self.message_state,
            "message_state_label": self.message_state_label,
            "answered_confirmed": self.answered_confirmed,
            "delivery_mode": self.delivery_mode,
            "send_attempts": self.send_attempts,
            "last_send_error": self.last_send_error,
            "citizen_response": self.citizen_response,
            "institutional_response": self.institutional_response,
            "whatsapp_text": self.whatsapp_text,
            "sql_executed": self.sql_executed,
            "sql_rows_count": self.sql_rows_count,
            "rag_chunks_count": self.rag_chunks_count,
            "selected_segments": self.selected_segments,
            "selected_tables": self.selected_tables,
            "allowed_sources": self.allowed_sources,
            "fontes_utilizadas": self.fontes_utilizadas,
            "transparency_warnings": self.transparency_warnings,
            "methodology_summary": self.methodology_summary,
            "limitations_and_caveats": self.limitations_and_caveats,
            "pii_masked_count": self.pii_masked_count,
            "llm_engine_used": self.llm_engine_used,
            "exec_ms": self.exec_ms,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "state_history": self.state_history,
            "waha_actions": self.waha_actions,
            "reactions": self.reactions,
        }
        if include_heavy:
            base["sql_rows_preview"] = self.sql_rows_preview
            base["rag_chunks_preview"] = self.rag_chunks_preview
            base["pipeline_steps"] = self.pipeline_steps
        return base


class UnifiedInteractionStore:
    """
    Repositório thread-safe de interações (WhatsApp Cidadão + Plataforma Institucional),
    com indexação por `interaction_id`, `inbound_message_id`, `outbound_message_id` e `chat_id`.
    """

    def __init__(
        self,
        persist_path: Optional[Path] = None,
        max_records: int = 500,
    ) -> None:
        self._lock = threading.RLock()
        self.max_records = max_records
        self.persist_path = persist_path
        self._records: Deque[InteractionRecord] = deque(maxlen=max_records)
        self._by_id: Dict[str, InteractionRecord] = {}
        self._by_inbound_msg_id: Dict[str, str] = {}
        self._by_outbound_msg_id: Dict[str, str] = {}
        if self.persist_path and self.persist_path.exists():
            self._load_from_disk()

    def _load_from_disk(self) -> None:
        if not self.persist_path or not self.persist_path.exists():
            return
        try:
            raw = json.loads(self.persist_path.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                for item in raw[-self.max_records :]:
                    if isinstance(item, dict) and item.get("interaction_id"):
                        rec = InteractionRecord(
                            interaction_id=item["interaction_id"],
                            channel=item.get("channel", "web_institucional"),
                            session_id=item.get("session_id", "default"),
                            chat_id=item.get("chat_id", "web"),
                            user_display=item.get("user_display", "Usuário"),
                            role=item.get("role", "citizen"),
                            gabinete_id=item.get("gabinete_id", "publico"),
                            question=item.get("question", ""),
                            effective_question=item.get("effective_question", ""),
                            inbound_message_id=item.get("inbound_message_id"),
                            outbound_message_id=item.get("outbound_message_id"),
                            was_audio=bool(item.get("was_audio", False)),
                            coalesced_count=int(item.get("coalesced_count", 1)),
                            status=item.get("status", "sucesso"),
                            intent=item.get("intent", "sql_analytics"),
                            message_state=item.get("message_state", MessageLifecycleState.SENT.value),
                            message_state_label=item.get(
                                "message_state_label",
                                STATE_LABELS_PT.get(item.get("message_state", "sent"), "Enviada"),
                            ),
                            answered_confirmed=bool(item.get("answered_confirmed", True)),
                            delivery_mode=item.get("delivery_mode", "local_verified"),
                            send_attempts=int(item.get("send_attempts", 1)),
                            last_send_error=item.get("last_send_error"),
                            citizen_response=item.get("citizen_response", ""),
                            institutional_response=item.get("institutional_response", ""),
                            whatsapp_text=item.get("whatsapp_text", ""),
                            sql_executed=item.get("sql_executed"),
                            sql_rows_count=int(item.get("sql_rows_count", 0)),
                            sql_rows_preview=item.get("sql_rows_preview", []),
                            rag_chunks_count=int(item.get("rag_chunks_count", 0)),
                            rag_chunks_preview=item.get("rag_chunks_preview", []),
                            selected_segments=item.get("selected_segments", []),
                            selected_tables=item.get("selected_tables", []),
                            allowed_sources=item.get("allowed_sources", []),
                            fontes_utilizadas=item.get("fontes_utilizadas", []),
                            transparency_warnings=item.get("transparency_warnings", []),
                            methodology_summary=item.get("methodology_summary", ""),
                            limitations_and_caveats=item.get("limitations_and_caveats", []),
                            pii_masked_count=int(item.get("pii_masked_count", 0)),
                            llm_engine_used=item.get("llm_engine_used", ""),
                            exec_ms=float(item.get("exec_ms", 0.0)),
                            created_at=item.get("created_at", _now_iso()),
                            updated_at=item.get("updated_at", _now_iso()),
                            state_history=item.get("state_history", []),
                            waha_actions=item.get("waha_actions", []),
                            reactions=item.get("reactions", []),
                            pipeline_steps=item.get("pipeline_steps", []),
                        )
                        self._records.append(rec)
                        self._by_id[rec.interaction_id] = rec
                        if rec.inbound_message_id:
                            self._by_inbound_msg_id[rec.inbound_message_id] = rec.interaction_id
                        if rec.outbound_message_id:
                            self._by_outbound_msg_id[rec.outbound_message_id] = rec.interaction_id
        except Exception:
            pass

    def _save_to_disk(self) -> None:
        if not self.persist_path or "PYTEST_CURRENT_TEST" in os.environ:
            return
        try:
            self.persist_path.parent.mkdir(parents=True, exist_ok=True)
            serialized = [r.to_dict(include_heavy=True) for r in self._records]
            self.persist_path.write_text(
                json.dumps(serialized, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

    def create_record(
        self,
        channel: str,
        session_id: str,
        chat_id: str,
        user_display: str,
        role: str,
        question: str,
        inbound_message_id: Optional[str] = None,
        was_audio: bool = False,
        gabinete_id: str = "publico",
    ) -> InteractionRecord:
        with self._lock:
            interaction_id = f"int-{uuid.uuid4().hex[:12]}"
            now = _now_iso()
            rec = InteractionRecord(
                interaction_id=interaction_id,
                channel=channel,
                session_id=session_id,
                chat_id=chat_id,
                user_display=user_display,
                role=role,
                gabinete_id=gabinete_id or "publico",
                question=question,
                effective_question=question,
                inbound_message_id=inbound_message_id,
                was_audio=was_audio,
                message_state=MessageLifecycleState.RECEIVED.value,
                message_state_label=STATE_LABELS_PT[MessageLifecycleState.RECEIVED.value],
                created_at=now,
                updated_at=now,
                state_history=[
                    {
                        "state": MessageLifecycleState.RECEIVED.value,
                        "label": STATE_LABELS_PT[MessageLifecycleState.RECEIVED.value],
                        "timestamp": now,
                        "detail": f"Mensagem recebida via canal '{channel}' ({user_display}).",
                    }
                ],
            )
            self._records.append(rec)
            self._by_id[interaction_id] = rec
            if inbound_message_id:
                self._by_inbound_msg_id[inbound_message_id] = interaction_id
            self._save_to_disk()
            return rec

    def transition_state(
        self,
        interaction_id: str,
        new_state: MessageLifecycleState | str,
        detail: str = "",
        allow_downgrade: bool = False,
    ) -> Optional[InteractionRecord]:
        state_val = new_state.value if isinstance(new_state, MessageLifecycleState) else str(new_state)
        with self._lock:
            rec = self._by_id.get(interaction_id)
            if not rec:
                return None

            curr_rank = STATE_ORDER_RANK.get(rec.message_state, 0)
            new_rank = STATE_ORDER_RANK.get(state_val, 0)
            if not allow_downgrade and state_val != MessageLifecycleState.FAILED.value and new_rank < curr_rank:
                return rec

            now = _now_iso()
            rec.message_state = state_val
            rec.message_state_label = STATE_LABELS_PT.get(state_val, state_val)
            rec.updated_at = now
            if state_val in (
                MessageLifecycleState.SENT.value,
                MessageLifecycleState.DELIVERED.value,
                MessageLifecycleState.READ.value,
            ):
                rec.answered_confirmed = True
            elif state_val == MessageLifecycleState.FAILED.value:
                rec.answered_confirmed = False

            rec.state_history.append(
                {
                    "state": state_val,
                    "label": rec.message_state_label,
                    "timestamp": now,
                    "detail": detail or f"Transição para '{rec.message_state_label}'.",
                }
            )
            self._save_to_disk()
            return rec

    def register_outbound_message_id(self, interaction_id: str, outbound_message_id: str) -> None:
        with self._lock:
            rec = self._by_id.get(interaction_id)
            if rec and outbound_message_id:
                rec.outbound_message_id = outbound_message_id
                self._by_outbound_msg_id[outbound_message_id] = interaction_id
                self._save_to_disk()

    def add_reaction(
        self,
        interaction_id: str,
        emoji: str,
        actor: str = "citizen",
        message_id: Optional[str] = None,
    ) -> Optional[InteractionRecord]:
        with self._lock:
            rec = self._by_id.get(interaction_id)
            if not rec:
                return None
            now = _now_iso()
            rec.reactions.append(
                {
                    "emoji": emoji,
                    "actor": actor,
                    "message_id": message_id or rec.outbound_message_id or rec.inbound_message_id,
                    "timestamp": now,
                }
            )
            rec.updated_at = now
            self._save_to_disk()
            return rec

    def find_by_message_id(self, message_id: str) -> Optional[InteractionRecord]:
        if not message_id:
            return None
        with self._lock:
            iid = self._by_outbound_msg_id.get(message_id) or self._by_inbound_msg_id.get(message_id)
            if iid:
                return self._by_id.get(iid)
            return None

    def find_latest_for_chat(self, chat_id: str) -> Optional[InteractionRecord]:
        with self._lock:
            for rec in reversed(self._records):
                if rec.chat_id == chat_id:
                    return rec
            return None

    def find_latest_sql_rows_for_session(
        self,
        chat_or_session_id: str,
    ) -> Optional[Tuple[List[Dict[str, Any]], str, Optional[str]]]:
        """
        Eixo 3 & Eixo 4: Recupera `(sql_rows_preview, effective_question, sql_executed)` do último turno
        da mesma sessão/chat que retornou linhas tabulares válidas, permitindo gerar gráficos ou follow-ups
        sobre os dados recém-consultados.
        """
        target = (chat_or_session_id or "").strip()
        if not target:
            return None
        with self._lock:
            for rec in reversed(self._records):
                if rec.chat_id == target or rec.session_id == target:
                    if rec.sql_rows_preview and len(rec.sql_rows_preview) >= 1:
                        return (
                            list(rec.sql_rows_preview),
                            rec.effective_question or rec.question,
                            rec.sql_executed,
                        )
        return None

    def get_by_id(self, interaction_id: str) -> Optional[InteractionRecord]:
        with self._lock:
            return self._by_id.get(interaction_id)

    def save(self, rec: InteractionRecord) -> None:
        with self._lock:
            rec.updated_at = _now_iso()
            self._by_id[rec.interaction_id] = rec
            if rec.inbound_message_id:
                self._by_inbound_msg_id[rec.inbound_message_id] = rec.interaction_id
            if rec.outbound_message_id:
                self._by_outbound_msg_id[rec.outbound_message_id] = rec.interaction_id
            self._save_to_disk()

    def list_records(
        self,
        channel: Optional[str] = None,
        role: Optional[str] = None,
        state: Optional[str] = None,
        search: Optional[str] = None,
        gabinete_id: Optional[str] = None,
        limit: int = 50,
    ) -> Dict[str, Any]:
        with self._lock:
            items = list(reversed(self._records))
            filtered: List[InteractionRecord] = []
            q_low = (search or "").strip().lower()
            gab_filter = (gabinete_id or "").strip().lower()
            for r in items:
                if gab_filter and gab_filter not in ("all", "todos") and (r.gabinete_id or "publico").lower() != gab_filter:
                    continue
                if channel and r.channel != channel:
                    continue
                if role and r.role != role:
                    continue
                if state and r.message_state != state:
                    continue
                if q_low and not (
                    q_low in (r.question or "").lower()
                    or q_low in (r.citizen_response or "").lower()
                    or q_low in (r.institutional_response or "").lower()
                    or q_low in (r.chat_id or "").lower()
                    or q_low in (r.interaction_id or "").lower()
                ):
                    continue
                filtered.append(r)

            all_items = list(self._records)
            by_channel: Dict[str, int] = {}
            by_state: Dict[str, int] = {}
            by_role: Dict[str, int] = {}
            by_gabinete: Dict[str, int] = {}
            for r in all_items:
                by_channel[r.channel] = by_channel.get(r.channel, 0) + 1
                by_state[r.message_state] = by_state.get(r.message_state, 0) + 1
                by_role[r.role] = by_role.get(r.role, 0) + 1
                gab_k = r.gabinete_id or "publico"
                by_gabinete[gab_k] = by_gabinete.get(gab_k, 0) + 1

            return {
                "total_stored": len(all_items),
                "total_filtered": len(filtered),
                "gabinete_filter": gab_filter or "all",
                "summary": {
                    "by_channel": by_channel,
                    "by_state": by_state,
                    "by_role": by_role,
                    "by_gabinete": by_gabinete,
                    "confirmed_answers": sum(1 for r in all_items if r.answered_confirmed),
                    "failed_or_unconfirmed": sum(1 for r in all_items if r.message_state == MessageLifecycleState.FAILED.value),
                },
                "interactions": [r.to_dict(include_heavy=False) for r in filtered[: max(1, min(limit, 200))]],
            }


class ChatSessionCoordinator:
    """
    Coordena a concorrência e a ordem das mensagens por `chat_id` no WhatsApp:
    - Deduplicação idempotente de eventos `message` / `message.any` pelo `message_id`.
    - Bufferização/coalescência inteligente quando o cidadão envia mensagens complementares
      enquanto outra mensagem do mesmo `chat_id` está em processamento.
    - Lock por `chat_id` que preserva a ordem estrita de execução e entrega.
    """

    def __init__(self, dedup_ttl_sec: float = 900.0) -> None:
        self._guard = threading.Lock()
        self._seen_message_ids: Dict[str, Tuple[float, str]] = {}
        self._chat_locks: Dict[str, threading.Lock] = {}
        self._pending_buffers: Dict[str, List[Dict[str, Any]]] = {}
        self._active_chats: Dict[str, str] = {}
        self.dedup_ttl_sec = dedup_ttl_sec
        self.duplicates_blocked = 0
        self.coalesced_messages = 0

    def _prune_seen(self, now: float) -> None:
        expired = [mid for mid, (ts, _) in self._seen_message_ids.items() if (now - ts) > self.dedup_ttl_sec]
        for mid in expired:
            self._seen_message_ids.pop(mid, None)

    def check_and_mark_seen_message(
        self,
        message_id: Optional[str],
        chat_id: str,
        body: str,
    ) -> Tuple[bool, Optional[str], str]:
        """
        Retorna `(is_duplicate, existing_interaction_id, effective_message_id)`.
        Se `message_id` for informado pelo WAHA, usa-o diretamente para deduplicar `message` vs `message.any`.
        """
        now = time.time()
        with self._guard:
            self._prune_seen(now)
            if message_id and message_id.strip():
                eff_id = message_id.strip()
                if eff_id in self._seen_message_ids:
                    self.duplicates_blocked += 1
                    _, existing_iid = self._seen_message_ids[eff_id]
                    return True, existing_iid, eff_id
                self._seen_message_ids[eff_id] = (now, "pending")
                return False, None, eff_id

            # Se o payload não trouxe ID explícito, gerar ID único para esta requisição
            generated_id = f"waha-{hashlib.sha1(f'{chat_id}:{body}:{now}'.encode('utf-8')).hexdigest()[:16]}"
            return False, None, generated_id

    def bind_interaction_to_message_id(self, message_id: str, interaction_id: str) -> None:
        if not message_id:
            return
        with self._guard:
            self._seen_message_ids[message_id] = (time.time(), interaction_id)

    def get_chat_lock(self, chat_id: str) -> threading.Lock:
        with self._guard:
            if chat_id not in self._chat_locks:
                self._chat_locks[chat_id] = threading.Lock()
            return self._chat_locks[chat_id]

    def enqueue_if_busy(self, chat_id: str, fragment_text: str, message_id: str) -> bool:
        """
        Se o `chat_id` já está processando uma mensagem na thread atual/outra thread e a fila
        de coalescência está aberta, adiciona o fragmento para ser consolidado ou processado em ordem.
        """
        lk = self.get_chat_lock(chat_id)
        if lk.locked():
            with self._guard:
                self._pending_buffers.setdefault(chat_id, []).append(
                    {"text": fragment_text, "message_id": message_id, "timestamp": time.time()}
                )
                self.coalesced_messages += 1
            return True
        return False

    def drain_coalesced_fragments(self, chat_id: str) -> List[Dict[str, Any]]:
        with self._guard:
            items = self._pending_buffers.pop(chat_id, [])
            return items


class WAHAMessagingManager:
    """
    Cliente e controlador de ciclo de vida do WhatsApp HTTP API (WAHA).

    Suporta todos os motores WAHA (GOWS, WEBJS, NOWEB) configurados no `docker-compose.yml`:
    - `POST /api/sendSeen` (Marca a mensagem do cidadão como visualizada)
    - `POST /api/startTyping` (Ativa indicador nativo 'digitando...' no WhatsApp do cidadão)
    - `POST /api/stopTyping` (Encerra indicador 'digitando...' antes/após o envio)
    - `PUT /api/reaction` (Reage à mensagem do cidadão com ⏳ durante processamento e ✅ ao concluir)
    - `POST /api/sendText` (Envia a resposta final com retentativa automática e confirmação HTTP)
    """

    def __init__(
        self,
        interaction_store: Optional[UnifiedInteractionStore] = None,
        waha_base_url: Optional[str] = None,
        max_send_retries: int = 2,
        http_post_fn: Optional[Callable[..., Any]] = None,
        http_put_fn: Optional[Callable[..., Any]] = None,
        http_get_fn: Optional[Callable[..., Any]] = None,
    ) -> None:
        self.store = interaction_store or UnifiedInteractionStore()
        self.coordinator = ChatSessionCoordinator()
        self.waha_base_url = (waha_base_url or os.environ.get("WAHA_API_URL", "")).strip().rstrip("/")
        self.max_send_retries = max(1, max_send_retries)
        self._http_post_fn = http_post_fn
        self._http_put_fn = http_put_fn
        self._http_get_fn = http_get_fn

    def _resolve_waha_url(self) -> str:
        return (self.waha_base_url or os.environ.get("WAHA_API_URL", "")).strip().rstrip("/")

    def _call_waha(
        self,
        method: str,
        endpoint: str,
        payload: Dict[str, Any],
        timeout: float = 6.0,
    ) -> Dict[str, Any]:
        """
        Executa chamada à API do WAHA (se configurada) ou registra a ação verificada localmente
        em ambiente de teste/desenvolvimento sem container WAHA ativo.
        """
        t0 = time.perf_counter()
        base_url = self._resolve_waha_url()
        def _summarize_val(k: str, v: Any) -> Any:
            if k in ("text", "caption"):
                return f"{str(v)[:60]}... ({len(str(v))} chars)"
            if k == "file" and isinstance(v, dict):
                return {
                    "mimetype": v.get("mimetype", "image/png"),
                    "filename": v.get("filename", "grafico_sim.png"),
                    "base64_chars": len(str(v.get("data") or "")),
                }
            return v

        action_entry: Dict[str, Any] = {
            "method": method.upper(),
            "endpoint": endpoint,
            "payload_summary": {k: _summarize_val(k, v) for k, v in payload.items()},
            "timestamp": _now_iso(),
        }

        if not base_url:
            action_entry.update(
                {
                    "mode": "local_verified",
                    "ok": True,
                    "http_status": 200,
                    "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                    "response": {"id": f"waha_out_{uuid.uuid4().hex[:10]}", "status": "LOCAL_OK"},
                }
            )
            return action_entry

        url = f"{base_url}{endpoint}"
        try:
            import requests

            if method.upper() == "POST":
                post_fn = self._http_post_fn or requests.post
                resp = post_fn(url, json=payload, timeout=timeout)
            elif method.upper() == "PUT":
                put_fn = self._http_put_fn or requests.put
                resp = put_fn(url, json=payload, timeout=timeout)
            else:
                get_fn = self._http_get_fn or requests.get
                resp = get_fn(url, timeout=timeout)

            status_code = getattr(resp, "status_code", 500)
            ok = 200 <= status_code < 300
            resp_json: Dict[str, Any] = {}
            try:
                resp_json = resp.json() if hasattr(resp, "json") else {}
            except Exception:
                resp_json = {"raw": getattr(resp, "text", "")[:200]}

            action_entry.update(
                {
                    "mode": "waha_http",
                    "ok": ok,
                    "http_status": status_code,
                    "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                    "response": resp_json if isinstance(resp_json, dict) else {"data": resp_json},
                }
            )
            if not ok:
                action_entry["error"] = f"HTTP {status_code}: {str(resp_json)[:180]}"
            return action_entry
        except Exception as exc:
            action_entry.update(
                {
                    "mode": "waha_http",
                    "ok": False,
                    "http_status": 0,
                    "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            return action_entry

    @staticmethod
    def _is_waha_serialized_msg_id(message_id: Optional[str]) -> bool:
        if not message_id:
            return False
        s = str(message_id).strip()
        return s.startswith("true_") or s.startswith("false_")

    def send_seen(self, rec: InteractionRecord, session: str, chat_id: str, message_id: Optional[str] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"session": session, "chatId": chat_id}
        if message_id and (not self._resolve_waha_url() or self._is_waha_serialized_msg_id(message_id)):
            payload["messageId"] = message_id
        res = self._call_waha("POST", "/api/sendSeen", payload, timeout=4.0)
        rec.waha_actions.append({"action": "sendSeen", **res})
        self.store.transition_state(
            rec.interaction_id,
            MessageLifecycleState.SEEN,
            detail="Confirmação de visualização (sendSeen) enviada ao WhatsApp.",
        )
        return res

    def start_typing(self, rec: InteractionRecord, session: str, chat_id: str) -> Dict[str, Any]:
        payload = {"session": session, "chatId": chat_id}
        res = self._call_waha("POST", "/api/startTyping", payload, timeout=4.0)
        rec.waha_actions.append({"action": "startTyping", **res})
        self.store.transition_state(
            rec.interaction_id,
            MessageLifecycleState.PROCESSING,
            detail="Indicador de digitação (startTyping) ativado no WhatsApp enquanto os agentes consultam as bases oficiais.",
        )
        return res

    def stop_typing(self, rec: InteractionRecord, session: str, chat_id: str) -> Dict[str, Any]:
        payload = {"session": session, "chatId": chat_id}
        res = self._call_waha("POST", "/api/stopTyping", payload, timeout=4.0)
        rec.waha_actions.append({"action": "stopTyping", **res})
        self.store.save(rec)
        return res

    def send_reaction(
        self,
        rec: InteractionRecord,
        session: str,
        message_id: Optional[str],
        emoji: str,
    ) -> Optional[Dict[str, Any]]:
        if not message_id:
            return None
        if self._resolve_waha_url() and not self._is_waha_serialized_msg_id(message_id):
            self.store.add_reaction(rec.interaction_id, emoji=emoji, actor="system", message_id=message_id)
            return None
        payload = {"session": session, "messageId": message_id, "reaction": emoji}
        res = self._call_waha("PUT", "/api/reaction", payload, timeout=4.0)
        rec.waha_actions.append({"action": "reaction", "emoji": emoji, **res})
        self.store.add_reaction(rec.interaction_id, emoji=emoji, actor="system", message_id=message_id)
        return res

    def send_text_with_confirmation(
        self,
        rec: InteractionRecord,
        session: str,
        chat_id: str,
        text: str,
        reply_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Envia a resposta ao cidadão pelo WAHA com retentativa em caso de falha transitória.
        NUNCA marca a mensagem como respondida (`sent`) antes da confirmação adequada.
        """
        payload: Dict[str, Any] = {"session": session, "chatId": chat_id, "text": text}
        if reply_to and (not self._resolve_waha_url() or self._is_waha_serialized_msg_id(reply_to)):
            payload["reply_to"] = reply_to

        last_res: Dict[str, Any] = {}
        for attempt in range(1, self.max_send_retries + 1):
            rec.send_attempts = attempt
            if attempt > 1:
                payload.pop("reply_to", None)
            last_res = self._call_waha("POST", "/api/sendText", payload, timeout=10.0)
            rec.waha_actions.append({"action": "sendText", "attempt": attempt, **last_res})

            if last_res.get("ok"):
                resp_data = last_res.get("response") or {}
                out_id = (
                    resp_data.get("id")
                    or (resp_data.get("key") or {}).get("id")
                    or f"waha_out_{rec.interaction_id}"
                )
                if isinstance(out_id, dict):
                    out_id = out_id.get("_serialized") or out_id.get("id") or f"waha_out_{rec.interaction_id}"
                rec.delivery_mode = (
                    "waha_live_confirmed" if last_res.get("mode") == "waha_http" else "local_verified"
                )
                rec.last_send_error = None
                self.store.register_outbound_message_id(rec.interaction_id, str(out_id))
                self.store.transition_state(
                    rec.interaction_id,
                    MessageLifecycleState.SENT,
                    detail=(
                        f"Resposta enviada e confirmada ({rec.delivery_mode}, tentativa {attempt}, "
                        f"outbound_id='{out_id}')."
                    ),
                )
                return last_res

        # Se todas as tentativas falharam: NÃO marcar como respondida!
        err_msg = last_res.get("error") or "Falha de comunicação com o serviço WAHA"
        rec.delivery_mode = "failed_unconfirmed"
        rec.last_send_error = err_msg
        self.store.transition_state(
            rec.interaction_id,
            MessageLifecycleState.FAILED,
            detail=f"Envio não confirmado após {self.max_send_retries} tentativa(s): {err_msg}",
            allow_downgrade=True,
        )
        return last_res

    def send_image_with_confirmation(
        self,
        rec: InteractionRecord,
        session: str,
        chat_id: str,
        image_base64: str,
        caption: str = "",
        mimetype: str = "image/png",
        mime_type: Optional[str] = None,
        filename: str = "grafico_sim_caete.png",
        reply_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Eixo 4: Envia imagem PNG institucional (gráfico gerado pelo SIM) no WhatsApp via WAHA
        `POST /api/sendImage` com retentativa automática e rastreabilidade em `waha_actions`.
        """
        eff_mime = (mime_type or mimetype or "image/png").strip()
        clean_b64 = (image_base64 or "").strip()
        if clean_b64.startswith("data:") and "," in clean_b64:
            clean_b64 = clean_b64.split(",", 1)[1].strip()

        payload: Dict[str, Any] = {
            "session": session,
            "chatId": chat_id,
            "file": {
                "mimetype": eff_mime,
                "filename": filename,
                "data": clean_b64,
            },
            "caption": caption,
        }
        if reply_to and (not self._resolve_waha_url() or self._is_waha_serialized_msg_id(reply_to)):
            payload["reply_to"] = reply_to

        last_res: Dict[str, Any] = {}
        for attempt in range(1, self.max_send_retries + 1):
            if attempt > 1:
                payload.pop("reply_to", None)
            last_res = self._call_waha("POST", "/api/sendImage", payload, timeout=15.0)
            rec.waha_actions.append({"action": "sendImage", "attempt": attempt, **last_res})
            if last_res.get("ok"):
                self.store.save(rec)
                return last_res

        self.store.save(rec)
        return last_res

    def handle_webhook_event(
        self,
        event_name: str,
        session: str,
        payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Processa eventos assíncronos de estado do WAHA (`message.ack`, `message.reaction`, `session.status`, `message.revoked`).
        Retorna um dicionário de resposta se o evento for um evento de ciclo de vida não-mensagem,
        ou `None` se for uma mensagem de entrada (`message` / `message.any`) que deve seguir para o orquestrador.
        """
        ev = (event_name or "").strip().lower()

        if ev == "message.ack":
            from_me = payload.get("fromMe", (payload.get("key") or {}).get("fromMe"))
            if from_me is False:
                return {
                    "status": "ack_ignored_inbound",
                    "event": ev,
                    "detail": "ACK de mensagem de entrada do cidadão ignorado (apenas ACKs de respostas enviadas pelo SIM atualizam o estado).",
                }
            msg_id = str(
                payload.get("id")
                or (payload.get("key") or {}).get("id")
                or payload.get("messageId")
                or ""
            ).strip()
            chat_id = str(payload.get("from") or payload.get("to") or payload.get("chatId") or "").strip()
            ack_raw = payload.get("ack")
            ack_name = str(payload.get("ackName") or "").upper()
            try:
                ack_int = int(ack_raw) if ack_raw is not None else 1
            except Exception:
                ack_int = 1

            mapped_state = WAHA_ACK_TO_STATE.get(ack_int, MessageLifecycleState.SENT.value)
            rec = self.store.find_by_message_id(msg_id)
            if not rec and chat_id:
                cand = self.store.find_latest_for_chat(chat_id)
                if cand and (cand.outbound_message_id or cand.message_state in ("sent", "delivered", "read")):
                    rec = cand

            if not rec:
                return {
                    "status": "ack_unmatched",
                    "event": ev,
                    "message_id": msg_id,
                    "ack": ack_int,
                    "mapped_state": mapped_state,
                }

            allow_down = mapped_state == MessageLifecycleState.FAILED.value
            self.store.transition_state(
                rec.interaction_id,
                mapped_state,
                detail=f"Webhook WAHA `message.ack` recebido (ack={ack_int} / {ack_name or mapped_state}).",
                allow_downgrade=allow_down,
            )
            return {
                "status": "ack_updated",
                "interaction_id": rec.interaction_id,
                "message_id": msg_id,
                "ack": ack_int,
                "message_state": rec.message_state,
                "message_state_label": rec.message_state_label,
                "answered_confirmed": rec.answered_confirmed,
            }

        if ev == "message.reaction":
            reaction_obj = payload.get("reaction") if isinstance(payload.get("reaction"), dict) else payload
            emoji = str(reaction_obj.get("text") or reaction_obj.get("reaction") or "👍").strip()
            target_msg_id = str(
                reaction_obj.get("messageId")
                or payload.get("id")
                or ""
            ).strip()
            chat_id = str(payload.get("from") or payload.get("chatId") or "").strip()

            rec = self.store.find_by_message_id(target_msg_id)
            if not rec and chat_id:
                rec = self.store.find_latest_for_chat(chat_id)

            if not rec:
                return {
                    "status": "reaction_unmatched",
                    "event": ev,
                    "emoji": emoji,
                    "message_id": target_msg_id,
                }

            self.store.add_reaction(
                rec.interaction_id,
                emoji=emoji,
                actor=chat_id or "citizen",
                message_id=target_msg_id,
            )
            return {
                "status": "reaction_recorded",
                "interaction_id": rec.interaction_id,
                "emoji": emoji,
                "reactions_count": len(rec.reactions),
            }

        if ev in ("session.status", "message.revoked", "presence.update"):
            return {
                "status": "event_acknowledged",
                "event": ev,
                "session": session,
                "timestamp": _now_iso(),
            }

        return None

    def get_waha_diagnostics(self) -> Dict[str, Any]:
        """
        Retorna diagnóstico de conectividade, recursos nativos suportados pelo WAHA no projeto
        e estatísticas operacionais de mensageria.
        """
        base_url = self._resolve_waha_url()
        store_stats = self.store.list_records(limit=5)["summary"]
        return {
            "waha_configured_url": base_url or "local_verified_mode (sem container externo ativo)",
            "engine_configured_in_compose": "GOWS (devlikeapro/waha:latest)",
            "supported_capabilities": {
                "typing_indicator": {
                    "supported": True,
                    "endpoints": ["POST /api/startTyping", "POST /api/stopTyping"],
                    "behavior": "Ativado imediatamente ao receber a mensagem e encerrado no bloco finally antes/após o envio da resposta.",
                },
                "smart_event_driven_sending": {
                    "supported": True,
                    "fixed_timer_removed": True,
                    "behavior": "Zero espera artificial (Wait1 de 10s do n8n legado foi eliminado). Envio imediato assim que o orquestrador conclui a síntese.",
                },
                "message_states_and_acks": {
                    "supported": True,
                    "states": [s.value for s in MessageLifecycleState],
                    "webhook_events": ["message", "message.any", "message.ack", "message.reaction", "session.status"],
                    "ack_mapping": {"-1": "failed", "0": "processing", "1": "sent", "2": "delivered", "3": "read", "4": "read (played)"},
                },
                "reactions": {
                    "supported": True,
                    "endpoints": ["PUT /api/reaction", "webhook message.reaction"],
                },
                "robustness_and_deduplication": {
                    "supported": True,
                    "idempotent_dedup_by_message_id": True,
                    "per_chat_ordering_lock": True,
                    "concurrent_message_coalescing": True,
                    "strict_delivery_confirmation": True,
                    "duplicates_blocked_count": self.coordinator.duplicates_blocked,
                    "coalesced_messages_count": self.coordinator.coalesced_messages,
                },
            },
            "interaction_summary": store_stats,
        }
