"""
Camada de Mensageria Institucional e Integração Avançada com WhatsApp / WAHA (SIM v3.1).

Gerencia:
- Ciclo completo de estados de mensagens (received -> seen -> processing -> sent -> delivered -> read / failed).
- Presença e indicador de digitação natural no WhatsApp (`startTyping` / `stopTyping` + `sendSeen`).
- Envio inteligente orientado a eventos (sem timers fixos artificiais).
- Deduplicação idempotente de eventos de webhook (`message.id`), fila/coalescência por `chat_id`
  para preservar a ordem das interações e lidar com mensagens concorrentes.
- Tratamento de confirmações (`message.ack`), reações (`message.reaction` / `PUT /api/reaction`) e retentativas.
- Repositório unificado de histórico de interações (WhatsApp Cidadão + Plataforma Institucional Web).
"""

from __future__ import annotations

from sim_platform.messaging.waha_manager import (
    ChatSessionCoordinator,
    InteractionRecord,
    MessageLifecycleState,
    UnifiedInteractionStore,
    WAHAMessagingManager,
)

__all__ = [
    "ChatSessionCoordinator",
    "InteractionRecord",
    "MessageLifecycleState",
    "UnifiedInteractionStore",
    "WAHAMessagingManager",
]
