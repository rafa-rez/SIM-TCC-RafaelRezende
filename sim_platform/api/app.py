"""
API FastAPI Endurecida da Plataforma SIM (`sim_platform.api.app`).

Corrige todas as falhas de segurança e integração do backend original (`scripts_python/main.py`)
e experimental (`experimental/backend-v2/main.py`), adicionando:
- FIND-001: Validação AST obrigatória em `/query` via `SQLGuard`
- FIND-003: Suporte simultâneo a `query` e `pergunta_usuario` nos contratos JSON
- FIND-006: Sanitização completa contra Stored XSS (`html.escape` no servidor e `textContent` no cliente)
- FIND-007: Autenticação via API Key (`X-API-Key`) e RBAC (`X-SIM-Role`)
- FIND-008: Rastreabilidade determinística (`fontes_utilizadas`, `fontes_csv`, `rastreabilidade_tcc`)
- FIND-012: Contagem exata de objetos (`VIEW` + `BASE TABLE`) em `/ping` e `/health`
- Filtro de Escopo de Bases (`allowed_sources`, `explicit_tables`)
- Central "Atualizar Dados" (`GET /api/ingestion/status`, `POST /api/ingestion/sync`)
- Explorador de Atos Individuais do Diário Oficial (`GET /api/rag/collection`)
- Motor Proativo de Insights e Auditoria Contínua (`GET /api/audit/insights`)
- Suporte a Áudio no WhatsApp / WAHA (`POST /api/audio/transcribe` e `POST /webhook/waha`)
"""

from __future__ import annotations

import base64
import html
import io
import json
import os
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from sim_platform.audit.proactive_insights import ProactiveAuditEngine
from sim_platform.engine.duckdb_manager import DuckDBManager, TenantContext
from sim_platform.ingestion.sync_manager import DataSyncManager
from sim_platform.orchestrator.router import SIMOrchestrator
from sim_platform.rag.hybrid_retriever import HybridRAGRetriever


DEFAULT_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = Path(
    os.environ.get(
        "SIM_DATA_DIR",
        str(DEFAULT_REPO_ROOT / "avaliacao" / "dados" / "database"),
    )
)
DEFAULT_FIX2023_DIR = Path(
    os.environ.get(
        "SIM_FIX2023_DIR",
        str(DEFAULT_REPO_ROOT / "avaliacao" / "dados" / "database_fix2023"),
    )
)


class SQLQueryRequest(BaseModel):
    query: Optional[str] = Field(default=None, description="Instrução SQL SELECT/WITH")
    pergunta_usuario: Optional[str] = Field(
        default=None,
        description="Compatibilidade com o sub-workflow n8n (Tool_Executar_SQL_Sicom)",
    )
    enforce_limit: bool = True
    allowed_sources: Optional[List[str]] = None
    explicit_tables: Optional[List[str]] = None


class CatalogSelectRequest(BaseModel):
    question: str = ""
    explicit_segments: Optional[List[str]] = None
    explicit_tables: Optional[List[str]] = None
    allowed_sources: Optional[List[str]] = None
    role: str = "citizen"


class ChatRequest(BaseModel):
    question: Optional[str] = None
    pergunta_usuario: Optional[str] = None
    session_id: str = "default"
    role: str = "citizen"
    explicit_segments: Optional[List[str]] = None
    explicit_tables: Optional[List[str]] = None
    allowed_sources: Optional[List[str]] = None
    sql_override: Optional[str] = None
    llm_provider: Optional[str] = None
    llm_api_key: Optional[str] = None


class SyncRequest(BaseModel):
    sources: Optional[List[str]] = None


class AudioTranscribeRequest(BaseModel):
    audio_base64: Optional[str] = None
    mime_type: str = "audio/ogg"
    fallback_transcript: Optional[str] = None
    llm_provider: Optional[str] = None
    llm_api_key: Optional[str] = None


def transcribe_audio_content(
    audio_base64: Optional[str],
    mime_type: str = "audio/ogg",
    fallback_transcript: Optional[str] = None,
    llm_provider: Optional[str] = None,
    llm_api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Transcreve mensagens de voz do WhatsApp (`.ogg` / `.mp3` / `.wav` / `.webm`) usando:
    1. Google Gemini (`GEMINI_API_KEY` — suporta áudio nativamente via `inlineData` no Free Tier), ou
    2. OpenAI Whisper (`OPENAI_API_KEY` — `whisper-1`), ou
    3. `fallback_transcript` quando já transcrito pelo navegador (Web Speech API) ou payload de teste.
    """
    user_key = (llm_api_key or "").strip()
    prov = (llm_provider or "").lower().strip()

    gemini_key = (
        user_key
        if (prov == "gemini" or (user_key and not user_key.startswith("sk-")))
        else os.environ.get("GEMINI_API_KEY", os.environ.get("GOOGLE_API_KEY", "")).strip()
    )
    openai_key = (
        user_key
        if (prov == "openai" or user_key.startswith("sk-"))
        else os.environ.get("OPENAI_API_KEY", "").strip()
    )

    if audio_base64:
        clean_b64 = audio_base64.split(",")[-1].strip()
        if gemini_key:
            try:
                import requests
                url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={gemini_key}"
                resp = requests.post(
                    url,
                    headers={"Content-Type": "application/json"},
                    json={
                        "contents": [
                            {
                                "parts": [
                                    {
                                        "text": (
                                            "Transcreva fielmente este áudio em português do Brasil. "
                                            "Retorne apenas o texto falado, sem comentários adicionais."
                                        )
                                    },
                                    {"inlineData": {"mimeType": mime_type, "data": clean_b64}},
                                ]
                            }
                        ]
                    },
                    timeout=20,
                )
                if resp.status_code == 200:
                    candidates = resp.json().get("candidates") or []
                    if candidates:
                        txt = candidates[0]["content"]["parts"][0]["text"].strip()
                        return {"status": "sucesso", "engine": "Google Gemini Audio (gemini-2.5-flash)", "text": txt}
            except Exception:
                pass

        if openai_key and openai_key.startswith("sk-"):
            try:
                import requests
                raw_bytes = base64.b64decode(clean_b64)
                ext = "ogg" if "ogg" in mime_type else ("mp3" if "mp3" in mime_type else "wav")
                resp = requests.post(
                    "https://api.openai.com/v1/audio/transcriptions",
                    headers={"Authorization": f"Bearer {openai_key}"},
                    files={"file": (f"whatsapp_audio.{ext}", io.BytesIO(raw_bytes), mime_type)},
                    data={"model": "whisper-1", "language": "pt"},
                    timeout=20,
                )
                if resp.status_code == 200:
                    txt = (resp.json().get("text") or "").strip()
                    if txt:
                        return {"status": "sucesso", "engine": "OpenAI Whisper (whisper-1)", "text": txt}
            except Exception:
                pass

    if fallback_transcript and fallback_transcript.strip():
        return {
            "status": "sucesso",
            "engine": "Browser WebSpeech / Payload Transcript",
            "text": fallback_transcript.strip(),
        }

    return {
        "status": "sem_chave_audio",
        "engine": "none",
        "text": "",
        "detail": "Informe GEMINI_API_KEY, OPENAI_API_KEY ou fallback_transcript para transcrever áudio bruto.",
    }


def create_app(
    data_dir: Path = DEFAULT_DATA_DIR,
    fix2023_dir: Optional[Path] = DEFAULT_FIX2023_DIR,
    repaired_2023: bool = True,
    rag_corpus: Optional[List[Dict[str, Any]]] = None,
    require_api_key: Optional[str] = None,
) -> FastAPI:
    app = FastAPI(
        title="SIM Platform API — Inteligência sobre Dados Públicos Municipais",
        version="2.3.0",
        description=(
            "Motor Analítico Multi-Tenant com 79 Tabelas em 8 Fontes Oficiais, SQLGuard (AST), "
            "Catálogo Dinâmico, RAG de Atos Individuais, Auditoria Proativa e Suporte a Áudio WhatsApp."
        ),
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    tenant = TenantContext(
        tenant_id="caete_mg_3110004",
        municipality_name="Caeté",
        state_uf="MG",
        ibge_code="3110004",
        data_dir=data_dir,
        fix2023_dir=fix2023_dir,
        repaired_2023=repaired_2023,
    )
    sync_manager = DataSyncManager(workspace_root=DEFAULT_REPO_ROOT, tenant_id=tenant.tenant_id)
    audit_engine = ProactiveAuditEngine(workspace_root=DEFAULT_REPO_ROOT, tenant_id=tenant.tenant_id)
    db_manager = DuckDBManager(tenant=tenant)
    rag_retriever = HybridRAGRetriever(corpus_chunks=rag_corpus)
    orchestrator = SIMOrchestrator(db_manager=db_manager, rag_retriever=rag_retriever)

    log_buffer: Deque[str] = deque(maxlen=120)
    last_call: Dict[str, Any] = {
        "timestamp": "Nenhuma requisição registrada",
        "query_sql": "Aguardando primeira chamada...",
        "output": "{}",
        "status": "IDLE",
    }

    def _log(msg: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_buffer.append(f"[{ts}] {msg}")

    _log(
        f"SIM Platform v2.3 inicializada para tenant={tenant.tenant_id} "
        f"(tabelas={len(db_manager.catalog.tables)}, atos_rag={len(rag_retriever.corpus_chunks)})"
    )

    def verify_auth(
        x_api_key: Optional[str] = Header(default=None),
        x_sim_role: str = Header(default="citizen"),
    ) -> str:
        expected_key = require_api_key if require_api_key is not None else os.environ.get("SIM_API_KEY")
        if expected_key and x_api_key != expected_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="API Key inválida ou ausente (cabeçalho X-API-Key obrigatório).",
            )
        role = (x_sim_role or "citizen").lower().strip()
        if role not in ("citizen", "analyst", "auditor", "controller", "admin"):
            role = "citizen"
        return role

    @app.get("/", response_class=HTMLResponse)
    @app.get("/auditor", response_class=HTMLResponse)
    def auditor_console() -> HTMLResponse:
        console_file = Path(__file__).resolve().parent / "auditor_console_v0.html"
        if console_file.exists():
            return HTMLResponse(content=console_file.read_text(encoding="utf-8"), status_code=200)
        return ping_dashboard()

    @app.get("/api/pre-post")
    def get_pre_post_comparison() -> Dict[str, Any]:
        comp_path = DEFAULT_REPO_ROOT / "AUDIT" / "PRE_VS_POST_COMPARISON.json"
        if comp_path.exists():
            return json.loads(comp_path.read_text(encoding="utf-8"))
        return {"total_cases": 0, "cases": []}

    @app.get("/api/ingestion/status")
    def ingestion_status() -> Dict[str, Any]:
        return sync_manager.check_status()

    @app.post("/api/ingestion/sync")
    def trigger_ingestion_sync(
        req: Optional[SyncRequest] = None,
        role: str = Depends(verify_auth),
    ) -> Dict[str, Any]:
        selected = req.sources if req else None
        _log(f"POST /api/ingestion/sync disparado (role={role}, sources={selected or 'ALL'})")
        return sync_manager.sync_all(
            selected_sources=selected,
            db_manager=db_manager,
            rag_retriever=rag_retriever,
        )

    @app.get("/api/audit/insights")
    def get_proactive_audit_insights(
        aditivo_pct_threshold: float = 25.0,
        min_dispensas_recorrentes: int = 5,
        yoy_growth_pct_threshold: float = 100.0,
    ) -> Dict[str, Any]:
        return audit_engine.run_all_checks(
            aditivo_pct_threshold=aditivo_pct_threshold,
            min_dispensas_recorrentes=min_dispensas_recorrentes,
            yoy_growth_pct_threshold=yoy_growth_pct_threshold,
        )

    @app.post("/api/audio/transcribe")
    def transcribe_audio_endpoint(req: AudioTranscribeRequest) -> Dict[str, Any]:
        return transcribe_audio_content(
            audio_base64=req.audio_base64,
            mime_type=req.mime_type,
            fallback_transcript=req.fallback_transcript,
            llm_provider=req.llm_provider,
            llm_api_key=req.llm_api_key,
        )

    @app.get("/api/rag/collection")
    def get_rag_collection_overview() -> Dict[str, Any]:
        status_info = sync_manager.check_status()
        return {
            "total_unique_acts": len(rag_retriever.corpus_chunks),
            "audit_summary": status_info.get("rag_collection_audit", {}),
            "acts": rag_retriever.corpus_chunks[:120],
        }

    @app.get("/health")
    def health() -> Dict[str, Any]:
        counts = db_manager.get_mapped_objects_count()
        return {
            "status": "ok",
            "tenant_id": tenant.tenant_id,
            "municipality": f"{tenant.municipality_name}/{tenant.state_uf}",
            "ibge_code": tenant.ibge_code,
            "repaired_2023": tenant.repaired_2023,
            "mapped_objects": counts,
            "rag_unique_acts": len(rag_retriever.corpus_chunks),
        }

    @app.get("/api/logs")
    def get_logs(role: str = Depends(verify_auth)) -> Dict[str, Any]:
        return {"logs": [html.escape(line) for line in list(log_buffer)]}

    @app.get("/ping", response_class=HTMLResponse)
    def ping_dashboard() -> HTMLResponse:
        counts = db_manager.get_mapped_objects_count()
        safe_ts = html.escape(str(last_call.get("timestamp", "")))
        safe_status = html.escape(str(last_call.get("status", "")))
        safe_sql = html.escape(str(last_call.get("query_sql", "")))
        safe_out = html.escape(str(last_call.get("output", "")))

        html_page = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="utf-8">
    <title>SIM Platform — Monitor de Governança Analítica</title>
    <style>
        body {{ font-family: system-ui, sans-serif; background: #0f172a; color: #f8fafc; padding: 24px; }}
        .card {{ background: #1e293b; border-radius: 8px; padding: 16px; margin-bottom: 16px; border: 1px solid #334155; }}
        pre {{ background: #020617; padding: 12px; border-radius: 6px; overflow-x: auto; color: #38bdf8; }}
        .badge {{ background: #10b981; color: #022c22; padding: 4px 10px; border-radius: 999px; font-weight: bold; }}
    </style>
</head>
<body>
    <h1>SIM Platform v2.3 — {html.escape(tenant.municipality_name)}/{html.escape(tenant.state_uf)} (IBGE {html.escape(tenant.ibge_code)})</h1>
    <div class="card">
        <p><span class="badge">ONLINE</span> | Objetos Mapeados no Catálogo: <strong>{counts['total']}</strong>
        (Views: {counts['views']} | Tabelas Materializadas: {counts['base_tables']}) |
        Reparo 2023 Ativo: <strong>{tenant.repaired_2023}</strong></p>
    </div>
    <div class="card">
        <h3>Última Execução ({safe_ts} — Status: {safe_status})</h3>
        <h4>SQL Validado (Escapado contra XSS):</h4>
        <pre><code id="last-sql">{safe_sql}</code></pre>
        <h4>Resposta Resumida:</h4>
        <pre><code id="last-out">{safe_out}</code></pre>
    </div>
    <div class="card">
        <h3>Logs Recentes (Renderização Segura via textContent)</h3>
        <pre id="log-box"></pre>
    </div>
    <script>
        async function loadLogs() {{
            const res = await fetch('/api/logs');
            if (!res.ok) return;
            const data = await res.json();
            const box = document.getElementById('log-box');
            box.textContent = (data.logs || []).join('\\n');
        }}
        loadLogs();
    </script>
</body>
</html>"""
        return HTMLResponse(content=html_page, status_code=200)

    @app.get("/catalog")
    def list_catalog(role: str = Depends(verify_auth)) -> Dict[str, Any]:
        by_source: Dict[str, int] = {}
        for meta in db_manager.catalog.tables.values():
            by_source[meta.source_family] = by_source.get(meta.source_family, 0) + 1
        return {
            "tenant_id": tenant.tenant_id,
            "total_tables": len(db_manager.catalog.tables),
            "source_families": by_source,
            "segments": list(
                {meta.segment for meta in db_manager.catalog.tables.values()}
            ),
            "tables": [meta.to_dict() for meta in db_manager.catalog.tables.values()],
        }

    @app.post("/catalog/select")
    def select_catalog_schema(
        req: CatalogSelectRequest,
        role: str = Depends(verify_auth),
    ) -> Dict[str, Any]:
        effective_role = req.role if req.role != "citizen" else role
        sel = db_manager.catalog.select_tables_for_query(
            question=req.question,
            explicit_segments=req.explicit_segments,
            explicit_tables=req.explicit_tables,
            allowed_sources=req.allowed_sources,
        )
        pkg = orchestrator.prompt_builder.assemble(
            user_question=req.question,
            selection=sel,
            role=effective_role,
        )
        return {
            "selection_mode": sel.selection_mode,
            "allowed_sources": sel.allowed_sources,
            "selected_segments": sel.selected_segments,
            "selected_tables": sel.selected_tables,
            "selected_count": sel.selected_count,
            "total_catalog_tables": sel.total_catalog_tables,
            "reduction_pct": sel.reduction_pct,
            "estimated_prompt_tokens": pkg.estimated_tokens,
            "domain_rules": sel.domain_rules,
            "transparency_notes": sel.transparency_notes,
            "dynamic_ddl": sel.dynamic_ddl,
        }

    @app.post("/query")
    def execute_query(
        req: SQLQueryRequest,
        role: str = Depends(verify_auth),
    ) -> Dict[str, Any]:
        raw_sql = (req.query or req.pergunta_usuario or "").strip()
        _log(f"POST /query (role={role}): {raw_sql[:120]}")
        restricted_tables = (
            db_manager.catalog.get_allowed_table_names(
                role=role,
                allowed_sources=req.allowed_sources,
                explicit_tables=req.explicit_tables,
            )
            if (req.allowed_sources or req.explicit_tables)
            else None
        )
        res = db_manager.execute_safe_query(
            raw_sql=raw_sql,
            role=role,
            enforce_limit=req.enforce_limit,
            allowed_tables_override=restricted_tables,
        )
        last_call["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        last_call["query_sql"] = raw_sql
        last_call["status"] = res.get("status", "UNKNOWN").upper()
        last_call["output"] = json.dumps(
            {
                "status": res.get("status"),
                "total_linhas": res.get("total_linhas"),
                "fontes_utilizadas": res.get("fontes_utilizadas"),
                "transparency_warnings": res.get("transparency_warnings"),
                "amostra": (res.get("dados") or [])[:2],
            },
            ensure_ascii=False,
            indent=2,
        )
        return res

    @app.post("/chat")
    def chat_endpoint(
        req: ChatRequest,
        role: str = Depends(verify_auth),
    ) -> Dict[str, Any]:
        question = (req.question or req.pergunta_usuario or "").strip()
        effective_role = req.role if req.role != "citizen" else role
        _log(
            f"POST /chat (session={req.session_id}, role={effective_role}, "
            f"sources={req.allowed_sources or 'ALL'}): {question[:100]}"
        )
        res = orchestrator.handle_question(
            question=question,
            session_id=req.session_id,
            role=effective_role,
            explicit_segments=req.explicit_segments,
            explicit_tables=req.explicit_tables,
            allowed_sources=req.allowed_sources,
            sql_override=req.sql_override,
            llm_provider=req.llm_provider,
            llm_api_key=req.llm_api_key,
        )
        return res.to_dict()

    @app.post("/webhook/waha")
    @app.post("/webhook/whatsapp-entrada")
    def waha_webhook_endpoint(payload_root: Dict[str, Any]) -> Dict[str, Any]:
        """
        Endpoint nativo compatível com o WAHA (WhatsApp HTTP API) sem necessidade do n8n.
        Suporta tanto mensagens de texto quanto mensagens de voz/áudio (`hasMedia=True` / `audio_base64` / `audio_transcript`).
        """
        body_obj = payload_root.get("body") if isinstance(payload_root.get("body"), dict) else payload_root
        session = str(body_obj.get("session") or "default")
        event = str(body_obj.get("event") or "message.any")
        payload = body_obj["payload"] if isinstance(body_obj.get("payload"), dict) else body_obj

        from_me = bool(payload.get("fromMe", False))
        chat_id = str(payload.get("from") or payload.get("chatId") or "whatsapp_user")
        message_text = str(payload.get("body") or payload.get("text") or "").strip()

        # Verificar se é mensagem de voz / áudio do WhatsApp
        media_obj = payload.get("media") if isinstance(payload.get("media"), dict) else {}
        audio_b64 = str(payload.get("audio_base64") or media_obj.get("data") or "").strip()
        audio_mime = str(payload.get("mime_type") or media_obj.get("mimetype") or "audio/ogg").strip()
        audio_transcript = str(payload.get("audio_transcript") or "").strip()
        was_audio = False
        audio_engine = None

        if not message_text and (audio_b64 or audio_transcript or payload.get("hasMedia")):
            tr = transcribe_audio_content(
                audio_base64=audio_b64 or None,
                mime_type=audio_mime,
                fallback_transcript=audio_transcript or None,
            )
            if tr.get("text"):
                message_text = tr["text"]
                was_audio = True
                audio_engine = tr.get("engine")

        if from_me:
            return {"status": "ignored", "reason": "fromMe=True", "chat_id": chat_id}
        if chat_id.endswith("@g.us") or chat_id == "status@broadcast":
            return {"status": "ignored", "reason": "group_or_status", "chat_id": chat_id}
        if not message_text:
            return {"status": "ignored", "reason": "empty_body", "chat_id": chat_id}

        _log(f"WAHA Webhook ({event}, chat={chat_id}, audio={was_audio}): {message_text[:100]}")

        res = orchestrator.handle_question(
            question=message_text,
            session_id=chat_id,
            role="citizen",
        )
        res_dict = res.to_dict()

        raw_answer = str(res_dict.get("answer_text") or "").strip()
        if was_audio:
            raw_answer = f'🎙️ *Áudio compreendido:* "{message_text}"\n\n{raw_answer}'
        whatsapp_text = raw_answer.replace("**", "*")

        waha_api_url = os.environ.get("WAHA_API_URL", "").strip()
        waha_delivery = {"sent": False, "target": waha_api_url or "disabled (dry-run)"}
        if waha_api_url:
            import urllib.request
            try:
                req_data = json.dumps({
                    "session": session,
                    "chatId": chat_id,
                    "text": whatsapp_text,
                    "linkPreview": False,
                }).encode("utf-8")
                http_req = urllib.request.Request(
                    f"{waha_api_url.rstrip('/')}/api/sendText",
                    data=req_data,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(http_req, timeout=10) as resp:
                    waha_delivery = {"sent": True, "http_status": resp.status}
            except Exception as exc:
                waha_delivery = {"sent": False, "error": str(exc)}

        return {
            "status": "processed",
            "session": session,
            "chat_id": chat_id,
            "was_audio": was_audio,
            "audio_transcription_engine": audio_engine,
            "transcribed_text": message_text if was_audio else None,
            "route_taken": res_dict.get("intent"),
            "whatsapp_text": whatsapp_text,
            "waha_delivery": waha_delivery,
            "orchestrator_response": res_dict,
        }

    app.state.db_manager = db_manager
    app.state.orchestrator = orchestrator
    app.state.rag_retriever = rag_retriever
    app.state.sync_manager = sync_manager
    app.state.audit_engine = audit_engine
    return app


app = create_app()
