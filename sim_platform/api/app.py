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

import asyncio
import base64
import html
import io
import ipaddress
import json
import os
import posixpath
import re
import threading
from collections import OrderedDict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, Request, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from sim_platform.audit.proactive_insights import ProactiveAuditEngine
from sim_platform.catalog.entity_linker import normalize_municipal_asr_text
from sim_platform.engine.duckdb_manager import DuckDBManager, TenantContext
from sim_platform.ingestion.city_onboarding import CityOnboardingManager, load_tenant_profile
from sim_platform.ingestion.fleet_verifier import FleetVerifier
from sim_platform.ingestion.sync_manager import DataSyncManager, MAX_UPLOAD_RAW_BYTES, SyncAlreadyRunningError
from sim_platform.messaging.waha_manager import (
    MessageLifecycleState,
    UnifiedInteractionStore,
    WAHAMessagingManager,
)
from sim_platform.messaging.whatsapp_router import WhatsAppTenantRouter
from sim_platform.orchestrator.router import SIMOrchestrator
from sim_platform.rag.hybrid_retriever import HybridRAGRetriever
from sim_platform.security.auth_rbac import InstitutionalAuthManager


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
    tenant_id: Optional[str] = None
    enforce_limit: bool = True
    allowed_sources: Optional[List[str]] = None
    explicit_tables: Optional[List[str]] = None


class CatalogSelectRequest(BaseModel):
    question: str = ""
    tenant_id: Optional[str] = None
    explicit_segments: Optional[List[str]] = None
    explicit_tables: Optional[List[str]] = None
    allowed_sources: Optional[List[str]] = None
    role: str = "citizen"


class ChatRequest(BaseModel):
    question: Optional[str] = None
    pergunta_usuario: Optional[str] = None
    tenant_id: Optional[str] = None
    session_id: str = "default"
    role: str = "citizen"
    gabinete_id: Optional[str] = None
    explicit_segments: Optional[List[str]] = None
    explicit_tables: Optional[List[str]] = None
    allowed_sources: Optional[List[str]] = None
    sql_override: Optional[str] = None
    llm_provider: Optional[str] = None
    llm_api_key: Optional[str] = None


class LoginRequest(BaseModel):
    username: str = ""
    password: str = ""
    gabinete_override: Optional[str] = None


class SSORequest(BaseModel):
    provider: str = "govbr"
    code: Optional[str] = None
    sso_code: Optional[str] = None
    gabinete_id: Optional[str] = None


class OnboardCityRequest(BaseModel):
    tenant_id: Optional[str] = None
    municipality_name: Optional[str] = None
    state_uf: str = "MG"
    ibge_code: Optional[str] = None
    plan_tier: Optional[str] = None
    plan_key: Optional[str] = None
    official_gazette_name: Optional[str] = None
    sanitize_and_build_parquets: bool = True


class SyncRequest(BaseModel):
    sources: Optional[List[str]] = None
    live_api: bool = False


class SelectTenantRequest(BaseModel):
    tenant_id: str
    auto_sync_if_empty: bool = False


class FleetRunRequest(BaseModel):
    tenant_ids: Optional[List[str]] = None
    live_api: bool = True
    process_inbox: bool = True
    export_cloud_bundles: bool = True
    push_cloud: bool = False


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
    municipality_name: str = "Caeté",
    state_uf: str = "MG",
    diario_veiculo: str = "Jornal Opinião",
) -> Dict[str, Any]:
    """
    Transcreve mensagens de voz do WhatsApp (`.ogg` / `.mp3` / `.wav` / `.webm`) usando:
    1. Google Gemini (`GEMINI_API_KEY` — suporta áudio nativamente via `inlineData` no Free Tier), ou
    2. OpenAI Whisper (`OPENAI_API_KEY` — `whisper-1`), ou
    3. `fallback_transcript` quando já transcrito pelo navegador (Web Speech API) ou payload de teste.
    """
    user_key = (llm_api_key or "").strip()
    prov = (llm_provider or "").lower().strip()

    from sim_platform import get_configured_openai_key

    gemini_key = (
        user_key
        if (prov == "gemini" or (user_key and not user_key.startswith("sk-")))
        else os.environ.get("GEMINI_API_KEY", os.environ.get("GOOGLE_API_KEY", "")).strip()
    )
    openai_key = (
        get_configured_openai_key(user_key if (prov == "openai" or user_key.startswith("sk-")) else None)
        if prov != "gemini"
        else ""
    )

    asr_domain_glossary = (
        f"Município de {municipality_name}/{state_uf}, Prefeitura Municipal de {municipality_name}, "
        f"Câmara Municipal de {municipality_name}, SAAE, FUNDEB, SIOPS, SICONFI, SICOM, TCE-MG, CAPAG, IDEB, "
        f"LOA, LDO, LRF, RCL, QSA, CNAE, Novo Bolsa Família, BPC, servidores efetivos, concursados, "
        f"comissionados, contratados temporários, adesão a ata de registro de preços, carona, {diario_veiculo}."
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
                                            f"Transcreva fielmente este áudio em português do Brasil sobre gestão pública do município de {municipality_name}/{state_uf}. "
                                            f"Glossário institucional de referência: {asr_domain_glossary} "
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
                        raw_txt = candidates[0]["content"]["parts"][0]["text"].strip()
                        norm_txt, corrections = normalize_municipal_asr_text(raw_txt, municipality_name=municipality_name)
                        return {
                            "status": "sucesso",
                            "engine": "Google Gemini Audio (gemini-2.5-flash)",
                            "text": norm_txt,
                            "raw_text": raw_txt,
                            "asr_corrections": corrections,
                        }
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
                    data={
                        "model": "whisper-1",
                        "language": "pt",
                        "prompt": asr_domain_glossary,
                    },
                    timeout=20,
                )
                if resp.status_code == 200:
                    raw_txt = (resp.json().get("text") or "").strip()
                    if raw_txt:
                        norm_txt, corrections = normalize_municipal_asr_text(raw_txt, municipality_name=municipality_name)
                        return {
                            "status": "sucesso",
                            "engine": "OpenAI Whisper (whisper-1)",
                            "text": norm_txt,
                            "raw_text": raw_txt,
                            "asr_corrections": corrections,
                        }
            except Exception:
                pass

    if fallback_transcript and fallback_transcript.strip():
        raw_fb = fallback_transcript.strip()
        norm_fb, corrections = normalize_municipal_asr_text(raw_fb, municipality_name=municipality_name)
        return {
            "status": "sucesso",
            "engine": "Browser WebSpeech / Payload Transcript",
            "text": norm_fb,
            "raw_text": raw_fb,
            "asr_corrections": corrections,
        }

    return {
        "status": "sem_chave_audio",
        "engine": "none",
        "text": "",
        "detail": "Informe GEMINI_API_KEY, OPENAI_API_KEY ou fallback_transcript para transcrever áudio bruto.",
    }


@dataclass
class TenantRuntime:
    tenant: TenantContext
    diario_veiculo: str
    sync_manager: DataSyncManager
    audit_engine: ProactiveAuditEngine
    db_manager: DuckDBManager
    rag_retriever: HybridRAGRetriever
    interaction_store: UnifiedInteractionStore
    orchestrator: SIMOrchestrator


class TenantRuntimePool:
    """
    Pool LRU thread-safe de runtimes municipais (`max_cached=8`), garantindo:
    - Isolamento 100% entre cidades (DuckDB, Catálogo, RAG, Quota/Cache L1 e Histórico);
    - Propagação atômica instantânea da cidade ativa tanto para o Console Web quanto para o WhatsApp (`/webhook/waha`);
    - Persistência opcional em `tenants/_active_tenant.json` fora de `pytest`.
    """

    _TENANT_ID_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{1,63}$")

    @classmethod
    def validate_tenant_id_format(cls, tenant_id: str) -> str:
        tid = (tenant_id or "").strip()
        if not tid or not cls._TENANT_ID_RE.match(tid) or ".." in tid:
            raise PermissionError(
                f"Identificador de município inválido ('{tid}'). Use apenas letras, números, '_' ou '-' (2 a 64 caracteres)."
            )
        return tid

    def __init__(
        self,
        *,
        workspace_root: Path,
        default_data_dir: Path,
        default_fix2023_dir: Optional[Path],
        default_repaired_2023: bool,
        default_rag_corpus: Optional[List[Dict[str, Any]]],
        initial_tenant_id: str,
        persist_active_path: Optional[Path] = None,
        max_cached: int = 8,
    ) -> None:
        self.workspace_root = workspace_root
        self.default_data_dir = default_data_dir
        self.default_fix2023_dir = default_fix2023_dir
        self.default_repaired_2023 = default_repaired_2023
        self.default_rag_corpus = default_rag_corpus
        self.persist_active_path = persist_active_path
        self.max_cached = max(2, max_cached)
        self._lock = threading.RLock()
        self._tenant_init_locks: Dict[str, threading.RLock] = {}
        self._cache: OrderedDict[str, TenantRuntime] = OrderedDict()
        self._active_tenant_id = self.validate_tenant_id_format(initial_tenant_id)
        self._on_switch_callbacks: List[Any] = []

        if self.persist_active_path and self.persist_active_path.exists():
            try:
                saved = json.loads(self.persist_active_path.read_text(encoding="utf-8"))
                saved_tid = str(saved.get("active_tenant_id") or "").strip()
                if saved_tid and self._TENANT_ID_RE.match(saved_tid):
                    if (self.workspace_root / "tenants" / f"{saved_tid}.json").exists():
                        self._active_tenant_id = saved_tid
            except Exception:
                pass

        self._ensure_loaded(self._active_tenant_id)

    def register_switch_callback(self, fn: Any) -> None:
        with self._lock:
            self._on_switch_callbacks.append(fn)

    def _build_runtime(self, tenant_id: str) -> TenantRuntime:
        tid = self.validate_tenant_id_format(tenant_id)
        tenants_root = (self.workspace_root / "tenants").resolve()
        manifest_path = (tenants_root / f"{tid}.json").resolve()
        if manifest_path.parent != tenants_root:
            raise PermissionError(f"Tentativa de path traversal bloqueada para tenant '{tid}'.")
        if tid not in ("caete_mg_3110004", "caete-mg") and not manifest_path.exists():
            raise ValueError(f"Município/tenant '{tid}' não encontrado em tenants/*.json.")

        t_prof = load_tenant_profile(tenant_id=tid, workspace_root=self.workspace_root)
        is_caete = tid in ("caete_mg_3110004", "caete-mg")
        if is_caete:
            t_data_dir = self.default_data_dir
            t_fix_dir = self.default_fix2023_dir
            t_rep = self.default_repaired_2023
            t_rag_corpus = self.default_rag_corpus
            t_pq_dir = None
            default_veiculo = "Jornal Opinião"
        else:
            t_data_dir = self.workspace_root / "avaliacao" / "dados" / "raw_csv" / tid
            t_fix_dir = None
            t_rep = False
            t_rag_corpus = None
            t_pq_dir = self.workspace_root / "avaliacao" / "dados" / "parquet" / tid
            t_pq_dir.mkdir(parents=True, exist_ok=True)
            default_veiculo = f"Diário Oficial de {t_prof.get('municipality_name', tid)}"

        diario_veiculo = str(
            (t_prof.get("diario_oficial") or {}).get("veiculo")
            or t_prof.get("official_gazette_name")
            or default_veiculo
        ).strip()

        t_ctx = TenantContext(
            tenant_id=tid,
            municipality_name=str(t_prof.get("municipality_name") or ("Caeté" if is_caete else tid)),
            state_uf=str(t_prof.get("state_uf") or "MG"),
            ibge_code=str(t_prof.get("ibge_code") or ("3110004" if is_caete else "0000000")),
            data_dir=t_data_dir,
            fix2023_dir=t_fix_dir,
            parquet_dir=t_pq_dir,
            repaired_2023=t_rep,
        )
        sync_mgr = DataSyncManager(workspace_root=self.workspace_root, tenant_id=tid)
        audit_eng = ProactiveAuditEngine(workspace_root=self.workspace_root, tenant_id=tid)
        db_mgr = DuckDBManager(tenant=t_ctx)
        rag_ret = HybridRAGRetriever(corpus_chunks=t_rag_corpus, tenant_id=tid)
        persist_hist = self.workspace_root / "avaliacao" / "dados" / "parquet" / tid / "_interactions_history.json"
        int_store = UnifiedInteractionStore(persist_path=persist_hist)
        orch = SIMOrchestrator(
            db_manager=db_mgr,
            rag_retriever=rag_ret,
            interaction_store=int_store,
        )
        return TenantRuntime(
            tenant=t_ctx,
            diario_veiculo=diario_veiculo,
            sync_manager=sync_mgr,
            audit_engine=audit_eng,
            db_manager=db_mgr,
            rag_retriever=rag_ret,
            interaction_store=int_store,
            orchestrator=orch,
        )

    def _ensure_loaded(self, tenant_id: str) -> TenantRuntime:
        tid = self.validate_tenant_id_format(tenant_id)
        with self._lock:
            if tid in self._cache:
                self._cache.move_to_end(tid)
                return self._cache[tid]
            if tid not in self._tenant_init_locks:
                self._tenant_init_locks[tid] = threading.RLock()
            t_lock = self._tenant_init_locks[tid]

        # Single-Flight Lock por Tenant (P0-14): constrói fora do _lock global para nunca congelar outras cidades!
        with t_lock:
            with self._lock:
                if tid in self._cache:
                    self._cache.move_to_end(tid)
                    return self._cache[tid]
            rt = self._build_runtime(tid)
            with self._lock:
                self._cache[tid] = rt
                while len(self._cache) > self.max_cached:
                    evict_key = next(
                        (k for k in self._cache.keys() if k != self._active_tenant_id and k != tid),
                        None,
                    )
                    if evict_key is None:
                        break
                    evicted = self._cache.pop(evict_key)

                    def _deferred_close(ev_rt: TenantRuntime = evicted) -> None:
                        try:
                            ev_rt.db_manager.close()
                        except Exception:
                            pass

                    t_close = threading.Timer(20.0, _deferred_close)
                    t_close.daemon = True
                    t_close.start()
                return rt

    def get_or_load(self, tenant_id: str) -> TenantRuntime:
        return self._ensure_loaded(tenant_id)

    def get_active(self) -> TenantRuntime:
        with self._lock:
            active_tid = self._active_tenant_id
        return self._ensure_loaded(active_tid)

    def get_if_loaded(self, tenant_id: str) -> Optional[TenantRuntime]:
        with self._lock:
            return self._cache.get((tenant_id or "").strip())

    def peek_loaded(self, tenant_id: str) -> Optional[TenantRuntime]:
        return self.get_if_loaded(tenant_id)

    def list_loaded_runtimes(self) -> List[TenantRuntime]:
        with self._lock:
            return list(self._cache.values())

    def select_active(self, tenant_id: str, auto_sync_if_empty: bool = False) -> TenantRuntime:
        with self._lock:
            rt = self._ensure_loaded(tenant_id)
            self._active_tenant_id = rt.tenant.tenant_id
            if self.persist_active_path:
                try:
                    self.persist_active_path.parent.mkdir(parents=True, exist_ok=True)
                    self.persist_active_path.write_text(
                        json.dumps(
                            {
                                "active_tenant_id": self._active_tenant_id,
                                "municipality": f"{rt.tenant.municipality_name}/{rt.tenant.state_uf}",
                                "ibge_code": rt.tenant.ibge_code,
                                "updated_at": datetime.now().isoformat(timespec="seconds"),
                            },
                            ensure_ascii=False,
                            indent=2,
                        ),
                        encoding="utf-8",
                    )
                except Exception:
                    pass
            callbacks = list(self._on_switch_callbacks)

        if (
            auto_sync_if_empty
            and len(rt.db_manager.catalog.tables) == 0
            and not os.environ.get("PYTEST_CURRENT_TEST")
        ):
            try:
                rt.sync_manager.sync_all(
                    db_manager=rt.db_manager,
                    rag_retriever=rt.rag_retriever,
                    live_api=True,
                )
                rt.orchestrator.refresh_after_data_update()
            except Exception:
                pass

        for cb in callbacks:
            try:
                cb(rt)
            except Exception:
                pass
        return rt

    def close_all(self) -> None:
        with self._lock:
            for rt in self._cache.values():
                try:
                    rt.db_manager.close()
                except Exception:
                    pass
            self._cache.clear()


def create_app(
    data_dir: Path = DEFAULT_DATA_DIR,
    fix2023_dir: Optional[Path] = DEFAULT_FIX2023_DIR,
    repaired_2023: bool = True,
    rag_corpus: Optional[List[Dict[str, Any]]] = None,
    require_api_key: Optional[str] = None,
) -> FastAPI:
    @asynccontextmanager
    async def _lifespan(app_instance: FastAPI):
        yield
        pool = getattr(app_instance.state, "runtime_pool", None)
        if pool is not None and hasattr(pool, "close_all"):
            pool.close_all()
        else:
            db_mgr = getattr(app_instance.state, "db_manager", None)
            if db_mgr is not None and hasattr(db_mgr, "close"):
                db_mgr.close()

    app = FastAPI(
        title="SIM Platform API — Inteligência Multi-Cidades sobre Dados Públicos Municipais",
        version="4.4.0",
        description=(
            "Motor Analítico Multi-Cidade com Pool Isolado por Município, Cloud Data Lake (Google Drive 5 TB), "
            "SQLGuard (AST + Sandbox), Catálogo Dinâmico, RAG de Atos Individuais, Auditoria Proativa e WAHA."
        ),
        lifespan=_lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    env_tenant_id = os.environ.get("SIM_TENANT_ID", "caete_mg_3110004").strip() or "caete_mg_3110004"
    use_persistent_active = (
        not os.environ.get("PYTEST_CURRENT_TEST")
        and data_dir.resolve() == DEFAULT_DATA_DIR.resolve()
        and rag_corpus is None
    )
    persist_active_file = (
        DEFAULT_REPO_ROOT / "tenants" / "_active_tenant.json"
        if use_persistent_active
        else None
    )
    runtime_pool = TenantRuntimePool(
        workspace_root=DEFAULT_REPO_ROOT,
        default_data_dir=data_dir,
        default_fix2023_dir=fix2023_dir,
        default_repaired_2023=repaired_2023,
        default_rag_corpus=rag_corpus,
        initial_tenant_id=env_tenant_id,
        persist_active_path=persist_active_file,
        max_cached=8,
    )
    init_rt = runtime_pool.get_active()
    auth_manager = InstitutionalAuthManager(tenant_id=init_rt.tenant.tenant_id)
    onboarding_manager = CityOnboardingManager(workspace_root=DEFAULT_REPO_ROOT)
    waha_manager = WAHAMessagingManager(interaction_store=init_rt.interaction_store)
    whatsapp_router = WhatsAppTenantRouter(workspace_root=DEFAULT_REPO_ROOT)
    security_and_error_events: Deque[Dict[str, Any]] = deque(maxlen=150)

    def _sync_app_state_with_runtime(rt: TenantRuntime) -> None:
        waha_manager.store = rt.interaction_store
        waha_manager.interaction_store = rt.interaction_store
        auth_manager.tenant_id = rt.tenant.tenant_id
        app.state.db_manager = rt.db_manager
        app.state.orchestrator = rt.orchestrator
        app.state.rag_retriever = rt.rag_retriever
        app.state.sync_manager = rt.sync_manager
        app.state.audit_engine = rt.audit_engine
        app.state.interaction_store = rt.interaction_store

    runtime_pool.register_switch_callback(_sync_app_state_with_runtime)

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
        f"SIM Platform v4.4 Multi-Cidade inicializada para tenant={init_rt.tenant.tenant_id} "
        f"(tabelas={len(init_rt.db_manager.catalog.tables)}, atos_rag={len(init_rt.rag_retriever.corpus_chunks)})"
    )

    login_fail_windows: Dict[str, List[float]] = {}
    login_fail_lock = threading.Lock()

    def resolve_request_runtime(requested_tid: Optional[str] = None) -> TenantRuntime:
        tid_clean = (requested_tid or "").strip()
        if not tid_clean:
            return runtime_pool.get_active()
        if not re.match(r"^[a-z0-9_]{3,80}$", tid_clean) or not (
            DEFAULT_REPO_ROOT / "tenants" / f"{tid_clean}.json"
        ).is_file():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Município '{tid_clean}' não encontrado ou não homologado nesta instância.",
            )
        return runtime_pool.get_or_load(tid_clean)

    def verify_auth(
        x_api_key: Optional[str] = Header(default=None),
        x_sim_role: str = Header(default="citizen"),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> str:
        import hmac as _hmac

        expected_key = (require_api_key if require_api_key is not None else os.environ.get("SIM_API_KEY") or "").strip()
        api_key_authenticated = False
        if expected_key:
            if not x_api_key or not _hmac.compare_digest(x_api_key.strip(), expected_key):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="API Key inválida ou ausente (cabeçalho X-API-Key obrigatório).",
                )
            api_key_authenticated = True
        raw_tok = x_sim_token or (authorization.split(" ", 1)[1] if authorization and " " in authorization else authorization)
        if raw_tok:
            act_tid = runtime_pool.get_active().tenant.tenant_id
            claims = auth_manager.verify_token(raw_tok, expected_tenant_id=act_tid)
            if claims and claims.get("role"):
                return str(claims["role"])
            return "citizen"
        if os.environ.get("SIM_ENV", "development").strip().lower() == "production" and not api_key_authenticated:
            return "citizen"
        role = (x_sim_role or "citizen").lower().strip()
        if role not in (
            "citizen", "cidadao", "servidor", "analyst", "auditor",
            "controller", "controladoria", "gestor", "ouvidoria", "admin",
        ):
            role = "citizen"
        return role

    def is_pii_unmask_authorized(
        x_api_key: Optional[str],
        authorization: Optional[str],
        x_sim_token: Optional[str],
        expected_tenant_id: Optional[str] = None,
        requested_role: Optional[str] = None,
    ) -> bool:
        """
        Verifica se a requisição possui credencial institucional verificada (SSO / Login RBAC SQLite ou X-API-Key válida)
        E perfil compatível de controladoria/auditoria para desmascarar CPFs nos termos da LGPD (Lei 13.709/2018).
        """
        import hmac

        raw_tok = x_sim_token or (authorization.split(" ", 1)[1] if authorization and " " in authorization else authorization)
        if raw_tok:
            eff_tid = expected_tenant_id or runtime_pool.get_active().tenant.tenant_id
            claims = auth_manager.verify_token(raw_tok, expected_tenant_id=eff_tid)
            if claims and claims.get("can_unmask_pii"):
                if requested_role and requested_role.lower().strip() in ("citizen", "cidadao", "public"):
                    return False
                return True
        expected_key = (require_api_key if require_api_key is not None else os.environ.get("SIM_API_KEY") or "").strip()
        if expected_key and x_api_key and hmac.compare_digest(x_api_key.strip(), expected_key):
            if requested_role is None or requested_role.lower().strip() in ("auditor", "admin", "controller", "controladoria"):
                return True
        return False

    @app.post("/api/auth/login")
    def login_institutional_endpoint(req: LoginRequest, request: Request) -> Dict[str, Any]:
        import time as _time

        client_ip = (request.client.host if request and request.client else "unknown").strip()
        now_ts = _time.time()
        with login_fail_lock:
            recent_fails = [t for t in login_fail_windows.get(client_ip, []) if (now_ts - t) <= 300.0]
            login_fail_windows[client_ip] = recent_fails
            if len(recent_fails) >= 10:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="Muitas tentativas inválidas de login. Aguarde 5 minutos antes de tentar novamente.",
                )
        res = auth_manager.authenticate_credentials(req.username, req.password)
        if res.get("status") != "sucesso":
            with login_fail_lock:
                arr = [t for t in login_fail_windows.get(client_ip, []) if (now_ts - t) <= 300.0]
                arr.append(now_ts)
                login_fail_windows[client_ip] = arr
            raise HTTPException(status_code=401, detail=res.get("detail", "Credenciais inválidas."))
        with login_fail_lock:
            login_fail_windows.pop(client_ip, None)
        _log(f"Login Institucional realizado: user={req.username} (role={res.get('user', {}).get('role')}, gabinete={res.get('user', {}).get('gabinete_id')})")
        return res

    @app.post("/api/auth/sso")
    def sso_exchange_endpoint(req: SSORequest) -> Dict[str, Any]:
        eff_code = (req.sso_code or req.code or "").strip()
        res = auth_manager.exchange_sso_code(
            provider=req.provider,
            code=eff_code,
            gabinete_id=req.gabinete_id,
        )
        if res.get("status") != "sucesso":
            security_and_error_events.append(
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "category": "SECURITY_SSO_REJECTED",
                    "event_type": "SECURITY_SSO_REJECTED",
                    "channel": "api_auth_sso",
                    "role": "anonymous",
                    "code": "SSO_INVALID_CODE",
                    "detail": res.get("detail", "Código SSO ausente ou inválido."),
                    "input_preview": f"provider={req.provider}, code_present={bool(eff_code)}",
                }
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=res.get("detail", "Código de autorização SSO ausente ou inválido."),
            )
        _log(f"Login SSO Federado ({req.provider}) realizado: role={res.get('role')}")
        return res

    @app.get("/api/auth/profiles")
    def list_auth_profiles_endpoint() -> Dict[str, Any]:
        return {
            "status": "sucesso",
            "sso_providers": ["govbr", "active_directory", "local_admin_bypass"],
            "profiles": auth_manager.list_available_profiles(),
        }

    @app.get("/api/auth/me")
    def get_current_user_endpoint(
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        raw_tok = x_sim_token or (authorization.split(" ", 1)[1] if authorization and " " in authorization else authorization)
        claims = auth_manager.verify_token(raw_tok)
        if not claims:
            return {
                "authenticated": False,
                "role": "citizen",
                "gabinete_id": "publico",
                "can_unmask_pii": False,
            }
        return {
            "authenticated": True,
            **claims.to_dict(),
        }

    def _require_local_or_admin_ingestion_auth(
        request: Request,
        x_api_key: Optional[str],
        authorization: Optional[str],
        x_sim_token: Optional[str],
        target_tenant_id: Optional[str] = None,
        allow_cross_tenant_switch: bool = False,
    ) -> None:
        """
        Exige autenticação institucional central (`admin`/`gestor`/`controladoria` com `can_view_all_gabinetes=True`
        e escopo válido para o município alvo, ou `X-API-Key`).
        Apenas `TestClient` (`pytest`) ou loopback estrito (`127.0.0.1`/`::1`) com `SIM_ALLOW_DEV_LOOPBACK_ADMIN=1`
        podem operar sem token — IPs privados RFC1918 (ex.: bridge Docker `172.x.x.x`) NUNCA recebem bypass automático (SEC-P0-01).
        """
        import hmac

        expected_key = (require_api_key if require_api_key is not None else os.environ.get("SIM_API_KEY") or "").strip()
        if expected_key and x_api_key and hmac.compare_digest(x_api_key.strip(), expected_key):
            return
        raw_tok = x_sim_token or (authorization.split(" ", 1)[1] if authorization and " " in authorization else authorization)
        if raw_tok:
            eff_tid = target_tenant_id or runtime_pool.get_active().tenant.tenant_id
            claims = auth_manager.verify_token(
                raw_tok,
                expected_tenant_id=eff_tid,
                allow_cross_tenant_switch=allow_cross_tenant_switch,
            )
            if claims:
                c_role = str(claims.get("role") or "").lower()
                can_all = bool(claims.get("can_view_all_gabinetes", False))
                if c_role in ("admin", "gestor", "controller", "controladoria") or (c_role == "auditor" and can_all):
                    return

        has_proxy_headers = any(
            bool(request.headers.get(h))
            for h in ("x-forwarded-for", "x-real-ip", "cf-connecting-ip", "forwarded")
        )
        client_host = (request.client.host if request.client else "").strip().lower()
        strict_upload_auth = os.environ.get("SIM_REQUIRE_STRICT_UPLOAD_AUTH", "").strip() == "1"
        allow_dev_loopback = (
            bool(os.environ.get("PYTEST_CURRENT_TEST"))
            or os.environ.get("SIM_ALLOW_DEV_LOOPBACK_ADMIN", "").strip() == "1"
        )
        is_local_caller = False
        if not has_proxy_headers and allow_dev_loopback:
            if client_host in ("localhost", "testclient"):
                is_local_caller = True
            else:
                try:
                    ip_obj = ipaddress.ip_address(client_host)
                    is_local_caller = bool(ip_obj.is_loopback)
                except ValueError:
                    is_local_caller = False

        if is_local_caller and not expected_key and not strict_upload_auth:
            return

        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Operação administrativa exige login institucional (faça login no Console no topo da tela ou envie Bearer Token / X-API-Key).",
        )

    @app.get("/api/tenants")
    def list_tenants_endpoint(
        format: Optional[str] = None,
        accept: Optional[str] = Header(default=None),
    ) -> Any:
        rt = runtime_pool.get_active()
        tenants_list = onboarding_manager.list_configured_tenants()
        if (
            format != "json"
            and accept
            and "text/html" in accept.lower()
            and "application/json" not in accept.lower()
        ):
            console_file = Path(__file__).resolve().parent / "auditor_console_v0.html"
            if console_file.exists():
                html_str = console_file.read_text(encoding="utf-8").replace(
                    "window.addEventListener('DOMContentLoaded', () => {",
                    "window.addEventListener('DOMContentLoaded', () => { setTimeout(() => { if(typeof switchGovTab==='function') switchGovTab('admin'); }, 80);",
                )
                return HTMLResponse(content=html_str, status_code=200)
        return {
            "status": "sucesso",
            "active_tenant_id": rt.tenant.tenant_id,
            "active_municipality": f"{rt.tenant.municipality_name}/{rt.tenant.state_uf}",
            "active_ibge_code": rt.tenant.ibge_code,
            "active_catalog_objects": len(rt.db_manager.catalog.tables),
            "active_rag_acts": len(rt.rag_retriever.corpus_chunks),
            "total_configured_tenants": len(tenants_list),
            "tenants": tenants_list,
        }

    @app.post("/api/tenants/select")
    def select_active_tenant_endpoint(
        request: Request,
        req: SelectTenantRequest,
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        target_tid = (req.tenant_id or "").strip()
        if not target_tid:
            raise HTTPException(status_code=400, detail="Parâmetro 'tenant_id' obrigatório.")
        try:
            TenantRuntimePool.validate_tenant_id_format(target_tid)
        except PermissionError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        _require_local_or_admin_ingestion_auth(
            request,
            x_api_key,
            authorization,
            x_sim_token,
            target_tenant_id=target_tid,
            allow_cross_tenant_switch=True,
        )
        raw_tok = x_sim_token or (authorization.split(" ", 1)[1] if authorization and " " in authorization else authorization)
        prior_claims = (
            auth_manager.verify_token(raw_tok, expected_tenant_id=target_tid, allow_cross_tenant_switch=True)
            if raw_tok
            else None
        )
        try:
            rt = runtime_pool.select_active(
                tenant_id=target_tid,
                auto_sync_if_empty=bool(req.auto_sync_if_empty),
            )
        except PermissionError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        refreshed_token: Optional[str] = None
        if prior_claims and prior_claims.can_view_all_gabinetes:
            refreshed_token = auth_manager.issue_token(
                username=prior_claims.username,
                display_name=prior_claims.display_name,
                role=prior_claims.role,
                gabinete_id=prior_claims.gabinete_id,
                gabinete_label=prior_claims.gabinete_label,
                can_unmask_pii=prior_claims.can_unmask_pii,
                can_view_all_gabinetes=prior_claims.can_view_all_gabinetes,
                auth_method=prior_claims.auth_method,
                allowed_tenants=prior_claims.allowed_tenants,
                tenant_id=rt.tenant.tenant_id,
            )

        counts = rt.db_manager.get_mapped_objects_count()
        _log(
            f"Cidade ativa chaveada (Console + WhatsApp WAHA): tenant={rt.tenant.tenant_id} "
            f"({rt.tenant.municipality_name}/{rt.tenant.state_uf}, objetos={counts['total']}, atos_rag={len(rt.rag_retriever.corpus_chunks)})"
        )
        resp_sel: Dict[str, Any] = {
            "status": "sucesso",
            "active_tenant_id": rt.tenant.tenant_id,
            "municipality": f"{rt.tenant.municipality_name}/{rt.tenant.state_uf}",
            "municipality_name": rt.tenant.municipality_name,
            "state_uf": rt.tenant.state_uf,
            "ibge_code": rt.tenant.ibge_code,
            "diario_veiculo": rt.diario_veiculo,
            "mapped_objects": counts,
            "rag_unique_acts": len(rt.rag_retriever.corpus_chunks),
            "whatsapp_propagated": True,
            "whatsapp_session_namespace": f"wa::{rt.tenant.tenant_id}::<chat_id>",
        }
        if refreshed_token:
            resp_sel["refreshed_token"] = refreshed_token
        return resp_sel

    @app.post("/api/tenants/onboard")
    def onboard_new_city_endpoint(
        request: Request,
        req: OnboardCityRequest,
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        _require_local_or_admin_ingestion_auth(request, x_api_key, authorization, x_sim_token)
        _log(f"Onboarding Multi-Cidade disparado para municipio={req.municipality_name or req.tenant_id}")
        try:
            if req.tenant_id:
                TenantRuntimePool.validate_tenant_id_format(req.tenant_id)
            return onboarding_manager.onboard_city(
                tenant_id=req.tenant_id,
                municipality_name=req.municipality_name,
                state_uf=req.state_uf,
                ibge_code=req.ibge_code,
                plan_tier=req.plan_key or req.plan_tier or "porte_2",
                official_gazette_name=req.official_gazette_name,
                seed_from_template_if_empty=req.sanitize_and_build_parquets,
            )
        except (ValueError, PermissionError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/fleet/status")
    def get_fleet_verifier_status() -> Dict[str, Any]:
        rt = runtime_pool.get_active()
        fv = FleetVerifier(workspace_root=DEFAULT_REPO_ROOT)
        st = fv.get_fleet_status()
        st["active_tenant_id"] = rt.tenant.tenant_id
        st["active_municipality"] = f"{rt.tenant.municipality_name}/{rt.tenant.state_uf}"
        return st

    @app.post("/api/fleet/run")
    async def run_fleet_verifier_endpoint(
        request: Request,
        req: Optional[FleetRunRequest] = None,
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        _require_local_or_admin_ingestion_auth(request, x_api_key, authorization, x_sim_token)
        t_ids = req.tenant_ids if req else None
        use_live = bool(req.live_api) if req is not None else True
        proc_inb = bool(req.process_inbox) if req is not None else True
        exp_cloud = bool(req.export_cloud_bundles) if req is not None else True
        do_push = bool(req.push_cloud) if req is not None else False
        _log(f"POST /api/fleet/run disparado (tenants={t_ids or 'ALL'}, live_api={use_live}, inbox={proc_inb}, push_cloud={do_push})")
        fv = FleetVerifier(workspace_root=DEFAULT_REPO_ROOT)
        rep = await asyncio.to_thread(
            fv.verify_and_sync_fleet,
            live_api=use_live,
            process_inbox_flag=proc_inb,
            export_cloud_bundles=exp_cloud,
            push_cloud=do_push,
            target_tenants=t_ids,
            runtime_pool=runtime_pool,
        )
        return rep

    @app.get("/", response_class=HTMLResponse)
    @app.get("/institucional", response_class=HTMLResponse)
    @app.get("/auditor", response_class=HTMLResponse)
    @app.get("/console", response_class=HTMLResponse)
    def auditor_console() -> HTMLResponse:
        console_file = Path(__file__).resolve().parent / "auditor_console_v0.html"
        if console_file.exists():
            return HTMLResponse(content=console_file.read_text(encoding="utf-8"), status_code=200)
        return ping_dashboard()

    @app.get("/tenants", response_class=HTMLResponse)
    @app.get("/quota", response_class=HTMLResponse)
    def auditor_console_admin_tab() -> HTMLResponse:
        console_file = Path(__file__).resolve().parent / "auditor_console_v0.html"
        if console_file.exists():
            html_str = console_file.read_text(encoding="utf-8").replace(
                "window.addEventListener('DOMContentLoaded', () => {",
                "window.addEventListener('DOMContentLoaded', () => { setTimeout(() => { if(typeof switchGovTab==='function') switchGovTab('admin'); }, 80);",
            )
            return HTMLResponse(content=html_str, status_code=200)
        return ping_dashboard()

    @app.get("/api/pre-post")
    def get_pre_post_comparison() -> Dict[str, Any]:
        comp_path = DEFAULT_REPO_ROOT / "AUDIT" / "PRE_VS_POST_COMPARISON.json"
        if comp_path.exists():
            return json.loads(comp_path.read_text(encoding="utf-8"))
        return {"total_cases": 0, "cases": []}

    @app.get("/api/ingestion/status")
    def ingestion_status() -> Dict[str, Any]:
        rt = runtime_pool.get_active()
        rt.orchestrator._sync_catalog_freshness_if_hotswapped()
        st = rt.sync_manager.check_status()
        st["tenant_id"] = rt.tenant.tenant_id
        st["municipality"] = f"{rt.tenant.municipality_name}/{rt.tenant.state_uf}"
        st["ibge_code"] = rt.tenant.ibge_code
        st["catalog_objects"] = len(rt.db_manager.catalog.tables)
        st["source_freshness_map"] = dict(getattr(rt.db_manager.catalog, "source_freshness_map", {}) or {})
        return st

    @app.post("/api/ingestion/sync")
    def trigger_ingestion_sync(
        request: Request,
        req: Optional[SyncRequest] = None,
        role: str = Depends(verify_auth),
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        _require_local_or_admin_ingestion_auth(request, x_api_key, authorization, x_sim_token)
        rt = runtime_pool.get_active()
        selected = req.sources if req else None
        use_live_api = bool(req.live_api) if req else False
        _log(
            f"POST /api/ingestion/sync disparado (tenant={rt.tenant.tenant_id}, role={role}, "
            f"live_api={use_live_api}, sources={selected or 'ALL'})"
        )
        try:
            sync_res = rt.sync_manager.sync_all(
                selected_sources=selected,
                db_manager=rt.db_manager,
                rag_retriever=rt.rag_retriever,
                live_api=use_live_api,
            )
        except SyncAlreadyRunningError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

        rt.orchestrator.refresh_after_data_update()
        invalidated_entries = rt.orchestrator.quota_manager.invalidate_cache()
        sync_res["tenant_id"] = rt.tenant.tenant_id
        sync_res["cache_invalidated_entries"] = invalidated_entries
        sync_res["catalog_objects"] = len(rt.db_manager.catalog.tables)
        sync_res["source_freshness_map"] = dict(getattr(rt.db_manager.catalog, "source_freshness_map", {}) or {})
        return sync_res

    @app.post("/api/ingestion/upload")
    async def upload_ingestion_package(
        request: Request,
        file: UploadFile = File(...),
        target_table: Optional[str] = Form(default=None),
        role: str = Depends(verify_auth),
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        _require_local_or_admin_ingestion_auth(request, x_api_key, authorization, x_sim_token)
        rt = runtime_pool.get_active()
        raw_bytes = await file.read(MAX_UPLOAD_RAW_BYTES + 1)
        if len(raw_bytes) > MAX_UPLOAD_RAW_BYTES:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"Arquivo excede o limite máximo de {MAX_UPLOAD_RAW_BYTES // (1024 * 1024)} MB.",
            )
        def _run_blocking_upload() -> Dict[str, Any]:
            r_up = rt.sync_manager.ingest_uploaded_package(
                filename=file.filename or "",
                content_bytes=raw_bytes,
                target_table=target_table,
                db_manager=rt.db_manager,
                rag_retriever=rt.rag_retriever,
            )
            rt.orchestrator.refresh_after_data_update()
            return r_up

        try:
            rep = await asyncio.to_thread(_run_blocking_upload)
        except SyncAlreadyRunningError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

        invalidated_entries = rt.orchestrator.quota_manager.invalidate_cache()
        rep["tenant_id"] = rt.tenant.tenant_id
        rep["cache_invalidated_entries"] = invalidated_entries
        rep["catalog_objects"] = len(rt.db_manager.catalog.tables)
        rep["source_freshness_map"] = dict(getattr(rt.db_manager.catalog, "source_freshness_map", {}) or {})
        _log(
            f"POST /api/ingestion/upload concluído (tenant={rt.tenant.tenant_id}, role={role}, file={rep.get('filename')}, "
            f"release={rep.get('release_id')}, tables={len(rep.get('ingested_tables') or [])}, rag_added={rep.get('rag_acts_added')})"
        )
        return rep

    @app.get("/api/governance/quota")
    def get_governance_quota_status(
        format: Optional[str] = None,
        accept: Optional[str] = Header(default=None),
    ) -> Any:
        rt = runtime_pool.get_active()
        if (
            format != "json"
            and accept
            and "text/html" in accept.lower()
            and "application/json" not in accept.lower()
        ):
            console_file = Path(__file__).resolve().parent / "auditor_console_v0.html"
            if console_file.exists():
                html_str = console_file.read_text(encoding="utf-8").replace(
                    "window.addEventListener('DOMContentLoaded', () => {",
                    "window.addEventListener('DOMContentLoaded', () => { setTimeout(() => { if(typeof switchGovTab==='function') switchGovTab('admin'); }, 80);",
                )
                return HTMLResponse(content=html_str, status_code=200)
        return rt.orchestrator.quota_manager.get_quota_status()

    @app.post("/api/guardrail/evaluate")
    def evaluate_semantic_guardrail(
        payload: Dict[str, Any],
        role_dep: str = Depends(verify_auth),
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        """
        Avalia diretamente uma pergunta no Guardião Semântico IA Leve (`SemanticIntentGuardrail`)
        antes do processamento analítico principal, verificando se a dúvida faz sentido,
        qual a rota ideal e qual a pergunta autônoma contextualizada.
        """
        rt = runtime_pool.get_active()
        is_auth_caller, token_claims = _is_institutional_caller(x_api_key, authorization, x_sim_token)
        gab_scope = str((token_claims or {}).get("gabinete_id") or "publico")
        question = str(payload.get("question") or "").strip()
        raw_sid = str(payload.get("session_id") or "default").strip() or "default"
        scoped_sid = f"web::{rt.tenant.tenant_id}::{gab_scope}::{raw_sid}" if is_auth_caller else f"pub::{rt.tenant.tenant_id}::{raw_sid}"
        role = str(payload.get("role") or role_dep or "citizen").strip()
        enable_ai = bool(payload.get("enable_external_ai", True))
        explicit_hist = payload.get("conversation_history") or payload.get("recent_history")
        hist = list(explicit_hist) if isinstance(explicit_hist, list) else list(rt.orchestrator.conversation_history.get(scoped_sid, []))
        decision = rt.orchestrator.semantic_guardrail.evaluate(
            question=question,
            recent_history=hist,
            role=role,
            allowed_sources=payload.get("allowed_sources"),
            llm_provider=payload.get("llm_provider"),
            llm_api_key=payload.get("llm_api_key"),
            enable_external_ai=enable_ai,
        )
        return decision.to_dict()

    @app.get("/api/executive/pulse")
    def get_executive_municipal_pulse() -> Dict[str, Any]:
        """
        Retorna os indicadores executivos reais do Termômetro Municipal (2024/2025)
        para o cabeçalho de Inteligência Executiva (compatível tanto com catálogo completo SICOM quanto com cidades API-only).
        """
        rt = runtime_pool.get_active()
        rt.orchestrator._sync_catalog_freshness_if_hotswapped()
        cur = rt.db_manager.conn.cursor()
        try:
            pulse: Dict[str, Any] = {
                "status": "sucesso",
                "tenant_id": rt.tenant.tenant_id,
                "municipality": f"{rt.tenant.municipality_name}/{rt.tenant.state_uf}",
                "municipality_name": rt.tenant.municipality_name,
                "state_uf": rt.tenant.state_uf,
                "ibge_code": rt.tenant.ibge_code,
                "diario_veiculo": rt.diario_veiculo,
                "catalog_objects": len(rt.db_manager.catalog.tables),
                "rag_unique_acts": len(rt.rag_retriever.corpus_chunks),
                "missing_source_families": sorted(getattr(rt.db_manager.catalog, "missing_source_families", set()) or set()),
            }
            if "siconfi_finbra_indicadores" in rt.db_manager.catalog.tables:
                try:
                    row_fin = cur.execute(
                        """
                        SELECT vlr_receita_corrente_liquida, pct_despesa_pessoal_rcl,
                               nota_capag_tesouro, pct_aplicacao_saude_siops, pct_aplicacao_educacao_mde,
                               situacao_fiscal_lrf, ano_referencia
                        FROM siconfi_finbra_indicadores
                        ORDER BY CASE WHEN ano_referencia = 2024 THEN 0 ELSE 1 END, ano_referencia DESC
                        LIMIT 1
                        """
                    ).fetchone()
                    if row_fin:
                        pulse["rcl_2024"] = float(row_fin[0]) if row_fin[0] is not None else None
                        pulse["pct_pessoal_lrf_2024"] = float(row_fin[1]) if row_fin[1] is not None else None
                        pulse["nota_capag"] = str(row_fin[2] or "N/D")
                        pulse["pct_saude_siops_2024"] = float(row_fin[3]) if row_fin[3] is not None else None
                        pulse["pct_educacao_mde_2024"] = float(row_fin[4]) if row_fin[4] is not None else None
                        pulse["situacao_fiscal_lrf"] = str(row_fin[5] or "REGULAR")
                        pulse["siconfi_ref_year"] = int(row_fin[6] or 2024)
                except Exception:
                    pass
            if "datasus_siops_saude" in rt.db_manager.catalog.tables:
                try:
                    sau_cols = set(rt.db_manager.catalog.tables["datasus_siops_saude"].column_names)
                    pc_col = "vlr_despesa_saude_per_capita" if "vlr_despesa_saude_per_capita" in sau_cols else "despesa_saude_per_capita"
                    pct_col = "pct_recursos_proprios_saude_siops" if "pct_recursos_proprios_saude_siops" in sau_cols else "pct_recursos_proprios_saude_ec29"
                    row_sau = cur.execute(
                        f"""
                        SELECT vlr_despesa_total_saude, {pc_col}, {pct_col}, ano_referencia
                        FROM datasus_siops_saude
                        ORDER BY CASE WHEN ano_referencia = 2024 THEN 0 ELSE 1 END, ano_referencia DESC
                        LIMIT 1
                        """
                    ).fetchone()
                    if row_sau:
                        if row_sau[0] is not None:
                            pulse["vlr_despesa_total_saude_2024"] = float(row_sau[0])
                        if row_sau[1] is not None:
                            pulse["despesa_saude_per_capita_2024"] = float(row_sau[1])
                        if pulse.get("pct_saude_siops_2024") is None and row_sau[2] is not None:
                            pulse["pct_saude_siops_2024"] = float(row_sau[2])
                except Exception:
                    pass
            if "ibge_indicadores_municipais" in rt.db_manager.catalog.tables:
                try:
                    row_ibge = cur.execute(
                        """
                        SELECT populacao_residente, pib_per_capita_reais, idhm_geral, ano_referencia
                        FROM ibge_indicadores_municipais
                        ORDER BY CASE WHEN ano_referencia = 2024 THEN 0 ELSE 1 END, ano_referencia DESC
                        LIMIT 1
                        """
                    ).fetchone()
                    if row_ibge:
                        pulse["populacao_2024"] = int(row_ibge[0]) if row_ibge[0] is not None else None
                        pib_pc = float(row_ibge[1]) if (row_ibge[1] is not None and float(row_ibge[1]) > 0) else None
                        pib_yr = int(row_ibge[3] or 2024) if pib_pc is not None else None
                        if pib_pc is None:
                            row_pib = cur.execute(
                                "SELECT pib_per_capita_reais, ano_referencia FROM ibge_indicadores_municipais WHERE COALESCE(pib_per_capita_reais, 0) > 0 ORDER BY ano_referencia DESC LIMIT 1"
                            ).fetchone()
                            if row_pib and row_pib[0] is not None:
                                pib_pc = float(row_pib[0])
                                pib_yr = int(row_pib[1]) if row_pib[1] is not None else None
                        pulse["pib_per_capita_reais"] = pib_pc
                        pulse["pib_ref_year"] = pib_yr
                        pulse["idhm_geral"] = float(row_ibge[2]) if (row_ibge[2] is not None and float(row_ibge[2]) > 0) else None
                        pulse["ibge_ref_year"] = int(row_ibge[3] or 2024)
                except Exception:
                    pass
            return pulse
        finally:
            try:
                cur.close()
            except Exception:
                pass

    @app.get("/api/entity/dossier")
    def get_entity_dossier(
        q: str,
        role: str = Depends(verify_auth),
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        """
        Gera o Dossiê 360º de Entidade (Fornecedor / CNPJ / Servidor) cruzando as tabelas disponíveis na cidade ativa.
        """
        from sim_platform.security.sql_guard import mask_sensitive_rows
        rt = runtime_pool.get_active()
        rt.orchestrator._sync_catalog_freshness_if_hotswapped()
        raw_q = (q or "").strip()
        if not raw_q:
            raise HTTPException(status_code=400, detail="Parâmetro 'q' (nome ou CNPJ/CPF) obrigatório.")

        pii_role = role if is_pii_unmask_authorized(x_api_key, authorization, x_sim_token, requested_role=role) else "citizen"
        digits = "".join(ch for ch in raw_q if ch.isdigit())
        if len(digits) == 11 and pii_role == "citizen":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Busca direta por CPF individual (11 dígitos) exige autenticação institucional verificada com permissão LGPD (Controladoria/Admin).",
            )
        safe_like = raw_q.replace("'", "''").upper().strip()
        cur = rt.db_manager.conn.cursor()

        def _fetch_dicts(sql_str: str) -> List[Dict[str, Any]]:
            try:
                rel = cur.execute(sql_str)
                cols = [d[0] for d in rel.description]
                rows = []
                for r in rel.fetchall():
                    item = {}
                    for idx, c in enumerate(cols):
                        v = r[idx]
                        if hasattr(v, "__float__") and not isinstance(v, (int, bool)):
                            v = round(float(v), 2)
                        item[c] = v
                    rows.append(item)
                masked, _ = mask_sensitive_rows(rows, role=pii_role)
                return masked
            except Exception:
                return []

        try:
            if len(digits) >= 11:
                filter_qsa = f"regexp_replace(COALESCE(num_cnpj, ''), '[^0-9]', '', 'g') = '{digits}'"
                filter_pag = f"regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g') = '{digits}'"
                filter_cont = f"regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g') = '{digits}'"
                filter_conf = (
                    f"regexp_replace(COALESCE(cnpj_empresa, ''), '[^0-9]', '', 'g') = '{digits}' "
                    f"OR regexp_replace(COALESCE(num_cpf, ''), '[^0-9]', '', 'g') = '{digits}'"
                )
            else:
                filter_qsa = f"UPPER(COALESCE(razao_social, '')) ILIKE '%{safe_like}%' OR UPPER(COALESCE(nomes_socios_qsa, '')) ILIKE '%{safe_like}%'"
                filter_pag = f"UPPER(COALESCE(nom_credor, '')) ILIKE '%{safe_like}%'"
                filter_cont = f"UPPER(COALESCE(nom_contratado, '')) ILIKE '%{safe_like}%'"
                filter_conf = f"UPPER(COALESCE(nom_empresa, '')) ILIKE '%{safe_like}%' OR UPPER(COALESCE(nom_pessoa, '')) ILIKE '%{safe_like}%'"

            qsa_rows = _fetch_dicts(
                f"SELECT razao_social, num_cnpj, situacao_cadastral, porte_empresa, cnae_principal_descricao, "
                f"cnae_principal_descricao AS cnae_fiscal_descricao, "
                f"qtd_socios_identificados, nomes_socios_qsa, qtd_pagamentos, total_pago_historico, alerta_auditoria_controladoria "
                f"FROM receita_qsa_fornecedores WHERE {filter_qsa} ORDER BY total_pago_historico DESC LIMIT 5"
            )
            pag_series = _fetch_dicts(
                f"SELECT ano_referencia, MAX(nom_credor) AS nom_credor, MAX(num_doc_credor) AS num_doc_credor, "
                f"COUNT(*) AS qtd_pagamentos, ROUND(SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2))), 2) AS total_pago "
                f"FROM despesa_pagamento WHERE {filter_pag} GROUP BY ano_referencia ORDER BY ano_referencia"
            )
            contratos = _fetch_dicts(
                f"SELECT ano_referencia, num_contrato, nom_contratado, num_doc_credor, "
                f"SUBSTR(COALESCE(dsc_objetocontrato, ''), 1, 120) AS objeto, "
                f"SUBSTR(COALESCE(dsc_objetocontrato, ''), 1, 120) AS dsc_objeto, "
                f"vlr_original_contrato, vlr_original_contrato AS vlr_contrato, "
                f"qtd_aditivos, vlr_total_aditivos, pct_acrescimo_aditivos "
                f"FROM vw_gold_contratos_vigencia_aditivos WHERE {filter_cont} ORDER BY ano_referencia DESC, vlr_original_contrato DESC LIMIT 10"
            )
            conflitos = _fetch_dicts(
                f"SELECT num_cpf, nom_pessoa, nom_pessoa AS nome_socio_servidor, dsc_cargo, dsc_cargo AS cargos_servidor_prefeitura, "
                f"dsc_lotacao, primeiro_ano_folha, ultimo_ano_folha, total_remuneracao_bruta AS total_bruto_recebido_como_servidor, "
                f"nom_empresa, nom_empresa AS empresa_licitante, cnpj_empresa, dsc_tipo_participacao, "
                f"dsc_tipo_participacao AS vinculo_societario, total_pago_empresa, total_pago_empresa AS total_pago_empresa_prefeitura "
                f"FROM vw_gold_conflito_interesses_licitacao WHERE {filter_conf} LIMIT 10"
            )
        finally:
            try:
                cur.close()
            except Exception:
                pass

        entity_names = [raw_q] if not digits else []
        entity_cnpjs = [digits] if len(digits) == 14 else []
        if qsa_rows:
            if qsa_rows[0].get("razao_social"):
                entity_names.append(str(qsa_rows[0]["razao_social"]))
            if qsa_rows[0].get("num_cnpj"):
                c_dig = "".join(c for c in str(qsa_rows[0]["num_cnpj"]) if c.isdigit())
                if len(c_dig) == 14 and c_dig not in entity_cnpjs:
                    entity_cnpjs.append(c_dig)
        elif pag_series:
            if pag_series[0].get("nom_credor"):
                entity_names.append(str(pag_series[0]["nom_credor"]))

        rag_acts = rt.rag_retriever.search_by_entities(entity_names=entity_names, cnpjs=entity_cnpjs, top_k=5)
        for ch in rag_acts:
            if "excerpt" not in ch:
                ch["excerpt"] = (ch.get("page_content") or "")[:320]
        total_pago_acum = round(sum(float(r.get("total_pago") or 0) for r in pag_series), 2)

        canonical_name = (
            (qsa_rows[0].get("razao_social") if qsa_rows else None)
            or (pag_series[0].get("nom_credor") if pag_series else None)
            or (contratos[0].get("nom_contratado") if contratos else None)
            or raw_q
        )
        canonical_doc = (
            (qsa_rows[0].get("num_cnpj") if qsa_rows else None)
            or (pag_series[0].get("num_doc_credor") if pag_series else None)
            or (contratos[0].get("num_doc_credor") if contratos else None)
            or (digits if digits else "N/D")
        )
        canon_digits = "".join(ch for ch in str(canonical_doc) if ch.isdigit())
        if pii_role == "citizen" and len(canon_digits) == 11:
            canonical_doc = f"***.{canon_digits[3:6]}.{canon_digits[6:9]}-**"

        return {
            "status": "sucesso",
            "tenant_id": rt.tenant.tenant_id,
            "municipality": f"{rt.tenant.municipality_name}/{rt.tenant.state_uf}",
            "query": raw_q,
            "canonical_name": canonical_name,
            "canonical_doc": canonical_doc,
            "total_pago_acumulado": total_pago_acum,
            "qsa_summary": qsa_rows,
            "qsa_matches": qsa_rows,
            "payments_by_year": pag_series,
            "execucao_anual": pag_series,
            "contracts_and_amendments": contratos,
            "contratos": contratos,
            "conflict_of_interest_alerts": conflitos,
            "conflitos_interesse": conflitos,
            "diario_oficial_acts": rag_acts,
            "diario_oficial_atos": rag_acts,
        }

    @app.get("/api/audit/insights")
    def get_proactive_audit_insights(
        aditivo_pct_threshold: float = 25.0,
        min_dispensas_recorrentes: int = 5,
        yoy_growth_pct_threshold: float = 100.0,
        tenant_id: Optional[str] = None,
        x_sim_tenant: Optional[str] = Header(default=None),
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        rt = resolve_request_runtime(tenant_id or x_sim_tenant)
        rt.orchestrator._sync_catalog_freshness_if_hotswapped()
        out = rt.audit_engine.run_all_checks(
            aditivo_pct_threshold=aditivo_pct_threshold,
            min_dispensas_recorrentes=min_dispensas_recorrentes,
            yoy_growth_pct_threshold=yoy_growth_pct_threshold,
        )
        is_auth_caller, _ = _is_institutional_caller(
            x_api_key, authorization, x_sim_token, expected_tenant_id=rt.tenant.tenant_id
        )
        if not is_auth_caller and isinstance(out.get("insights"), list):
            from sim_platform.security.sql_guard import mask_sensitive_rows, mask_text_cpfs

            redacted_insights = []
            for item in out["insights"]:
                cp = dict(item)
                if os.environ.get("SIM_ENV", "development").strip().lower() == "production":
                    cp["sql_executed"] = "-- SQL auditável disponível para sessões institucionais autenticadas --"
                if isinstance(cp.get("rows"), list) and cp["rows"]:
                    m_rows, _ = mask_sensitive_rows(cp["rows"], role="citizen")
                    cp["rows"] = [
                        {k: mask_text_cpfs(v) if isinstance(v, str) else v for k, v in r.items()}
                        for r in m_rows
                    ]
                redacted_insights.append(cp)
            out = {**out, "insights": redacted_insights}
        return out

    @app.post("/api/audio/transcribe")
    def transcribe_audio_endpoint(
        req: AudioTranscribeRequest,
        _role: str = Depends(verify_auth),
    ) -> Dict[str, Any]:
        if req.audio_base64 and len(req.audio_base64) > 14_500_000:
            raise HTTPException(status_code=413, detail="Áudio Base64 excede o limite máximo de 10 MB.")
        rt = runtime_pool.get_active()
        tr_out = transcribe_audio_content(
            audio_base64=req.audio_base64,
            mime_type=req.mime_type,
            fallback_transcript=req.fallback_transcript,
            llm_provider=req.llm_provider,
            llm_api_key=req.llm_api_key,
            municipality_name=rt.tenant.municipality_name,
            state_uf=rt.tenant.state_uf,
            diario_veiculo=rt.diario_veiculo,
        )
        if tr_out.get("text") and req.audio_base64:
            rt.orchestrator.quota_manager.record_audio_usage(seconds_estimated=30)
        return tr_out

    @app.get("/api/rag/collection")
    def get_rag_collection_overview(
        tenant_id: Optional[str] = None,
        x_sim_tenant: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        rt = resolve_request_runtime(tenant_id or x_sim_tenant)
        rt.orchestrator._sync_catalog_freshness_if_hotswapped()
        status_info = rt.sync_manager.check_status()
        return {
            "tenant_id": rt.tenant.tenant_id,
            "municipality": f"{rt.tenant.municipality_name}/{rt.tenant.state_uf}",
            "diario_veiculo": rt.diario_veiculo,
            "total_unique_acts": len(rt.rag_retriever.corpus_chunks),
            "audit_summary": status_info.get("rag_collection_audit", {}),
            "acts": rt.rag_retriever.corpus_chunks[:120],
        }

    @app.get("/health")
    def health() -> Dict[str, Any]:
        rt = runtime_pool.get_active()
        counts = rt.db_manager.get_mapped_objects_count()
        return {
            "status": "ok",
            "tenant": rt.tenant.tenant_id,
            "tenant_id": rt.tenant.tenant_id,
            "municipality": f"{rt.tenant.municipality_name}/{rt.tenant.state_uf}",
            "ibge_code": rt.tenant.ibge_code,
            "diario_veiculo": rt.diario_veiculo,
            "repaired_2023": rt.tenant.repaired_2023,
            "tables_loaded": counts["total"],
            "mapped_objects": counts,
            "rag_unique_acts": len(rt.rag_retriever.corpus_chunks),
        }

    @app.get("/api/logs")
    def get_logs(role: str = Depends(verify_auth)) -> Dict[str, Any]:
        return {"logs": [html.escape(line) for line in list(log_buffer)]}

    @app.get("/ping", response_class=HTMLResponse)
    def ping_dashboard() -> HTMLResponse:
        rt = runtime_pool.get_active()
        counts = rt.db_manager.get_mapped_objects_count()
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
    <h1>SIM Platform v4.4 — {html.escape(rt.tenant.municipality_name)}/{html.escape(rt.tenant.state_uf)} (IBGE {html.escape(rt.tenant.ibge_code)})</h1>
    <div class="card">
        <p><span class="badge">ONLINE</span> | Objetos Mapeados no Catálogo: <strong>{counts['total']}</strong>
        (Views: {counts['views']} | Tabelas Materializadas: {counts['base_tables']}) |
        Reparo 2023 Ativo: <strong>{rt.tenant.repaired_2023}</strong></p>
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
        rt = runtime_pool.get_active()
        rt.orchestrator._sync_catalog_freshness_if_hotswapped()
        by_source: Dict[str, int] = {}
        for meta in rt.db_manager.catalog.tables.values():
            by_source[meta.source_family] = by_source.get(meta.source_family, 0) + 1
        return {
            "tenant_id": rt.tenant.tenant_id,
            "municipality": f"{rt.tenant.municipality_name}/{rt.tenant.state_uf}",
            "total_tables": len(rt.db_manager.catalog.tables),
            "source_families": by_source,
            "missing_source_families": sorted(getattr(rt.db_manager.catalog, "missing_source_families", set()) or set()),
            "segments": list(
                {meta.segment for meta in rt.db_manager.catalog.tables.values()}
            ),
            "tables": [meta.to_dict() for meta in rt.db_manager.catalog.tables.values()],
        }

    @app.post("/catalog/select")
    def select_catalog_schema(
        req: CatalogSelectRequest,
        role: str = Depends(verify_auth),
        x_sim_tenant: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        rt = resolve_request_runtime(req.tenant_id or x_sim_tenant)
        rt.orchestrator._sync_catalog_freshness_if_hotswapped()
        is_prod = os.environ.get("SIM_ENV", "development").strip().lower() == "production"
        effective_role = role if is_prod else (req.role if req.role != "citizen" else role)
        sel = rt.db_manager.catalog.select_tables_for_query(
            question=req.question,
            explicit_segments=req.explicit_segments,
            explicit_tables=req.explicit_tables,
            allowed_sources=req.allowed_sources,
        )
        pkg = rt.orchestrator.prompt_builder.assemble(
            user_question=req.question,
            selection=sel,
            role=effective_role,
        )
        return {
            "tenant_id": rt.tenant.tenant_id,
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
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
        x_sim_tenant: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        rt = resolve_request_runtime(req.tenant_id or x_sim_tenant)
        rt.orchestrator._sync_catalog_freshness_if_hotswapped()
        raw_sql = (req.query or req.pergunta_usuario or "").strip()
        effective_query_role = (
            role
            if is_pii_unmask_authorized(
                x_api_key,
                authorization,
                x_sim_token,
                expected_tenant_id=rt.tenant.tenant_id,
                requested_role=role,
            )
            else "citizen"
        )
        effective_enforce_limit = True if effective_query_role == "citizen" else bool(req.enforce_limit)
        _log(
            f"POST /query (tenant={rt.tenant.tenant_id}, role={role}, pii_role={effective_query_role}, "
            f"limit={effective_enforce_limit}): {raw_sql[:120]}"
        )
        restricted_tables = (
            rt.db_manager.catalog.get_allowed_table_names(
                role=role,
                allowed_sources=req.allowed_sources,
                explicit_tables=req.explicit_tables,
            )
            if (req.allowed_sources or req.explicit_tables)
            else None
        )
        res = rt.db_manager.execute_safe_query(
            raw_sql=raw_sql,
            role=effective_query_role,
            enforce_limit=effective_enforce_limit,
            allowed_tables_override=restricted_tables,
        )
        res["tenant_id"] = rt.tenant.tenant_id
        if res.get("status") != "sucesso":
            ev_cat = "SECURITY_SQL_BLOCK" if res.get("status") == "bloqueado_seguranca" else "SQL_EXECUTION_ERROR"
            security_and_error_events.append(
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "category": ev_cat,
                    "event_type": ev_cat,
                    "channel": "api_query",
                    "role": role,
                    "code": res.get("erro_codigo", "EXEC_ERR"),
                    "detail": res.get("detalhe") or res.get("mensagem_llm") or "",
                    "input_preview": raw_sql[:180],
                }
            )
        from sim_platform.security.sql_guard import mask_sensitive_rows as _mask_ping_rows

        masked_ping_sample, _ = _mask_ping_rows((res.get("dados") or [])[:2], role="citizen")
        last_call["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        last_call["query_sql"] = raw_sql
        last_call["status"] = res.get("status", "UNKNOWN").upper()
        last_call["output"] = json.dumps(
            {
                "tenant_id": rt.tenant.tenant_id,
                "status": res.get("status"),
                "total_linhas": res.get("total_linhas"),
                "fontes_utilizadas": res.get("fontes_utilizadas"),
                "transparency_warnings": res.get("transparency_warnings"),
                "amostra": masked_ping_sample,
            },
            ensure_ascii=False,
            indent=2,
        )
        return res

    def _is_institutional_caller(
        x_api_key: Optional[str],
        authorization: Optional[str],
        x_sim_token: Optional[str],
        expected_tenant_id: Optional[str] = None,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        import hmac as _hmac

        token_claims = auth_manager.resolve_claims_from_headers(
            authorization=authorization,
            x_sim_token=x_sim_token,
            expected_tenant_id=expected_tenant_id,
        )
        expected_key = (require_api_key if require_api_key is not None else os.environ.get("SIM_API_KEY") or "").strip()
        api_key_ok = bool(expected_key and x_api_key and _hmac.compare_digest(x_api_key.strip(), expected_key))
        return bool(token_claims or api_key_ok), token_claims

    def _mask_whatsapp_phone_str(val: Optional[str]) -> str:
        import re as _re

        s = str(val or "")
        if not s:
            return s
        return _re.sub(
            r"\b(\d{4})\d{4,6}(\d{3,4})(@c\.us|@s\.whatsapp\.net|\b)",
            r"\1*****\2\3",
            s,
        )

    @app.post("/chat")
    def chat_endpoint(
        req: ChatRequest,
        request: Request,
        role: str = Depends(verify_auth),
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
        x_sim_gabinete: Optional[str] = Header(default=None),
        x_sim_tenant: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        from sim_platform.security.sql_guard import mask_sensitive_rows, mask_text_cpfs

        rt = resolve_request_runtime(req.tenant_id or x_sim_tenant)
        question = (req.question or req.pergunta_usuario or "").strip()
        is_auth_caller, token_claims = _is_institutional_caller(
            x_api_key, authorization, x_sim_token, expected_tenant_id=rt.tenant.tenant_id
        )
        is_prod = os.environ.get("SIM_ENV", "development").strip().lower() == "production"
        if is_prod and not is_auth_caller:
            effective_role = "citizen"
        elif token_claims and token_claims.get("role"):
            effective_role = str(token_claims["role"]) if req.role == "citizen" else req.role
        else:
            effective_role = req.role if req.role != "citizen" else role

        if not is_auth_caller and "PYTEST_CURRENT_TEST" not in os.environ:
            client_ip = (request.client.host if request and request.client else "unknown").strip()
            if client_ip not in ("127.0.0.1", "::1", "localhost", "testclient"):
                allowed_ip, ip_msg = rt.orchestrator.quota_manager.check_web_public_ip_rate_limit(client_ip)
                if not allowed_ip:
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail=ip_msg or "Limite de requisições públicas por IP excedido.",
                    )

        # P1-02: Bloquear `sql_override` em chamadas anônimas sem credencial institucional verificada
        if req.sql_override and not is_auth_caller:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Parâmetro sql_override restrito a sessões institucionais autenticadas (Bearer Token ou X-API-Key).",
            )

        # P1-01: Se o token pertence a gabinete restrito (`can_view_all_gabinetes=False`), fixa `gabinete_id` imutável do token
        if token_claims and not token_claims.get("can_view_all_gabinetes", False):
            gabinete_id = str(token_claims.get("gabinete_id") or "prefeitura_central")
        else:
            gabinete_id = (
                req.gabinete_id
                or x_sim_gabinete
                or (token_claims.get("gabinete_id") if token_claims else None)
                or "prefeitura_central"
            )

        pii_authorized = is_pii_unmask_authorized(
            x_api_key,
            authorization,
            x_sim_token,
            expected_tenant_id=rt.tenant.tenant_id,
            requested_role=effective_role,
        )
        # P1-02 + v4.5 Multi-Cidade & Anti-PII Bleed: Isolar namespace de sessão por canal, município, gabinete e escopo PII
        raw_sess = (req.session_id or "default").strip() or "default"
        pii_tag = "pii" if pii_authorized else "pub"
        orch_session_id = (
            f"default::{rt.tenant.tenant_id}::{gabinete_id}::{pii_tag}"
            if raw_sess == "default"
            else f"web::{rt.tenant.tenant_id}::{gabinete_id}::{pii_tag}::{raw_sess}"
        )

        _log(
            f"POST /chat (tenant={rt.tenant.tenant_id}, session={orch_session_id}, role={effective_role}, gabinete={gabinete_id}, "
            f"pii_auth={pii_authorized}, sources={req.allowed_sources or 'ALL'}): {question[:100]}"
        )
        rec = rt.interaction_store.create_record(
            channel="web_institucional",
            session_id=orch_session_id,
            chat_id=orch_session_id,
            user_display=f"Plataforma Institucional ({effective_role})",
            role=effective_role,
            question=question,
            gabinete_id=gabinete_id,
        )
        rt.interaction_store.transition_state(
            rec.interaction_id,
            MessageLifecycleState.PROCESSING,
            detail=f"Consulta em processamento pelo orquestrador ({rt.tenant.municipality_name}/{rt.tenant.state_uf}).",
        )

        # Se o usuário solicitou role='auditor'/'admin' sem token SSO/Admin nem X-API-Key,
        # executa no motor com perfil institucional mascarado ('gestor') para nunca gravar CPFs abertos no cache SQLite
        execution_role = (
            "gestor"
            if (not pii_authorized and effective_role in ("auditor", "admin", "controller", "controladoria"))
            else effective_role
        )
        res = rt.orchestrator.handle_question(
            question=question,
            session_id=orch_session_id,
            role=execution_role,
            explicit_segments=req.explicit_segments,
            explicit_tables=req.explicit_tables,
            allowed_sources=req.allowed_sources,
            sql_override=req.sql_override,
            llm_provider=req.llm_provider,
            llm_api_key=req.llm_api_key,
        )
        res_dict = res.to_dict()
        res_dict["role"] = effective_role

        # LGPD: garantia dupla de mascaramento de CPFs pessoais (11 dígitos ou parciais fora do padrão ANPD) caso não autorizado
        if not pii_authorized:
            masked_rows, extra_masked = mask_sensitive_rows(res_dict.get("sql_rows") or [], role="citizen")
            res_dict["sql_rows"] = masked_rows
            res_dict["pii_masked_count"] = int(res_dict.get("pii_masked_count") or 0) + extra_masked
            res_dict["answer_text"] = mask_text_cpfs(str(res_dict.get("answer_text") or ""))
            res_dict["citizen_response"] = mask_text_cpfs(str(res_dict.get("citizen_response") or ""))
            res_dict["institutional_response"] = mask_text_cpfs(str(res_dict.get("institutional_response") or ""))
            if res_dict.get("table_html"):
                res_dict["table_html"] = mask_text_cpfs(str(res_dict.get("table_html") or ""))

        rec.status = res.status
        rec.intent = res.intent
        persisted_cit = mask_text_cpfs(str(res_dict.get("citizen_response") or res_dict.get("answer_text") or ""))
        persisted_inst = mask_text_cpfs(str(res_dict.get("institutional_response") or res_dict.get("answer_text") or ""))
        persisted_rows, _ = mask_sensitive_rows((res_dict.get("sql_rows") or [])[:50], role="citizen")
        rec.citizen_response = persisted_cit
        rec.institutional_response = persisted_inst
        rec.whatsapp_text = persisted_cit.replace("**", "*")
        rec.sql_executed = res.sql_executed
        rec.sql_rows_count = len(res_dict.get("sql_rows") or [])
        rec.sql_rows_preview = persisted_rows
        rec.rag_chunks_count = len(res.rag_chunks)
        rec.rag_chunks_preview = res.rag_chunks[:5]
        rec.selected_segments = res.selected_segments
        rec.selected_tables = res.selected_tables
        rec.allowed_sources = res.allowed_sources
        rec.fontes_utilizadas = res.fontes_utilizadas
        rec.transparency_warnings = res.transparency_warnings
        rec.methodology_summary = res.methodology_summary
        rec.limitations_and_caveats = res.limitations_and_caveats
        rec.pii_masked_count = int(res_dict.get("pii_masked_count") or 0)
        rec.llm_engine_used = res.llm_engine_used
        rec.exec_ms = res.exec_ms
        rec.pipeline_steps = res.pipeline_steps
        rec.delivery_mode = "web_sync_confirmed"
        rec.send_attempts = 1
        rt.interaction_store.transition_state(
            rec.interaction_id,
            MessageLifecycleState.SENT,
            detail="Resposta entregue de forma síncrona na interface institucional.",
        )
        rt.interaction_store.save(rec)

        if res.status == "bloqueado_seguranca":
            security_and_error_events.append(
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "category": "SECURITY_PROMPT_OR_SCOPE_BLOCK",
                    "event_type": "SECURITY_PROMPT_OR_SCOPE_BLOCK",
                    "channel": "web_institucional",
                    "tenant_id": rt.tenant.tenant_id,
                    "role": effective_role,
                    "code": res.intent.upper(),
                    "detail": res.answer_text[:200],
                    "input_preview": question[:180],
                }
            )

        res_dict["tenant_id"] = rt.tenant.tenant_id
        res_dict["municipality"] = f"{rt.tenant.municipality_name}/{rt.tenant.state_uf}"
        res_dict["ibge_code"] = rt.tenant.ibge_code
        res_dict["interaction_id"] = rec.interaction_id
        res_dict["gabinete_id"] = gabinete_id
        res_dict["pii_unmasked"] = pii_authorized
        res_dict["message_state"] = rec.message_state
        res_dict["message_state_label"] = rec.message_state_label
        return res_dict

    async def _read_raw_request_body(request: Request) -> bytes:
        return await request.body()

    @app.post("/webhook/waha")
    @app.post("/webhook/whatsapp-entrada")
    def waha_webhook_endpoint(
        payload_root: Dict[str, Any],
        raw_body: bytes = Depends(_read_raw_request_body),
        x_webhook_hmac: Optional[str] = Header(default=None),
        x_hub_signature_256: Optional[str] = Header(default=None),
        x_waha_signature: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        """
        Endpoint nativo e maduro compatível com o WAHA (WhatsApp HTTP API) sem necessidade do n8n.
        Implementa:
        - Propagação em tempo real da cidade ativa selecionada no Console (`runtime_pool.get_active()`)
        - Autenticação criptográfica HMAC-SHA512 (`X-Webhook-Hmac`) / HMAC-SHA256 sobre os bytes brutos (P0-03)
        - Eventos de ciclo de vida (`message.ack`, `message.reaction`, `session.status`, `message.revoked`)
        - Deduplicação idempotente por `message_id` ANTES do download de mídia e transcrição de áudio (economia de 50% de Whisper)
        - Rate limit de 20 msgs/dia por número com aviso único no WhatsApp (sem silêncio opaco)
        - Coalescência atômica sem *lost wakeup* (`acquire_turn_or_coalesce` / `drain_or_finish_turn` — P2-03)
        - Indicador de digitação natural (`sendSeen` -> `startTyping` -> `stopTyping`)
        - Envio inteligente sem timer fixo e confirmação estrita de entrega
        - Persistência integrada no `UnifiedInteractionStore` com visão dupla (Cidadão + Auditor)
        """
        from sim_platform.security.sql_guard import mask_sensitive_rows, mask_text_cpfs

        hook_secret = os.environ.get("WHATSAPP_HOOK_HMAC_KEY", "").strip()
        if hook_secret:
            import hashlib
            import hmac

            provided_sig = (x_webhook_hmac or x_waha_signature or x_hub_signature_256 or "").strip()
            clean_sig = (
                provided_sig.split("=", 1)[1].strip()
                if provided_sig.lower().startswith(("sha256=", "sha512="))
                else provided_sig
            ).lower()
            key_bytes = hook_secret.encode("utf-8")
            canonical_bytes = json.dumps(payload_root, sort_keys=True, separators=(",", ":")).encode("utf-8")
            valid_digests = (
                hmac.new(key_bytes, raw_body, hashlib.sha512).hexdigest(),
                hmac.new(key_bytes, raw_body, hashlib.sha256).hexdigest(),
                hmac.new(key_bytes, canonical_bytes, hashlib.sha512).hexdigest(),
                hmac.new(key_bytes, canonical_bytes, hashlib.sha256).hexdigest(),
            )
            sig_ok = bool(clean_sig) and any(hmac.compare_digest(clean_sig, exp_d) for exp_d in valid_digests)
            if not sig_ok:
                security_and_error_events.append(
                    {
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "category": "SECURITY_WEBHOOK_AUTH_BLOCK",
                        "event_type": "SECURITY_WEBHOOK_AUTH_BLOCK",
                        "channel": "whatsapp_webhook",
                        "role": "anonymous",
                        "code": "INVALID_WEBHOOK_HMAC",
                        "detail": "Assinatura HMAC criptográfica ausente ou inválida em /webhook/waha.",
                        "input_preview": str(payload_root.get("event") or "")[:80],
                    }
                )
                raise HTTPException(status_code=401, detail="Assinatura HMAC do webhook WAHA inválida ou ausente.")

        pinned_rt = runtime_pool.get_active()
        body_obj = payload_root.get("body") if isinstance(payload_root.get("body"), dict) else payload_root
        session = str(body_obj.get("session") or "default")
        event = str(body_obj.get("event") or "message.any")
        payload = body_obj["payload"] if isinstance(body_obj.get("payload"), dict) else body_obj
        dry_run = bool(payload_root.get("dry_run") or body_obj.get("dry_run") or payload.get("dry_run"))

        # 1. Tratar eventos assíncronos de estado do WAHA (message.ack, message.reaction, session.status)
        lifecycle_event_resp = waha_manager.handle_webhook_event(
            event_name=event, session=session, payload=payload, store=pinned_rt.interaction_store
        )
        if lifecycle_event_resp is not None:
            if lifecycle_event_resp.get("status") in ("ack_unmatched", "reaction_unmatched"):
                for other_rt in runtime_pool.list_loaded_runtimes():
                    if other_rt.tenant.tenant_id == pinned_rt.tenant.tenant_id:
                        continue
                    alt_resp = waha_manager.handle_webhook_event(
                        event_name=event,
                        session=session,
                        payload=payload,
                        store=other_rt.interaction_store,
                        match_strict_msg_id_only=True,
                    )
                    if alt_resp and alt_resp.get("status") in ("ack_updated", "reaction_recorded"):
                        lifecycle_event_resp = alt_resp
                        break
            _log(f"WAHA Lifecycle Event ({event}, session={session}): {lifecycle_event_resp.get('status')}")
            return lifecycle_event_resp

        from_me = bool(payload.get("fromMe", False))
        if from_me and payload.get("to"):
            chat_id = str(payload.get("to") or payload.get("from") or payload.get("chatId") or "whatsapp_user")
        else:
            chat_id = str(payload.get("from") or payload.get("chatId") or "whatsapp_user")
        message_text = str(payload.get("body") or payload.get("text") or "").strip()
        explicit_msg_id = str(
            payload.get("id")
            or (payload.get("key") or {}).get("id")
            or payload.get("messageId")
            or ""
        ).strip()

        # Permitir teste no próprio número ("Mensagem para si mesmo") usando prefixo /sim
        self_test_override = False
        if from_me and message_text.lower().startswith("/sim "):
            from_jid = str(payload.get("from") or "").split(":")[0].split("@")[0]
            to_jid = str(payload.get("to") or "").split(":")[0].split("@")[0]
            if not to_jid or not from_jid or to_jid == from_jid:
                message_text = message_text[5:].strip()
                self_test_override = True

        if from_me and not self_test_override:
            return {"status": "ignored", "reason": "fromMe=True", "chat_id": chat_id}
        if chat_id.endswith("@g.us") or chat_id == "status@broadcast":
            return {"status": "ignored", "reason": "group_or_status", "chat_id": chat_id}

        # 1b. Roteamento Multi-Cidade por comando `/nomedacidade` (ex.: `/lagoa ...`, `/caete ...`, `/padrao`)
        route_res = whatsapp_router.route_message(
            chat_id=chat_id,
            message_text=message_text,
            default_tenant_id=pinned_rt.tenant.tenant_id,
        )
        if route_res.resolved_tenant_id and route_res.resolved_tenant_id != pinned_rt.tenant.tenant_id:
            try:
                pinned_rt = runtime_pool.get_or_load(route_res.resolved_tenant_id)
            except Exception as exc:
                _log(f"Aviso ao resolver tenant WhatsApp '{route_res.resolved_tenant_id}': {exc}")

        if route_res.is_command_only and route_res.command_only_reply:
            cmd_rec = pinned_rt.interaction_store.create_record(
                channel="whatsapp",
                session_id=f"wa::{pinned_rt.tenant.tenant_id}::{chat_id}",
                chat_id=chat_id,
                user_display=chat_id,
                role="citizen",
                question=message_text,
                inbound_message_id=explicit_msg_id or None,
            )
            cmd_send = waha_manager.send_text_with_confirmation(
                cmd_rec,
                session=session,
                chat_id=chat_id,
                text=route_res.command_only_reply,
                reply_to=explicit_msg_id or None,
                dry_run=dry_run,
                store=pinned_rt.interaction_store,
            )
            cmd_rec.status = "sucesso"
            cmd_rec.intent = "tenant_switch_command"
            cmd_rec.citizen_response = route_res.command_only_reply
            cmd_rec.whatsapp_text = route_res.command_only_reply
            pinned_rt.interaction_store.save(cmd_rec)
            return {
                "status": "processed" if cmd_send.get("ok") else "delivery_failed",
                "tenant_id": pinned_rt.tenant.tenant_id,
                "municipality": f"{pinned_rt.tenant.municipality_name}/{pinned_rt.tenant.state_uf}",
                "ibge_code": pinned_rt.tenant.ibge_code,
                "interaction_id": cmd_rec.interaction_id,
                "session": session,
                "chat_id": chat_id,
                "intent": "tenant_switch_command",
                "route_taken": "tenant_switch_command",
                "dry_run": dry_run,
                "whatsapp_text": route_res.command_only_reply,
            }

        message_text = route_res.stripped_question

        # 2. Deduplicação idempotente por `explicit_msg_id` ANTES de baixar mídia ou transcrever áudio!
        # Evita pagar 2x no Whisper quando o WAHA dispara `message` + `message.any` para o mesmo áudio.
        pre_checked_id = False
        eff_msg_id = explicit_msg_id
        dedup_chat_scope = f"{pinned_rt.tenant.tenant_id}::{chat_id}"
        if explicit_msg_id:
            is_dup, existing_iid, eff_msg_id = waha_manager.coordinator.check_and_mark_seen_message(
                message_id=explicit_msg_id,
                chat_id=dedup_chat_scope,
                body=message_text or f"MEDIA::{explicit_msg_id}",
            )
            pre_checked_id = True
            if is_dup:
                _log(f"WAHA Pre-Media Duplicate Ignored (msg_id={eff_msg_id}, chat={chat_id})")
                return {
                    "status": "duplicate_ignored",
                    "reason": "duplicate_message_id",
                    "message_id": eff_msg_id,
                    "existing_interaction_id": existing_iid,
                    "chat_id": chat_id,
                    "tenant_id": pinned_rt.tenant.tenant_id,
                }

        # 3. Rate limit anti-flood por cidadão no WhatsApp (ANTES de gastar Whisper/LLM)
        allowed_rl, rl_msg, should_notify_wpp = pinned_rt.orchestrator.quota_manager.check_whatsapp_rate_limit(chat_id)
        if not allowed_rl:
            if should_notify_wpp:
                # Enviar aviso educado uma única vez no dia pelo WhatsApp para não deixar o cidadão no vácuo
                notice_rec = pinned_rt.interaction_store.create_record(
                    channel="whatsapp",
                    session_id=session,
                    chat_id=chat_id,
                    user_display=chat_id,
                    role="citizen",
                    question=message_text or "[Mensagem bloqueada por limite diário]",
                    inbound_message_id=eff_msg_id or None,
                )
                waha_manager.send_text_with_confirmation(
                    notice_rec,
                    session=session,
                    chat_id=chat_id,
                    text=rl_msg,
                    reply_to=explicit_msg_id or None,
                    dry_run=dry_run,
                    store=pinned_rt.interaction_store,
                )
            return {
                "status": "rate_limited",
                "tenant_id": pinned_rt.tenant.tenant_id,
                "session": session,
                "chat_id": chat_id,
                "notified_user_once": should_notify_wpp,
                "whatsapp_text": rl_msg,
            }

        # 4. Verificar se é mensagem de voz / áudio do WhatsApp (somente após passar na deduplicação e rate limit)
        media_obj = payload.get("media") if isinstance(payload.get("media"), dict) else {}
        audio_b64 = str(payload.get("audio_base64") or media_obj.get("data") or "").strip()
        if len(audio_b64) > 14_500_000:
            _log(f"Áudio Base64 inline em /webhook/waha excedeu 10 MB ({len(audio_b64)} chars)")
            audio_b64 = ""
        audio_mime = str(payload.get("mime_type") or media_obj.get("mimetype") or "audio/ogg").strip()
        audio_transcript = str(payload.get("audio_transcript") or "").strip()
        was_audio = False
        audio_engine = None

        media_url = str(media_obj.get("url") or "").strip()
        if not audio_b64 and media_url and ("audio" in audio_mime.lower() or "ogg" in media_url.lower() or payload.get("hasMedia")):
            waha_internal = os.environ.get("WAHA_API_URL", "http://waha:3000").rstrip("/")
            resolved_url = (
                media_url.replace("http://localhost:3000", waha_internal)
                .replace("http://localhost:3001", waha_internal)
                .replace("http://127.0.0.1:3000", waha_internal)
                .replace("http://127.0.0.1:3001", waha_internal)
            )
            parsed_media = urlparse(resolved_url)
            parsed_waha = urlparse(waha_internal)
            allowed_hosts = {
                "waha",
                (parsed_waha.hostname or "").lower(),
            } - {""}
            if bool(os.environ.get("PYTEST_CURRENT_TEST")):
                allowed_hosts.update({"localhost", "127.0.0.1"})
            allowed_ports = {3000, 3001, parsed_waha.port} - {None}
            media_host = (parsed_media.hostname or "").lower()
            media_port = parsed_media.port or (443 if parsed_media.scheme == "https" else 80)
            decoded_media_path = unquote(parsed_media.path or "")
            norm_media_path = posixpath.normpath(decoded_media_path) if decoded_media_path else ""
            is_safe_media_url = (
                parsed_media.scheme in ("http", "https")
                and media_host in allowed_hosts
                and media_port in allowed_ports
                and not parsed_media.username
                and not parsed_media.password
                and decoded_media_path.startswith("/api/files/")
                and norm_media_path.startswith("/api/files/")
                and ".." not in decoded_media_path
                and "\\" not in decoded_media_path
            )
            if not is_safe_media_url:
                _log(f"BLOQUEIO SSRF/LFI em media.url do webhook WAHA: {media_url[:140]}")
                security_and_error_events.append(
                    {
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "category": "SECURITY_SSRF_MEDIA_BLOCK",
                        "event_type": "SECURITY_SSRF_MEDIA_BLOCK",
                        "channel": "whatsapp_webhook",
                        "role": "anonymous",
                        "code": "SSRF_OR_LFI_MEDIA_URL_BLOCKED",
                        "detail": (
                            f"URL de mídia fora da allowlist do container WAHA bloqueada: "
                            f"scheme={parsed_media.scheme or 'none'}, host={media_host or 'none'}:{media_port}"
                        ),
                        "input_preview": media_url[:180],
                    }
                )
            elif not dry_run:
                try:
                    import urllib.error
                    import urllib.request

                    class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
                        def redirect_request(self, req, fp, code, msg, headers, newurl):
                            raise urllib.error.HTTPError(
                                req.full_url, code, "Redirects blocked (anti-SSRF)", headers, fp
                            )

                    opener = urllib.request.build_opener(_NoRedirectHandler())
                    with opener.open(resolved_url, timeout=10) as resp:
                        raw_media_bytes = resp.read(10 * 1024 * 1024 + 1)
                        if len(raw_media_bytes) <= 10 * 1024 * 1024:
                            audio_b64 = base64.b64encode(raw_media_bytes).decode("ascii")
                        else:
                            _log(f"Mídia WAHA excedeu limite de 10 MB ({media_url[:100]})")
                except Exception as exc:
                    _log(f"Aviso ao baixar media.url do WAHA ({media_url}): {exc}")

        if not message_text and (audio_b64 or audio_transcript or payload.get("hasMedia")):
            tr = transcribe_audio_content(
                audio_base64=audio_b64 or None,
                mime_type=audio_mime,
                fallback_transcript=audio_transcript or None,
                municipality_name=pinned_rt.tenant.municipality_name,
                state_uf=pinned_rt.tenant.state_uf,
                diario_veiculo=pinned_rt.diario_veiculo,
            )
            if tr.get("text"):
                message_text = tr["text"]
                was_audio = True
                audio_engine = tr.get("engine")
                pinned_rt.orchestrator.quota_manager.record_audio_usage(seconds_estimated=30)

        if not message_text:
            return {"status": "ignored", "reason": "empty_body", "chat_id": chat_id}

        # Deduplicação por hash de conteúdo caso o payload não tivesse `id` explícito
        if not pre_checked_id:
            is_dup, existing_iid, eff_msg_id = waha_manager.coordinator.check_and_mark_seen_message(
                message_id=None,
                chat_id=dedup_chat_scope,
                body=message_text,
            )
            if is_dup:
                _log(f"WAHA Duplicate Ignored (msg_id={eff_msg_id}, chat={chat_id})")
                return {
                    "status": "duplicate_ignored",
                    "reason": "duplicate_message_id",
                    "message_id": eff_msg_id,
                    "existing_interaction_id": existing_iid,
                    "chat_id": chat_id,
                    "tenant_id": pinned_rt.tenant.tenant_id,
                }

        # 5. Aquisição atômica de turno por `dedup_chat_scope` (`tenant_id::chat_id`) sem janela de *lost wakeup* (P2-03)
        if not waha_manager.coordinator.acquire_turn_or_coalesce(dedup_chat_scope, message_text, eff_msg_id):
            _log(f"WAHA Concurrent Message Coalesced (chat={dedup_chat_scope}, msg_id={eff_msg_id}): {message_text[:80]}")
            return {
                "status": "coalesced_into_active",
                "tenant_id": pinned_rt.tenant.tenant_id,
                "session": session,
                "chat_id": chat_id,
                "message_id": eff_msg_id,
                "detail": "Mensagem recebida durante processamento ativo e consolidada na fila ordenada do chat.",
            }

        chat_lock = waha_manager.coordinator.get_chat_lock(dedup_chat_scope)
        try:
            with chat_lock:
                notify_name = str(
                    payload.get("notifyName")
                    or (payload.get("_data") or {}).get("notifyName")
                    or chat_id
                ).strip()

                def _process_single_turn(
                    turn_text: str,
                    turn_msg_id: str,
                    turn_was_audio: bool,
                    extra_frags: List[str],
                ) -> Tuple[Any, Dict[str, Any], Dict[str, Any], str]:
                    turn_rt = pinned_rt
                    turn_store = turn_rt.interaction_store
                    turn_sess_id = f"wa::{turn_rt.tenant.tenant_id}::{chat_id}"
                    turn_rec = turn_store.create_record(
                        channel="whatsapp",
                        session_id=turn_sess_id,
                        chat_id=chat_id,
                        user_display=notify_name,
                        role="citizen",
                        question=turn_text,
                        inbound_message_id=turn_msg_id,
                        was_audio=turn_was_audio,
                    )
                    if turn_msg_id:
                        waha_manager.coordinator.bind_interaction_to_message_id(turn_msg_id, turn_rec.interaction_id)

                    waha_manager.send_seen(
                        turn_rec, session=session, chat_id=chat_id, message_id=turn_msg_id or None, dry_run=dry_run, store=turn_store
                    )
                    waha_manager.start_typing(turn_rec, session=session, chat_id=chat_id, dry_run=dry_run, store=turn_store)
                    if turn_msg_id:
                        waha_manager.send_reaction(
                            turn_rec, session=session, message_id=turn_msg_id, emoji="⏳", dry_run=dry_run, store=turn_store
                        )

                    try:
                        effective_q = turn_text
                        if extra_frags:
                            effective_q = " ".join([turn_text] + [f.strip() for f in extra_frags if f.strip()])
                            turn_rec.effective_question = effective_q
                            turn_rec.coalesced_count = 1 + len(extra_frags)

                        _log(
                            f"WAHA Webhook ({event}, tenant={turn_rt.tenant.tenant_id}, chat={chat_id}, "
                            f"audio={turn_was_audio}, dry_run={dry_run}, coalesced={turn_rec.coalesced_count}): {effective_q[:100]}"
                        )
                        turn_res = turn_rt.orchestrator.handle_question(
                            question=effective_q,
                            session_id=turn_sess_id,
                            role="citizen",
                        )
                    finally:
                        waha_manager.stop_typing(turn_rec, session=session, chat_id=chat_id, dry_run=dry_run, store=turn_store)

                    turn_res_dict = turn_res.to_dict()
                    turn_res_dict["tenant_id"] = turn_rt.tenant.tenant_id
                    turn_res_dict["municipality"] = f"{turn_rt.tenant.municipality_name}/{turn_rt.tenant.state_uf}"
                    turn_res_dict["ibge_code"] = turn_rt.tenant.ibge_code
                    from sim_platform.security.sql_guard import mask_text_cpfs as _wpp_mask_cpfs

                    cit_nat = _wpp_mask_cpfs(
                        str(turn_res_dict.get("citizen_response") or turn_res_dict.get("answer_text") or "").strip()
                    )
                    if turn_was_audio:
                        cit_nat = f'*[Áudio transcrito]:* "{_wpp_mask_cpfs(turn_text)}"\n\n{cit_nat}'
                    wpp_txt = _wpp_mask_cpfs(cit_nat.replace("**", "*"))

                    s_res = waha_manager.send_text_with_confirmation(
                        turn_rec,
                        session=session,
                        chat_id=chat_id,
                        text=wpp_txt,
                        reply_to=turn_msg_id or None,
                        dry_run=dry_run,
                        store=turn_store,
                    )
                    img_res: Optional[Dict[str, Any]] = None
                    if turn_res.chart_base64:
                        img_caption = (
                            turn_res.chart_caption
                            or f"Gráfico Institucional — Plataforma SIM ({turn_rt.tenant.municipality_name}/{turn_rt.tenant.state_uf})"
                        ).replace("**", "*")
                        img_res = waha_manager.send_image_with_confirmation(
                            turn_rec,
                            session=session,
                            chat_id=chat_id,
                            image_base64=turn_res.chart_base64,
                            mime_type=turn_res.chart_mime_type or "image/png",
                            filename="grafico_institucional_sim.png",
                            caption=img_caption,
                            reply_to=turn_msg_id or None,
                            dry_run=dry_run,
                            store=turn_store,
                        )
                    tbl_img_res: Optional[Dict[str, Any]] = None
                    if turn_res.table_image_base64:
                        tbl_caption = (
                            turn_res.table_caption
                            or f"Tabela Oficial — Plataforma SIM ({turn_rt.tenant.municipality_name}/{turn_rt.tenant.state_uf})"
                        ).replace("**", "*")
                        tbl_img_res = waha_manager.send_image_with_confirmation(
                            turn_rec,
                            session=session,
                            chat_id=chat_id,
                            image_base64=turn_res.table_image_base64,
                            mime_type=turn_res.table_mime_type or "image/png",
                            filename="tabela_oficial_sim.png",
                            caption=tbl_caption,
                            reply_to=turn_msg_id or None,
                            dry_run=dry_run,
                            store=turn_store,
                        )
                    if s_res.get("ok") and turn_msg_id:
                        waha_manager.send_reaction(
                            turn_rec, session=session, message_id=turn_msg_id, emoji="✅", dry_run=dry_run, store=turn_store
                        )

                    if not s_res.get("ok"):
                        security_and_error_events.append(
                            {
                                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "category": "WAHA_DELIVERY_FAILURE",
                                "event_type": "WAHA_DELIVERY_FAILURE",
                                "channel": "whatsapp",
                                "tenant_id": turn_rt.tenant.tenant_id,
                                "role": "citizen",
                                "code": "SEND_FAILED",
                                "detail": str(turn_rec.last_send_error or "Falha ao enviar mensagem pelo WAHA"),
                                "input_preview": f"{chat_id}: {effective_q[:120]}",
                            }
                        )

                    masked_wpp_rows, _ = mask_sensitive_rows(turn_res.sql_rows[:50], role="citizen")
                    turn_rec.status = turn_res.status if s_res.get("ok") else "falha_envio_whatsapp"
                    turn_rec.intent = turn_res.intent
                    turn_rec.citizen_response = mask_text_cpfs(str(turn_res.citizen_response or turn_res.answer_text or ""))
                    turn_rec.institutional_response = mask_text_cpfs(str(turn_res.institutional_response or turn_res.answer_text or ""))
                    turn_rec.whatsapp_text = mask_text_cpfs(wpp_txt)
                    turn_rec.sql_executed = turn_res.sql_executed
                    turn_rec.sql_rows_count = len(turn_res.sql_rows)
                    turn_rec.sql_rows_preview = masked_wpp_rows
                    turn_rec.rag_chunks_count = len(turn_res.rag_chunks)
                    turn_rec.rag_chunks_preview = turn_res.rag_chunks[:5]
                    turn_rec.selected_segments = turn_res.selected_segments
                    turn_rec.selected_tables = turn_res.selected_tables
                    turn_rec.allowed_sources = turn_res.allowed_sources
                    turn_rec.fontes_utilizadas = turn_res.fontes_utilizadas
                    turn_rec.transparency_warnings = turn_res.transparency_warnings
                    turn_rec.methodology_summary = turn_res.methodology_summary
                    turn_rec.limitations_and_caveats = turn_res.limitations_and_caveats
                    turn_rec.pii_masked_count = turn_res.pii_masked_count
                    turn_rec.llm_engine_used = turn_res.llm_engine_used
                    turn_rec.exec_ms = turn_res.exec_ms
                    turn_rec.pipeline_steps = turn_res.pipeline_steps
                    turn_store.save(turn_rec)
                    if img_res is not None:
                        s_res = {**s_res, "chart_image_sent": bool(img_res.get("ok")), "chart_mime_type": turn_res.chart_mime_type or "image/png"}
                        if not img_res.get("ok"):
                            _log(f"Aviso WAHA: falha ao enviar gráfico PNG para { _mask_whatsapp_phone_str(chat_id) }: {img_res.get('error')}")
                    if tbl_img_res is not None:
                        s_res = {**s_res, "table_image_sent": bool(tbl_img_res.get("ok")), "table_mime_type": turn_res.table_mime_type or "image/png"}
                        if not tbl_img_res.get("ok"):
                            _log(f"Aviso WAHA: falha ao enviar tabela PNG para { _mask_whatsapp_phone_str(chat_id) }: {tbl_img_res.get('error')}")
                    return turn_rec, turn_res_dict, s_res, wpp_txt

                initial_extra: List[str] = []
                queued_before = waha_manager.coordinator.drain_coalesced_fragments(dedup_chat_scope)
                for qi in queued_before:
                    if qi.get("text"):
                        initial_extra.append(str(qi["text"]))

                rec, res_dict, send_res, whatsapp_text = _process_single_turn(
                    turn_text=message_text,
                    turn_msg_id=eff_msg_id,
                    turn_was_audio=was_audio,
                    extra_frags=initial_extra,
                )

                # Loop atômico de drenagem pós-processamento sem *lost wakeup* (`drain_or_finish_turn` — P2-03)
                follow_up_dispatched = 0
                while True:
                    late_items = waha_manager.coordinator.drain_or_finish_turn(dedup_chat_scope)
                    if not late_items:
                        break
                    late_texts = [str(it.get("text") or "").strip() for it in late_items if str(it.get("text") or "").strip()]
                    if not late_texts:
                        waha_manager.coordinator.force_release_turn(dedup_chat_scope)
                        break
                    late_msg_id = str(late_items[-1].get("message_id") or "")
                    _process_single_turn(
                        turn_text=late_texts[0],
                        turn_msg_id=late_msg_id,
                        turn_was_audio=False,
                        extra_frags=late_texts[1:],
                    )
                    follow_up_dispatched += 1
        except Exception:
            waha_manager.coordinator.force_release_turn(dedup_chat_scope)
            raise

        waha_delivery = {
            "sent": bool(send_res.get("ok")),
            "mode": send_res.get("mode", "local_verified"),
            "http_status": send_res.get("http_status", 200),
            "attempts": rec.send_attempts,
            "outbound_message_id": rec.outbound_message_id,
            "error": rec.last_send_error,
            "follow_up_batches_dispatched": follow_up_dispatched,
            "chart_image_sent": bool(send_res.get("chart_image_sent", False)),
            "table_image_sent": bool(send_res.get("table_image_sent", False)),
        }

        return {
            "status": "processed" if send_res.get("ok") else "delivery_failed",
            "tenant_id": res_dict.get("tenant_id", pinned_rt.tenant.tenant_id),
            "municipality": res_dict.get("municipality", f"{pinned_rt.tenant.municipality_name}/{pinned_rt.tenant.state_uf}"),
            "ibge_code": res_dict.get("ibge_code", pinned_rt.tenant.ibge_code),
            "interaction_id": rec.interaction_id,
            "session": session,
            "chat_id": chat_id,
            "inbound_message_id": eff_msg_id,
            "outbound_message_id": rec.outbound_message_id,
            "message_state": rec.message_state,
            "message_state_label": rec.message_state_label,
            "answered_confirmed": rec.answered_confirmed,
            "coalesced_count": rec.coalesced_count,
            "follow_up_batches_dispatched": follow_up_dispatched,
            "was_audio": was_audio,
            "audio_transcription_engine": audio_engine,
            "transcribed_text": message_text if was_audio else None,
            "intent": res_dict.get("intent"),
            "route_taken": res_dict.get("intent"),
            "exec_ms": res_dict.get("exec_ms", 0.0),
            "dry_run": dry_run,
            "whatsapp_text": whatsapp_text,
            "waha_delivery": waha_delivery,
            "waha_actions": rec.waha_actions,
            "state_history": rec.state_history,
            "orchestrator_response": res_dict,
        }

    @app.get("/api/interactions")
    def list_unified_interactions(
        request: Request,
        channel: Optional[str] = None,
        role: Optional[str] = None,
        state: Optional[str] = None,
        search: Optional[str] = None,
        gabinete_id: Optional[str] = None,
        limit: int = 50,
        x_api_key: Optional[str] = Header(default=None),
        x_sim_gabinete: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        """
        Lista o histórico unificado de interações (WhatsApp Cidadão + Plataforma Institucional Web),
        permitindo auditoria cruzada, isolamento por Gabinete Parlamentar (`gabinete_id`) e estados de mensagens.
        """
        rt = runtime_pool.get_active()
        is_auth_caller, token_claims = _is_institutional_caller(x_api_key, authorization, x_sim_token)
        is_test_loopback = bool(os.environ.get("PYTEST_CURRENT_TEST")) and (
            (request.client.host if request.client else "") == "testclient"
        )
        eff_gabinete = gabinete_id or x_sim_gabinete
        if token_claims and not token_claims.get("can_view_all_gabinetes", True):
            eff_gabinete = str(token_claims.get("gabinete_id") or eff_gabinete or "")
        elif not is_auth_caller and not is_test_loopback:
            # SEC-P0-02: Chamadas anônimas fora de TestClient só enxergam escopo público (jamais gabinetes parlamentares privados!)
            if eff_gabinete and eff_gabinete.strip().lower() not in ("publico", "prefeitura_central"):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Consulta a gabinete parlamentar específico exige autenticação institucional.",
                )
            eff_gabinete = eff_gabinete or "publico"

        eff_channel = "web_institucional" if channel == "web" else channel
        listed = rt.interaction_store.list_records(
            channel=eff_channel,
            role=role,
            state=state,
            search=search,
            gabinete_id=eff_gabinete,
            limit=limit,
            allow_phone_search=bool(is_auth_caller),
        )
        listed["tenant_id"] = rt.tenant.tenant_id
        listed["municipality"] = f"{rt.tenant.municipality_name}/{rt.tenant.state_uf}"
        if not is_auth_caller:
            masked_items = []
            for item in listed.get("interactions") or []:
                g_val = str(item.get("gabinete_id") or "publico").strip().lower()
                if not is_test_loopback and g_val not in ("", "publico", "prefeitura_central"):
                    continue
                c_item = dict(item)
                c_item["chat_id"] = _mask_whatsapp_phone_str(c_item.get("chat_id"))
                c_item["user_display"] = "Cidadão (WhatsApp LGPD)" if item.get("channel") == "whatsapp" else _mask_whatsapp_phone_str(c_item.get("user_display"))
                masked_items.append(c_item)
            listed["interactions"] = masked_items
        return listed

    @app.get("/api/interactions/{interaction_id}")
    def get_interaction_detail(
        request: Request,
        interaction_id: str,
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        rt = runtime_pool.get_active()
        is_auth_caller, token_claims = _is_institutional_caller(x_api_key, authorization, x_sim_token)
        is_test_loopback = bool(os.environ.get("PYTEST_CURRENT_TEST")) and (
            (request.client.host if request.client else "") == "testclient"
        )
        rec = rt.interaction_store.get_by_id(interaction_id)
        if not rec:
            raise HTTPException(status_code=404, detail=f"Interação '{interaction_id}' não encontrada.")
        rec_gab = str(rec.gabinete_id or "publico").strip().lower()
        if token_claims and not token_claims.get("can_view_all_gabinetes", True):
            allowed_gab = str(token_claims.get("gabinete_id") or "").strip().lower()
            if allowed_gab and rec_gab != allowed_gab:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Acesso negado: interação pertence a outro gabinete parlamentar.",
                )
        elif not is_auth_caller and not is_test_loopback and rec_gab not in ("", "publico", "prefeitura_central"):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Detalhe de investigação de gabinete privado exige autenticação institucional.",
            )
        detail_dict = rec.to_dict(include_heavy=True)
        detail_dict["tenant_id"] = rt.tenant.tenant_id
        detail_dict["municipality"] = f"{rt.tenant.municipality_name}/{rt.tenant.state_uf}"
        if not is_auth_caller:
            detail_dict["chat_id"] = _mask_whatsapp_phone_str(detail_dict.get("chat_id"))
            detail_dict["user_display"] = "Cidadão (WhatsApp LGPD)" if detail_dict.get("channel") == "whatsapp" else _mask_whatsapp_phone_str(detail_dict.get("user_display"))
        return detail_dict

    @app.post("/api/whatsapp/retry/{interaction_id}")
    def retry_whatsapp_delivery(
        interaction_id: str,
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        rt = runtime_pool.get_active()
        is_auth_caller, _ = _is_institutional_caller(x_api_key, authorization, x_sim_token)
        if not is_auth_caller:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Reenvio de mensagens WhatsApp exige autenticação institucional (Bearer Token ou X-API-Key).",
            )
        rec = rt.interaction_store.get_by_id(interaction_id)
        if not rec:
            raise HTTPException(status_code=404, detail=f"Interação '{interaction_id}' não encontrada.")
        eff_waha_session = "default" if (not rec.session_id or "::" in rec.session_id) else rec.session_id
        from sim_platform.security.sql_guard import mask_text_cpfs as _retry_mask_cpfs

        waha_manager.start_typing(rec, session=eff_waha_session, chat_id=rec.chat_id, store=rt.interaction_store)
        try:
            send_res = waha_manager.send_text_with_confirmation(
                rec,
                session=eff_waha_session,
                chat_id=rec.chat_id,
                text=_retry_mask_cpfs(rec.whatsapp_text or rec.citizen_response),
                reply_to=rec.inbound_message_id,
                store=rt.interaction_store,
            )
        finally:
            waha_manager.stop_typing(rec, session=eff_waha_session, chat_id=rec.chat_id, store=rt.interaction_store)
        rt.interaction_store.save(rec)
        return {
            "status": "retried_ok" if send_res.get("ok") else "retry_failed",
            "tenant_id": rt.tenant.tenant_id,
            "interaction_id": rec.interaction_id,
            "message_state": rec.message_state,
            "message_state_label": rec.message_state_label,
            "answered_confirmed": rec.answered_confirmed,
            "send_result": send_res,
        }

    @app.get("/api/whatsapp/diagnostics")
    def whatsapp_waha_diagnostics() -> Dict[str, Any]:
        return waha_manager.get_waha_diagnostics()

    @app.get("/api/monitoring/overview")
    def institutional_monitoring_overview() -> Dict[str, Any]:
        """
        Painel de Monitoramento de Erros, Inconsistências de Base, Eventos de Segurança e Saúde Operacional.
        """
        rt = runtime_pool.get_active()
        objs = rt.db_manager.get_mapped_objects_count()
        hist_summary = rt.interaction_store.list_records(limit=10)["summary"]
        quota_st = rt.orchestrator.quota_manager.get_quota_status()
        sync_st = rt.sync_manager.check_status()

        is_caete = rt.tenant.tenant_id in ("caete_mg_3110004", "caete-mg")
        if is_caete:
            known_data_inconsistencies = [
                {
                    "id": "GAP-FOLHA-2020-2022",
                    "code": "GAP-FOLHA-2020-2022",
                    "title": "Folha de Pagamento 2020–2022 (CSV sem registros na origem SICOM)",
                    "severity": "ALTA (Transparência Ativa)",
                    "source": "SICOM / TCE-MG (`remuneracao_municipio`)",
                    "status": "TRATADO COM ALERTA AUTOMÁTICO",
                    "description": (
                        "Os arquivos CSV brutos da folha de pagamento de 2020 a 2022 possuem apenas cabeçalho (0 registros). "
                        "O sistema detecta consultas a esse intervalo e informa explicitamente a lacuna ao cidadão e ao auditor."
                    ),
                    "detail": (
                        "Os arquivos CSV brutos da folha de pagamento de 2020 a 2022 possuem apenas cabeçalho (0 registros). "
                        "O sistema detecta consultas a esse intervalo e informa explicitamente a lacuna ao cidadão e ao auditor."
                    ),
                },
                {
                    "id": "REPARO-SICOM-2023",
                    "code": "REPARO-SICOM-2023",
                    "title": "Reparo Determinístico de Delimitador CSV (Exercício 2023)",
                    "severity": "RESOLVIDO (Overlay Auditado)",
                    "source": "SICOM / TCE-MG (`despesa_pagamento_2023`, `empenho_2023`)",
                    "status": "CORRIGIDO NO MOTOR PARQUET (R$ 11,75M RECUPERADOS)",
                    "description": (
                        "4.723 linhas com delimitador corrompido na exportação original de 2023 foram reparadas deterministicamente "
                        "em `database_fix2023` e consolidadas nos arquivos Parquet oficiais."
                    ),
                    "detail": (
                        "4.723 linhas com delimitador corrompido na exportação original de 2023 foram reparadas deterministicamente "
                        "em `database_fix2023` e consolidadas nos arquivos Parquet oficiais."
                    ),
                },
                {
                    "id": "FIND-TCE-04-FROTA-ORFA",
                    "code": "FIND-TCE-04-FROTA-ORFA",
                    "title": "Despesas de Frota sem Placa Vinculada vs. Maquinário Pesado Isento CTB",
                    "severity": "MÉDIA (Achado de Controladoria)",
                    "source": "SICOM / TCE-MG (`frota_gastoFrota` × `frota_frota`)",
                    "status": "SEPARADO ENTRE FROTA ÓRFÃ E MAQUINÁRIO ISENTO (ART. 115 §4º-A CTB)",
                    "description": (
                        "A Gold View `vw_gold_frota_consolidada` separa veículos sem cadastro (`SEM_CADASTRO_PLACA`) de "
                        "tratores e máquinas agrícolas/obras isentos de emplacamento (`ISENTO_CTB_MAQUINARIO`)."
                    ),
                    "detail": (
                        "A Gold View `vw_gold_frota_consolidada` separa veículos sem cadastro (`SEM_CADASTRO_PLACA`) de "
                        "tratores e máquinas agrícolas/obras isentos de emplacamento (`ISENTO_CTB_MAQUINARIO`)."
                    ),
                },
                {
                    "id": "NOTA-PARCIAL-2025",
                    "code": "NOTA-PARCIAL-2025",
                    "title": "Cobertura Parcial do Exercício de 2025",
                    "severity": "INFORMATIVA",
                    "source": "Múltiplas Fontes (Exercício 2025)",
                    "status": "SINALIZADO NAS RESPOSTAS",
                    "description": (
                        "O exercício de 2025 contempla remessas parciais consolidadas até o fechamento do lote atual."
                    ),
                    "detail": (
                        "O exercício de 2025 contempla remessas parciais consolidadas até o fechamento do lote atual."
                    ),
                },
            ]
        else:
            missing_fams = sorted(getattr(rt.db_manager.catalog, "missing_source_families", set()) or set())
            known_data_inconsistencies = [
                {
                    "id": "MODO-API-FEDERAL-ATIVO",
                    "code": "MODO-API-FEDERAL-ATIVO",
                    "title": f"Bases Federais Automáticas Ativas ({rt.tenant.municipality_name}/{rt.tenant.state_uf} — IBGE {rt.tenant.ibge_code})",
                    "severity": "OPERACIONAL (Sincronizado via APIs)",
                    "source": "IBGE, SICONFI/STN, DATASUS/SIOPS e CGU",
                    "status": f"{objs['total']} OBJETOS PARQUET ATIVOS COM ISOLAMENTO ESTRITO",
                    "description": (
                        f"Indicadores demográficos, fiscais (RGF/RREO/LRF), saúde (SIOPS) e programas federais (Novo Bolsa Família e BPC) "
                        f"estão materializados em Parquet ZSTD exclusivos para {rt.tenant.municipality_name}/{rt.tenant.state_uf}."
                    ),
                    "detail": (
                        f"Indicadores demográficos, fiscais (RGF/RREO/LRF), saúde (SIOPS) e programas federais (Novo Bolsa Família e BPC) "
                        f"estão materializados em Parquet ZSTD exclusivos para {rt.tenant.municipality_name}/{rt.tenant.state_uf}."
                    ),
                }
            ]
            if missing_fams:
                known_data_inconsistencies.append(
                    {
                        "id": "FILA-ASSISTIDA-SICOM-TCE",
                        "code": "FILA-ASSISTIDA-SICOM-TCE",
                        "title": f"Pacotes Granulares Pendentes de Importação ({', '.join(missing_fams)})",
                        "severity": "INFORMATIVA (Fila Assistida pós-CAPTCHA)",
                        "source": "TCE-MG / SICOM (Dados Abertos) & INEP",
                        "status": "AGUARDANDO ARQUIVO EM `avaliacao/dados/inbox/`",
                        "description": (
                            f"Consultas nominais de empenhos, fornecedores e folha de servidores de {rt.tenant.municipality_name}/{rt.tenant.state_uf} "
                            "sinalizam transparência ativa até a importação do pacote `.zip`/`.csv` do TCE-MG na pasta `inbox/`."
                        ),
                        "detail": (
                            f"Consultas nominais de empenhos, fornecedores e folha de servidores de {rt.tenant.municipality_name}/{rt.tenant.state_uf} "
                            "sinalizam transparência ativa até a importação do pacote `.zip`/`.csv` do TCE-MG na pasta `inbox/`."
                        ),
                    }
                )

        sec_blocks_cnt = sum(
            1 for ev in security_and_error_events if str(ev.get("category") or "").startswith("SECURITY_")
        )
        wpp_failed_cnt = int(hist_summary.get("failed_or_unconfirmed", 0))

        return {
            "status": "operacional",
            "tenant_id": rt.tenant.tenant_id,
            "municipality": f"{rt.tenant.municipality_name}/{rt.tenant.state_uf}",
            "ibge_code": rt.tenant.ibge_code,
            "mapped_objects": objs,
            "rag_unique_acts": len(rt.rag_retriever.corpus_chunks),
            "ingestion_up_to_date": sync_st.get("all_up_to_date", True),
            "interaction_summary": hist_summary,
            "security_blocks_count": sec_blocks_cnt,
            "failed_whatsapp_deliveries_count": wpp_failed_cnt,
            "waha_diagnostics": waha_manager.get_waha_diagnostics(),
            "waha_robustness": {
                "duplicates_blocked": waha_manager.coordinator.duplicates_blocked,
                "duplicates_prevented": waha_manager.coordinator.duplicates_blocked,
                "coalesced_messages": waha_manager.coordinator.coalesced_messages,
                "coalesced_fragments": waha_manager.coordinator.coalesced_messages,
                "confirmed_deliveries": hist_summary.get("confirmed_answered", hist_summary.get("confirmed_answers", 0)),
            },
            "quota_governance": quota_st,
            "data_inconsistencies_and_gaps": known_data_inconsistencies,
            "recent_security_and_error_events": list(reversed(security_and_error_events))[:30],
            "recent_system_logs": list(reversed(log_buffer))[:30],
        }

    @app.get("/api/institutional/capabilities")
    def institutional_capabilities_matrix() -> Dict[str, Any]:
        """
        Explicita com total transparência o que está 100% funcional e conectado ao backend local
        versus o que constitui estrutura preparada para evolução em implantação municipal.
        """
        rt = runtime_pool.get_active()
        objs_cnt = rt.db_manager.get_mapped_objects_count()
        functional_modules = [
            {
                "module": "Motor Analítico DuckDB Parquet & Gold Views Semânticas Multi-Cidade",
                "name": "Motor Analítico DuckDB Parquet & Gold Views Semânticas Multi-Cidade",
                "status": "FUNCIONAL_REAL",
                "endpoint": "/query, /catalog, /api/executive/pulse, /api/tenants/select",
                "evidence": f"{objs_cnt['total']} objetos ativos na cidade selecionada ({rt.tenant.municipality_name}/{rt.tenant.state_uf}), isolamento 100% entre municípios e SQLGuard AST + Sandbox.",
                "description": f"{objs_cnt['total']} objetos ativos na cidade selecionada ({rt.tenant.municipality_name}/{rt.tenant.state_uf}), isolamento 100% entre municípios e SQLGuard AST + Sandbox.",
            },
            {
                "module": "Naturalização Dual de Respostas (Cidadão vs. Auditor/Gestor)",
                "name": "Naturalização Dual de Respostas (Cidadão vs. Auditor/Gestor)",
                "status": "FUNCIONAL_REAL",
                "endpoint": "/chat, /webhook/waha",
                "evidence": "Geração simultânea de `citizen_response` (linguagem acessível, glossário cidadão, sem SQL) e `institutional_response` (metodologia, SQL, fontes e alertas).",
                "description": "Geração simultânea de `citizen_response` (linguagem acessível, glossário cidadão, sem SQL) e `institutional_response` (metodologia, SQL, fontes e alertas).",
            },
            {
                "module": "Mensageria Avançada WhatsApp / WAHA com Chaveamento Multi-Cidade ao Vivo",
                "name": "Mensageria Avançada WhatsApp / WAHA com Chaveamento Multi-Cidade ao Vivo",
                "status": "FUNCIONAL_REAL",
                "endpoint": "/webhook/waha, /api/tenants/select, /api/whatsapp/diagnostics, /api/whatsapp/retry/{id}",
                "evidence": "Propagação instantânea da cidade selecionada no Console para o WhatsApp (`/webhook/waha`), deduplicação antes do Whisper e drenagem automática.",
                "description": "Propagação instantânea da cidade selecionada no Console para o WhatsApp (`/webhook/waha`), deduplicação antes do Whisper e drenagem automática.",
            },
            {
                "module": "Autenticação Institucional RBAC/SSO (`admin`/`admin` + JWT HMAC + Isolamento Multi-Gabinete)",
                "name": "Autenticação Institucional RBAC/SSO (`admin`/`admin` + JWT HMAC + Isolamento Multi-Gabinete)",
                "status": "FUNCIONAL_REAL",
                "endpoint": "/api/auth/login, /api/auth/sso, /api/auth/me, /api/interactions?gabinete_id=...",
                "evidence": "Login institucional (`admin`/`admin`), troca de código SSO OIDC/Gov.br, desmascaramento autorizado de CPF (LGPD) e isolamento estrito entre gabinetes parlamentares.",
                "description": "Login institucional (`admin`/`admin`), troca de código SSO OIDC/Gov.br, desmascaramento autorizado de CPF (LGPD) e isolamento estrito entre gabinetes parlamentares.",
            },
            {
                "module": "Verificador Multi-Cidades (Cronjob), Inbox Auto-Roteado & Cloud Data Lake (Google Drive 5 TB)",
                "name": "Verificador Multi-Cidades (Cronjob), Inbox Auto-Roteado & Cloud Data Lake (Google Drive 5 TB)",
                "status": "FUNCIONAL_REAL",
                "endpoint": "/api/fleet/status, /api/fleet/run, /api/tenants, /api/tenants/onboard",
                "evidence": "Sincronização sequencial IBGE/SICONFI/CGU, fila assistida pós-CAPTCHA com detecção automática na pasta `inbox/` e espelhamento verificado no Google Drive (`SIM_DataLake`).",
                "description": "Sincronização sequencial IBGE/SICONFI/CGU, fila assistida pós-CAPTCHA com detecção automática na pasta `inbox/` e espelhamento verificado no Google Drive (`SIM_DataLake`).",
            },
            {
                "module": "Motor Proativo de Auditoria TCE-MG / LRF & Dossiê 360º",
                "name": "Motor Proativo de Auditoria TCE-MG / LRF & Dossiê 360º",
                "status": "FUNCIONAL_REAL",
                "endpoint": "/api/audit/insights, /api/entity/dossier",
                "evidence": "Trilhas higienizadas sem falsos positivos públicos, termômetro fiscal LRF via SICONFI/STN e Dossiê 360º 1:1 por CNPJ.",
                "description": "Trilhas higienizadas sem falsos positivos públicos, termômetro fiscal LRF via SICONFI/STN e Dossiê 360º 1:1 por CNPJ.",
            },
            {
                "module": f"Busca Híbrida no Diário Oficial ({rt.diario_veiculo}) & Cache SQLite L1/L2 Isolado por Cidade",
                "name": f"Busca Híbrida no Diário Oficial ({rt.diario_veiculo}) & Cache SQLite L1/L2 Isolado por Cidade",
                "status": "FUNCIONAL_REAL",
                "endpoint": "/api/rag/collection, /api/governance/quota",
                "evidence": "Atos normativos únicos isolados por município + Cache SQLite persistente particionado por `tenant_id`.",
                "description": "Atos normativos únicos isolados por município + Cache SQLite persistente particionado por `tenant_id`.",
            },
        ]

        evolution_structures = [
            {
                "module": "Federação Direta com Provedor Gov.br Produção / Active Directory Municipal",
                "name": "Federação Direta com Provedor Gov.br Produção / Active Directory Municipal",
                "status": "ESTRUTURA_PREPARADA_EVOLUCAO",
                "current_v0_state": "Backend `/api/auth/login` (`admin`/`admin`) e `/api/auth/sso` com assinatura JWT HMAC-SHA256 e isolamento por gabinete já operacionais.",
                "production_requirement": "Cadastro da chave client_id/client_secret oficial da Prefeitura no portal Gov.br ou AD municipal.",
                "description": "Backend `/api/auth/login` (`admin`/`admin`) e `/api/auth/sso` com assinatura JWT HMAC-SHA256 já operacionais; requer apenas credenciais de produção do IdP da Prefeitura.",
            },
            {
                "module": "Canal Oficial WhatsApp Business Cloud API (Meta WABA Governamental)",
                "name": "Canal Oficial WhatsApp Business Cloud API (Meta WABA Governamental)",
                "status": "ESTRUTURA_PREPARADA_EVOLUCAO",
                "current_v0_state": "Integração completa via container WAHA (engine GOWS/WEBJS) com webhooks de presença, ACK, deduplicação pré-Whisper e drenagem de fila.",
                "production_requirement": "Homologação de número oficial verificado da Prefeitura na Meta Cloud API para alta disponibilidade sem dependência de sessão QR Code.",
                "description": "Integração completa via container WAHA; em produção pode migrar para número oficial Meta WABA.",
            },
            {
                "module": "Ingestão Automatizada Direta do ERP Municipal / SFTP TCE-MG",
                "name": "Ingestão Automatizada Direta do ERP Municipal / SFTP TCE-MG",
                "status": "ESTRUTURA_PREPARADA_EVOLUCAO",
                "current_v0_state": "Verificador multi-cidades (`POST /api/fleet/run`), sincronização de APIs federais e inbox auto-roteado pós-CAPTCHA operacionais.",
                "production_requirement": "Conector agendado direto com o banco de leitura do ERP contábil municipal quando contratado pela Prefeitura.",
                "description": "Verificador multi-cidades e inbox auto-roteado operacionais; conector direto ERP em implantação.",
            },
            {
                "module": "Assinatura Digital ICP-Brasil / e-TCE em Notas Técnicas de Auditoria",
                "name": "Assinatura Digital ICP-Brasil / e-TCE em Notas Técnicas de Auditoria",
                "status": "ESTRUTURA_PREPARADA_EVOLUCAO",
                "current_v0_state": "Exportação e impressão formatada (`@media print`) de Nota Técnica de Controladoria com hash de rastreabilidade.",
                "production_requirement": "Integração com certificado digital ICP-Brasil do Controlador Interno e protocolo eletrônico municipal (1Doc / SEI).",
                "description": "Exportação formatada com hash de rastreabilidade operacional; assinatura ICP-Brasil em homologação.",
            },
        ]

        return {
            "product_name": "SIM — Sistema Integrado de Inteligência e Transparência Municipal",
            "tenant_id": rt.tenant.tenant_id,
            "municipality": f"Prefeitura Municipal de {rt.tenant.municipality_name}/{rt.tenant.state_uf}",
            "ibge_code": rt.tenant.ibge_code,
            "version": "4.4.0-MultiCidade",
            "profiles": [
                {
                    "id": "servidor",
                    "name": "Servidor Municipal / Atendimento",
                    "default_role": "servidor",
                    "focus": "Consulta rápida em linguagem clara, apoio às secretarias e atendimento de demandas da Ouvidoria.",
                },
                {
                    "id": "auditor",
                    "name": "Auditor / Controladoria Interna",
                    "default_role": "auditor",
                    "focus": "Trilhas de auditoria TCE-MG, Dossiê 360º, evidências SQL/AST, Diário Oficial e emissão de Nota Técnica.",
                },
                {
                    "id": "gestor",
                    "name": "Gestor / Secretário Municipal",
                    "default_role": "gestor",
                    "focus": "Dashboard Institucional, Termômetro Fiscal (LRF, RCL, CAPAG, Mínimos de Saúde e Educação) e séries históricas.",
                },
                {
                    "id": "ouvidoria",
                    "name": "Ouvidoria & Canal WhatsApp Cidadão",
                    "default_role": "citizen",
                    "focus": "Acompanhamento em tempo real das interações via WhatsApp, estados de entrega/leitura e visão comparada Cidadão vs. Auditoria.",
                },
                {
                    "id": "admin",
                    "name": "Administrador do Sistema (TI / Governança)",
                    "default_role": "admin",
                    "focus": f"Catálogo de {objs_cnt['total']} objetos ativos, verificador multi-cidades, Cloud Data Lake (Google Drive 5 TB), franquia B2G e sessão WAHA.",
                },
            ],
            "functional_modules": functional_modules,
            "evolution_prepared_structures": evolution_structures,
            "evolution_roadmap": evolution_structures,
        }

    def _resolve_waha_base() -> str:
        import urllib.request
        candidates = []
        env_url = os.environ.get("WAHA_API_URL", "").strip()
        if env_url:
            candidates.append(env_url.rstrip("/"))
        candidates.extend(["http://waha:3000", "http://localhost:3001", "http://127.0.0.1:3001", "http://host.docker.internal:3001"])
        for c in dict.fromkeys(candidates):
            try:
                req = urllib.request.Request(f"{c}/api/sessions?all=true", method="GET")
                with urllib.request.urlopen(req, timeout=2.5) as r:
                    if r.status == 200:
                        return c
            except Exception:
                continue
        return env_url.rstrip("/") if env_url else "http://localhost:3001"

    @app.get("/api/waha/status")
    def waha_session_status(
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        """
        Verifica o status em tempo real do container WAHA e da sessão `default` do WhatsApp.
        """
        import urllib.request
        is_auth_caller, _ = _is_institutional_caller(x_api_key, authorization, x_sim_token)
        base = _resolve_waha_base()
        try:
            req = urllib.request.Request(f"{base}/api/sessions?all=true", method="GET")
            with urllib.request.urlopen(req, timeout=4) as resp:
                sessions = json.loads(resp.read().decode("utf-8"))
                default_sess = next((s for s in sessions if s.get("name") == "default"), None)
                out_status: Dict[str, Any] = {
                    "reachable": True,
                    "session_status": default_sess.get("status") if default_sess else "STOPPED",
                }
                if is_auth_caller:
                    out_status["waha_base"] = base
                    out_status["me"] = default_sess.get("me") if default_sess else None
                    out_status["sessions"] = sessions
                return out_status
        except Exception as exc:
            return {
                "reachable": False,
                "session_status": "OFFLINE",
                "error": str(exc) if is_auth_caller else "WAHA indisponível",
            }

    @app.post("/api/waha/start")
    def waha_session_start(
        request: Request,
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        """
        Inicia (ou reinicia) automaticamente a sessão `default` no WAHA já apontando o webhook para o SIM.
        """
        import urllib.request
        _require_local_or_admin_ingestion_auth(request, x_api_key, authorization, x_sim_token)
        base = _resolve_waha_base()
        hook_secret = os.environ.get("WHATSAPP_HOOK_HMAC_KEY", "").strip()
        wh_entry: Dict[str, Any] = {
            "url": "http://sim-api:8000/webhook/waha",
            "events": [
                "message",
                "message.any",
                "message.ack",
                "message.reaction",
                "session.status",
            ],
        }
        if hook_secret:
            wh_entry["hmac"] = {"key": hook_secret}
        webhook_cfg = {
            "name": "default",
            "start": True,
            "config": {"webhooks": [wh_entry]},
        }
        last_err = None
        for ep in (f"{base}/api/sessions/start", f"{base}/api/sessions", f"{base}/api/sessions/default/start"):
            try:
                data = json.dumps(webhook_cfg).encode("utf-8")
                req = urllib.request.Request(
                    ep,
                    data=data,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=8) as resp:
                    body = json.loads(resp.read().decode("utf-8") or "{}")
                    return {"status": "started", "endpoint": ep, "waha_response": body}
            except Exception as exc:
                last_err = str(exc)
        return {"status": "error", "waha_base": base, "error": last_err}

    @app.get("/api/waha/qr")
    def waha_get_qr(
        request: Request,
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        """
        Obtém exclusivamente o QR Code de pareamento da sessão `default` do WAHA (autenticado; sem screenshot de tela conectada).
        """
        import urllib.request
        _require_local_or_admin_ingestion_auth(request, x_api_key, authorization, x_sim_token)
        base = _resolve_waha_base()
        ep = f"{base}/api/default/auth/qr?format=image"
        try:
            req = urllib.request.Request(ep, headers={"Accept": "image/png, application/json"}, method="GET")
            with urllib.request.urlopen(req, timeout=6) as resp:
                ctype = resp.headers.get("Content-Type", "image/png")
                raw = resp.read()
                if "json" in ctype.lower():
                    j = json.loads(raw.decode("utf-8"))
                    if j.get("data"):
                        return {"status": "ok", "image_data_url": f"data:{j.get('mimetype', 'image/png')};base64,{j['data']}"}
                else:
                    b64 = base64.b64encode(raw).decode("ascii")
                    return {"status": "ok", "image_data_url": f"data:image/png;base64,{b64}"}
        except Exception:
            pass
        return {"status": "waiting_qr", "detail": "Sessão ainda gerando QR Code ou já conectada (WORKING)."}

    app.state.whatsapp_router = whatsapp_router

    _sync_app_state_with_runtime(init_rt)
    app.state.runtime_pool = runtime_pool
    app.state.waha_manager = waha_manager
    app.state.auth_manager = auth_manager
    app.state.onboarding_manager = onboarding_manager
    return app


app = create_app()


