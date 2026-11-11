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
from typing import Any, Deque, Dict, List, Optional, Tuple

from fastapi import Depends, FastAPI, Header, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from sim_platform.audit.proactive_insights import ProactiveAuditEngine
from sim_platform.catalog.entity_linker import normalize_municipal_asr_text
from sim_platform.engine.duckdb_manager import DuckDBManager, TenantContext
from sim_platform.ingestion.city_onboarding import CityOnboardingManager
from sim_platform.ingestion.sync_manager import DataSyncManager
from sim_platform.messaging.waha_manager import (
    MessageLifecycleState,
    UnifiedInteractionStore,
    WAHAMessagingManager,
)
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
    gabinete_id: Optional[str] = None
    explicit_segments: Optional[List[str]] = None
    explicit_tables: Optional[List[str]] = None
    allowed_sources: Optional[List[str]] = None
    sql_override: Optional[str] = None
    llm_provider: Optional[str] = None
    llm_api_key: Optional[str] = None


class LoginRequest(BaseModel):
    username: str = "admin"
    password: str = "admin"


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
        "Município de Caeté, Minas Gerais, Prefeitura Municipal de Caeté, Câmara Municipal de Caeté, "
        "SEDESMA (Secretaria de Desenvolvimento Sustentável e Meio Ambiente), SEMAS (Assistência Social), "
        "SAAE, FUNDEB, SIOPS, SICONFI, SICOM, TCE-MG, CAPAG, IDEB, LOA, LDO, LRF, QSA, CNAE, "
        "servidores efetivos, concursados, comissionados, contratados temporários, adesão a ata de registro de preços, "
        "carona, Jornal Opinião, Sabará, Nova Lima."
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
                                            "Transcreva fielmente este áudio em português do Brasil sobre gestão pública do município de Caeté/MG. "
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
                        norm_txt, corrections = normalize_municipal_asr_text(raw_txt, municipality_name="Caeté")
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
                        norm_txt, corrections = normalize_municipal_asr_text(raw_txt, municipality_name="Caeté")
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
        norm_fb, corrections = normalize_municipal_asr_text(raw_fb, municipality_name="Caeté")
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
    auth_manager = InstitutionalAuthManager()
    onboarding_manager = CityOnboardingManager(workspace_root=DEFAULT_REPO_ROOT)
    sync_manager = DataSyncManager(workspace_root=DEFAULT_REPO_ROOT, tenant_id=tenant.tenant_id)
    audit_engine = ProactiveAuditEngine(workspace_root=DEFAULT_REPO_ROOT, tenant_id=tenant.tenant_id)
    db_manager = DuckDBManager(tenant=tenant)
    rag_retriever = HybridRAGRetriever(corpus_chunks=rag_corpus)
    persist_hist_path = DEFAULT_REPO_ROOT / "avaliacao" / "dados" / "parquet" / tenant.tenant_id / "_interactions_history.json"
    interaction_store = UnifiedInteractionStore(persist_path=persist_hist_path)
    orchestrator = SIMOrchestrator(
        db_manager=db_manager,
        rag_retriever=rag_retriever,
        interaction_store=interaction_store,
    )
    waha_manager = WAHAMessagingManager(interaction_store=interaction_store)
    security_and_error_events: Deque[Dict[str, Any]] = deque(maxlen=150)

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
        f"SIM Platform v3.2 Institucional inicializada para tenant={tenant.tenant_id} "
        f"(tabelas={len(db_manager.catalog.tables)}, atos_rag={len(rag_retriever.corpus_chunks)})"
    )

    def verify_auth(
        x_api_key: Optional[str] = Header(default=None),
        x_sim_role: str = Header(default="citizen"),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> str:
        expected_key = require_api_key if require_api_key is not None else os.environ.get("SIM_API_KEY")
        if expected_key and x_api_key != expected_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="API Key inválida ou ausente (cabeçalho X-API-Key obrigatório).",
            )
        raw_tok = x_sim_token or (authorization.split(" ", 1)[1] if authorization and " " in authorization else authorization)
        if raw_tok:
            claims = auth_manager.verify_token(raw_tok)
            if claims and claims.get("role"):
                return str(claims["role"])
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
    ) -> bool:
        """
        Verifica se a requisição possui credencial institucional verificada (SSO / Login admin/admin ou X-API-Key válida)
        para desmascarar CPFs de pessoas físicas nos termos da LGPD (Lei 13.709/2018).
        """
        import hmac

        expected_key = (require_api_key if require_api_key is not None else os.environ.get("SIM_API_KEY") or "").strip()
        if expected_key and x_api_key and hmac.compare_digest(x_api_key.strip(), expected_key):
            return True
        raw_tok = x_sim_token or (authorization.split(" ", 1)[1] if authorization and " " in authorization else authorization)
        if raw_tok:
            claims = auth_manager.verify_token(raw_tok)
            if claims and claims.get("can_unmask_pii"):
                return True
        return False

    @app.post("/api/auth/login")
    def login_institutional_endpoint(req: LoginRequest) -> Dict[str, Any]:
        res = auth_manager.authenticate_credentials(req.username, req.password)
        if res.get("status") != "sucesso":
            raise HTTPException(status_code=401, detail=res.get("detail", "Credenciais inválidas."))
        _log(f"Login Institucional realizado: user={req.username} (role={res.get('user', {}).get('role')}, gabinete={res.get('user', {}).get('gabinete_id')})")
        return res

    @app.post("/api/auth/sso")
    def sso_exchange_endpoint(req: SSORequest) -> Dict[str, Any]:
        eff_code = req.sso_code or req.code or "govbr_demo_admin"
        res = auth_manager.exchange_sso_code(
            provider=req.provider,
            code=eff_code,
            gabinete_id=req.gabinete_id,
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

    @app.get("/api/tenants")
    def list_tenants_endpoint(
        format: Optional[str] = None,
        accept: Optional[str] = Header(default=None),
    ) -> Any:
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
            "active_tenant_id": tenant.tenant_id,
            "total_configured_tenants": len(tenants_list),
            "tenants": tenants_list,
        }

    @app.post("/api/tenants/onboard")
    def onboard_new_city_endpoint(req: OnboardCityRequest) -> Dict[str, Any]:
        _log(f"Onboarding Multi-Cidade disparado para municipio={req.municipality_name or req.tenant_id}")
        return onboarding_manager.onboard_city(
            tenant_id=req.tenant_id,
            municipality_name=req.municipality_name,
            state_uf=req.state_uf,
            ibge_code=req.ibge_code,
            plan_tier=req.plan_key or req.plan_tier or "porte_2",
            official_gazette_name=req.official_gazette_name,
            seed_from_template_if_empty=req.sanitize_and_build_parquets,
        )

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
        return sync_manager.check_status()

    @app.post("/api/ingestion/sync")
    def trigger_ingestion_sync(
        req: Optional[SyncRequest] = None,
        role: str = Depends(verify_auth),
    ) -> Dict[str, Any]:
        from sim_platform.catalog.entity_linker import EntityLinker
        selected = req.sources if req else None
        _log(f"POST /api/ingestion/sync disparado (role={role}, sources={selected or 'ALL'})")
        sync_res = sync_manager.sync_all(
            selected_sources=selected,
            db_manager=db_manager,
            rag_retriever=rag_retriever,
        )
        orchestrator.entity_linker = EntityLinker(
            workspace_root=DEFAULT_REPO_ROOT,
            conn=db_manager.conn,
        )
        return sync_res

    @app.get("/api/governance/quota")
    def get_governance_quota_status(
        format: Optional[str] = None,
        accept: Optional[str] = Header(default=None),
    ) -> Any:
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
        return orchestrator.quota_manager.get_quota_status()

    @app.post("/api/guardrail/evaluate")
    def evaluate_semantic_guardrail(payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Avalia diretamente uma pergunta no Guardião Semântico IA Leve (`SemanticIntentGuardrail`)
        antes do processamento analítico principal, verificando se a dúvida faz sentido,
        qual a rota ideal e qual a pergunta autônoma contextualizada.
        """
        question = str(payload.get("question") or "").strip()
        session_id = str(payload.get("session_id") or "default").strip()
        role = str(payload.get("role") or "citizen").strip()
        enable_ai = bool(payload.get("enable_external_ai", True))
        explicit_hist = payload.get("conversation_history") or payload.get("recent_history")
        hist = list(explicit_hist) if isinstance(explicit_hist, list) else list(orchestrator.conversation_history.get(session_id, []))
        decision = orchestrator.semantic_guardrail.evaluate(
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
        para o cabeçalho de Inteligência Executiva.
        """
        cur = db_manager.conn.cursor()
        pulse: Dict[str, Any] = {
            "status": "sucesso",
            "tenant_id": tenant.tenant_id,
            "municipality": f"{tenant.municipality_name}/{tenant.state_uf}",
            "ibge_code": tenant.ibge_code,
            "catalog_objects": len(db_manager.catalog.tables),
            "rag_unique_acts": len(rag_retriever.corpus_chunks),
        }
        try:
            row_fin = cur.execute(
                """
                SELECT vlr_receita_corrente_liquida, pct_despesa_pessoal_rcl,
                       nota_capag_tesouro, pct_aplicacao_saude_siops, pct_aplicacao_educacao_mde,
                       situacao_fiscal_lrf
                FROM siconfi_finbra_indicadores
                WHERE ano_referencia = 2024
                LIMIT 1
                """
            ).fetchone()
            if row_fin:
                pulse["rcl_2024"] = float(row_fin[0] or 0)
                pulse["pct_pessoal_lrf_2024"] = float(row_fin[1] or 0)
                pulse["nota_capag"] = str(row_fin[2] or "A")
                pulse["pct_saude_siops_2024"] = float(row_fin[3] or 0)
                pulse["pct_educacao_mde_2024"] = float(row_fin[4] or 0)
                pulse["situacao_fiscal_lrf"] = str(row_fin[5] or "REGULAR")
        except Exception:
            pass
        try:
            row_ibge = cur.execute(
                """
                SELECT populacao_residente, pib_per_capita_reais, idhm_geral
                FROM ibge_indicadores_municipais
                WHERE ano_referencia = 2024
                LIMIT 1
                """
            ).fetchone()
            if row_ibge:
                pulse["populacao_2024"] = int(row_ibge[0] or 0)
                pulse["pib_per_capita_reais"] = float(row_ibge[1] or 0)
                pulse["idhm_geral"] = float(row_ibge[2] or 0)
        except Exception:
            pass
        return pulse

    @app.get("/api/entity/dossier")
    def get_entity_dossier(
        q: str,
        role: str = Depends(verify_auth),
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        """
        Gera o Dossiê 360º de Entidade (Fornecedor / CNPJ / Servidor) cruzando:
        1. Cadastro & QSA Reconciliado (`receita_qsa_fornecedores`)
        2. Série Histórica de Pagamentos 2020-2025 (`despesa_pagamento`)
        3. Contratos e Termos Aditivos (`vw_gold_contratos_vigencia_aditivos`)
        4. Verificação de Conflito de Interesses (`vw_gold_conflito_interesses_licitacao`)
        5. Atos Publicados no Diário Oficial (`Jornal Opinião`)
        """
        from sim_platform.security.sql_guard import mask_sensitive_rows
        raw_q = (q or "").strip()
        if not raw_q:
            raise HTTPException(status_code=400, detail="Parâmetro 'q' (nome ou CNPJ/CPF) obrigatório.")

        pii_role = role if is_pii_unmask_authorized(x_api_key, authorization, x_sim_token) else "citizen"
        digits = "".join(ch for ch in raw_q if ch.isdigit())
        safe_like = raw_q.replace("'", "''").upper().strip()
        cur = db_manager.conn.cursor()

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

        rag_acts = rag_retriever.search_by_entities(entity_names=entity_names, cnpjs=entity_cnpjs, top_k=5)
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

        return {
            "status": "sucesso",
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
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        raw_sql = (req.query or req.pergunta_usuario or "").strip()
        effective_query_role = (
            role if is_pii_unmask_authorized(x_api_key, authorization, x_sim_token) else "citizen"
        )
        _log(f"POST /query (role={role}, pii_role={effective_query_role}): {raw_sql[:120]}")
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
            role=effective_query_role,
            enforce_limit=req.enforce_limit,
            allowed_tables_override=restricted_tables,
        )
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
        x_api_key: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
        x_sim_gabinete: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        from sim_platform.security.sql_guard import mask_sensitive_rows, mask_text_cpfs

        question = (req.question or req.pergunta_usuario or "").strip()
        effective_role = req.role if req.role != "citizen" else role
        token_claims = auth_manager.resolve_claims_from_headers(authorization=authorization, x_sim_token=x_sim_token)
        gabinete_id = (
            req.gabinete_id
            or x_sim_gabinete
            or (token_claims.get("gabinete_id") if token_claims else None)
            or "prefeitura_central"
        )
        pii_authorized = is_pii_unmask_authorized(x_api_key, authorization, x_sim_token)
        _log(
            f"POST /chat (session={req.session_id}, role={effective_role}, gabinete={gabinete_id}, "
            f"pii_auth={pii_authorized}, sources={req.allowed_sources or 'ALL'}): {question[:100]}"
        )
        rec = interaction_store.create_record(
            channel="web_institucional",
            session_id=req.session_id,
            chat_id=req.session_id,
            user_display=f"Plataforma Institucional ({effective_role})",
            role=effective_role,
            question=question,
            gabinete_id=gabinete_id,
        )
        interaction_store.transition_state(
            rec.interaction_id,
            MessageLifecycleState.PROCESSING,
            detail="Consulta em processamento pelo orquestrador na Plataforma Institucional.",
        )

        # Se o usuário solicitou role='auditor'/'admin' sem token SSO/Admin nem X-API-Key,
        # executa no motor com perfil institucional mascarado ('gestor') para nunca gravar CPFs abertos no cache SQLite
        execution_role = (
            "gestor"
            if (not pii_authorized and effective_role in ("auditor", "admin", "controller", "controladoria"))
            else effective_role
        )
        res = orchestrator.handle_question(
            question=question,
            session_id=req.session_id,
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

        # LGPD: garantia dupla de mascaramento de CPFs pessoais (11 dígitos) caso não autorizado
        if not pii_authorized and effective_role != "citizen":
            masked_rows, extra_masked = mask_sensitive_rows(res_dict.get("sql_rows") or [], role="citizen")
            res_dict["sql_rows"] = masked_rows
            res_dict["pii_masked_count"] = int(res_dict.get("pii_masked_count") or 0) + extra_masked
            res_dict["answer_text"] = mask_text_cpfs(str(res_dict.get("answer_text") or ""))
            res_dict["institutional_response"] = mask_text_cpfs(str(res_dict.get("institutional_response") or ""))

        rec.status = res.status
        rec.intent = res.intent
        rec.citizen_response = res_dict.get("citizen_response") or res_dict.get("answer_text") or ""
        rec.institutional_response = res_dict.get("institutional_response") or res_dict.get("answer_text") or ""
        rec.whatsapp_text = (rec.citizen_response).replace("**", "*")
        rec.sql_executed = res.sql_executed
        rec.sql_rows_count = len(res_dict.get("sql_rows") or [])
        rec.sql_rows_preview = (res_dict.get("sql_rows") or [])[:30]
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
        interaction_store.transition_state(
            rec.interaction_id,
            MessageLifecycleState.SENT,
            detail="Resposta entregue de forma síncrona na interface institucional.",
        )
        interaction_store.save(rec)

        if res.status == "bloqueado_seguranca":
            security_and_error_events.append(
                {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "category": "SECURITY_PROMPT_OR_SCOPE_BLOCK",
                    "event_type": "SECURITY_PROMPT_OR_SCOPE_BLOCK",
                    "channel": "web_institucional",
                    "role": effective_role,
                    "code": res.intent.upper(),
                    "detail": res.answer_text[:200],
                    "input_preview": question[:180],
                }
            )

        res_dict["interaction_id"] = rec.interaction_id
        res_dict["gabinete_id"] = gabinete_id
        res_dict["pii_unmasked"] = pii_authorized
        res_dict["message_state"] = rec.message_state
        res_dict["message_state_label"] = rec.message_state_label
        return res_dict

    @app.post("/webhook/waha")
    @app.post("/webhook/whatsapp-entrada")
    def waha_webhook_endpoint(payload_root: Dict[str, Any]) -> Dict[str, Any]:
        """
        Endpoint nativo e maduro compatível com o WAHA (WhatsApp HTTP API) sem necessidade do n8n.
        Implementa:
        - Eventos de ciclo de vida (`message.ack`, `message.reaction`, `session.status`, `message.revoked`)
        - Deduplicação idempotente por `message_id` ANTES do download de mídia e transcrição de áudio (economia de 50% de Whisper)
        - Rate limit de 20 msgs/dia por número com aviso único no WhatsApp (sem silêncio opaco)
        - Coalescência e fila ordenada por `chat_id` com loop de drenagem pós-processamento
        - Indicador de digitação natural (`sendSeen` -> `startTyping` -> `stopTyping`)
        - Envio inteligente sem timer fixo e confirmação estrita de entrega
        - Persistência integrada no `UnifiedInteractionStore` com visão dupla (Cidadão + Auditor)
        """
        body_obj = payload_root.get("body") if isinstance(payload_root.get("body"), dict) else payload_root
        session = str(body_obj.get("session") or "default")
        event = str(body_obj.get("event") or "message.any")
        payload = body_obj["payload"] if isinstance(body_obj.get("payload"), dict) else body_obj

        # 1. Tratar eventos assíncronos de estado do WAHA (message.ack, message.reaction, session.status)
        lifecycle_event_resp = waha_manager.handle_webhook_event(event_name=event, session=session, payload=payload)
        if lifecycle_event_resp is not None:
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
            message_text = message_text[5:].strip()
            self_test_override = True

        if from_me and not self_test_override:
            return {"status": "ignored", "reason": "fromMe=True", "chat_id": chat_id}
        if chat_id.endswith("@g.us") or chat_id == "status@broadcast":
            return {"status": "ignored", "reason": "group_or_status", "chat_id": chat_id}

        # 2. Deduplicação idempotente por `explicit_msg_id` ANTES de baixar mídia ou transcrever áudio!
        # Evita pagar 2x no Whisper quando o WAHA dispara `message` + `message.any` para o mesmo áudio.
        pre_checked_id = False
        eff_msg_id = explicit_msg_id
        if explicit_msg_id:
            is_dup, existing_iid, eff_msg_id = waha_manager.coordinator.check_and_mark_seen_message(
                message_id=explicit_msg_id,
                chat_id=chat_id,
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
                }

        # 3. Rate limit anti-flood por cidadão no WhatsApp (ANTES de gastar Whisper/LLM)
        allowed_rl, rl_msg, should_notify_wpp = orchestrator.quota_manager.check_whatsapp_rate_limit(chat_id)
        if not allowed_rl:
            if should_notify_wpp:
                # Enviar aviso educado uma única vez no dia pelo WhatsApp para não deixar o cidadão no vácuo
                notice_rec = interaction_store.create_record(
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
                )
            return {
                "status": "rate_limited",
                "session": session,
                "chat_id": chat_id,
                "notified_user_once": should_notify_wpp,
                "whatsapp_text": rl_msg,
            }

        # 4. Verificar se é mensagem de voz / áudio do WhatsApp (somente após passar na deduplicação e rate limit)
        media_obj = payload.get("media") if isinstance(payload.get("media"), dict) else {}
        audio_b64 = str(payload.get("audio_base64") or media_obj.get("data") or "").strip()
        audio_mime = str(payload.get("mime_type") or media_obj.get("mimetype") or "audio/ogg").strip()
        audio_transcript = str(payload.get("audio_transcript") or "").strip()
        was_audio = False
        audio_engine = None

        media_url = str(media_obj.get("url") or "").strip()
        if not audio_b64 and media_url and ("audio" in audio_mime.lower() or "ogg" in media_url.lower() or payload.get("hasMedia")):
            try:
                import urllib.request
                waha_internal = os.environ.get("WAHA_API_URL", "http://waha:3000").rstrip("/")
                resolved_url = (
                    media_url.replace("http://localhost:3000", waha_internal)
                    .replace("http://localhost:3001", waha_internal)
                    .replace("http://127.0.0.1:3000", waha_internal)
                )
                with urllib.request.urlopen(resolved_url, timeout=15) as resp:
                    audio_b64 = base64.b64encode(resp.read()).decode("ascii")
            except Exception as exc:
                _log(f"Aviso ao baixar media.url do WAHA ({media_url}): {exc}")

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
                orchestrator.quota_manager.record_audio_usage(seconds_estimated=30)

        if not message_text:
            return {"status": "ignored", "reason": "empty_body", "chat_id": chat_id}

        # Deduplicação por hash de conteúdo caso o payload não tivesse `id` explícito
        if not pre_checked_id:
            is_dup, existing_iid, eff_msg_id = waha_manager.coordinator.check_and_mark_seen_message(
                message_id=None,
                chat_id=chat_id,
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
                }

        # 5. Se outra mensagem do mesmo `chat_id` está sendo processada neste exato instante,
        # enfileirar fragmento para preservar ordem/coalescência
        if waha_manager.coordinator.enqueue_if_busy(chat_id, message_text, eff_msg_id):
            _log(f"WAHA Concurrent Message Coalesced (chat={chat_id}, msg_id={eff_msg_id}): {message_text[:80]}")
            return {
                "status": "coalesced_into_active",
                "session": session,
                "chat_id": chat_id,
                "message_id": eff_msg_id,
                "detail": "Mensagem recebida durante processamento ativo e consolidada na fila ordenada do chat.",
            }

        chat_lock = waha_manager.coordinator.get_chat_lock(chat_id)
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
                turn_rec = interaction_store.create_record(
                    channel="whatsapp",
                    session_id=session,
                    chat_id=chat_id,
                    user_display=notify_name,
                    role="citizen",
                    question=turn_text,
                    inbound_message_id=turn_msg_id,
                    was_audio=turn_was_audio,
                )
                if turn_msg_id:
                    waha_manager.coordinator.bind_interaction_to_message_id(turn_msg_id, turn_rec.interaction_id)

                waha_manager.send_seen(turn_rec, session=session, chat_id=chat_id, message_id=turn_msg_id or None)
                waha_manager.start_typing(turn_rec, session=session, chat_id=chat_id)
                if turn_msg_id:
                    waha_manager.send_reaction(turn_rec, session=session, message_id=turn_msg_id, emoji="⏳")

                try:
                    effective_q = turn_text
                    if extra_frags:
                        effective_q = " ".join([turn_text] + [f.strip() for f in extra_frags if f.strip()])
                        turn_rec.effective_question = effective_q
                        turn_rec.coalesced_count = 1 + len(extra_frags)

                    _log(
                        f"WAHA Webhook ({event}, chat={chat_id}, audio={turn_was_audio}, "
                        f"coalesced={turn_rec.coalesced_count}): {effective_q[:100]}"
                    )
                    turn_res = orchestrator.handle_question(
                        question=effective_q,
                        session_id=chat_id,
                        role="citizen",
                    )
                finally:
                    waha_manager.stop_typing(turn_rec, session=session, chat_id=chat_id)

                turn_res_dict = turn_res.to_dict()
                cit_nat = str(turn_res_dict.get("citizen_response") or turn_res_dict.get("answer_text") or "").strip()
                if turn_was_audio:
                    cit_nat = f'*[Áudio transcrito]:* "{turn_text}"\n\n{cit_nat}'
                wpp_txt = cit_nat.replace("**", "*")

                s_res = waha_manager.send_text_with_confirmation(
                    turn_rec,
                    session=session,
                    chat_id=chat_id,
                    text=wpp_txt,
                    reply_to=turn_msg_id or None,
                )
                img_res: Optional[Dict[str, Any]] = None
                if turn_res.chart_base64:
                    img_caption = (turn_res.chart_caption or "Gráfico Institucional — Plataforma SIM").replace("**", "*")
                    img_res = waha_manager.send_image_with_confirmation(
                        turn_rec,
                        session=session,
                        chat_id=chat_id,
                        image_base64=turn_res.chart_base64,
                        mime_type=turn_res.chart_mime_type or "image/png",
                        filename="grafico_institucional_sim.png",
                        caption=img_caption,
                        reply_to=turn_msg_id or None,
                    )
                if s_res.get("ok") and turn_msg_id:
                    waha_manager.send_reaction(turn_rec, session=session, message_id=turn_msg_id, emoji="✅")

                if not s_res.get("ok"):
                    security_and_error_events.append(
                        {
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "category": "WAHA_DELIVERY_FAILURE",
                            "event_type": "WAHA_DELIVERY_FAILURE",
                            "channel": "whatsapp",
                            "role": "citizen",
                            "code": "SEND_FAILED",
                            "detail": str(turn_rec.last_send_error or "Falha ao enviar mensagem pelo WAHA"),
                            "input_preview": f"{chat_id}: {effective_q[:120]}",
                        }
                    )

                turn_rec.status = turn_res.status if s_res.get("ok") else "falha_envio_whatsapp"
                turn_rec.intent = turn_res.intent
                turn_rec.citizen_response = turn_res.citizen_response or turn_res.answer_text
                turn_rec.institutional_response = turn_res.institutional_response or turn_res.answer_text
                turn_rec.whatsapp_text = wpp_txt
                turn_rec.sql_executed = turn_res.sql_executed
                turn_rec.sql_rows_count = len(turn_res.sql_rows)
                turn_rec.sql_rows_preview = turn_res.sql_rows[:30]
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
                interaction_store.save(turn_rec)
                if img_res is not None:
                    s_res = {**s_res, "chart_image_sent": bool(img_res.get("ok")), "chart_mime_type": turn_res.chart_mime_type or "image/png"}
                return turn_rec, turn_res_dict, s_res, wpp_txt

            initial_extra = list(payload.get("coalesce_fragments") or [])
            queued_before = waha_manager.coordinator.drain_coalesced_fragments(chat_id)
            for qi in queued_before:
                if qi.get("text"):
                    initial_extra.append(str(qi["text"]))

            rec, res_dict, send_res, whatsapp_text = _process_single_turn(
                turn_text=message_text,
                turn_msg_id=eff_msg_id,
                turn_was_audio=was_audio,
                extra_frags=initial_extra,
            )

            # Loop de drenagem pós-processamento: se chegaram novos fragmentos ENQUANTO o LLM processava,
            # drená-los e respondê-los imediatamente antes de liberar o lock do chat!
            follow_up_dispatched = 0
            while True:
                late_items = waha_manager.coordinator.drain_coalesced_fragments(chat_id)
                if not late_items:
                    break
                late_texts = [str(it.get("text") or "").strip() for it in late_items if str(it.get("text") or "").strip()]
                if not late_texts:
                    break
                late_msg_id = str(late_items[-1].get("message_id") or "")
                _process_single_turn(
                    turn_text=late_texts[0],
                    turn_msg_id=late_msg_id,
                    turn_was_audio=False,
                    extra_frags=late_texts[1:],
                )
                follow_up_dispatched += 1

            waha_delivery = {
                "sent": bool(send_res.get("ok")),
                "mode": send_res.get("mode", "local_verified"),
                "http_status": send_res.get("http_status", 200),
                "attempts": rec.send_attempts,
                "outbound_message_id": rec.outbound_message_id,
                "error": rec.last_send_error,
                "follow_up_batches_dispatched": follow_up_dispatched,
            }

            return {
                "status": "processed" if send_res.get("ok") else "delivery_failed",
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
                "route_taken": res_dict.get("intent"),
                "whatsapp_text": whatsapp_text,
                "waha_delivery": waha_delivery,
                "waha_actions": rec.waha_actions,
                "state_history": rec.state_history,
                "orchestrator_response": res_dict,
            }

    @app.get("/api/interactions")
    def list_unified_interactions(
        channel: Optional[str] = None,
        role: Optional[str] = None,
        state: Optional[str] = None,
        search: Optional[str] = None,
        gabinete_id: Optional[str] = None,
        limit: int = 50,
        x_sim_gabinete: Optional[str] = Header(default=None),
        authorization: Optional[str] = Header(default=None),
        x_sim_token: Optional[str] = Header(default=None),
    ) -> Dict[str, Any]:
        """
        Lista o histórico unificado de interações (WhatsApp Cidadão + Plataforma Institucional Web),
        permitindo auditoria cruzada, isolamento por Gabinete Parlamentar (`gabinete_id`) e estados de mensagens.
        """
        token_claims = auth_manager.resolve_claims_from_headers(authorization=authorization, x_sim_token=x_sim_token)
        eff_gabinete = gabinete_id or x_sim_gabinete
        if token_claims and not token_claims.get("can_view_all_gabinetes", True):
            eff_gabinete = str(token_claims.get("gabinete_id") or eff_gabinete or "")
        eff_channel = "web_institucional" if channel == "web" else channel
        return interaction_store.list_records(
            channel=eff_channel,
            role=role,
            state=state,
            search=search,
            gabinete_id=eff_gabinete,
            limit=limit,
        )

    @app.get("/api/interactions/{interaction_id}")
    def get_interaction_detail(interaction_id: str) -> Dict[str, Any]:
        rec = interaction_store.get_by_id(interaction_id)
        if not rec:
            raise HTTPException(status_code=404, detail=f"Interação '{interaction_id}' não encontrada.")
        return rec.to_dict(include_heavy=True)

    @app.post("/api/whatsapp/retry/{interaction_id}")
    def retry_whatsapp_delivery(interaction_id: str) -> Dict[str, Any]:
        rec = interaction_store.get_by_id(interaction_id)
        if not rec:
            raise HTTPException(status_code=404, detail=f"Interação '{interaction_id}' não encontrada.")
        waha_manager.start_typing(rec, session=rec.session_id, chat_id=rec.chat_id)
        try:
            send_res = waha_manager.send_text_with_confirmation(
                rec,
                session=rec.session_id,
                chat_id=rec.chat_id,
                text=rec.whatsapp_text or rec.citizen_response,
                reply_to=rec.inbound_message_id,
            )
        finally:
            waha_manager.stop_typing(rec, session=rec.session_id, chat_id=rec.chat_id)
        interaction_store.save(rec)
        return {
            "status": "retried_ok" if send_res.get("ok") else "retry_failed",
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
        objs = db_manager.get_mapped_objects_count()
        hist_summary = interaction_store.list_records(limit=10)["summary"]
        quota_st = orchestrator.quota_manager.get_quota_status()
        sync_st = sync_manager.check_status()

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

        return {
            "status": "operacional",
            "tenant_id": tenant.tenant_id,
            "municipality": f"{tenant.municipality_name}/{tenant.state_uf}",
            "mapped_objects": objs,
            "rag_unique_acts": len(rag_retriever.corpus_chunks),
            "ingestion_up_to_date": sync_st.get("all_up_to_date", True),
            "interaction_summary": hist_summary,
            "waha_robustness": {
                "duplicates_blocked": waha_manager.coordinator.duplicates_blocked,
                "duplicates_prevented": waha_manager.coordinator.duplicates_blocked,
                "coalesced_messages": waha_manager.coordinator.coalesced_messages,
                "coalesced_fragments": waha_manager.coordinator.coalesced_messages,
                "confirmed_deliveries": hist_summary.get("confirmed_answered", 0),
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
        functional_modules = [
            {
                "module": "Motor Analítico DuckDB Parquet & 6 Gold Views Semânticas",
                "name": "Motor Analítico DuckDB Parquet & 6 Gold Views Semânticas",
                "status": "FUNCIONAL_REAL",
                "endpoint": "/query, /catalog, /api/executive/pulse",
                "evidence": "79 tabelas Parquet + 6 Gold Views pré-desduplicadas (85 objetos ativos), reparo 2023 (R$ 11,75M recuperados) e SQLGuard AST.",
                "description": "79 tabelas Parquet + 6 Gold Views pré-desduplicadas (85 objetos ativos), reparo 2023 (R$ 11,75M recuperados) e SQLGuard AST.",
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
                "module": "Mensageria Avançada WhatsApp / WAHA (Typing, ACK, Reações, Deduplicação Pré-Whisper e Drain Loop)",
                "name": "Mensageria Avançada WhatsApp / WAHA (Typing, ACK, Reações, Deduplicação Pré-Whisper e Drain Loop)",
                "status": "FUNCIONAL_REAL",
                "endpoint": "/webhook/waha, /api/whatsapp/diagnostics, /api/whatsapp/retry/{id}",
                "evidence": "Ciclo `received -> seen -> processing (typing) -> sent -> delivered -> read / failed`, deduplicação antes do Whisper e drenagem automática de mensagens coalescidas.",
                "description": "Ciclo `received -> seen -> processing (typing) -> sent -> delivered -> read / failed`, deduplicação antes do Whisper e drenagem automática de mensagens coalescidas.",
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
                "module": "Onboarding Declarativo Multi-Cidade (`tenants/*.json` + Vacina Universal de CSV)",
                "name": "Onboarding Declarativo Multi-Cidade (`tenants/*.json` + Vacina Universal de CSV)",
                "status": "FUNCIONAL_REAL",
                "endpoint": "/api/tenants, /api/tenants/onboard",
                "evidence": "Onboarding imediato de novos municípios (ex.: Sabará/MG 3156700, Nova Lima/MG 3144805) com reparo automático de deslocamento de colunas na folha SICOM.",
                "description": "Onboarding imediato de novos municípios (ex.: Sabará/MG 3156700, Nova Lima/MG 3144805) com reparo automático de deslocamento de colunas na folha SICOM.",
            },
            {
                "module": "Motor Proativo de Auditoria TCE-MG (7 Trilhas Reais Higienizadas) & Dossiê 360º",
                "name": "Motor Proativo de Auditoria TCE-MG (7 Trilhas Reais Higienizadas) & Dossiê 360º",
                "status": "FUNCIONAL_REAL",
                "endpoint": "/api/audit/insights, /api/entity/dossier",
                "evidence": "7 trilhas sem falsos positivos públicos (INSS/TJMG/Prefeitura excluídos de Caronas/Top PJ), CTB Art. 115 §4º-A aplicado à frota e `vw_gold_fornecedores_qsa_360` 1:1 por CNPJ.",
                "description": "7 trilhas sem falsos positivos públicos (INSS/TJMG/Prefeitura excluídos de Caronas/Top PJ), CTB Art. 115 §4º-A aplicado à frota e `vw_gold_fornecedores_qsa_360` 1:1 por CNPJ.",
            },
            {
                "module": "Busca Híbrida no Diário Oficial (*Jornal Opinião*) & Cache SQLite L1/L2 (Trava 1)",
                "name": "Busca Híbrida no Diário Oficial (*Jornal Opinião*) & Cache SQLite L1/L2 (Trava 1)",
                "status": "FUNCIONAL_REAL",
                "endpoint": "/api/rag/collection, /api/governance/quota",
                "evidence": "Atos normativos únicos (incluindo DISPENSA) enriquecidos com inventário completo + Cache SQLite persistente de respostas e franquia de tokens por plano comercial.",
                "description": "Atos normativos únicos (incluindo DISPENSA) enriquecidos com inventário completo + Cache SQLite persistente de respostas e franquia de tokens por plano comercial.",
            },
        ]

        evolution_structures = [
            {
                "module": "Federação Direta com Provedor Gov.br Produção / Active Directory Municipal",
                "name": "Federação Direta com Provedor Gov.br Produção / Active Directory Municipal",
                "status": "ESTRUTURA_PREPARADA_EVOLUCAO",
                "current_v0_state": "Backend `/api/auth/login` (`admin`/`admin`) e `/api/auth/sso` com assinatura JWT HMAC-SHA256 e isolamento por gabinete já operacionais.",
                "production_requirement": "Cadastro da chave client_id/client_secret oficial da Prefeitura no portal Gov.br ou AD municipal.",
                "description": "Backend `/api/auth/login` (`admin`/`admin`) e `/api/auth/sso` com assinatura JWT HMAC-SHA226 já operacionais; requer apenas credenciais de produção do IdP da Prefeitura.",
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
                "current_v0_state": "Sincronização incremental one-click (`POST /api/ingestion/sync`) e onboarding multi-cidade (`POST /api/tenants/onboard`).",
                "production_requirement": "Conector agendado (cron/Airflow) direto com o banco de leitura do ERP contábil municipal e API automática do TCE-MG.",
                "description": "Sincronização incremental one-click e onboarding multi-cidade operacionais; conector agendado ERP em implantação.",
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
            "municipality": f"Prefeitura Municipal de {tenant.municipality_name}/{tenant.state_uf}",
            "ibge_code": tenant.ibge_code,
            "version": "3.2.0-V0-Institucional",
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
                    "focus": "Catálogo de 85 objetos, sincronização Parquet/RAG, onboarding multi-cidade, franquia B2G e sessão WAHA.",
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
    def waha_session_status() -> Dict[str, Any]:
        """
        Verifica o status em tempo real do container WAHA e da sessão `default` do WhatsApp.
        """
        import urllib.request
        base = _resolve_waha_base()
        try:
            req = urllib.request.Request(f"{base}/api/sessions?all=true", method="GET")
            with urllib.request.urlopen(req, timeout=4) as resp:
                sessions = json.loads(resp.read().decode("utf-8"))
                default_sess = next((s for s in sessions if s.get("name") == "default"), None)
                return {
                    "reachable": True,
                    "waha_base": base,
                    "session_status": default_sess.get("status") if default_sess else "STOPPED",
                    "me": default_sess.get("me") if default_sess else None,
                    "sessions": sessions,
                }
        except Exception as exc:
            return {
                "reachable": False,
                "waha_base": base,
                "session_status": "OFFLINE",
                "error": str(exc),
            }

    @app.post("/api/waha/start")
    def waha_session_start() -> Dict[str, Any]:
        """
        Inicia (ou reinicia) automaticamente a sessão `default` no WAHA já apontando o webhook para o SIM.
        """
        import urllib.request
        base = _resolve_waha_base()
        webhook_cfg = {
            "name": "default",
            "start": True,
            "config": {
                "webhooks": [
                    {
                        "url": "http://sim-api:8000/webhook/waha",
                        "events": [
                            "message",
                            "message.any",
                            "message.ack",
                            "message.reaction",
                            "session.status",
                        ],
                    }
                ]
            },
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
    def waha_get_qr() -> Dict[str, Any]:
        """
        Obtém o QR Code ou screenshot da sessão `default` do WAHA em Base64 para exibir direto no Console SIM.
        """
        import urllib.request
        base = _resolve_waha_base()
        for ep in (
            f"{base}/api/default/auth/qr?format=image",
            f"{base}/api/screenshot?session=default",
        ):
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
                continue
        return {"status": "waiting_qr", "detail": "Sessão ainda gerando QR Code ou já conectada (WORKING)."}

    app.state.db_manager = db_manager
    app.state.orchestrator = orchestrator
    app.state.rag_retriever = rag_retriever
    app.state.sync_manager = sync_manager
    app.state.audit_engine = audit_engine
    app.state.interaction_store = interaction_store
    app.state.waha_manager = waha_manager
    app.state.auth_manager = auth_manager
    app.state.onboarding_manager = onboarding_manager
    return app


app = create_app()


