"""
Suíte Adversarial Red Team — Hardening Multi-Cidades, Contabilidade Pública, LGPD e Cloud Two-Phase Delete.
"""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from typing import Any, Dict

import pytest
from fastapi.testclient import TestClient

from sim_platform.api.app import TenantRuntimePool, create_app
from sim_platform.catalog.entity_linker import EntityLinker
from sim_platform.engine.duckdb_manager import DuckDBManager, TenantContext
from sim_platform.ingestion.city_onboarding import CityOnboardingManager
from sim_platform.ingestion.fleet_verifier import FleetVerifier
from sim_platform.ingestion.sync_manager import DataSyncManager
from sim_platform.orchestrator.router import SIMOrchestrator
from sim_platform.rag.hybrid_retriever import HybridRAGRetriever
from sim_platform.security.quota_manager import TenantQuotaManager
from sim_platform.visualization.chart_generator import InstitutionalChartGenerator


@pytest.fixture(autouse=True)
def _clean_waha_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WAHA_API_URL", raising=False)
    monkeypatch.delenv("WHATSAPP_HOOK_HMAC_KEY", raising=False)
    monkeypatch.delenv("WAHA_WEBHOOK_SECRET", raising=False)


@pytest.fixture(scope="module")
def redteam_client() -> TestClient:
    app = create_app()
    with TestClient(app) as client:
        yield client


class TestRedTeamSecurityAndRBAC:
    def test_path_traversal_and_invalid_ibge_strictly_rejected(self, redteam_client: TestClient, tmp_path: Path) -> None:
        # 1. Path traversal em /api/tenants/select -> HTTP 400
        for bad_tid in ("../secrets", "..\\tenants\\evil", "cidade/invalida", "x", "a" * 80):
            res = redteam_client.post("/api/tenants/select", json={"tenant_id": bad_tid})
            assert res.status_code == 400, f"Esperado 400 para '{bad_tid}', obtido {res.status_code}"

        # 2. Onboarding sem código IBGE ou com IBGE inválido -> HTTP 400 (nunca cai em Sabará 3156700)
        res_missing = redteam_client.post(
            "/api/tenants/onboard",
            json={"municipality_name": "Cidade Teste", "state_uf": "MG"},
        )
        assert res_missing.status_code == 400
        assert "Sabará" not in res_missing.text

        res_bad_ibge = redteam_client.post(
            "/api/tenants/onboard",
            json={"municipality_name": "Cidade Teste", "state_uf": "MG", "ibge_code": "123"},
        )
        assert res_bad_ibge.status_code == 400

        with pytest.raises(ValueError):
            CityOnboardingManager(workspace_root=tmp_path).onboard_city(
                municipality_name="Sem IBGE",
                state_uf="MG",
                ibge_code=None,
            )

    def test_proxy_headers_block_local_bypass_and_admin_token_refreshes_on_switch(
        self, redteam_client: TestClient
    ) -> None:
        try:
            # 1. Cabeçalho X-Forwarded-For bloqueia bypass local anônimo em /api/tenants/select -> HTTP 403
            spoof_res = redteam_client.post(
                "/api/tenants/select",
                headers={"X-Forwarded-For": "203.0.113.42"},
                json={"tenant_id": "lagoa_da_prata_mg_3137205"},
            )
            assert spoof_res.status_code == 403

            # 2. Gabinete parlamentar restrito (vereador01: can_view_all_gabinetes=False) não pode chavear cidade globalmente via proxy externo
            login_ver = redteam_client.post(
                "/api/auth/login",
                json={"username": "vereador01", "password": "admin"},
            ).json()
            ver_tok = login_ver["access_token"]
            ver_switch = redteam_client.post(
                "/api/tenants/select",
                headers={"Authorization": f"Bearer {ver_tok}", "X-Forwarded-For": "203.0.113.42"},
                json={"tenant_id": "lagoa_da_prata_mg_3137205"},
            )
            assert ver_switch.status_code == 403

            # 3. Admin global (can_view_all_gabinetes=True) chaveia via proxy externo e recebe refreshed_token vinculado à nova cidade
            login_adm = redteam_client.post(
                "/api/auth/login",
                json={"username": "admin", "password": "admin"},
            ).json()
            caete_adm_tok = login_adm["access_token"]

            adm_switch = redteam_client.post(
                "/api/tenants/select",
                headers={"Authorization": f"Bearer {caete_adm_tok}", "X-Forwarded-For": "203.0.113.42"},
                json={"tenant_id": "lagoa_da_prata_mg_3137205", "auto_sync_if_empty": False},
            )
            assert adm_switch.status_code == 200
            sw_body = adm_switch.json()
            lp_adm_tok = sw_body.get("refreshed_token")
            assert lp_adm_tok and lp_adm_tok != caete_adm_tok

            # Token antigo de Caeté é estritamente rejeitado enquanto Lagoa da Prata está ativa
            me_old = redteam_client.get(
                "/api/auth/me",
                headers={"Authorization": f"Bearer {caete_adm_tok}"},
            ).json()
            assert me_old["authenticated"] is False

            # Novo refreshed_token de Lagoa da Prata autentica perfeitamente
            me_new = redteam_client.get(
                "/api/auth/me",
                headers={"Authorization": f"Bearer {lp_adm_tok}"},
            ).json()
            assert me_new["authenticated"] is True
            assert me_new["tenant_id"] == "lagoa_da_prata_mg_3137205"
        finally:
            redteam_client.post(
                "/api/tenants/select",
                json={"tenant_id": "caete_mg_3110004", "auto_sync_if_empty": False},
            )

    def test_lgpd_cpf_dossier_block_query_limit_and_waha_media_ssrf_path_guard(
        self, redteam_client: TestClient
    ) -> None:
        # 1. Busca anônima por CPF de 11 dígitos em /api/entity/dossier -> HTTP 403 LGPD
        anon_cpf = redteam_client.get("/api/entity/dossier", params={"q": "123.456.789-01"})
        assert anon_cpf.status_code == 403

        # Admin autenticado pode consultar CPF para auditoria de controle interno -> HTTP 200
        adm_tok = redteam_client.post(
            "/api/auth/login", json={"username": "admin", "password": "admin"}
        ).json()["access_token"]
        auth_cpf = redteam_client.get(
            "/api/entity/dossier",
            params={"q": "123.456.789-01"},
            headers={"Authorization": f"Bearer {adm_tok}"},
        )
        assert auth_cpf.status_code == 200

        # 2. Cidadão anônimo não consegue desativar enforce_limit=False em /query
        q_res = redteam_client.post(
            "/query",
            json={"query": "SELECT * FROM despesa_pagamento", "enforce_limit": False},
        )
        assert q_res.status_code == 200
        q_json = q_res.json()
        assert q_json["status"] == "sucesso"
        assert len(q_json["dados"]) <= 500

        # 3. Webhook WAHA bloqueia media.url fora de /api/files/ ou com '..' (SSRF/LFI)
        before_mon = redteam_client.get("/api/monitoring/overview").json()["security_blocks_count"]
        redteam_client.post(
            "/webhook/waha",
            json={
                "event": "message",
                "session": "default",
                "dry_run": True,
                "payload": {
                    "id": "wamid.redteam.ssrf.1",
                    "from": "5531999998888@c.us",
                    "fromMe": False,
                    "hasMedia": True,
                    "media": {
                        "mimetype": "audio/ogg",
                        "url": "http://waha:3000/api/sessions",
                    },
                    "body": "",
                },
            },
        )
        after_mon = redteam_client.get("/api/monitoring/overview").json()["security_blocks_count"]
        assert after_mon >= before_mon + 1


class TestRedTeamMultiCityIsolationAndSemantics:
    def test_zero_caete_leak_in_non_caete_components_and_monitoring(
        self, redteam_client: TestClient, tmp_path: Path
    ) -> None:
        CityOnboardingManager(workspace_root=tmp_path).onboard_city(
            municipality_name="Lagoa da Prata",
            state_uf="MG",
            ibge_code="3137205",
        )
        pq_dir = tmp_path / "avaliacao" / "dados" / "parquet" / "lagoa_da_prata_mg_3137205"
        raw_dir = tmp_path / "avaliacao" / "dados" / "raw_csv" / "lagoa_da_prata_mg_3137205"
        pq_dir.mkdir(parents=True, exist_ok=True)
        raw_dir.mkdir(parents=True, exist_ok=True)

        t_lp = TenantContext(
            tenant_id="lagoa_da_prata_mg_3137205",
            municipality_name="Lagoa da Prata",
            state_uf="MG",
            ibge_code="3137205",
            data_dir=raw_dir,
            parquet_dir=pq_dir,
            use_parquet=True,
        )
        db_lp = DuckDBManager(tenant=t_lp)
        try:
            orch_lp = SIMOrchestrator(
                db_manager=db_lp,
                rag_retriever=HybridRAGRetriever(tenant_id="lagoa_da_prata_mg_3137205"),
            )
            orch_lp.quota_manager = TenantQuotaManager(
                tenant_id="lagoa_da_prata_mg_3137205",
                db_path=tmp_path / "q_lp.sqlite",
            )

            # Explicação conceitual para cidadão nunca vaza "em Caeté"
            res_concept = orch_lp.handle_question("O que significa empenho e liquidação?")
            assert "Caeté" not in res_concept.citizen_response
            assert "Lagoa da Prata" in res_concept.citizen_response

            # EntityLinker e PromptBuilder sem CNPJ ou hospital de Caeté
            linker_lp = EntityLinker(
                conn=db_lp.conn,
                city_name="Lagoa da Prata",
                tenant_id="lagoa_da_prata_mg_3137205",
            )
            bindings = linker_lp.extract_value_bindings("Quanto foi pago ao hospital e à Santa Casa em 2024?")
            joined_hints = " ".join(bindings)
            assert "Nossa Senhora da Piedade" not in joined_hints
            assert "18.302.299/0001-02" not in joined_hints

            sel = db_lp.catalog.select_tables_for_query("Qual a população de Lagoa da Prata?")
            pkg = orch_lp.prompt_builder.assemble(user_question="Qual a população?", selection=sel, role="auditor")
            assert "Jornal Opinião" not in pkg.system_prompt
            assert "18.302.299/0001-02" not in pkg.system_prompt
            assert "Nossa Senhora da Piedade" not in pkg.system_prompt
        finally:
            db_lp.close()

        # Verifica formatação limpa de contadores em /api/monitoring/overview e /api/institutional/capabilities
        try:
            redteam_client.post(
                "/api/tenants/select",
                json={"tenant_id": "lagoa_da_prata_mg_3137205", "auto_sync_if_empty": False},
            )
            mon = redteam_client.get("/api/monitoring/overview").json()
            caps = redteam_client.get("/api/institutional/capabilities").json()
            assert "{'total':" not in json.dumps(mon, ensure_ascii=False)
            assert "{'total':" not in json.dumps(caps, ensure_ascii=False)
        finally:
            redteam_client.post(
                "/api/tenants/select",
                json={"tenant_id": "caete_mg_3110004", "auto_sync_if_empty": False},
            )

    def test_ambiguous_city_nouns_not_false_blocked(self, redteam_client: TestClient) -> None:
        # Substantivos comuns ("contagem", "luz", "campanha", "passos") NÃO devem ser bloqueados como outros municípios
        for valid_q in (
            "Qual a contagem total de servidores em 2024?",
            "Quanto a prefeitura gastou com conta de luz em 2024?",
            "Qual o gasto com campanha de vacinação?",
        ):
            r = redteam_client.post("/chat", json={"question": valid_q, "session_id": "rt_ambig"}).json()
            assert r["intent"] != "out_of_scope_municipality", f"Falso bloqueio em: {valid_q}"

        # Já menções locativas explícitas a esses municípios DEVEM ser bloqueadas
        for cross_q in (
            "Quanto gastou a prefeitura de Contagem em 2024?",
            "Qual a população em Luz?",
        ):
            r_cross = redteam_client.post("/chat", json={"question": cross_q, "session_id": "rt_ambig2"}).json()
            assert r_cross["intent"] == "out_of_scope_municipality", f"Deveria bloquear: {cross_q}"


class TestRedTeamAccountingZipBombAndCloudDelete:
    def test_sync_manager_never_fabricates_mde_siops_or_copies_2021_pib(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        CityOnboardingManager(workspace_root=tmp_path).onboard_city(
            municipality_name="Lagoa da Prata",
            state_uf="MG",
            ibge_code="3137205",
        )
        sm = DataSyncManager(workspace_root=tmp_path, tenant_id="lagoa_da_prata_mg_3137205")

        class _FakeResp:
            def __init__(self, payload: Any, status_code: int = 200) -> None:
                self._payload = payload
                self.status_code = status_code

            def json(self) -> Any:
                return self._payload

        def _fake_requests_get(url: str, headers: Dict[str, str] | None = None, timeout: float = 10.0, **kwargs: Any) -> _FakeResp:
            if "localidades/municipios/3137205" in url:
                return _FakeResp({"id": 3137205, "nome": "Lagoa da Prata"})
            if "agregados/4714" in url:
                return _FakeResp([{"resultados": [{"series": [{"serie": {"2022": "51412"}}]}]}])
            if "agregados/6579" in url:
                return _FakeResp([{"resultados": [{"series": [{"serie": {"2020": "52000", "2021": "52500", "2024": "53200", "2025": "53600"}}]}]}])
            if "agregados/5938" in url:
                return _FakeResp([{"resultados": [{"series": [{"serie": {"2020": "1800000", "2021": "2100000"}}]}]}])
            if "agregados/1301" in url:
                return _FakeResp([{"resultados": [{"series": [{"serie": {"2010": "439.984"}}]}]}])
            if "/rgf?" in url:
                return _FakeResp(
                    {
                        "items": [
                            {"cod_conta": "ReceitaCorrenteLiquidaAjustada", "coluna": "VALOR", "valor": 250000000.0},
                            {"cod_conta": "DespesaComPessoalLiquida", "coluna": "TOTAL (ÚLTIMOS 12 MESES)", "valor": 110000000.0},
                            {"cod_conta": "DespesaComPessoalLiquida", "coluna": "% sobre a RCL Ajustada", "valor": 44.0},
                        ]
                    }
                )
            if "/rreo?" in url:
                # Sem anexo 08/12 homologado: retorna apenas despesa em reais no Anexo 02, sem % constitucional
                if "RREO-Anexo%2002" in url or "RREO-Anexo 02" in url:
                    return _FakeResp(
                        {
                            "items": [
                                {"conta": "Saúde", "coluna": "DESPESAS LIQUIDADAS ATÉ O BIMESTRE", "valor": 62000000.0},
                                {"conta": "Educação", "coluna": "DESPESAS LIQUIDADAS ATÉ O BIMESTRE", "valor": 58000000.0},
                            ]
                        }
                    )
                return _FakeResp({"items": []})
            if "api-de-dados" in url:
                return _FakeResp([])
            return _FakeResp({})

        import requests

        monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
        monkeypatch.setattr(requests, "get", _fake_requests_get)
        monkeypatch.setattr("time.sleep", lambda _: None)
        res = sm.sync_all(live_api=True)
        assert res["active_release_id"] is not None
        assert res["active_release_id"] != "base_v1"

        t_lp = TenantContext(
            tenant_id="lagoa_da_prata_mg_3137205",
            municipality_name="Lagoa da Prata",
            state_uf="MG",
            ibge_code="3137205",
            data_dir=tmp_path / "avaliacao" / "dados" / "raw_csv" / "lagoa_da_prata_mg_3137205",
            parquet_dir=sm.parquet_dir,
            use_parquet=True,
        )
        db_lp = DuckDBManager(tenant=t_lp)
        try:
            # 1. MDE e SIOPS % são estritamente NULL quando não homologados no RREO Anexo 08/12 (jamais RCL * 0.78!)
            q_fin = db_lp.execute_safe_query(
                "SELECT ano_referencia, vlr_receita_corrente_liquida, pct_aplicacao_saude_siops, pct_aplicacao_educacao_mde "
                "FROM siconfi_finbra_indicadores WHERE ano_referencia = 2024"
            )
            row_2024 = q_fin["dados"][0]
            assert float(row_2024["vlr_receita_corrente_liquida"]) == 250000000.0
            assert row_2024["pct_aplicacao_saude_siops"] is None
            assert row_2024["pct_aplicacao_educacao_mde"] is None

            # 2. PIB de 2021 preservado em 2021 e NULL em 2024/2025; IDHM é NULL (nunca 0.000) e 2023 tem população!
            q_ibge = db_lp.execute_safe_query(
                "SELECT ano_referencia, populacao_residente, pib_municipal_mil_reais, pib_per_capita_reais, idhm_geral "
                "FROM ibge_indicadores_municipais ORDER BY ano_referencia"
            )
            by_yr = {int(r["ano_referencia"]): r for r in q_ibge["dados"]}
            assert 2023 in by_yr and int(by_yr[2023]["populacao_residente"]) == 51412
            assert float(by_yr[2021]["pib_municipal_mil_reais"]) == 2100000.0
            assert by_yr[2024]["pib_municipal_mil_reais"] is None
            assert by_yr[2024]["pib_per_capita_reais"] is None
            assert by_yr[2024]["idhm_geral"] is None
        finally:
            db_lp.close()

    def test_streaming_anti_zip_bomb_and_strict_two_phase_cloud_delete(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import sim_platform.ingestion.sync_manager as sm_mod

        CityOnboardingManager(workspace_root=tmp_path).onboard_city(
            municipality_name="Lagoa da Prata",
            state_uf="MG",
            ibge_code="3137205",
        )
        sm = DataSyncManager(workspace_root=tmp_path, tenant_id="lagoa_da_prata_mg_3137205")

        # 1. Streaming anti-Zip-Bomb corta descompressão em tempo de leitura mesmo em ZIP altamente comprimido
        monkeypatch.setattr(sm_mod, "MAX_ZIP_UNCOMPRESSED_BYTES", 4096)
        bomb_buf = io.BytesIO()
        with zipfile.ZipFile(bomb_buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("tabela.csv", "col1;col2\n" + ("0;0\n" * 5000))

        with pytest.raises(ValueError, match="anti-Zip-Bomb"):
            sm.ingest_uploaded_package(filename="bomb.zip", content_bytes=bomb_buf.getvalue())

        # 2. Two-Phase Delete no FleetVerifier NUNCA apaga arquivo local se size remoto for 0 ou ausente
        (tmp_path / "tenants" / "_cloud_datalake_manifest.json").write_text(
            json.dumps(
                {
                    "provider": "google_drive",
                    "tenants": {
                        "lagoa_da_prata_mg_3137205": {
                            "releases_folder_id": "folder_rel_123",
                            "raw_packages_folder_id": "folder_raw_123",
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        outbox_file = (
            tmp_path
            / "avaliacao"
            / "dados"
            / "cloud_outbox"
            / "lagoa_da_prata_mg_3137205"
            / "releases"
            / "bundle_test.zip"
        )
        outbox_file.parent.mkdir(parents=True, exist_ok=True)
        payload_bytes = b"PK_FAKE_PARQUET_BUNDLE_1234567890"
        outbox_file.write_bytes(payload_bytes)

        fv = FleetVerifier(workspace_root=tmp_path)

        class _FakeProc:
            def __init__(self, returncode: int, stdout: str) -> None:
                self.returncode = returncode
                self.stdout = stdout
                self.stderr = ""

        # Caso A: gdrive readonly info retorna size=0 (ou omitido) -> NÃO pode deletar outbox_file!
        def _run_zero_size(cmd: list[str], **kwargs: Any) -> _FakeProc:
            if "upload" in cmd:
                return _FakeProc(0, json.dumps({"id": "drv_file_999", "name": "bundle_test.zip"}))
            return _FakeProc(0, json.dumps({"id": "drv_file_999", "size": 0}))

        monkeypatch.setattr("shutil.which", lambda _: "gdrive")
        monkeypatch.setattr("subprocess.run", _run_zero_size)
        res_bad = fv.push_outbox_to_gdrive()
        assert len(res_bad["uploaded"]) == 0
        assert outbox_file.exists(), "Two-Phase Delete jamais pode remover arquivo local quando size remoto != local_size!"

        # Caso B: gdrive readonly info confirma id exato e size == len(payload_bytes) -> deleta com segurança!
        def _run_exact_size(cmd: list[str], **kwargs: Any) -> _FakeProc:
            if "upload" in cmd:
                return _FakeProc(0, json.dumps({"id": "drv_file_999", "name": "bundle_test.zip"}))
            return _FakeProc(0, json.dumps({"id": "drv_file_999", "size": len(payload_bytes)}))

        monkeypatch.setattr("subprocess.run", _run_exact_size)
        res_ok = fv.push_outbox_to_gdrive()
        assert len(res_ok["uploaded"]) == 1
        assert not outbox_file.exists()

    def test_multicity_chart_generator_multiyear_series_and_question_metric(self) -> None:
        gen = InstitutionalChartGenerator(
            municipality_name="Lagoa da Prata",
            state_uf="MG",
            max_partial_year=None,
        )
        ibge_rows = [
            {
                "ano_referencia": 2022,
                "cod_ibge": "3137205",
                "nom_municipio": "Lagoa da Prata",
                "uf": "MG",
                "populacao_residente": 51412,
                "pib_per_capita_reais": None,
            },
            {
                "ano_referencia": 2023,
                "cod_ibge": "3137205",
                "nom_municipio": "Lagoa da Prata",
                "uf": "MG",
                "populacao_residente": 51412,
                "pib_per_capita_reais": None,
            },
            {
                "ano_referencia": 2024,
                "cod_ibge": "3137205",
                "nom_municipio": "Lagoa da Prata",
                "uf": "MG",
                "populacao_residente": 53202,
                "pib_per_capita_reais": None,
            },
            {
                "ano_referencia": 2025,
                "cod_ibge": "3137205",
                "nom_municipio": "Lagoa da Prata",
                "uf": "MG",
                "populacao_residente": 53615,
                "pib_per_capita_reais": None,
            },
        ]
        spec = gen.extract_chart_series(
            ibge_rows,
            question="Qual a evolução da população de Lagoa da Prata por ano?",
        )
        assert spec is not None
        assert spec["labels"] == ["2022", "2023", "2024", "2025"]
        assert spec["series"][0]["values"] == [51412.0, 51412.0, 53202.0, 53615.0]

        art = gen.generate_chart(
            rows=ibge_rows,
            question="Qual a evolução da população de Lagoa da Prata por ano?",
        )
        assert art is not None
        assert art.filename == "grafico_sim_lagoa_da_prata.png"
        assert "Caeté" not in art.caption
        assert "parcial" not in art.caption.lower()


class TestV45AuditRemediationAndMGScale:
    def test_rbac_sqlite_pbkdf2_and_no_private_ip_admin_bypass(
        self, redteam_client: TestClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import sqlite3
        from sim_platform.security.auth_rbac import InstitutionalAuthManager

        db_file = tmp_path / "rbac_test.sqlite"
        monkeypatch.setenv("SIM_ADMIN_USER", "admin")
        monkeypatch.setenv("SIM_ADMIN_PASSWORD", "sim2026_segura")

        mgr = InstitutionalAuthManager(
            tenant_id="caete_mg_3110004",
            db_path=db_file,
        )
        assert db_file.exists() and (tmp_path / ".jwt_secret").exists()

        # Garantir no SQLite que a senha nunca é persistida em texto claro
        with sqlite3.connect(str(db_file)) as con:
            row = con.execute(
                "SELECT password_hash, password_salt, role, allowed_tenants_json FROM rbac_users WHERE username = 'admin'"
            ).fetchone()
            assert row is not None
            assert row[0] != "sim2026_segura" and len(row[0]) == 64
            assert len(row[1]) == 32
            assert row[2] == "admin"
            assert json.loads(row[3]) == ["*"]

        assert mgr.verify_credentials("admin", "senha_errada") is None
        u_ok = mgr.verify_credentials("admin", "sim2026_segura")
        assert u_ok is not None and u_ok["role"] == "admin"

        # Simular requisição originada da bridge Docker privada (172.18.0.5) em modo não-dev:
        # JAMAIS pode ganhar admin sem Bearer token!
        monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
        monkeypatch.setenv("SIM_ENV", "production")
        res_bridge = redteam_client.post(
            "/api/tenants/select",
            json={"tenant_id": "caete_mg_3110004"},
            headers={"X-Forwarded-For": "172.18.0.5"},
        )
        assert res_bridge.status_code in (401, 403)

    def test_whatsapp_city_command_router_and_webhook_override(self, tmp_path: Path) -> None:
        from sim_platform.messaging.whatsapp_router import WhatsAppTenantRouter

        router = WhatsAppTenantRouter(db_path=tmp_path / "wa_routes.sqlite")

        # 1. Consulta avulsa com prefixo /lagoa redireciona imediatamente
        d1 = router.route_message(
            chat_id="553799991111@c.us",
            message_text="/lagoa qual a receita de 2026?",
            default_tenant_id="caete_mg_3110004",
            persist_inline=False,
        )
        assert d1.resolved_tenant_id == "lagoa_da_prata_mg_3137205"
        assert d1.stripped_question == "qual a receita de 2026?"
        assert d1.command_only_reply is None

        # 2. Comando puro /lagoa fixa a preferência daquele chat_id
        d2 = router.route_message(
            chat_id="553799991111@c.us",
            message_text="/lagoa",
            default_tenant_id="caete_mg_3110004",
        )
        assert d2.resolved_tenant_id == "lagoa_da_prata_mg_3137205"
        assert d2.command_only_reply is not None and "Lagoa da Prata" in d2.command_only_reply

        # Mensagem subsequente sem barra usa a cidade fixada do chat_id
        d3 = router.route_message(
            chat_id="553799991111@c.us",
            message_text="qual o gasto com saúde?",
            default_tenant_id="caete_mg_3110004",
        )
        assert d3.resolved_tenant_id == "lagoa_da_prata_mg_3137205"
        assert d3.stripped_question == "qual o gasto com saúde?"

        # /padrao limpa a fixação e volta a seguir o Console
        d4 = router.route_message(
            chat_id="553799991111@c.us",
            message_text="/padrao",
            default_tenant_id="caete_mg_3110004",
        )
        assert d4.command_only_reply is not None
        d5 = router.route_message(
            chat_id="553799991111@c.us",
            message_text="qual o gasto com saúde?",
            default_tenant_id="caete_mg_3110004",
        )
        assert d5.resolved_tenant_id == "caete_mg_3110004"

    def test_mg_calendar_scheduler_4_windows_and_year_visual_protection(self) -> None:
        from datetime import date
        from sim_platform.ingestion.connectors.calendar_scheduler import MGCalendarScheduler
        from sim_platform.visualization.chart_generator import _smart_shorten_gov_label
        from sim_platform.visualization.table_renderer import InstitutionalTableRenderer

        sched = MGCalendarScheduler()
        assert len(sched.windows) == 4
        assert [w.window_id for w in sched.get_due_windows(date(2026, 10, 2))] == ["lote_fiscal_siconfi_siops"]
        assert sched.get_due_windows(date(2026, 11, 2)) == []  # Mês ímpar não dispara RREO bimestral
        assert [w.window_id for w in sched.get_due_windows(date(2026, 11, 3))] == ["portais_abertos_pncp_diarios"]
        assert [w.window_id for w in sched.get_due_windows(date(2026, 11, 10))] == ["lote_assistido_sicom_tcemg"]
        assert [w.window_id for w in sched.get_due_windows(date(2026, 11, 18))] == ["lote_federal_cgu"]

        short_lbl = _smart_shorten_gov_label("SECRETARIA MUNICIPAL DE OBRAS E INFRAESTRUTURA URBANA", max_len=28)
        assert "SEC. MUN." in short_lbl and len(short_lbl) <= 28

        tr = InstitutionalTableRenderer(municipality_name="Lagoa da Prata", state_uf="MG")
        art = tr.render(
            [
                {"exercicio": 2025, "vlr_pago": 100.0},
                {"exercicio": 2026, "vlr_pago": 250.0},
            ],
            title="Execução por Exercício",
            truncated_by_security_limit=False,
        )
        assert art is not None
        assert ">2026<" in art.html
        assert "2.026" not in art.html
        assert "exercicio" not in art.totals_by_column
        assert "vlr_pago" in art.totals_by_column

