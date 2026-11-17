"""
Suíte de Testes Unitários e Invariantes Multi-ERP / 5 Cidades-Canário (SIM v4.7).

Valida:
1. `PortalDetector`: fingerprinting determinístico das 5 famílias de portais de MG + fallback anti-CAPTCHA.
2. `MemoryPortalConnector` (M3): multi-entidade (Prefeitura + Câmara), máscara LGPD e conciliação RREO.
3. `PortalFacilElConnector` (M4): expurgo de contas sintéticas PCASP, separação de Restos a Pagar e máscara CPF.
4. `CkanPortalConnector` (M5): unpivot de matriz mensal (`escrituracao='S'`), unpivot de frota e filtro de Agente Político.
5. `WhatsAppRouter` + `SemanticIntentGuardrail`: resolução automática de `/bh`, `/gv`, `/valadares`, `/matozinhos` e apelidos locais.
"""

from __future__ import annotations

from pathlib import Path

from sim_platform.ingestion.connectors import (
    CkanPortalConnector,
    MemoryPortalConnector,
    PortalDetector,
    PortalFacilElConnector,
)
from sim_platform.messaging.whatsapp_router import WhatsAppTenantRouter
from sim_platform.security.semantic_guardrail import SemanticIntentGuardrail


REPO_ROOT = Path(__file__).resolve().parents[1]


class TestPortalDetectorV47:
    def test_detects_all_five_mg_portal_families(self) -> None:
        fp_m1 = PortalDetector.classify_sample(
            url="https://caete.cidade360.com.br/transparencia",
            html_or_json="<title>Portal Cidade360</title>",
        )
        assert fp_m1.connector_id == "cidade360"
        assert not fp_m1.captcha_detected

        fp_m2 = PortalDetector.classify_sample(
            url="https://transparencia.betha.cloud/#/u123",
            html_or_json='<a href="https://www.diariomunicipal.com.br/amm-mg/">Diário AMM-MG</a>',
        )
        assert fp_m2.connector_id == "betha_cloud"
        assert fp_m2.gazette_family == "amm_mg"

        fp_m3 = PortalDetector.classify_sample(
            url="https://matozinhos.mg.gov.br",
            html_or_json=(
                '<a href="https://ilai.memory.com.br/#/entidades/login/9BHSQH/1">Transparência</a>'
                '<a href="https://imprensaoficialmunicipal.com.br/matozinhos">DiOE</a>'
            ),
        )
        assert fp_m3.connector_id == "memory_portal"
        assert fp_m3.detected_endpoints["memory_tenant_code"] == "9BHSQH"
        assert fp_m3.gazette_family == "dosp_imprensa_oficial"

        fp_m4 = PortalDetector.classify_sample(
            url="https://transparencia.valadares.mg.gov.br/dados-abertos",
            html_or_json='<a href="https://dadosabertos-portalfacil.azurewebsites.net/94">API</a>',
        )
        assert fp_m4.connector_id == "portalfacil_el"
        assert fp_m4.detected_endpoints["portalfacil_client_id"] == "94"

        fp_m5 = PortalDetector.classify_sample(
            url="https://dados.pbh.gov.br/api/3/action/package_list",
            html_or_json='<meta name="generator" content="ckan 2.9" /> https://dom-web.pbh.gov.br',
        )
        assert fp_m5.connector_id == "ckan_portal"
        assert fp_m5.gazette_family == "dom_proprio_api"

    def test_never_bypasses_captcha_routes_to_assisted_sicom(self) -> None:
        fp_cap = PortalDetector.classify_sample(
            url="https://diario.exemplo.mg.gov.br",
            html_or_json='<img src="/BotDetectCaptcha.ashx?get=image" /><div class="g-recaptcha"></div>',
        )
        assert fp_cap.captcha_detected is True
        assert fp_cap.connector_id == "sicom_tcemg"
        assert fp_cap.sync_mode == "HYBRID_ASSISTED_ZIP"


class TestMultiErpConnectorsV47:
    def test_portalfacil_prunes_synthetic_pcasp_and_masks_individual_cpf(self) -> None:
        conn = PortalFacilElConnector(tenant_id="governador_valadares_mg_3127701", workspace_root=REPO_ROOT)
        rec_rows, dropped = conn.map_receitas(
            [
                {"numExercicio": "2025", "numReceita": "1.0.0.0.00.00.00", "descReceita": "RECEITAS CORRENTES", "vlPrevisto": "1000.00", "vlRealizado": "1100.00"},
                {"numExercicio": "2025", "numReceita": "1.1.1.2.50.01.00", "descReceita": "IPTU - PRINCIPAL", "vlPrevisto": "600.00", "vlRealizado": "650.00"},
                {"numExercicio": "2025", "numReceita": "1.7.1.1.51.11.00", "descReceita": "FPM - COTA MENSAL", "vlPrevisto": "400.00", "vlRealizado": "450.00"},
            ]
        )
        assert dropped == 1
        assert len(rec_rows) == 2
        assert round(sum(r["vlr_realizadoateperiodo"] for r in rec_rows), 2) == 1100.00

        # Garantir que IPTU (1.1.1.2.50.00.00 -> prefixo '1.1.1.2.50') NÃO colida com ITBI ('1.1.1.2.53.01.00')!
        rec_iptu_itbi, dropped_iptu = conn.map_receitas(
            [
                {"numExercicio": "2025", "numReceita": "1.1.1.2.50.00.00", "descReceita": "IPTU CONSOLIDADO", "vlPrevisto": "500.00", "vlRealizado": "520.00"},
                {"numExercicio": "2025", "numReceita": "1.1.1.2.53.01.00", "descReceita": "ITBI PRINCIPAL", "vlPrevisto": "200.00", "vlRealizado": "210.00"},
            ]
        )
        assert dropped_iptu == 0
        assert len(rec_iptu_itbi) == 2

        _, _, _, pag_rows, _ = conn.map_empenhos_pagamentos(
            raw_empenhos=[],
            raw_liquidacoes=[],
            raw_pagamentos=[
                {
                    "ano_consulta": 2026,
                    "numExercicio": "2018",
                    "numPagamento": "99",
                    "numEmpenho": "450",
                    "dtPagamento": "15/03/2026",
                    "vlPago": "1500.00",
                    "numCpfCnpj": "12345678901",
                    "descFornecedor": "JOSE DA SILVA",
                    "descDespesa": "AUXILIO FINANCEIRO PESSOA FISICA CPF 123.456.789-01",
                }
            ],
        )
        assert len(pag_rows) == 1
        assert pag_rows[0]["ano_referencia"] == 2026
        assert pag_rows[0]["dsc_tipo_pagamento"] == "RESTOS A PAGAR"
        assert pag_rows[0]["num_doc_credor"] == "***.456.789-**"
        assert "123.456.789-01" not in pag_rows[0]["dsc_pagamento"]

        # Testar decodificador UTF-16LE real do PortalFácil/E&L + chaves ALL-CAPS
        utf16_bytes = "NUMMATRICULA;DESCSERVIDOR\n001;MARIA\n".encode("utf-16")
        assert "DESCSERVIDOR" in PortalFacilElConnector.decode_portal_csv_bytes(utf16_bytes)
        rem_gv, _, _ = conn.map_servidores(
            [
                {
                    "NUMMATRICULA": "0001",
                    "DESCSERVIDOR": "PREFEITO EXEMPLO GV",
                    "DESCUNIDADE": "GABINETE DO PREFEITO",
                    "DESCCARGO": "PREFEITO MUNICIPAL",
                    "DESCVINCULO": "AGENTE POLITICO",
                    "DESCTPFOLHA": "FOLHA DO MÊS",
                    "DTCOMPETENCIA": "10/2026",
                    "VLPROVENTO": "29500.00",
                    "VLDESCONTO": "6200.00",
                    "VLLIQUIDO": "23300.00",
                }
            ]
        )
        assert len(rem_gv) == 1
        assert rem_gv[0]["dsc_situacao"] == "Agente Político"
        assert rem_gv[0]["dsc_tipo_pag"] == "Mensal"

    def test_ckan_unpivots_analytical_revenues_and_fleet_matrix(self) -> None:
        conn = CkanPortalConnector(tenant_id="belo_horizonte_mg_3106200", workspace_root=REPO_ROOT)
        csv_rec = (
            "receita;nome_receita;escrituracao;Janeiro;Fevereiro;Maro\n"
            "1.0.0.0.00.0.0;RECEITAS CORRENTES;N;300.000,00;400.000,00; - \n"
            "1.1.1.2.50.0.1;IPTU PRINCIPAL;S;100.000,00;150.000,00; - \n"
            "1.7.1.1.51.1.1;COTA PARTE FPM;S;200.000,00;250.000,00; - \n"
        )
        rows, skipped = conn.unpivot_ckan_monthly_receitas_csv(csv_rec, ano_referencia=2026)
        assert skipped == 1
        assert len(rows) == 4
        assert round(sum(r["vlr_realizadoateperiodo"] for r in rows), 2) == 700000.00

        csv_frota = (
            "Setor Tipo de Veiculo;CAMINHAO PIPA;UTILITARIO\n"
            "SLU LIMPEZA URBANA;2;3\n"
        )
        vehicles = conn.unpivot_ckan_fleet_matrix_csv(csv_frota, ano_referencia=2026)
        assert len(vehicles) == 5

    def test_memory_multi_entity_legislativo_vs_executivo(self) -> None:
        conn = MemoryPortalConnector(tenant_id="matozinhos_mg_3141108", workspace_root=REPO_ROOT)
        rem_rows, pes_rows, _ = conn.map_servidores(
            [
                {
                    "nome_servidor": "JOAO VEREADOR EXEMPLO",
                    "numero_matricula": 101,
                    "cod_orgao": "01",
                    "situacao_admissional": "AGENTE POLITICO",
                    "situacao_atual": "ATIVO",
                    "funcao": "VEREADOR",
                    "proventos": "8500.00000000",
                    "ano_referencia": 2025,
                },
                {
                    "nome_servidor": "MARIA PROFESSORA EXEMPLO",
                    "numero_matricula": 202,
                    "cod_orgao": "02",
                    "situacao_admissional": "EFETIVO",
                    "situacao_atual": "ATIVO",
                    "funcao": "PEB II - MATEMATICA",
                    "departamento_lotacao": "GABINETE DO PREFEITO",
                    "proventos": "5400.50000000",
                    "ano_referencia": 2025,
                },
            ]
        )
        assert len(rem_rows) == 2
        assert len(pes_rows) == 2
        by_mat = {r["num_matricula"]: r for r in rem_rows}
        assert by_mat["101"]["dsc_situacao"] == "Agente Político"
        assert by_mat["101"]["cod_orgao"] == 1
        # Lotação "GABINETE DO PREFEITO" jamais contamina professora efetiva como Agente Político!
        assert by_mat["202"]["dsc_situacao"] != "Agente Político"
        assert by_mat["202"]["cod_orgao"] == 2

    def test_whatsapp_and_guardrail_five_canary_nicknames(self, tmp_path: Path) -> None:
        router = WhatsAppTenantRouter(
            workspace_root=REPO_ROOT,
            db_path=tmp_path / "wpp_test.sqlite",
        )
        for cmd, expected_tid in [
            ("/caete quanto entrou de IPTU?", "caete_mg_3110004"),
            ("/lagoa qual a receita?", "lagoa_da_prata_mg_3137205"),
            ("/matozinhos folha de servidores", "matozinhos_mg_3141108"),
            ("/valadares gastos com saude", "governador_valadares_mg_3127701"),
            ("/gv maiores fornecedores", "governador_valadares_mg_3127701"),
            ("/bh orcamento de 2026", "belo_horizonte_mg_3106200"),
            ("/belohorizonte contratos vigentes", "belo_horizonte_mg_3106200"),
        ]:
            tid, q, ack = router.route_message(
                "5531999990000@c.us",
                cmd,
                default_tenant_id="caete_mg_3110004",
            )
            assert ack is None
            assert tid == expected_tid
            assert len(q) >= 10

        sg_gv = SemanticIntentGuardrail(
            municipality_name="Governador Valadares",
            state_uf="MG",
            ibge_code="3127701",
        )
        assert sg_gv._mentions_real_other_municipality("quanto gastou com saude em valadares?") is False
        assert sg_gv._mentions_real_other_municipality("obras no distrito de Baguari em GV") is False

        sg_bh = SemanticIntentGuardrail(
            municipality_name="Belo Horizonte",
            state_uf="MG",
            ibge_code="3106200",
        )
        assert sg_bh._mentions_real_other_municipality("gastos com educacao em bh na Pampulha e Venda Nova") is False
        # Red Team #11: Colisão de prefixo entre cidades irmãs de MG NÃO pode vazar!
        assert sg_bh._mentions_real_other_municipality("qual a receita de Belo Vale em 2025?") is True

        sg_lp = SemanticIntentGuardrail(
            municipality_name="Lagoa da Prata",
            state_uf="MG",
            ibge_code="3137205",
        )
        assert sg_lp._mentions_real_other_municipality("quanto gastou com saude em Lagoa Santa?") is True

    def test_redteam_sqlguard_ssrf_and_visual_ux_regressions(self) -> None:
        import pytest
        from sim_platform.ingestion.connectors.portal_detector import _assert_safe_public_http_url
        from sim_platform.orchestrator.router import _fix_brl_thousands
        from sim_platform.security.sql_guard import SQLGuard
        from sim_platform.visualization.chart_generator import InstitutionalChartGenerator
        from sim_platform.visualization.table_renderer import InstitutionalTableRenderer

        # 1. Anti-SSRF em PortalDetector
        for bad_url in (
            "http://127.0.0.1:8000/admin",
            "http://169.254.169.254/latest/meta-data/",
            "file:///etc/passwd",
            "http://localhost:3000/api",
        ):
            with pytest.raises(ValueError):
                _assert_safe_public_http_url(bad_url)

        # 2. SQLGuard: bloqueio de CTE posicional, fatiamento por colchetes e busca binária (>, BETWEEN) sobre CPF
        guard = SQLGuard(allowed_tables={"vw_gold_folha_servidores"})
        for evasion_sql in (
            "WITH c(x) AS (SELECT num_cpf FROM vw_gold_folha_servidores) SELECT SUBSTR(x, 1, 3) FROM c",
            "SELECT num_cpf[1:3] AS p FROM vw_gold_folha_servidores",
            "SELECT COUNT(*) FROM vw_gold_folha_servidores WHERE num_cpf > '123'",
            "SELECT COUNT(*) FROM vw_gold_folha_servidores WHERE num_cpf BETWEEN '100' AND '200'",
        ):
            res = guard.validate_and_rewrite(evasion_sql, role="citizen")
            assert not res.is_valid
            assert res.error_code == "PII_MASKING_EVASION_BLOCKED"

        # 3. Formatação de bilhões em R$ e gráficos mensais 1D com sufixo `_mes`
        assert _fix_brl_thousands("O valor foi R$ 1608.699.263,03.") == "O valor foi R$ 1.608.699.263,03."

        cg = InstitutionalChartGenerator(municipality_name="Belo Horizonte", state_uf="MG")
        monthly_rows = [
            {"num_mesexercicio": 1, "arrecadacao_total_mes": 1200000.0},
            {"num_mesexercicio": 2, "arrecadacao_total_mes": 1350000.0},
            {"num_mesexercicio": 3, "arrecadacao_total_mes": 1480000.0},
            {"num_mesexercicio": 12, "arrecadacao_total_mes": 0.0},
        ]
        spec = cg.extract_chart_series(monthly_rows, question="gere um grafico da evolucao mensal da arrecadacao")
        assert spec is not None
        assert spec["labels"] == ["Jan", "Fev", "Mar"]

        tr = InstitutionalTableRenderer(municipality_name="Belo Horizonte", state_uf="MG")
        tbl = tr.render(
            [{"fornecedor": "CONSORCIO EXEMPLO", "vlr_contrato": 1608699263.03}],
            title="Maiores Contratos",
        )
        assert tbl is not None
        assert "1.608.699.263,03" in (getattr(tbl, "html", None) or getattr(tbl, "table_html", ""))

        # 4. CkanPortalConnector: JAMAIS fabrica pagamentos sintéticos a partir de contratos PNCP
        ckan = CkanPortalConnector(tenant_id="belo_horizonte_mg_3106200", workspace_root=REPO_ROOT)
        out = ckan.transform_raw_payload(
            {
                "pncp_contratos": [
                    {
                        "numeroContratoEmpenho": "001/2026",
                        "anoContrato": 2026,
                        "dataAssinatura": "2026-02-10",
                        "valorGlobal": 5000000.0,
                        "niFornecedor": "12345678000199",
                        "nomeRazaoSocialFornecedor": "EMPRESA TESTE LTDA",
                        "objetoContrato": "SERVICOS DE ENGENHARIA PLURIANUAL",
                    }
                ]
            }
        )
        assert "contrato_contratos" in out.tables_normalized
        assert "despesa_pagamento" not in out.tables_normalized
        assert "empenho_empenho" not in out.tables_normalized

