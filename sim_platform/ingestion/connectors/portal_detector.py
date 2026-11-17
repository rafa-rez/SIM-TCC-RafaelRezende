"""
Auto-Detector de Família de Portal da Transparência, Dados Abertos e Diário Oficial (`PortalDetector`).

Permite identificar programaticamente o molde tecnológico de qualquer prefeitura de Minas Gerais
a partir de URLs institucionais, cabeçalhos HTTP ou amostras de HTML/JSON, sem jamais tentar resolver
ou contornar CAPTCHAs ou barreiras anti-bot (quando detectadas, classifica como `sicom_tcemg` em
modo `HYBRID_ASSISTED_ZIP` com `captcha_detected=True`).
"""

from __future__ import annotations

import ipaddress
import json
import re
import socket
import ssl
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse


def _assert_safe_public_http_url(raw_url: str) -> str:
    """Valida esquema HTTP/HTTPS público e bloqueia IPs privados, loopback, link-local ou cloud metadata (anti-SSRF)."""
    parsed = urlparse((raw_url or "").strip())
    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError(f"URL inválida ou esquema não permitido: {raw_url!r}")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    infos = socket.getaddrinfo(parsed.hostname, port, proto=socket.IPPROTO_TCP)
    if not infos:
        raise ValueError(f"Não foi possível resolver DNS para {parsed.hostname!r}")
    for _, _, _, _, sockaddr in infos:
        ip_obj = ipaddress.ip_address(sockaddr[0])
        if (
            ip_obj.is_private
            or ip_obj.is_loopback
            or ip_obj.is_link_local
            or ip_obj.is_multicast
            or ip_obj.is_reserved
            or ip_obj.is_unspecified
        ):
            raise ValueError(f"Destino bloqueado por proteção anti-SSRF ({ip_obj}).")
    return parsed.geturl()


class _SafePublicRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> Any:
        _assert_safe_public_http_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


CAPTCHA_GATE_MARKERS: Tuple[str, ...] = (
    "botdetectcaptcha",
    "cf-turnstile",
    "challenges.cloudflare.com/turnstile",
    "g-recaptcha",
    "hcaptcha.com",
    "h-captcha",
    "altcha-widget",
    "verify you are human",
)


@dataclass(frozen=True)
class PortalFingerprint:
    """Resultado auditável da detecção automática do modelo de portal e Diário Oficial."""

    erp_family: str
    connector_id: str
    gazette_family: str
    sync_mode: str
    confidence: float
    captcha_detected: bool = False
    detected_endpoints: Dict[str, str] = field(default_factory=dict)
    matched_signals: Tuple[str, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "erp_family": self.erp_family,
            "connector_id": self.connector_id,
            "gazette_family": self.gazette_family,
            "sync_mode": self.sync_mode,
            "confidence": round(float(self.confidence), 3),
            "captcha_detected": bool(self.captcha_detected),
            "detected_endpoints": dict(self.detected_endpoints),
            "matched_signals": list(self.matched_signals),
        }


class PortalDetector:
    """
    Classifica portais municipais nas 5 famílias canônicas da frota SIM em Minas Gerais:
    - M1: `cidade360` (`Cidade360Connector` + `SicomTceMgConnector`)
    - M2: `betha_cloud` (`BethaCloudConnector`)
    - M3: `memory_portal` (`MemoryPortalConnector` — `ilai.memory.com.br` / `openknowledge`)
    - M4: `portalfacil_el` (`PortalFacilElConnector` — Actcon/PortalFácil + E&L / SMARapd / IPM / Fiorilli)
    - M5: `ckan_portal` (`CkanPortalConnector` — CKAN v3 `api/3/action` + DOM Eletrônico Próprio)
    """

    @staticmethod
    def has_captcha_gate(html_or_text: str) -> bool:
        low = (html_or_text or "").lower()
        return any(marker in low for marker in CAPTCHA_GATE_MARKERS)

    @classmethod
    def detect_gazette_family(cls, url: str = "", html_or_text: str = "") -> Tuple[str, Optional[str]]:
        combined = f"{url}\n{html_or_text}"
        low = combined.lower()

        if "api-dom.pbh.gov.br" in low or "dom-web.pbh.gov.br" in low:
            return "dom_proprio_api", "https://api-dom.pbh.gov.br/api"
        if "imprensaoficialmunicipal.com.br" in low or "dosp.com.br" in low:
            m = re.search(r"https?://(?:www\.)?imprensaoficialmunicipal\.com\.br/[a-z0-9_-]+", combined, re.I)
            return "dosp_imprensa_oficial", (m.group(0) if m else None)
        if "diariomunicipal.com.br/amm-mg" in low or "amm-mg" in low:
            return "amm_mg", "https://www.diariomunicipal.com.br/amm-mg/"
        if "diario-eletronico" in low or "diario-publicacoes" in low or "e_atos.php" in low:
            return "dom_pdf_eletronico", None
        return "pdf_diario_eletronico", None

    @classmethod
    def classify_sample(
        cls,
        url: str = "",
        html_or_json: str = "",
        headers: Optional[Dict[str, str]] = None,
    ) -> PortalFingerprint:
        """Classifica deterministicamente uma amostra já coletada (offline-safe e testável em unit tests)."""
        hdr_txt = " ".join(f"{k}:{v}" for k, v in (headers or {}).items())
        blob = f"{url}\n{hdr_txt}\n{html_or_json}"
        low = blob.lower()

        gaz_family, gaz_url = cls.detect_gazette_family(url=url, html_or_text=html_or_json)
        endpoints: Dict[str, str] = {}
        if gaz_url:
            endpoints["gazette_url"] = gaz_url

        if cls.has_captcha_gate(blob):
            return PortalFingerprint(
                erp_family="sicom_tcemg",
                connector_id="sicom_tcemg",
                gazette_family=gaz_family,
                sync_mode="HYBRID_ASSISTED_ZIP",
                confidence=0.95,
                captcha_detected=True,
                detected_endpoints=endpoints,
                matched_signals=("captcha_gate_detected_fallback_to_assisted_sicom",),
            )

        # M5: CKAN v3 Open Data Portal (ex.: Belo Horizonte `dados.pbh.gov.br`, Ouro Preto `dados.ouropreto.mg.gov.br`)
        if (
            "/api/3/action/" in low
            or 'content="ckan' in low
            or "ckan.pbh.gov.br" in low
            or "dados.pbh.gov.br" in low
            or "dados.ouropreto.mg.gov.br" in low
        ):
            signals: List[str] = ["ckan_v3_signature"]
            m_ckan = re.search(r"(https?://[a-z0-9.-]+(?:\.gov\.br|\.org\.br))(?:/api/3/action|\b)", blob, re.I)
            if m_ckan:
                endpoints["ckan_base_url"] = m_ckan.group(1).rstrip("/")
            return PortalFingerprint(
                erp_family="ckan_portal",
                connector_id="ckan_portal",
                gazette_family=gaz_family,
                sync_mode="AUTO_REST_CKAN",
                confidence=0.98,
                detected_endpoints=endpoints,
                matched_signals=tuple(signals),
            )

        # M3: Memory Sistemas (`ilai.memory.com.br` / `publico.memory.com.br/openknowledge`)
        if (
            "ilai.memory.com.br" in low
            or "publico.memory.com.br/openknowledge" in low
            or "api.memory.com.br/lai-prod" in low
            or "sistemaweb.memory.com.br" in low
        ):
            signals = ["memory_ilai_openknowledge"]
            endpoints["openknowledge_base_url"] = "https://publico.memory.com.br/openknowledge"
            m_ten = re.search(r"entidades/login/([A-Za-z0-9]{4,12})/(\d+)", blob)
            if m_ten:
                endpoints["memory_tenant_code"] = m_ten.group(1)
                endpoints["memory_entity_id"] = m_ten.group(2)
            return PortalFingerprint(
                erp_family="memory_portal",
                connector_id="memory_portal",
                gazette_family=gaz_family,
                sync_mode="AUTO_REST_OPENKNOWLEDGE",
                confidence=0.98,
                detected_endpoints=endpoints,
                matched_signals=tuple(signals),
            )

        # M2: Betha Sistemas (`betha.cloud` / `flytransparencia` / `e-gov.betha.com.br`)
        if "betha.cloud" in low or "betha.com.br" in low or "flytransparencia" in low:
            return PortalFingerprint(
                erp_family="betha_cloud",
                connector_id="betha_cloud",
                gazette_family=gaz_family if gaz_family != "pdf_diario_eletronico" else "amm_mg",
                sync_mode="AUTO_REST_SICONFI_RECONCILED",
                confidence=0.96,
                detected_endpoints=endpoints,
                matched_signals=("betha_cloud_domain_signature",),
            )

        # M1: Cidade360 / Pública (`cidade360` / `publica.inf.br`)
        if "cidade360" in low or "publica.inf.br" in low:
            return PortalFingerprint(
                erp_family="cidade360",
                connector_id="cidade360",
                gazette_family=gaz_family,
                sync_mode="AUTO_REST_PNCP_ENRICHED",
                confidence=0.96,
                detected_endpoints=endpoints,
                matched_signals=("cidade360_portal_signature",),
            )

        # M4: Actcon / PortalFácil + E&L / SMARapd / Fiorilli / IPM / Sonner
        if any(
            sig in low
            for sig in (
                "dadosabertos-portalfacil.azurewebsites.net",
                "portalfacil.com.br",
                "portalfacilarquivos.blob.core.windows.net",
                "sistemas.el.com.br",
                "smarapd.com.br",
                "dcfiorilli.com.br",
                ".atende.net",
                "/portalcidadao",
            )
        ):
            signals = ["m4_regional_erp_portal_signature"]
            m_pf = re.search(r"dadosabertos-portalfacil\.azurewebsites\.net/(\d+)", blob, re.I)
            if m_pf:
                endpoints["portalfacil_client_id"] = m_pf.group(1)
                endpoints["portalfacil_api_base"] = "https://dadosabertos-portalfacil.azurewebsites.net/api"
            return PortalFingerprint(
                erp_family="portalfacil_el",
                connector_id="portalfacil_el",
                gazette_family=gaz_family,
                sync_mode="AUTO_REST_CSV_OPEN_API",
                confidence=0.94,
                detected_endpoints=endpoints,
                matched_signals=tuple(signals),
            )

        return PortalFingerprint(
            erp_family="sicom_tcemg",
            connector_id="sicom_tcemg",
            gazette_family=gaz_family,
            sync_mode="HYBRID_ASSISTED_ZIP",
            confidence=0.60,
            detected_endpoints=endpoints,
            matched_signals=("generic_portal_fallback_to_sicom_tcemg",),
        )

    @classmethod
    def probe_live_url(cls, url: str, timeout: int = 8) -> PortalFingerprint:
        """Sonda passivamente uma URL pública municipal (1 GET HTTP com proteção anti-SSRF, sem contorno de anti-bot)."""
        try:
            safe_url = _assert_safe_public_http_url(url)
            ctx = ssl.create_default_context()
            opener = urllib.request.build_opener(
                _SafePublicRedirectHandler(),
                urllib.request.HTTPSHandler(context=ctx),
            )
            req = urllib.request.Request(
                safe_url,
                headers={
                    "User-Agent": "SIM-GovTech-PortalDetector/1.0 (+https://dados.gov.br)",
                    "Accept": "text/html,application/json,*/*",
                },
            )
            with opener.open(req, timeout=timeout) as resp:
                final_url = resp.geturl()
                hdrs = {k: v for k, v in resp.headers.items()}
                body = resp.read(350_000).decode("utf-8", errors="ignore")
                fp = cls.classify_sample(url=final_url, html_or_json=body, headers=hdrs)
                if fp.connector_id == "ckan_portal" and fp.detected_endpoints.get("ckan_base_url"):
                    try:
                        ckan_u = _assert_safe_public_http_url(
                            fp.detected_endpoints["ckan_base_url"] + "/api/3/action/package_list"
                        )
                        req2 = urllib.request.Request(ckan_u, headers={"User-Agent": "SIM-GovTech-PortalDetector/1.0"})
                        with opener.open(req2, timeout=timeout) as r2:
                            j2 = json.loads(r2.read(200_000).decode("utf-8", errors="ignore"))
                            if j2.get("success"):
                                fp.detected_endpoints["ckan_verified_packages"] = str(len(j2.get("result") or []))
                    except Exception:
                        pass
                return fp
        except Exception as exc:
            return PortalFingerprint(
                erp_family="sicom_tcemg",
                connector_id="sicom_tcemg",
                gazette_family="pdf_diario_eletronico",
                sync_mode="HYBRID_ASSISTED_ZIP",
                confidence=0.50,
                matched_signals=(f"probe_network_fallback:{type(exc).__name__}",),
            )
