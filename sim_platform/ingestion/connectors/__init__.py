"""
Pacote Oficial de Conectores Municipais, Auto-Detector de Portais e Régua Mensal de Escala para Minas Gerais (SIM v4.7).

Exporta as 5 Famílias Canônicas de Portais de Minas Gerais + Auto-Detector:
- M1: `Cidade360Connector` + `SicomTceMgConnector` (Cidade360 / Pública + SICOM)
- M2: `BethaCloudConnector` (Betha Cloud + conciliação funcional SICONFI)
- M3: `MemoryPortalConnector` (Memory Sistemas iLAI / OpenKnowledge FastAPI)
- M4: `PortalFacilElConnector` (Actcon / PortalFácil + E&L / SMARapd Open Data)
- M5: `CkanPortalConnector` (CKAN v3 Open Data + DOM REST API — Grande Porte)
- `PortalDetector`, `PortalFingerprint` (auto-detecção sem bypass de CAPTCHA)
- `AmmMgGazetteConnector` (Diário Oficial AMM-MG + PDFs locais)
- `MGCalendarScheduler`, `MonthlyWindowSpec`, `MG_MONTHLY_WINDOWS`
"""

from __future__ import annotations

from sim_platform.ingestion.connectors.amm_mg_gazette import AmmMgGazetteConnector
from sim_platform.ingestion.connectors.base import (
    BaseMunicipalConnector,
    ConnectorProfile,
    ConnectorRunResult,
    deterministic_seq_id,
    parse_date_int_yyyymmdd,
    parse_ptbr_money,
)
from sim_platform.ingestion.connectors.betha_cloud import BethaCloudConnector
from sim_platform.ingestion.connectors.calendar_scheduler import (
    MG_MONTHLY_WINDOWS,
    MGCalendarScheduler,
    MonthlyWindowSpec,
)
from sim_platform.ingestion.connectors.cidade360 import Cidade360Connector
from sim_platform.ingestion.connectors.ckan_portal import CkanPortalConnector
from sim_platform.ingestion.connectors.memory_portal import MemoryPortalConnector
from sim_platform.ingestion.connectors.portal_detector import PortalDetector, PortalFingerprint
from sim_platform.ingestion.connectors.portalfacil_el import PortalFacilElConnector
from sim_platform.ingestion.connectors.sicom_tcemg import (
    SUPPORTED_ERP_FAMILIES,
    SicomTceMgConnector,
)

__all__ = [
    "AmmMgGazetteConnector",
    "BaseMunicipalConnector",
    "BethaCloudConnector",
    "Cidade360Connector",
    "CkanPortalConnector",
    "ConnectorProfile",
    "ConnectorRunResult",
    "MG_MONTHLY_WINDOWS",
    "MGCalendarScheduler",
    "MemoryPortalConnector",
    "MonthlyWindowSpec",
    "PortalDetector",
    "PortalFacilElConnector",
    "PortalFingerprint",
    "SUPPORTED_ERP_FAMILIES",
    "SicomTceMgConnector",
    "deterministic_seq_id",
    "parse_date_int_yyyymmdd",
    "parse_ptbr_money",
]
