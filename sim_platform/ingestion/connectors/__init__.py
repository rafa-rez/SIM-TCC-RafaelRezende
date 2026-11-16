"""
Pacote Oficial de Conectores Municipais e Régua Mensal de Escala para Minas Gerais (SIM v4.5).

Exporta:
- `BaseMunicipalConnector`, `ConnectorProfile`, `ConnectorRunResult`, `deterministic_seq_id`
- `BethaCloudConnector` (deduplicação `_id` + conciliação funcional SICONFI)
- `Cidade360Connector` (deduplicação de folha sem contaminação de Agente Político por Lotação + PNCP)
- `SicomTceMgConnector` (pacotes `.zip` oficiais TCE-MG para prefeituras Pronim, Memory, Fiorilli, Sonner)
- `AmmMgGazetteConnector` (filtro estrito de seção municipal, trava `<= hoje`, dedup sem código volátil e hook OCR)
- `MGCalendarScheduler`, `MonthlyWindowSpec`, `MG_MONTHLY_WINDOWS` (as 4 Janelas Mensais de MG)
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
from sim_platform.ingestion.connectors.sicom_tcemg import (
    SUPPORTED_ERP_FAMILIES,
    SicomTceMgConnector,
)

__all__ = [
    "AmmMgGazetteConnector",
    "BaseMunicipalConnector",
    "BethaCloudConnector",
    "Cidade360Connector",
    "ConnectorProfile",
    "ConnectorRunResult",
    "MG_MONTHLY_WINDOWS",
    "MGCalendarScheduler",
    "MonthlyWindowSpec",
    "SUPPORTED_ERP_FAMILIES",
    "SicomTceMgConnector",
    "deterministic_seq_id",
    "parse_date_int_yyyymmdd",
    "parse_ptbr_money",
]
