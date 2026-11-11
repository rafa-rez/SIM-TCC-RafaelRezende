"""
SIM Platform — Sistema de Inteligência sobre Dados Públicos Municipais (v3.2).
Arquitetura modular multi-tenant com governança determinística de SQL,
6 Gold Views semânticas, 7 trilhas reais de auditoria TCE-MG, Dossiê 360º,
busca híbrida RAG (Okapi BM25 + RRF), SSO/RBAC com isolamento por gabinete e segurança em camadas.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

__version__ = "3.2.0"

_REPO_ROOT = Path(__file__).resolve().parents[1]
_ENV_CANDIDATES = [
    _REPO_ROOT / ".env",
    Path("D:/tiokk-n8n/.env"),
]

for _env_file in _ENV_CANDIDATES:
    if _env_file.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(_env_file, override=False)
        except Exception:
            for _line in _env_file.read_text(encoding="utf-8", errors="ignore").splitlines():
                _s = _line.strip()
                if _s and not _s.startswith("#") and "=" in _s:
                    _k, _v = _s.split("=", 1)
                    os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))


def get_configured_openai_key(explicit_key: Optional[str] = None, allow_in_pytest: bool = False) -> str:
    """
    Retorna a chave OpenAI configurada via parâmetro explícito ou variável de ambiente (.env).
    Nenhum segredo é chumbado no código-fonte Python.
    Durante a suíte offline do pytest, mantém o modo offline determinístico a menos que
    explicit_key ou allow_in_pytest=True seja informado.
    """
    if explicit_key and explicit_key.strip():
        return explicit_key.strip()
    if "PYTEST_CURRENT_TEST" in os.environ and not allow_in_pytest:
        return ""
    env_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if env_key.startswith("sk-"):
        return env_key
    return ""
