"""Carregamento de config, .env e caminhos do repositório."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from eval.lib.paths import AVALIACAO_ROOT, EVAL_ROOT, REPO_ROOT

CONFIG_PATH = EVAL_ROOT / "config.yaml"


def load_config() -> dict[str, Any]:
    load_dotenv(REPO_ROOT / ".env")
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["_repo_root"] = REPO_ROOT
    cfg["_avaliacao_root"] = AVALIACAO_ROOT
    cfg["_eval_root"] = EVAL_ROOT
    return cfg


def resolve_path(relative: str) -> Path:
    p = Path(relative)
    if p.is_absolute():
        return p
    return AVALIACAO_ROOT / p


def get_openai_api_key() -> str:
    load_dotenv(REPO_ROOT / ".env", override=True)
    key = os.getenv("OPENAI_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "OPENAI_API_KEY não encontrada. Defina no arquivo .env na raiz do repositório."
        )
    return key


def budget_limit_usd(cfg: dict[str, Any]) -> float:
    total = float(cfg["budget_usd"])
    margin = float(cfg.get("budget_safety_margin_usd", 0))
    return max(0.0, total - margin)
