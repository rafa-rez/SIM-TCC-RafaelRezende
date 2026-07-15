"""Caminhos canônicos do repositório SIM."""

from __future__ import annotations

from pathlib import Path

_PKG = Path(__file__).resolve().parent
EVAL_ROOT = _PKG.parent
AVALIACAO_ROOT = EVAL_ROOT.parent
REPO_ROOT = AVALIACAO_ROOT.parent

DADOS_ROOT = AVALIACAO_ROOT / "dados"
GOLDEN_DIR = DADOS_ROOT / "golden"
EXPERIMENTOS_ROOT = AVALIACAO_ROOT / "experimentos"
SCRIPTS_ROOT = AVALIACAO_ROOT / "scripts"

GOLDEN_DATASET = GOLDEN_DIR / "golden_dataset_v1.0.csv"
REFERENCE_RESULTS_DIR = GOLDEN_DIR / "reference_results"

GOLDEN_V2_DIR = GOLDEN_DIR / "v2.0"
GOLDEN_DATASET_V2 = GOLDEN_V2_DIR / "golden_dataset_v2.0.csv"
GOLDEN_V2_DRAFT_DIR = GOLDEN_V2_DIR / "draft"
GOLDEN_V2_REFERENCE_DIR = GOLDEN_V2_DIR / "reference_results"
GOLDEN_V2_REVISAO_DIR = GOLDEN_V2_DIR / "revisao"
