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
