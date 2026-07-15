"""Montagem do system prompt a partir dos arquivos Póscagada / Précagada."""

from __future__ import annotations

from pathlib import Path

from eval.lib.config_loader import resolve_path


def load_prompt(variant_id: str, cfg: dict) -> tuple[str, Path]:
    rel = cfg["prompts"][variant_id]
    path = resolve_path(rel)
    if not path.exists():
        raise FileNotFoundError(f"Prompt não encontrado: {path}")
    return path.read_text(encoding="utf-8"), path


def estimate_tokens(text: str) -> int:
    try:
        import tiktoken

        enc = tiktoken.get_encoding("cl100k_base")
        return len(enc.encode(text))
    except Exception:
        return len(text) // 4
