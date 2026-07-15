"""Checkpoint incremental em JSONL para retomada segura."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable


def load_completed_ids(checkpoint_path: Path) -> set[str]:
    done: set[str] = set()
    if not checkpoint_path.exists():
        return done
    with open(checkpoint_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                if row.get("status") == "completed":
                    done.add(str(row["id_teste"]))
            except json.JSONDecodeError:
                continue
    return done


def append_checkpoint(checkpoint_path: Path, record: dict[str, Any]) -> None:
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    with open(checkpoint_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


def read_all_checkpoints(checkpoint_path: Path) -> list[dict[str, Any]]:
    if not checkpoint_path.exists():
        return []
    rows = []
    with open(checkpoint_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows
