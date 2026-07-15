"""Controle de orçamento OpenAI com persistência em disco."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


class BudgetTracker:
    def __init__(
        self,
        state_path: Path,
        limit_usd: float,
        warn_usd: float,
        pricing: dict[str, dict[str, float]],
    ):
        self.state_path = state_path
        self.limit_usd = limit_usd
        self.warn_usd = warn_usd
        self.pricing = pricing
        self.state = self._load()

    def _load(self) -> dict[str, Any]:
        if self.state_path.exists():
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        return {
            "total_usd": 0.0,
            "total_tokens_in": 0,
            "total_tokens_out": 0,
            "calls": 0,
            "history": [],
        }

    def save(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(
            json.dumps(self.state, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def estimate_call_cost(
        self, model: str, tokens_in: int, tokens_out: int
    ) -> float:
        p = self.pricing.get(model, {"input_per_1m": 1.0, "output_per_1m": 3.0})
        return (tokens_in / 1_000_000) * p["input_per_1m"] + (
            tokens_out / 1_000_000
        ) * p["output_per_1m"]

    def can_afford(self, estimated_usd: float) -> bool:
        return (self.state["total_usd"] + estimated_usd) <= self.limit_usd

    def register_usage(
        self,
        model: str,
        tokens_in: int,
        tokens_out: int,
        meta: dict | None = None,
    ) -> float:
        cost = self.estimate_call_cost(model, tokens_in, tokens_out)
        self.state["total_usd"] = round(self.state["total_usd"] + cost, 6)
        self.state["total_tokens_in"] += tokens_in
        self.state["total_tokens_out"] += tokens_out
        self.state["calls"] += 1
        self.state["history"].append(
            {
                "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                "model": model,
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "cost_usd": round(cost, 6),
                "meta": meta or {},
            }
        )
        self.save()
        return cost

    def status_line(self) -> str:
        t = self.state["total_usd"]
        flag = ""
        if t >= self.warn_usd:
            flag = " [AVISO]"
        if t >= self.limit_usd:
            flag = " [LIMITE]"
        return f"Gasto acumulado: US$ {t:.4f} / US$ {self.limit_usd:.2f}{flag}"
