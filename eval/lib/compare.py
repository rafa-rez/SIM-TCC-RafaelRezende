"""Comparação de resultados SQL e métricas de avaliação Text-to-SQL."""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any


def _normalize_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return str(v)
        try:
            d = Decimal(str(v))
            return float(d.quantize(Decimal("0.0001")))
        except (InvalidOperation, ValueError):
            return str(v).strip().upper()
    if isinstance(v, str):
        s = v.strip()
        try:
            d = Decimal(s.replace(",", "."))
            return float(d.quantize(Decimal("0.0001")))
        except (InvalidOperation, ValueError):
            return s.upper()
    return str(v)


def row_signature(row: dict[str, Any]) -> tuple:
    items = []
    for k in sorted(row.keys()):
        items.append((k.lower(), _normalize_value(row[k])))
    return tuple(items)


def _sort_key(v: Any) -> tuple:
    nv = _normalize_value(v)
    if isinstance(nv, (int, float)):
        return (0, float(nv))
    if isinstance(nv, bool):
        return (1, int(nv))
    return (2, str(nv))


def row_value_signature(row: dict[str, Any]) -> tuple:
    values = sorted((_sort_key(v) for v in row.values()))
    return tuple(values)


def multiset(rows: list[dict[str, Any]]) -> list[tuple]:
    sigs = [row_signature(r) for r in rows]
    return sorted(sigs)


def multiset_values(rows: list[dict[str, Any]]) -> list[tuple]:
    sigs = [row_value_signature(r) for r in rows]
    return sorted(sigs)


def column_vectors(rows: list[dict[str, Any]]) -> list[tuple]:
    if not rows:
        return []
    cols = list(rows[0].keys())
    sorted_rows = sorted(rows, key=row_value_signature)
    vectors = []
    for col in cols:
        vec = tuple(_sort_key(r[col]) for r in sorted_rows)
        vectors.append(vec)
    return sorted(vectors)


def execution_match_strict(
    generated: list[dict[str, Any]],
    reference: list[dict[str, Any]],
) -> bool:
    return multiset(generated) == multiset(reference)


def execution_match_content(
    generated: list[dict[str, Any]],
    reference: list[dict[str, Any]],
) -> bool:
    if len(generated) != len(reference):
        return False
    if not reference:
        return True
    ncol_ref = len(reference[0])
    ncol_gen = len(generated[0]) if generated else 0
    if ncol_ref != ncol_gen:
        return False
    return multiset_values(generated) == multiset_values(reference)


def execution_match_colmap(
    generated: list[dict[str, Any]],
    reference: list[dict[str, Any]],
) -> bool:
    if len(generated) != len(reference):
        return False
    if not reference:
        return True
    ncol_ref = len(reference[0])
    ncol_gen = len(generated[0]) if generated else 0
    if ncol_ref != ncol_gen:
        return False
    return column_vectors(generated) == column_vectors(reference)


# Retrocompatibilidade
execution_match = execution_match_strict


def rows_nonempty(rows: list[dict[str, Any]]) -> bool:
    return len(rows) > 0


def row_count_match(
    generated: list[dict[str, Any]],
    reference: list[dict[str, Any]],
) -> bool:
    return len(generated) == len(reference)


def col_count_match(
    generated: list[dict[str, Any]],
    reference: list[dict[str, Any]],
) -> bool:
    if not reference and not generated:
        return True
    if not reference or not generated:
        return False
    return len(reference[0]) == len(generated[0])


def parse_sql_tables(sql: str) -> list[str]:
    if not sql:
        return []
    found = set()
    for m in re.finditer(
        r"\b(?:FROM|JOIN)\s+([a-zA-Z_][\w.]*)",
        sql,
        flags=re.IGNORECASE,
    ):
        t = m.group(1).split(".")[-1].lower()
        if t not in ("select", "where", "on", "as"):
            found.add(t)
    return sorted(found)


def table_selection_accuracy(sql: str, expected_tables: list[str]) -> bool:
    if not expected_tables:
        return True
    present = parse_sql_tables(sql)
    return all(t.lower() in present for t in expected_tables)


def condition_heuristic_score(sql: str, conditions: list[str]) -> float:
    if not conditions:
        return 1.0
    sql_l = sql.lower()
    hits = sum(1 for c in conditions if c.strip().lower() in sql_l)
    return hits / len(conditions)


def classify_ex_divergence(
    generated: list[dict[str, Any]],
    reference: list[dict[str, Any]],
) -> str | None:
    if execution_match_strict(generated, reference):
        return None
    if not row_count_match(generated, reference):
        return "cardinalidade_linhas"
    if not col_count_match(generated, reference):
        return "cardinalidade_colunas"
    if execution_match_colmap(generated, reference):
        return "alias_colunas"
    if execution_match_content(generated, reference):
        return "permutacao_valores_linha"
    return "divergencia_valores"


@dataclass
class MetricBundle:
    vsr: bool = False
    ex_strict: bool = False
    ex_rows: bool = False
    ex_cols: bool = False
    ex_content: bool = False
    ex_colmap: bool = False
    nea_ok: bool = True
    tsa: bool = False
    chs: float = 0.0
    ref_rows: int = 0
    gen_rows: int = 0
    ref_cols: int = 0
    gen_cols: int = 0
    divergencia_ex: str | None = None
    alias_apenas: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["ex"] = d["ex_strict"]
        return d


def compute_metrics(
    generated: list[dict[str, Any]],
    reference: list[dict[str, Any]],
    *,
    vsr: bool,
    sql_generated: str = "",
    expected_tables: list[str] | None = None,
    conditions: list[str] | None = None,
    espera_dados: str = "sim",
) -> MetricBundle:
    ref_cols = len(reference[0]) if reference else 0
    gen_cols = len(generated[0]) if generated else 0

    ex_strict = (
        vsr
        and bool(reference)
        and execution_match_strict(generated, reference)
    )
    ex_rows = vsr and bool(reference) and row_count_match(generated, reference)
    ex_cols = vsr and bool(reference) and col_count_match(generated, reference)
    ex_content = (
        vsr
        and bool(reference)
        and execution_match_content(generated, reference)
    )
    ex_colmap = (
        vsr
        and bool(reference)
        and execution_match_colmap(generated, reference)
    )

    divergencia = None
    alias_apenas = False
    if vsr and reference and not ex_strict:
        divergencia = classify_ex_divergence(generated, reference)
        alias_apenas = divergencia == "alias_colunas"

    nea_ok = (
        rows_nonempty(generated)
        if espera_dados.lower() == "sim"
        else True
    )

    return MetricBundle(
        vsr=vsr,
        ex_strict=ex_strict,
        ex_rows=ex_rows,
        ex_cols=ex_cols,
        ex_content=ex_content,
        ex_colmap=ex_colmap,
        nea_ok=nea_ok,
        tsa=table_selection_accuracy(sql_generated, expected_tables or []),
        chs=condition_heuristic_score(sql_generated, conditions or []),
        ref_rows=len(reference),
        gen_rows=len(generated),
        ref_cols=ref_cols,
        gen_cols=gen_cols,
        divergencia_ex=divergencia,
        alias_apenas=alias_apenas,
    )


def truncate_for_log(obj: Any, max_len: int = 4000) -> str:
    s = json.dumps(obj, ensure_ascii=False, default=str)
    if len(s) <= max_len:
        return s
    return s[:max_len] + "...[TRUNCADO]"
