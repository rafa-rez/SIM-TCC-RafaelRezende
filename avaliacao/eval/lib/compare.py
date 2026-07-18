"""Comparação de resultados SQL e métricas de avaliação Text-to-SQL."""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any

# Tolerância relativa padrão para EX_proj_tol (0,5%)
DEFAULT_TAU = 0.005


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


def _sort_key(v: Any) -> tuple:
    nv = _normalize_value(v)
    if isinstance(nv, (int, float)):
        return (0, float(nv))
    if isinstance(nv, bool):
        return (1, int(nv))
    return (2, str(nv))


def _sig_sort_key(sig: tuple) -> str:
    return json.dumps(sig, default=str, ensure_ascii=False)


def row_signature(row: dict[str, Any]) -> tuple:
    items = []
    for k in sorted(row.keys()):
        items.append((k.lower(), _normalize_value(row[k])))
    return tuple(items)


def row_value_signature(row: dict[str, Any]) -> tuple:
    values = sorted((_sort_key(v) for v in row.values()), key=_sig_sort_key)
    return tuple(values)


def multiset(rows: list[dict[str, Any]]) -> list[tuple]:
    sigs = [row_signature(r) for r in rows]
    return sorted(sigs, key=_sig_sort_key)


def multiset_values(rows: list[dict[str, Any]]) -> list[tuple]:
    sigs = [row_value_signature(r) for r in rows]
    return sorted(sigs, key=_sig_sort_key)


def column_vectors(rows: list[dict[str, Any]]) -> list[tuple]:
    if not rows:
        return []
    cols = list(rows[0].keys())
    sorted_rows = sorted(rows, key=lambda r: _sig_sort_key(row_value_signature(r)))
    vectors = []
    for col in cols:
        vec = tuple(_sort_key(r[col]) for r in sorted_rows)
        vectors.append(vec)
    return sorted(vectors)


def selected_column_vectors(
    rows: list[dict[str, Any]],
    columns: list[str],
) -> list[tuple]:
    if not rows or not columns:
        return []
    sorted_rows = sorted(rows, key=lambda r: _sig_sort_key(row_value_signature(r)))
    vectors = []
    for col in columns:
        if col not in rows[0]:
            continue
        vectors.append(tuple(_sort_key(r[col]) for r in sorted_rows))
    return vectors


def _numeric_from_sort_key(sk: tuple) -> float | None:
    if sk[0] == 0:
        return float(sk[1])
    return None


def cells_match_sort_key(a: tuple, b: tuple, tau: float = 0.0) -> bool:
    if a == b:
        return True
    na = _numeric_from_sort_key(a)
    nb = _numeric_from_sort_key(b)
    if na is not None and nb is not None:
        if tau <= 0:
            return na == nb
        denom = max(abs(nb), 1e-9)
        return abs(na - nb) / denom <= tau
    return a == b


def vectors_match(v1: tuple, v2: tuple, tau: float = 0.0) -> bool:
    if len(v1) != len(v2):
        return False
    return all(cells_match_sort_key(a, b, tau) for a, b in zip(v1, v2))


def vector_in_list(
    ref_vec: tuple,
    gen_vectors: list[tuple],
    tau: float = 0.0,
) -> bool:
    return any(vectors_match(ref_vec, gv, tau) for gv in gen_vectors)


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
    if len(reference[0]) != len(generated[0] if generated else []):
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
    if len(reference[0]) != len(generated[0] if generated else []):
        return False
    return column_vectors(generated) == column_vectors(reference)


def execution_match_proj(
    generated: list[dict[str, Any]],
    reference: list[dict[str, Any]],
    *,
    tau: float = 0.0,
    colunas_resposta: list[str] | None = None,
) -> bool:
    if not reference:
        return True
    if len(generated) != len(reference):
        return False
    if not generated:
        return False
    if len(generated[0]) < len(reference[0]):
        return False

    gen_vectors = column_vectors(generated)
    if colunas_resposta:
        ref_vectors = selected_column_vectors(reference, colunas_resposta)
    else:
        ref_vectors = column_vectors(reference)

    if not ref_vectors:
        return False

    return all(vector_in_list(rv, gen_vectors, tau) for rv in ref_vectors)


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


def parse_colunas_resposta(value: str | None) -> list[str]:
    if not value:
        return []
    return [c.strip() for c in value.split(",") if c.strip()]


def classify_ex_divergence(
    generated: list[dict[str, Any]],
    reference: list[dict[str, Any]],
    *,
    colunas_resposta: list[str] | None = None,
    tau: float = DEFAULT_TAU,
) -> str | None:
    if execution_match_strict(generated, reference):
        return None
    if not row_count_match(generated, reference):
        return "cardinalidade_linhas"
    if execution_match_proj(
        generated, reference, tau=tau, colunas_resposta=colunas_resposta
    ):
        if execution_match_colmap(generated, reference):
            return "alias_colunas"
        if not col_count_match(generated, reference):
            return "colunas_extras_aceitas"
        return "tolerancia_numerica"
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
    ex_proj: bool = False
    ex_proj_tol: bool = False
    ex_resposta: bool = False
    nea_ok: bool = True
    tsa: bool = False
    chs: float = 0.0
    ref_rows: int = 0
    gen_rows: int = 0
    ref_cols: int = 0
    gen_cols: int = 0
    divergencia_ex: str | None = None
    alias_apenas: bool = False
    proj_sem_tol_apenas: bool = False
    tau: float = DEFAULT_TAU
    colunas_resposta: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["ex"] = d["ex_strict"]
        d["ex_principal"] = d["ex_resposta"]
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
    colunas_resposta: str | None = None,
    tau: float = DEFAULT_TAU,
) -> MetricBundle:
    ref_cols = len(reference[0]) if reference else 0
    gen_cols = len(generated[0]) if generated else 0
    cols_resp = parse_colunas_resposta(colunas_resposta)

    has_ref = bool(reference) and vsr

    ex_strict = has_ref and execution_match_strict(generated, reference)
    ex_rows = has_ref and row_count_match(generated, reference)
    ex_cols = has_ref and col_count_match(generated, reference)
    ex_content = has_ref and execution_match_content(generated, reference)
    ex_colmap = has_ref and execution_match_colmap(generated, reference)
    ex_proj = has_ref and execution_match_proj(generated, reference, tau=0.0)
    ex_proj_tol = has_ref and execution_match_proj(
        generated, reference, tau=tau
    )
    ex_resposta = has_ref and execution_match_proj(
        generated,
        reference,
        tau=tau,
        colunas_resposta=cols_resp if cols_resp else None,
    )

    divergencia = None
    alias_apenas = False
    proj_sem_tol_apenas = False
    if has_ref and not ex_strict:
        divergencia = classify_ex_divergence(
            generated,
            reference,
            colunas_resposta=cols_resp if cols_resp else None,
            tau=tau,
        )
        alias_apenas = divergencia == "alias_colunas"
        proj_sem_tol_apenas = (
            not ex_proj and ex_proj_tol and divergencia == "tolerancia_numerica"
        )

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
        ex_proj=ex_proj,
        ex_proj_tol=ex_proj_tol,
        ex_resposta=ex_resposta,
        nea_ok=nea_ok,
        tsa=table_selection_accuracy(sql_generated, expected_tables or []),
        chs=condition_heuristic_score(sql_generated, conditions or []),
        ref_rows=len(reference),
        gen_rows=len(generated),
        ref_cols=ref_cols,
        gen_cols=gen_cols,
        divergencia_ex=divergencia,
        alias_apenas=alias_apenas,
        proj_sem_tol_apenas=proj_sem_tol_apenas,
        tau=tau,
        colunas_resposta=colunas_resposta or "",
    )


def truncate_for_log(obj: Any, max_len: int = 4000) -> str:
    s = json.dumps(obj, ensure_ascii=False, default=str)
    if len(s) <= max_len:
        return s
    return s[:max_len] + "...[TRUNCADO]"
