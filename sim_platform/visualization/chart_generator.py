"""
Gerador Nativo de Gráficos Institucionais gov.br (PNG em Base64) para WhatsApp (`POST /api/sendImage`) e Console Web.

Projetado para operar com zero dependências externas obrigatórias dentro do container enxuto `duckdb_prefeitura`
(motor rasterizador PNG nativo via `zlib` + `struct` com `bytearray` acelerado em C e fonte bitmap UTF-8 PT-BR),
com suporte automático opcional ao `matplotlib` (`Agg`) quando instalado no ambiente.
"""

from __future__ import annotations

import base64
import io
import math
import re
import struct
import unicodedata
import zlib
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple


@dataclass
class ChartArtifact:
    png_bytes: bytes
    base64_png: str
    mime_type: str
    filename: str
    title: str
    subtitle: str
    caption: str
    chart_type: str  # "grouped_vertical_bar", "vertical_bar", "horizontal_bar"
    series_names: List[str]
    points_count: int
    engine_used: str
    width_px: int = 960
    height_px: int = 580

    @property
    def image_base64(self) -> str:
        return self.base64_png

    @property
    def chart_kind(self) -> str:
        return self.chart_type


# ==============================================================================
# FONTE BITMAP 5x7 ESCALÁVEL (2x = 10x14 / 3x = 15x21) PARA RENDERIZADOR NATIVO
# ==============================================================================
_GLYPHS_5X7: Dict[str, Tuple[int, ...]] = {
    " ": (0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00),
    "!": (0x04, 0x04, 0x04, 0x04, 0x04, 0x00, 0x04),
    '"': (0x0A, 0x0A, 0x00, 0x00, 0x00, 0x00, 0x00),
    "#": (0x0A, 0x1F, 0x0A, 0x0A, 0x1F, 0x0A, 0x00),
    "$": (0x04, 0x0F, 0x14, 0x0E, 0x05, 0x1E, 0x04),
    "%": (0x18, 0x19, 0x02, 0x04, 0x08, 0x13, 0x03),
    "&": (0x0C, 0x12, 0x14, 0x08, 0x15, 0x12, 0x0D),
    "'": (0x04, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00),
    "(": (0x02, 0x04, 0x08, 0x08, 0x08, 0x04, 0x02),
    ")": (0x08, 0x04, 0x02, 0x02, 0x02, 0x04, 0x08),
    "*": (0x00, 0x04, 0x15, 0x0E, 0x15, 0x04, 0x00),
    "+": (0x00, 0x04, 0x04, 0x1F, 0x04, 0x04, 0x00),
    ",": (0x00, 0x00, 0x00, 0x00, 0x06, 0x04, 0x08),
    "-": (0x00, 0x00, 0x00, 0x1F, 0x00, 0x00, 0x00),
    ".": (0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x06),
    "/": (0x01, 0x02, 0x02, 0x04, 0x08, 0x08, 0x10),
    "0": (0x0E, 0x11, 0x13, 0x15, 0x19, 0x11, 0x0E),
    "1": (0x04, 0x0C, 0x04, 0x04, 0x04, 0x04, 0x0E),
    "2": (0x0E, 0x11, 0x01, 0x06, 0x08, 0x10, 0x1F),
    "3": (0x1F, 0x02, 0x04, 0x06, 0x01, 0x11, 0x0E),
    "4": (0x02, 0x06, 0x0A, 0x12, 0x1F, 0x02, 0x02),
    "5": (0x1F, 0x10, 0x1E, 0x01, 0x01, 0x11, 0x0E),
    "6": (0x06, 0x08, 0x10, 0x1E, 0x11, 0x11, 0x0E),
    "7": (0x1F, 0x01, 0x02, 0x04, 0x08, 0x08, 0x08),
    "8": (0x0E, 0x11, 0x11, 0x0E, 0x11, 0x11, 0x0E),
    "9": (0x0E, 0x11, 0x11, 0x0F, 0x01, 0x02, 0x0C),
    ":": (0x00, 0x06, 0x06, 0x00, 0x06, 0x06, 0x00),
    ";": (0x00, 0x06, 0x06, 0x00, 0x06, 0x04, 0x08),
    "=": (0x00, 0x00, 0x1F, 0x00, 0x1F, 0x00, 0x00),
    "?": (0x0E, 0x11, 0x01, 0x06, 0x04, 0x00, 0x04),
    "A": (0x0E, 0x11, 0x11, 0x1F, 0x11, 0x11, 0x11),
    "B": (0x1E, 0x11, 0x11, 0x1E, 0x11, 0x11, 0x1E),
    "C": (0x0E, 0x11, 0x10, 0x10, 0x10, 0x11, 0x0E),
    "D": (0x1C, 0x12, 0x11, 0x11, 0x11, 0x12, 0x1C),
    "E": (0x1F, 0x10, 0x10, 0x1E, 0x10, 0x10, 0x1F),
    "F": (0x1F, 0x10, 0x10, 0x1E, 0x10, 0x10, 0x10),
    "G": (0x0E, 0x11, 0x10, 0x17, 0x11, 0x11, 0x0F),
    "H": (0x11, 0x11, 0x11, 0x1F, 0x11, 0x11, 0x11),
    "I": (0x0E, 0x04, 0x04, 0x04, 0x04, 0x04, 0x0E),
    "J": (0x07, 0x02, 0x02, 0x02, 0x02, 0x12, 0x0C),
    "K": (0x11, 0x12, 0x14, 0x18, 0x14, 0x12, 0x11),
    "L": (0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x1F),
    "M": (0x11, 0x1B, 0x15, 0x15, 0x11, 0x11, 0x11),
    "N": (0x11, 0x19, 0x15, 0x13, 0x11, 0x11, 0x11),
    "O": (0x0E, 0x11, 0x11, 0x11, 0x11, 0x11, 0x0E),
    "P": (0x1E, 0x11, 0x11, 0x1E, 0x10, 0x10, 0x10),
    "Q": (0x0E, 0x11, 0x11, 0x11, 0x15, 0x12, 0x0D),
    "R": (0x1E, 0x11, 0x11, 0x1E, 0x14, 0x12, 0x11),
    "S": (0x0F, 0x10, 0x10, 0x0E, 0x01, 0x01, 0x1E),
    "T": (0x1F, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04),
    "U": (0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x0E),
    "V": (0x11, 0x11, 0x11, 0x11, 0x11, 0x0A, 0x04),
    "W": (0x11, 0x11, 0x11, 0x15, 0x15, 0x1B, 0x11),
    "X": (0x11, 0x11, 0x0A, 0x04, 0x0A, 0x11, 0x11),
    "Y": (0x11, 0x11, 0x0A, 0x04, 0x04, 0x04, 0x04),
    "Z": (0x1F, 0x01, 0x02, 0x04, 0x08, 0x10, 0x1F),
    "_": (0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1F),
    "|": (0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04),
}


def _ascii_clean_upper(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", str(text or ""))
    no_acc = "".join(c for c in nfkd if not unicodedata.combining(c))
    return no_acc.upper()


class _NativePNGCanvas:
    """Canvas RGB acelerado por slices de `bytearray` para renderização PNG rápida e limpa."""

    def __init__(self, width: int, height: int, bg_rgb: Tuple[int, int, int] = (248, 250, 252)) -> None:
        self.width = width
        self.height = height
        self.stride = width * 3
        row_bytes = bytes(bg_rgb) * width
        self.buf = bytearray(row_bytes * height)

    def fill_rect(self, x0: int, y0: int, x1: int, y1: int, rgb: Tuple[int, int, int]) -> None:
        cx0 = max(0, min(self.width, int(x0)))
        cx1 = max(0, min(self.width, int(x1)))
        cy0 = max(0, min(self.height, int(y0)))
        cy1 = max(0, min(self.height, int(y1)))
        if cx1 <= cx0 or cy1 <= cy0:
            return
        span = bytes(rgb) * (cx1 - cx0)
        s0 = cx0 * 3
        s1 = cx1 * 3
        buf = self.buf
        stride = self.stride
        for y in range(cy0, cy1):
            off = y * stride
            buf[off + s0 : off + s1] = span

    def draw_text(
        self,
        x: int,
        y: int,
        text: str,
        rgb: Tuple[int, int, int],
        scale: int = 2,
        align: str = "left",
    ) -> None:
        clean = _ascii_clean_upper(text)
        if not clean:
            return
        char_advance = 6 * scale
        total_w = len(clean) * char_advance - scale
        if align == "center":
            x = int(x - total_w // 2)
        elif align == "right":
            x = int(x - total_w)

        cx = int(x)
        cy = int(y)
        for ch in clean:
            rows_bits = _GLYPHS_5X7.get(ch, _GLYPHS_5X7[" "])
            for ry, bits in enumerate(rows_bits):
                if bits == 0:
                    continue
                for rx in range(5):
                    if bits & (1 << (4 - rx)):
                        self.fill_rect(
                            cx + rx * scale,
                            cy + ry * scale,
                            cx + (rx + 1) * scale,
                            cy + (ry + 1) * scale,
                            rgb,
                        )
            cx += char_advance

    def to_png_bytes(self) -> bytes:
        raw_scanlines = bytearray()
        stride = self.stride
        buf = self.buf
        for y in range(self.height):
            raw_scanlines.append(0)  # PNG filter type 0 (None)
            off = y * stride
            raw_scanlines.extend(buf[off : off + stride])

        def _chunk(chunk_type: bytes, data: bytes) -> bytes:
            crc = zlib.crc32(chunk_type)
            crc = zlib.crc32(data, crc) & 0xFFFFFFFF
            return struct.pack(">I", len(data)) + chunk_type + data + struct.pack(">I", crc)

        ihdr = struct.pack(">IIBBBBB", self.width, self.height, 8, 2, 0, 0, 0)
        idat = zlib.compress(bytes(raw_scanlines), level=6)
        return b"\x89PNG\r\n\x1a\n" + _chunk(b"IHDR", ihdr) + _chunk(b"IDAT", idat) + _chunk(b"IEND", b"")


# ==============================================================================
# FORMATAÇÃO DE VALORES E DETECÇÃO INTELIGENTE DE SÉRIES
# ==============================================================================
_ID_OR_SKIP_NUMERIC = {
    "ano_referencia",
    "ano",
    "ano_exercicio",
    "num_anoexercicio",
    "num_mesexercicio",
    "num_mes_referencia",
    "mes_referencia",
    "ultimo_mes_receita",
    "ultimo_mes_despesa",
    "primeiro_ano_pagamento",
    "ultimo_ano_pagamento",
    "primeiro_ano_folha",
    "ultimo_ano_folha",
    "de_ano",
    "ate_ano",
    "seq_dat_pagamento",
    "ultima_competencia",
    "competencia",
    "cod_municipio",
    "cod_orgao",
    "seq_orgao",
    "cod_unidade",
    "seq_unidade",
    "seq_veiculo",
    "seq_empenho",
    "seq_contrato",
    "seq_dim_pessoa",
}

_MONETARY_HINTS = (
    "vlr", "valor", "total_pago", "orcamento", "receita", "despesa",
    "dotacao", "empenhad", "liquidad", "arrecadad", "folha", "media_salarial",
    "media_bruta", "mediana_bruta", "gasto", "remuneracao", "pib", "rcl",
)

_HUMAN_COLUMN_LABELS: Dict[str, str] = {
    "orcamento_previsto_loa": "Orcamento LOA",
    "orcamento_atualizado_loa": "Orcamento LOA Atualiz.",
    "despesa_dotacao_atualizada": "Dotacao Atualizada",
    "receita_arrecadada_realizada": "Receita Arrecadada",
    "despesa_empenhada": "Despesa Empenhada",
    "despesa_liquidada": "Despesa Liquidada",
    "despesa_paga": "Despesa Paga",
    "total_pago_saude": "Total Pago Saude",
    "total_pago_historico": "Total Pago (2020-2025)",
    "total_pago_carona": "Total Pago via Carona",
    "total_empenhado_carona": "Total Empenhado Carona",
    "qtd_servidores_ativos_mes": "Servidores Ativos",
    "qtd_servidores_ultimo_mes": "Servidores no Ultimo Mes",
    "qtd_servidores": "Servidores",
    "qtd_veiculos": "Quantidade de Veiculos",
    "vlr_rmn_bruta": "Remuneracao Bruta",
    "media_bruta_municipio": "Media Salarial Municipio",
    "vlr_gasto": "Gasto de Frota",
}


def _to_float(val: Any) -> Optional[float]:
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, (int, float, Decimal)):
        f = float(val)
        return f if math.isfinite(f) else None
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            f = float(s)
            return f if math.isfinite(f) else None
        except ValueError:
            return None
    return None


def _is_monetary_col(col_name: str) -> bool:
    cl = col_name.lower()
    if cl.startswith(("qtd_", "pct_", "razao_", "nota_", "ideb_")):
        return False
    return any(h in cl for h in _MONETARY_HINTS)


def _is_pct_col(col_name: str) -> bool:
    cl = col_name.lower()
    return cl.startswith("pct_") or "percentual" in cl


def _format_value_compact(val: float, is_monetary: bool = False, is_pct: bool = False) -> str:
    if is_pct:
        return f"{val:.1f}%".replace(".", ",")
    prefix = "R$ " if is_monetary else ""
    abs_v = abs(val)
    if abs_v >= 1_000_000_000:
        return f"{prefix}{val / 1_000_000_000:.2f} bi".replace(".", ",")
    if abs_v >= 1_000_000:
        return f"{prefix}{val / 1_000_000:.1f}M".replace(".", ",")
    if abs_v >= 10_000:
        return f"{prefix}{val / 1_000:.1f} mil".replace(".", ",")
    if is_monetary or abs(val - round(val)) > 0.01:
        s = f"{val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"{prefix}{s}"
    s_int = f"{int(round(val)):,}".replace(",", ".")
    return f"{prefix}{s_int}"


def _pretty_col_name(col: str) -> str:
    cl = col.lower()
    if cl in _HUMAN_COLUMN_LABELS:
        return _HUMAN_COLUMN_LABELS[cl]
    return col.replace("_", " ").strip().title()[:28]


class InstitutionalChartGenerator:
    """
    Renderiza gráficos institucionais PNG em Base64 a partir das linhas retornadas pelo DuckDB
    (ou recuperadas do turno anterior na memória de sessão).
    """

    PALETTE_RGB: Tuple[Tuple[int, int, int], ...] = (
        (30, 58, 138),   # #1E3A8A Gov.br Navy Primary
        (13, 148, 136),  # #0D9488 Institutional Teal
        (2, 132, 199),   # #0284C7 Sky Accent
        (217, 119, 6),   # #D97706 Amber Highlight
    )

    def __init__(self, municipality_name: str = "Caeté", state_uf: str = "MG") -> None:
        self.municipality_name = municipality_name
        self.state_uf = state_uf
        self.default_municipality_label = f"Prefeitura Municipal de {municipality_name}/{state_uf}"

    def extract_chart_series(
        self,
        rows: Sequence[Dict[str, Any]],
        question: str = "",
    ) -> Optional[Dict[str, Any]]:
        if not rows:
            return None
        clean_rows = [r for r in rows if isinstance(r, dict) and r]
        if not clean_rows:
            return None

        cols = list(clean_rows[0].keys())
        q_low = (question or "").lower()

        # Caso especial 1: apenas 1 linha com múltiplas métricas comparáveis -> pivota colunas como categorias!
        if len(clean_rows) == 1:
            r0 = clean_rows[0]
            pivoted_labels: List[str] = []
            pivoted_vals: List[float] = []
            monetary_flags: List[bool] = []
            for c in cols:
                cl = c.lower()
                if cl in _ID_OR_SKIP_NUMERIC or cl.startswith("pct_") or cl.startswith("razao_"):
                    continue
                fv = _to_float(r0.get(c))
                if fv is not None and fv > 0:
                    pivoted_labels.append(_pretty_col_name(c))
                    pivoted_vals.append(fv)
                    monetary_flags.append(_is_monetary_col(c))
            if len(pivoted_vals) >= 2:
                # Evitar misturar quantidades (ex: 1496 servidores) com milhões em R$ no mesmo eixo
                has_mon = any(monetary_flags)
                has_non_mon = any(not m for m in monetary_flags)
                if has_mon and has_non_mon:
                    want_qty = any(w in q_low for w in ("quantos", "quantidade", "numero", "total de funcion", "veiculo"))
                    keep_mon = not want_qty
                    filtered = [
                        (lbl, val)
                        for lbl, val, is_m in zip(pivoted_labels, pivoted_vals, monetary_flags)
                        if is_m == keep_mon
                    ]
                    if len(filtered) >= 1:
                        pivoted_labels = [x[0] for x in filtered]
                        pivoted_vals = [x[1] for x in filtered]
                        monetary_flags = [keep_mon] * len(filtered)

                return {
                    "chart_type": "horizontal_bar" if len(pivoted_labels) > 4 else "vertical_bar",
                    "labels": pivoted_labels[:10],
                    "series": [{"name": "Valor Apurado", "values": pivoted_vals[:10]}],
                    "is_monetary": any(monetary_flags),
                    "is_pct": False,
                }

        # Identificar eixo X (ano_referencia ou dimensão categórica)
        year_col = next((c for c in cols if c.lower() in ("ano_referencia", "ano", "ano_exercicio")), None)
        label_candidates = [
            c
            for c in cols
            if c != year_col
            and c.lower() not in _ID_OR_SKIP_NUMERIC
            and not c.lower().startswith(("num_doc", "num_cpf", "cpf_", "cnpj_"))
            and any(isinstance(r.get(c), str) and str(r.get(c)).strip() for r in clean_rows[:5])
            and c.lower() != "status_exercicio"
        ]

        numeric_cols: List[str] = []
        for c in cols:
            cl = c.lower()
            if cl in _ID_OR_SKIP_NUMERIC or c == year_col or c in label_candidates:
                continue
            vals = [_to_float(r.get(c)) for r in clean_rows]
            if any(v is not None for v in vals):
                numeric_cols.append(c)

        if not numeric_cols:
            return None

        # Se existe coluna de ano e as linhas são anos distintos -> Série Temporal Vertical!
        distinct_years = {r.get(year_col) for r in clean_rows if year_col and r.get(year_col) is not None}
        is_time_series = bool(year_col and len(distinct_years) >= 2 and not label_candidates)

        if is_time_series and year_col:
            sorted_rows = sorted(clean_rows, key=lambda r: int(_to_float(r.get(year_col)) or 0))[:12]
            labels = []
            for r in sorted_rows:
                yr_str = str(r.get(year_col))
                st_ex = str(r.get("status_exercicio") or "")
                if yr_str == "2025" or "parcial" in st_ex.lower():
                    labels.append(f"{yr_str}*")
                else:
                    labels.append(yr_str)

            # Verificar se há par comparativo clássico (ex: Orçamento LOA vs Receita Realizada / Despesa)
            loa_col = next((c for c in numeric_cols if "orcamento_previsto" in c.lower() or "previsaoatualizada" in c.lower()), None)
            real_col = next((c for c in numeric_cols if "arrecadada" in c.lower() or "realizado" in c.lower() or "despesa_empenhada" in c.lower() or "despesa_paga" in c.lower()), None)

            if loa_col and real_col and loa_col != real_col:
                return {
                    "chart_type": "grouped_vertical_bar",
                    "labels": labels,
                    "series": [
                        {
                            "name": _pretty_col_name(loa_col),
                            "values": [float(_to_float(r.get(loa_col)) or 0.0) for r in sorted_rows],
                        },
                        {
                            "name": _pretty_col_name(real_col),
                            "values": [float(_to_float(r.get(real_col)) or 0.0) for r in sorted_rows],
                        },
                    ],
                    "is_monetary": True,
                    "is_pct": False,
                }

            primary_col = numeric_cols[0]
            for c in numeric_cols:
                if _is_monetary_col(c) and not _is_pct_col(c):
                    primary_col = c
                    break
            return {
                "chart_type": "vertical_bar",
                "labels": labels,
                "series": [
                    {
                        "name": _pretty_col_name(primary_col),
                        "values": [float(_to_float(r.get(primary_col)) or 0.0) for r in sorted_rows],
                    }
                ],
                "is_monetary": _is_monetary_col(primary_col),
                "is_pct": _is_pct_col(primary_col),
            }

        # Ranking ou distribuição categórica -> Horizontal Bar Chart
        lbl_col = label_candidates[0] if label_candidates else (year_col or cols[0])
        # Escolher métrica principal coerente com a pergunta
        want_count = any(w in q_low for w in ("quantos", "quantidade", "numero", "veiculo", "concursad", "funcionari", "servidor"))
        primary_col = numeric_cols[0]
        if want_count:
            cnt_col = next((c for c in numeric_cols if c.lower().startswith(("qtd_", "total_serv", "total_reg", "count"))), None)
            if cnt_col:
                primary_col = cnt_col
        else:
            mon_col = next((c for c in numeric_cols if _is_monetary_col(c)), None)
            if mon_col:
                primary_col = mon_col

        top_rows = clean_rows[:10]
        labels = [str(r.get(lbl_col) or "N/I")[:34] for r in top_rows]
        values = [float(_to_float(r.get(primary_col)) or 0.0) for r in top_rows]
        return {
            "chart_type": "horizontal_bar",
            "labels": labels,
            "series": [{"name": _pretty_col_name(primary_col), "values": values}],
            "is_monetary": _is_monetary_col(primary_col),
            "is_pct": _is_pct_col(primary_col),
        }

    def generate_chart(
        self,
        rows: Sequence[Dict[str, Any]],
        title: str = "",
        subtitle: str = "Dados auditados SICOM/TCE-MG e Fontes Federais",
        municipality: str = "",
        question: str = "",
        prefer_native_renderer: bool = False,
    ) -> Optional[ChartArtifact]:
        spec = self.extract_chart_series(rows, question=question)
        if not spec or not spec.get("labels") or not spec.get("series"):
            return None

        eff_muni = (municipality or getattr(self, "default_municipality_label", "Prefeitura Municipal de Caeté/MG")).strip()
        city_tag = f"{getattr(self, 'municipality_name', 'Caeté')}/{getattr(self, 'state_uf', 'MG')}"
        derived_title = title.strip() if title and title.strip() else (
            question.strip().rstrip("?") if question and len(question.strip()) >= 8 else f"Painel Analítico Oficial — {city_tag}"
        )
        clean_title = derived_title[:68]
        clean_sub = (subtitle or f"Fonte: Base Auditada SIM ({city_tag})").strip()[:86]
        municipality = eff_muni

        png_bytes: Optional[bytes] = None
        engine_used = "sim_native_png_govbr"

        if not prefer_native_renderer:
            png_bytes = self._try_render_matplotlib(spec, clean_title, clean_sub, municipality)
            if png_bytes:
                engine_used = "matplotlib_agg_govbr"

        if not png_bytes:
            png_bytes = self._render_native_png(spec, clean_title, clean_sub, municipality)
            engine_used = "sim_native_png_govbr"

        b64 = base64.b64encode(png_bytes).decode("ascii")
        s_names = [s["name"] for s in spec["series"]]
        has_2025_star = any("2025*" in str(lbl) for lbl in spec["labels"])
        partial_note = " (*2025 em execucao parcial Jan-Ago no SICOM/TCE-MG)." if has_2025_star else "."
        caption = (
            f"Grafico institucional gerado pelo SIM ({municipality}): {clean_title} — "
            f"Metrica(s): {', '.join(s_names)}{partial_note}"
        )

        return ChartArtifact(
            png_bytes=png_bytes,
            base64_png=b64,
            mime_type="image/png",
            filename="grafico_sim_caete.png",
            title=clean_title,
            subtitle=clean_sub,
            caption=caption,
            chart_type=spec["chart_type"],
            series_names=s_names,
            points_count=len(spec["labels"]),
            engine_used=engine_used,
        )

    def _try_render_matplotlib(
        self,
        spec: Dict[str, Any],
        title: str,
        subtitle: str,
        municipality: str,
    ) -> Optional[bytes]:
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt  # type: ignore
        except Exception:
            return None

        try:
            labels: List[str] = spec["labels"]
            series: List[Dict[str, Any]] = spec["series"]
            is_mon: bool = spec["is_monetary"]
            is_pct: bool = spec["is_pct"]
            ctype: str = spec["chart_type"]

            fig, ax = plt.subplots(figsize=(10.0, 5.8), dpi=130)
            fig.patch.set_facecolor("#F8FAFC")
            ax.set_facecolor("#FFFFFF")

            colors = ["#1E3A8A", "#0D9488", "#0284C7", "#D97706"]
            if ctype == "horizontal_bar":
                y_pos = list(range(len(labels)))[::-1]
                vals = series[0]["values"]
                bars = ax.barh(y_pos, vals, color=colors[0], height=0.62, edgecolor="none")
                ax.set_yticks(y_pos)
                ax.set_yticklabels(labels, fontsize=9, color="#0F172A")
                max_v = max(vals) if vals else 1.0
                ax.set_xlim(0, max_v * 1.24 if max_v > 0 else 1.0)
                ax.grid(axis="x", linestyle="--", alpha=0.35, color="#CBD5E1")
                for bar, v in zip(bars, vals):
                    ax.text(
                        bar.get_width() + max_v * 0.015,
                        bar.get_y() + bar.get_height() / 2,
                        _format_value_compact(v, is_mon, is_pct),
                        va="center",
                        fontsize=8.5,
                        fontweight="bold",
                        color="#1E293B",
                    )
            elif ctype == "grouped_vertical_bar" and len(series) >= 2:
                x_idx = list(range(len(labels)))
                width = 0.36
                all_v = [v for s in series[:2] for v in s["values"]]
                max_v = max(all_v) if all_v else 1.0
                for s_i, s_obj in enumerate(series[:2]):
                    offset = -width / 2 if s_i == 0 else width / 2
                    xs = [x + offset for x in x_idx]
                    bars = ax.bar(xs, s_obj["values"], width=width, label=s_obj["name"], color=colors[s_i])
                    for bar, v in zip(bars, s_obj["values"]):
                        ax.text(
                            bar.get_x() + bar.get_width() / 2,
                            bar.get_height() + max_v * 0.015,
                            _format_value_compact(v, is_mon, is_pct),
                            ha="center",
                            va="bottom",
                            fontsize=7.5,
                            fontweight="bold",
                            color="#1E293B",
                        )
                ax.set_xticks(x_idx)
                ax.set_xticklabels(labels, fontsize=9.5, color="#0F172A")
                ax.set_ylim(0, max_v * 1.24 if max_v > 0 else 1.0)
                ax.grid(axis="y", linestyle="--", alpha=0.35, color="#CBD5E1")
                ax.legend(frameon=False, fontsize=9, loc="upper left")
            else:
                x_idx = list(range(len(labels)))
                vals = series[0]["values"]
                max_v = max(vals) if vals else 1.0
                bars = ax.bar(x_idx, vals, width=0.55, color=colors[0])
                ax.set_xticks(x_idx)
                ax.set_xticklabels(labels, fontsize=9, rotation=12 if len(labels) > 5 else 0, color="#0F172A")
                ax.set_ylim(0, max_v * 1.22 if max_v > 0 else 1.0)
                ax.grid(axis="y", linestyle="--", alpha=0.35, color="#CBD5E1")
                for bar, v in zip(bars, vals):
                    ax.text(
                        bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + max_v * 0.018,
                        _format_value_compact(v, is_mon, is_pct),
                        ha="center",
                        va="bottom",
                        fontsize=8.5,
                        fontweight="bold",
                        color="#1E293B",
                    )

            for spine in ("top", "right"):
                ax.spines[spine].set_visible(False)
            ax.spines["left"].set_color("#CBD5E1")
            ax.spines["bottom"].set_color("#94A3B8")

            fig.suptitle(title, fontsize=13, fontweight="bold", color="#0F172A", x=0.04, ha="left", y=0.96)
            ax.set_title(subtitle, fontsize=9.5, color="#475569", loc="left", pad=12)
            fig.text(
                0.04,
                0.015,
                f"SIM - Sistema de Inteligencia Municipal | {municipality} | *2025 = execucao parcial acumulada",
                fontsize=8,
                color="#64748B",
            )

            buf = io.BytesIO()
            plt.tight_layout(rect=(0.02, 0.04, 0.98, 0.91))
            fig.savefig(buf, format="png")
            plt.close(fig)
            return buf.getvalue()
        except Exception:
            return None

    def _render_native_png(
        self,
        spec: Dict[str, Any],
        title: str,
        subtitle: str,
        municipality: str,
    ) -> bytes:
        width, height = 960, 580
        canvas = _NativePNGCanvas(width, height, bg_rgb=(241, 245, 249))

        # Cabeçalho institucional gov.br azul-marinho escuro (#0F172A)
        canvas.fill_rect(0, 0, width, 86, (15, 23, 42))
        canvas.fill_rect(0, 86, width, 90, (2, 132, 199))
        canvas.draw_text(28, 20, title[:62], (255, 255, 255), scale=2, align="left")
        canvas.draw_text(28, 52, subtitle[:76], (186, 230, 253), scale=2, align="left")

        # Card branco central
        card_x0, card_y0, card_x1, card_y1 = 24, 106, width - 24, height - 44
        canvas.fill_rect(card_x0, card_y0, card_x1, card_y1, (255, 255, 255))

        labels: List[str] = spec["labels"]
        series: List[Dict[str, Any]] = spec["series"]
        is_mon: bool = spec["is_monetary"]
        is_pct: bool = spec["is_pct"]
        ctype: str = spec["chart_type"]

        # Legenda no topo do card
        leg_x = card_x0 + 20
        for s_idx, s_obj in enumerate(series[:2]):
            col_rgb = self.PALETTE_RGB[s_idx % len(self.PALETTE_RGB)]
            canvas.fill_rect(leg_x, card_y0 + 14, leg_x + 18, card_y0 + 28, col_rgb)
            s_label = str(s_obj["name"])[:28]
            canvas.draw_text(leg_x + 26, card_y0 + 14, s_label, (30, 41, 59), scale=2, align="left")
            leg_x += 26 + len(s_label) * 12 + 28

        if ctype == "horizontal_bar":
            n = max(1, len(labels))
            vals = series[0]["values"]
            max_v = max([abs(v) for v in vals] + [1.0])
            plot_x0 = card_x0 + 285
            plot_x1 = card_x1 - 150
            plot_y0 = card_y0 + 50
            plot_y1 = card_y1 - 24
            avail_h = max(60, plot_y1 - plot_y0)
            slot_h = avail_h // n
            bar_h = max(14, min(32, int(slot_h * 0.64)))

            # Linhas de grade verticais
            for g in range(5):
                gx = plot_x0 + int((plot_x1 - plot_x0) * (g / 4.0))
                canvas.fill_rect(gx, plot_y0, gx + 1, plot_y1, (226, 232, 240))

            for idx, (lbl, val) in enumerate(zip(labels, vals)):
                cy = plot_y0 + idx * slot_h + (slot_h - bar_h) // 2
                short_lbl = str(lbl)[:22]
                canvas.draw_text(plot_x0 - 12, cy + max(0, (bar_h - 14) // 2), short_lbl, (15, 23, 42), scale=2, align="right")
                bw = int((max(0.0, val) / max_v) * (plot_x1 - plot_x0))
                bw = max(3, bw) if val > 0 else 0
                canvas.fill_rect(plot_x0, cy, plot_x0 + bw, cy + bar_h, self.PALETTE_RGB[0])
                v_txt = _format_value_compact(val, is_mon, is_pct)
                canvas.draw_text(plot_x0 + bw + 10, cy + max(0, (bar_h - 14) // 2), v_txt, (30, 58, 138), scale=2, align="left")
        else:
            n = max(1, len(labels))
            n_series = min(2, len(series))
            all_vals = [abs(v) for s in series[:n_series] for v in s["values"]]
            max_v = max(all_vals + [1.0])
            plot_x0 = card_x0 + 125
            plot_x1 = card_x1 - 36
            plot_y0 = card_y0 + 62
            plot_y1 = card_y1 - 48
            plot_h = max(80, plot_y1 - plot_y0)
            plot_w = max(120, plot_x1 - plot_x0)

            # Grade horizontal + rótulos do eixo Y
            for g in range(5):
                frac = g / 4.0
                gy = plot_y1 - int(plot_h * frac)
                canvas.fill_rect(plot_x0, gy, plot_x1, gy + 1, (203, 213, 225) if g == 0 else (226, 232, 240))
                yv = max_v * frac
                canvas.draw_text(plot_x0 - 10, gy - 7, _format_value_compact(yv, is_mon, is_pct), (100, 116, 139), scale=1, align="right")

            slot_w = plot_w // n
            for i, lbl in enumerate(labels):
                group_cx = plot_x0 + i * slot_w + slot_w // 2
                if n_series == 2:
                    bw = max(14, min(42, int(slot_w * 0.34)))
                    for s_i in range(2):
                        val = series[s_i]["values"][i]
                        bh = int((max(0.0, val) / max_v) * plot_h)
                        bx0 = (group_cx - bw - 3) if s_i == 0 else (group_cx + 3)
                        by0 = plot_y1 - max(2, bh) if val > 0 else plot_y1
                        canvas.fill_rect(bx0, by0, bx0 + bw, plot_y1, self.PALETTE_RGB[s_i])
                        v_txt = _format_value_compact(val, is_mon, is_pct)
                        canvas.draw_text(bx0 + bw // 2, by0 - 14 - (s_i * 11), v_txt, self.PALETTE_RGB[s_i], scale=1, align="center")
                else:
                    bw = max(18, min(64, int(slot_w * 0.56)))
                    val = series[0]["values"][i]
                    bh = int((max(0.0, val) / max_v) * plot_h)
                    bx0 = group_cx - bw // 2
                    by0 = plot_y1 - max(2, bh) if val > 0 else plot_y1
                    canvas.fill_rect(bx0, by0, bx0 + bw, plot_y1, self.PALETTE_RGB[0])
                    v_txt = _format_value_compact(val, is_mon, is_pct)
                    canvas.draw_text(group_cx, by0 - 18, v_txt, (30, 58, 138), scale=2 if n <= 7 else 1, align="center")

                short_lbl = str(lbl)[:11]
                canvas.draw_text(group_cx, plot_y1 + 12, short_lbl, (15, 23, 42), scale=2 if n <= 8 else 1, align="center")

        # Rodapé institucional
        footer = f"SIM - TRANSPARENCIA PUBLICA | {municipality} | *2025 EM EXECUCAO PARCIAL"
        canvas.draw_text(28, height - 26, footer[:78], (71, 85, 105), scale=1, align="left")
        return canvas.to_png_bytes()
