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
    "[": (0x0E, 0x08, 0x08, 0x08, 0x08, 0x08, 0x0E),
    "]": (0x0E, 0x02, 0x02, 0x02, 0x02, 0x02, 0x0E),
    "<": (0x02, 0x04, 0x08, 0x10, 0x08, 0x04, 0x02),
    ">": (0x08, 0x04, 0x02, 0x01, 0x02, 0x04, 0x08),
    "°": (0x06, 0x09, 0x09, 0x06, 0x00, 0x00, 0x00),
}

_PUNCT_TRANSLATE = str.maketrans({
    "\u2014": "-",  # em-dash —
    "\u2013": "-",  # en-dash –
    "\u2212": "-",  # minus −
    "\u201c": '"',
    "\u201d": '"',
    "\u2018": "'",
    "\u2019": "'",
    "\u2022": "*",
    "\u00ba": "°",  # º
    "\u00aa": "°",  # ª
    "\u00b0": "°",  # °
})

_CHART_REQUEST_PREFIX_RE = re.compile(
    r"^(?:oi\s*,?\s*|ola\s*,?\s*|bom\s+dia\s*,?\s*|boa\s+tarde\s*,?\s*|boa\s+noite\s*,?\s*)?"
    r"(?:voce\s+|vc\s+)?"
    r"(?:consegue\s+|pode\s+|poderia\s+|quero\s+|gostaria\s+de\s+|teria\s+como\s+|tem\s+como\s+)?"
    r"(?:me\s+|nos\s+)?"
    r"(?:gerar|gere|fazer|faca|criar|crie|montar|monte|mostrar|mostre|plotar|plote|trazer|traga|enviar|envie|mandar|mande)\s+"
    r"(?:agora\s+|tambem\s+|entao\s+)?"
    r"(?:um\s+|uma\s+|o\s+)?"
    r"(?:grafico|visualizacao|imagem|quadro\s+visual)"
    r"(?:\s+de\s+(?:barras?|linhas?|colunas?|pizza|evolucao|comparacao))?"
    r"(?:\s+(?:sobre|de|da|do|das|dos|com|para|referente\s+a|acerca\s+de))?\s*",
    flags=re.IGNORECASE,
)


def _ascii_clean_upper(text: str) -> str:
    raw = str(text or "").translate(_PUNCT_TRANSLATE)
    # Preservar símbolo de grau antes da decomposição NFKD
    raw = raw.replace("°", "\x01")
    nfkd = unicodedata.normalize("NFKD", raw)
    no_acc = "".join(c for c in nfkd if not unicodedata.combining(c))
    return no_acc.replace("\x01", "°").upper()


def _truncate_words(text: str, max_chars: int) -> str:
    s = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(s) <= max_chars:
        return s
    if max_chars <= 4:
        return s[:max_chars]
    cut = s[: max_chars - 3].rstrip()
    if " " in cut and len(cut.rsplit(" ", 1)[0]) >= max(6, (max_chars - 3) // 2):
        cut = cut.rsplit(" ", 1)[0].rstrip(" ,;:-/")
    return f"{cut}..."


_GOV_PREFIX_ABBREVS: Tuple[Tuple[re.Pattern[str], str, str], ...] = (
    (
        re.compile(r"\bSERVI[CÇ]O\s+AUT[OÔ]NOMO\s+DE\s+[AÁ]GUA\s+E\s+ESGOTO\b", re.IGNORECASE),
        "SAAE",
        "SAAE",
    ),
    (
        re.compile(r"^SECRETARIA\s+MUNICIPAL\s+D[EAO]S?\s+", re.IGNORECASE),
        "SEC. MUN. ",
        "Sec. Mun. ",
    ),
    (
        re.compile(r"^SECRETARIA\s+MUNICIPAL\s+", re.IGNORECASE),
        "SEC. MUN. ",
        "Sec. Mun. ",
    ),
    (
        re.compile(r"^FUNDO\s+MUNICIPAL\s+D[EAO]S?\s+", re.IGNORECASE),
        "FUNDO MUN. ",
        "Fundo Mun. ",
    ),
    (
        re.compile(r"^FUNDO\s+MUNICIPAL\s+", re.IGNORECASE),
        "FUNDO MUN. ",
        "Fundo Mun. ",
    ),
    (
        re.compile(r"^PREFEITURA\s+MUNICIPAL\s+D[EAO]S?\s+", re.IGNORECASE),
        "PREF. ",
        "Pref. ",
    ),
    (
        re.compile(r"^PREFEITURA\s+MUNICIPAL\s+", re.IGNORECASE),
        "PREF. ",
        "Pref. ",
    ),
    (
        re.compile(r"^FUNDA[CÇ][AÃ]O\s+MUNICIPAL\s+D[EAO]S?\s+", re.IGNORECASE),
        "FUND. MUN. ",
        "Fund. Mun. ",
    ),
    (
        re.compile(r"^FUNDA[CÇ][AÃ]O\s+", re.IGNORECASE),
        "FUND. ",
        "Fund. ",
    ),
    (
        re.compile(r"^C[AÂ]MARA\s+MUNICIPAL\s+D[EAO]S?\s+", re.IGNORECASE),
        "CÂMARA MUN. ",
        "Câmara Mun. ",
    ),
)


def _smart_shorten_gov_label(lbl: str, max_len: int = 26) -> str:
    s = re.sub(r"\s+", " ", str(lbl or "")).strip()
    if not s:
        return "N/I"
    is_upper = s == s.upper()
    for pat, repl_up, repl_tc in _GOV_PREFIX_ABBREVS:
        s = pat.sub(repl_up if is_upper else repl_tc, s)
    s = re.sub(r"\s+", " ", s).strip()
    return _truncate_words(s, max_len)


def _extract_starred_years(labels: Sequence[str], series: Optional[Sequence[Dict[str, Any]]] = None) -> Tuple[bool, List[str]]:
    starred_years: List[str] = []
    has_star = False
    pool: List[str] = [str(lbl or "") for lbl in labels]
    if series:
        pool.extend(str(s.get("name") or "") for s in series if isinstance(s, dict))
    for txt in pool:
        if "*" in txt:
            has_star = True
            for m_yr in re.finditer(r"\b(19\d{2}|20\d{2}|2100)\*", txt):
                yr_tok = m_yr.group(1)
                if yr_tok not in starred_years:
                    starred_years.append(yr_tok)
    return has_star, starred_years


def _build_partial_footer_tag(labels: Sequence[str], series: Optional[Sequence[Dict[str, Any]]] = None) -> Optional[str]:
    has_star, starred_years = _extract_starred_years(labels, series)
    if not has_star:
        return None
    if len(starred_years) == 1:
        return f"*{starred_years[0]} EM EXECUCAO PARCIAL"
    if len(starred_years) >= 2:
        return f"*{'/'.join(starred_years)} EM EXECUCAO PARCIAL"
    return "*EXERCICIO EM EXECUCAO PARCIAL"


def _wrap_words(text: str, max_chars_per_line: int = 72, max_lines: int = 2) -> List[str]:
    s = re.sub(r"\s+", " ", str(text or "")).strip()
    if not s:
        return []
    words = s.split(" ")
    lines: List[str] = []
    cur = ""
    idx = 0
    while idx < len(words):
        w = words[idx]
        cand = f"{cur} {w}".strip() if cur else w
        if len(cand) <= max_chars_per_line:
            cur = cand
            idx += 1
        else:
            if not cur:
                lines.append(w[:max_chars_per_line])
                idx += 1
            else:
                lines.append(cur)
                cur = ""
            if len(lines) == max_lines - 1:
                rem = " ".join(words[idx:])
                lines.append(_truncate_words(rem, max_chars_per_line))
                return [ln for ln in lines if ln]
    if cur:
        lines.append(cur)
    return lines[:max_lines]


def _derive_editorial_title(question: str, explicit_title: str, series_names: Sequence[str], city_tag: str) -> str:
    if explicit_title and explicit_title.strip():
        return _truncate_words(explicit_title.strip(), 72)
    q_raw = re.sub(r"\s+", " ", (question or "")).strip().rstrip("?!.")
    if q_raw:
        q_norm = unicodedata.normalize("NFKD", q_raw)
        q_ascii = "".join(c for c in q_norm if not unicodedata.combining(c))
        m = _CHART_REQUEST_PREFIX_RE.match(q_ascii)
        cleaned = q_raw[m.end() :].strip(" :,-?!") if m else q_raw
        generic_refs = {
            "", "dessa informacao", "desta informacao", "disso", "disto",
            "dessa resposta", "dos dados", "desses dados", "para mim", "pra mim",
        }
        cleaned_key = "".join(c for c in unicodedata.normalize("NFKD", cleaned.lower()) if not unicodedata.combining(c))
        if cleaned and cleaned_key not in generic_refs and len(cleaned) >= 5:
            titled = cleaned[0].upper() + cleaned[1:]
            if city_tag.split("/")[0].lower() not in titled.lower() and len(titled) <= 52:
                titled = f"{titled} - {city_tag}"
            return _truncate_words(titled, 72)
    if series_names:
        joined = " vs. ".join(series_names[:2])
        return _truncate_words(f"{joined} - {city_tag}", 72)
    return f"Painel Analitico Oficial - {city_tag}"


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

    def draw_line(
        self,
        x0: int,
        y0: int,
        x1: int,
        y1: int,
        rgb: Tuple[int, int, int],
        thickness: int = 3,
    ) -> None:
        """Rasterização pixelada de linha inteira (algoritmo de Bresenham) sem anti-aliasing borrado."""
        ix0, iy0, ix1, iy1 = int(x0), int(y0), int(x1), int(y1)
        dx = abs(ix1 - ix0)
        dy = -abs(iy1 - iy0)
        sx = 1 if ix0 < ix1 else -1
        sy = 1 if iy0 < iy1 else -1
        err = dx + dy
        half = max(1, thickness // 2)
        while True:
            self.fill_rect(ix0 - half, iy0 - half, ix0 - half + thickness, iy0 - half + thickness, rgb)
            if ix0 == ix1 and iy0 == iy1:
                break
            e2 = 2 * err
            if e2 >= dy:
                err += dy
                ix0 += sx
            if e2 <= dx:
                err += dx
                iy0 += sy

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
    "exercicio",
    "num_exercicio",
    "num_anoexercicio",
    "num_ano_licitacao",
    "ano_folha",
    "ano_licitacao",
    "ano_contrato",
    "ano_edicao",
    "ano_empenho",
    "ano_processo",
    "num_ano",
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

_YEAR_COL_CANDIDATES = (
    "ano_referencia",
    "ano",
    "ano_exercicio",
    "exercicio",
    "num_exercicio",
    "num_anoexercicio",
    "num_ano_licitacao",
    "ano_folha",
    "ano_licitacao",
    "ano_contrato",
    "ano_edicao",
)

_YEAR_OR_ID_COL_RE = re.compile(
    r"(^ano|ano_|_ano$|num_ano|exercicio|competencia|ultima_competencia|"
    r"^mes$|mes_|_mes$|num_mes|seq_dat_pagamento|"
    r"^id$|^id_|_id$|^cod_|_cod$|^seq_|_seq$|"
    r"num_contrato|nro_contrato|num_licitacao|num_processo|num_empenho|numero_ato)",
    re.IGNORECASE,
)

_MONETARY_HINTS = (
    "vlr", "valor", "total_pago", "orcamento", "receita", "despesa",
    "dotacao", "empenhad", "liquidad", "arrecadad", "folha", "media_salarial",
    "media_bruta", "mediana_bruta", "gasto", "remuneracao", "pib", "rcl",
    "resultado", "saldo", "superavit", "deficit",
)

_NON_ADDITIVE_STOCK_HINTS = (
    "servidor", "funcionari", "veiculo", "frota", "matricula", "escola",
    "leito", "populacao", "habitante", "ubs", "cnes", "media_", "mediana_",
    "ideb", "idhm", "nota_", "isf_", "taxa_", "ies_", "ieq_",
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
    "total_pago_historico": "Total Pago Histórico",
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


def _is_year_or_skip_numeric_col(col_name: str, rows: Sequence[Dict[str, Any]]) -> bool:
    cl = col_name.lower().strip()
    if cl in _ID_OR_SKIP_NUMERIC or _YEAR_OR_ID_COL_RE.search(cl):
        return True
    non_null = [r.get(col_name) for r in rows if r.get(col_name) is not None and str(r.get(col_name)).strip() != ""]
    if non_null and re.search(r"(ano|exerc|perio)", cl):
        fvals = [_to_float(v) for v in non_null]
        if all(fv is not None and abs(fv - round(fv)) < 1e-6 and 1990 <= int(round(fv)) <= 2100 for fv in fvals):
            return True
    return False


def _is_monetary_col(col_name: str) -> bool:
    cl = col_name.lower()
    if cl.startswith(
        (
            "qtd_",
            "pct_",
            "razao_",
            "nota_",
            "ideb_",
            "registros_",
            "num_",
            "nro_",
            "numero_",
            "count",
            "total_serv",
            "total_func",
            "total_veic",
            "total_reg",
            "total_lic",
            "total_emp",
            "total_cont",
            "populac",
            "habitant",
        )
    ):
        return False
    if re.search(r"(_qtd$|_count$|registros|qtd_|veiculos|servidores|socios|familias|habitantes|populacao|matriculas)", cl):
        return False
    return any(h in cl for h in _MONETARY_HINTS)


def _is_pct_col(col_name: str) -> bool:
    cl = col_name.lower()
    return cl.startswith("pct_") or "percentual" in cl


def _is_stock_or_non_additive_col(col_name: str) -> bool:
    cl = col_name.lower()
    if _is_pct_col(cl):
        return True
    return any(h in cl for h in _NON_ADDITIVE_STOCK_HINTS)


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

    def __init__(
        self,
        municipality_name: str = "Caeté",
        state_uf: str = "MG",
        prefer_native_renderer: bool = True,
        max_partial_year: Optional[int] = None,
    ) -> None:
        self.municipality_name = municipality_name
        self.state_uf = state_uf
        self.prefer_native_renderer = prefer_native_renderer
        self.max_partial_year = max_partial_year
        self.default_municipality_label = f"Prefeitura Municipal de {municipality_name}/{state_uf}"

    def _is_partial_year_row(
        self,
        row: Dict[str, Any],
        yr_int: int,
        distinct_years: Sequence[int],
        is_stock_metric: bool,
    ) -> bool:
        st_ex = str(row.get("status_exercicio") or row.get("situacao_exercicio") or "").lower()
        if "parcial" in st_ex or "em aberto" in st_ex or "em execucao" in st_ex or "em execução" in st_ex:
            return True
        if is_stock_metric:
            return False
        max_yr = max(distinct_years) if distinct_years else 0
        if self.max_partial_year is not None:
            eff_partial = max_yr if (max_yr > self.max_partial_year and max_yr >= 2026) else self.max_partial_year
            return yr_int == eff_partial
        if max_yr >= 2026 and yr_int == max_yr:
            return True
        return False

    def extract_chart_series(
        self,
        rows: Sequence[Dict[str, Any]],
        question: str = "",
        chart_spec: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if not rows:
            return None
        clean_rows = [r for r in rows if isinstance(r, dict) and r]
        if not clean_rows:
            return None

        cols = list(clean_rows[0].keys())
        q_norm = _ascii_clean_upper(question).lower()
        requested_type = str((chart_spec or {}).get("chart_type") or "").strip().lower()
        wants_line = (
            requested_type == "line"
            or any(w in q_norm for w in ("grafico de linha", "grafico em linha", "em linhas", "linha do tempo"))
        )

        # Caso especial 1: apenas 1 linha -> SÓ plota se houver >= 2 métricas comparáveis na mesma unidade!
        # Escalar único (1x1, ex: [{"qtd_servidores": 1496}]) retorna None para o orquestrador expandir em série anual.
        if len(clean_rows) == 1:
            r0 = clean_rows[0]
            pivoted_labels: List[str] = []
            pivoted_vals: List[float] = []
            monetary_flags: List[bool] = []
            for c in cols:
                cl = c.lower()
                if _is_year_or_skip_numeric_col(c, clean_rows) or cl.startswith("pct_") or cl.startswith("razao_"):
                    continue
                fv = _to_float(r0.get(c))
                if fv is not None and fv != 0:
                    pivoted_labels.append(_pretty_col_name(c))
                    pivoted_vals.append(fv)
                    monetary_flags.append(_is_monetary_col(c))
            if len(pivoted_vals) >= 2:
                has_mon = any(monetary_flags)
                has_non_mon = any(not m for m in monetary_flags)
                if has_mon and has_non_mon:
                    want_qty = any(w in q_norm for w in ("quantos", "quantidade", "numero", "total de funcion", "veiculo"))
                    keep_mon = not want_qty
                    filtered = [
                        (lbl, val)
                        for lbl, val, is_m in zip(pivoted_labels, pivoted_vals, monetary_flags)
                        if is_m == keep_mon
                    ]
                    if len(filtered) >= 2:
                        pivoted_labels = [x[0] for x in filtered]
                        pivoted_vals = [x[1] for x in filtered]
                        monetary_flags = [keep_mon] * len(filtered)
                    else:
                        return None

                return {
                    "chart_type": "horizontal_bar" if len(pivoted_labels) > 4 else "vertical_bar",
                    "labels": pivoted_labels[:10],
                    "series": [{"name": "Valor Apurado", "values": pivoted_vals[:10]}],
                    "is_monetary": any(monetary_flags),
                    "is_pct": False,
                }
            return None

        # Identificar eixo X (ano_referencia / exercicio / variantes de ano ou dimensão categórica)
        year_col = next((c for c in cols if c.lower() in _YEAR_COL_CANDIDATES), None)
        if year_col is None:
            for c in cols:
                cl = c.lower()
                if cl.startswith(("primeiro_", "ultimo_", "de_", "ate_")):
                    continue
                if re.search(r"(^ano_|_ano$|^exercicio|exercicio_|num_ano)", cl):
                    non_null_v = [_to_float(r.get(c)) for r in clean_rows if r.get(c) is not None and str(r.get(c)).strip() != ""]
                    if non_null_v and all(fv is not None and abs(fv - round(fv)) < 1e-6 and 1990 <= int(round(fv)) <= 2100 for fv in non_null_v):
                        year_col = c
                        break

        _META_TEXT_SKIP = {
            "status_exercicio",
            "situacao_exercicio",
            "nom_municipio",
            "municipio",
            "uf",
            "cod_ibge",
            "fonte_populacao",
            "fonte_dados",
            "situacao_fiscal_lrf",
            "hospital_referencia",
            "nota_capag_tesouro",
            "icf_ranking_qualidade_info",
        }
        label_candidates = [
            c
            for c in cols
            if c != year_col
            and not _is_year_or_skip_numeric_col(c, clean_rows)
            and c.lower() not in _META_TEXT_SKIP
            and not c.lower().startswith(("num_doc", "num_cpf", "cpf_", "cnpj_", "fonte_", "status_"))
            and any(isinstance(r.get(c), str) and str(r.get(c)).strip() for r in clean_rows[:5])
            and (
                not year_col
                or len({str(r.get(c)).strip() for r in clean_rows if r.get(c) is not None and str(r.get(c)).strip()}) > 1
            )
        ]

        numeric_cols: List[str] = []
        for c in cols:
            if c == year_col or c in label_candidates or _is_year_or_skip_numeric_col(c, clean_rows):
                continue
            vals = [_to_float(r.get(c)) for r in clean_rows]
            if any(v is not None for v in vals):
                numeric_cols.append(c)

        if not numeric_cols:
            return None

        asks_money = any(
            w in q_norm
            for w in (
                "quanto gast",
                "gasto",
                "gastou",
                "despesa",
                "valor",
                "custo",
                "pago",
                "recebeu",
                "arrecad",
                "remunerac",
                "salario",
                "per capita",
                "por habitante",
                "por aluno",
                "r$",
            )
        )
        explicit_how_many = any(w in q_norm for w in ("quantos ", "quantas ", "quantidade de ", "numero de ", "qtd "))
        want_count = explicit_how_many or (
            not asks_money
            and any(w in q_norm for w in ("quantos", "quantidade", "numero", "veiculo", "concursad", "funcionari", "servidor", "modalidade", "licitac", "beneficiari"))
        )

        def _pick_primary_metric(cands: Sequence[str]) -> str:
            if any(w in q_norm for w in ("per capita", "por habitante", "por aluno")):
                m_hit = next((c for c in cands if "per_capita" in c.lower() or "por_hab" in c.lower() or "por_aluno" in c.lower()), None)
                if m_hit:
                    return m_hit
            if not asks_money and any(w in q_norm for w in ("populac", "habitante", "demograf")):
                m_hit = next((c for c in cands if "populac" in c.lower() or "habitante" in c.lower()), None)
                if m_hit:
                    return m_hit
            if "idhm" in q_norm or "idh" in q_norm:
                m_hit = next((c for c in cands if "idhm" in c.lower()), None)
                if m_hit:
                    return m_hit
            if "ideb" in q_norm:
                m_hit = next((c for c in cands if "ideb" in c.lower()), None)
                if m_hit:
                    return m_hit
            if "pib" in q_norm:
                m_hit = next((c for c in cands if "pib_municipal" in c.lower() or "pib" in c.lower()), None)
                if m_hit:
                    return m_hit
            if any(w in q_norm for w in ("rcl", "receita corrente")):
                m_hit = next((c for c in cands if "receita_corrente_liquida" in c.lower() or "rcl" in c.lower()), None)
                if m_hit:
                    return m_hit
            if "pessoal" in q_norm:
                prefer_pct = "%" in q_norm or "percentual" in q_norm or "indice" in q_norm or "limite" in q_norm
                if prefer_pct:
                    m_hit = next((c for c in cands if "pct_despesa_pessoal" in c.lower()), None)
                    if m_hit:
                        return m_hit
                m_hit = next((c for c in cands if "despesa_pessoal" in c.lower()), None)
                if m_hit:
                    return m_hit
            if "saude" in q_norm or "siops" in q_norm:
                prefer_pct = "%" in q_norm or "percentual" in q_norm or "indice" in q_norm or "minimo" in q_norm
                if prefer_pct:
                    m_hit = next((c for c in cands if "pct_" in c.lower() and "saude" in c.lower()), None)
                    if m_hit:
                        return m_hit
                m_hit = next((c for c in cands if "saude" in c.lower()), None)
                if m_hit:
                    return m_hit
            if "educac" in q_norm or "mde" in q_norm:
                m_hit = next((c for c in cands if "educac" in c.lower() or "mde" in c.lower()), None)
                if m_hit:
                    return m_hit
            if want_count:
                cnt_c = next((c for c in cands if c.lower().startswith(("qtd_", "beneficiarios", "total_serv", "total_reg", "total_lic", "total_func", "count"))), None)
                if cnt_c:
                    return cnt_c
            mon_c = next((c for c in cands if _is_monetary_col(c) and not _is_pct_col(c)), None)
            return mon_c or cands[0]

        distinct_years = sorted(
            {int(_to_float(r.get(year_col)) or 0) for r in clean_rows if year_col and _to_float(r.get(year_col)) is not None}
        )

        # Caso especial 2: Tabela longa com (ano_referencia + categoria + métrica), ex: modalidades por ano
        if year_col and len(distinct_years) >= 2 and label_candidates:
            cat_col = label_candidates[0]
            prim_col = _pick_primary_metric(numeric_cols)
            is_stock_cat = _is_stock_or_non_additive_col(prim_col)
            cat_totals: Dict[str, float] = {}
            by_year_cat: Dict[Tuple[int, str], float] = {}
            partial_years: set[int] = set()
            for r in clean_rows:
                yv_f = _to_float(r.get(year_col))
                if yv_f is None:
                    continue
                yv = int(yv_f)
                cat_v = str(r.get(cat_col) or "N/I").strip()
                mv = float(_to_float(r.get(prim_col)) or 0.0)
                cat_totals[cat_v] = cat_totals.get(cat_v, 0.0) + mv
                by_year_cat[(yv, cat_v)] = by_year_cat.get((yv, cat_v), 0.0) + mv
                if self._is_partial_year_row(r, yv, distinct_years, is_stock_cat):
                    partial_years.add(yv)

            ranked_cats = [k for k, v in sorted(cat_totals.items(), key=lambda kv: abs(kv[1]), reverse=True) if v != 0]
            if not ranked_cats:
                return None

            # Se há exatamente 2 categorias (ex: Concursados vs Contratados por ano), gera barras agrupadas anuais!
            if len(ranked_cats) == 2:
                yr_labels = [f"{y}*" if y in partial_years else str(y) for y in distinct_years[:10]]
                s_list = [
                    {
                        "name": _smart_shorten_gov_label(cat_name, 26),
                        "values": [by_year_cat.get((y, cat_name), 0.0) for y in distinct_years[:10]],
                    }
                    for cat_name in ranked_cats[:2]
                ]
                return {
                    "chart_type": "line" if (wants_line and len(yr_labels) >= 3) else "grouped_vertical_bar",
                    "labels": yr_labels,
                    "series": s_list,
                    "is_monetary": _is_monetary_col(prim_col),
                    "is_pct": _is_pct_col(prim_col),
                }

            # Se há >= 3 categorias repetidas em múltiplos anos:
            # - Para métricas de ESTOQUE/MÉDIA/PERCENTUAL (não-aditivas no tempo), usa o corte do último ano (`max_yr`), NUNCA somando anos!
            # - Para métricas de FLUXO (pagamentos, despesas, licitações), consolida a soma acumulada do período.
            if len(ranked_cats) >= 3 and len(clean_rows) > len(ranked_cats):
                if is_stock_cat:
                    max_yr = distinct_years[-1]
                    latest_slice = {
                        c_name: by_year_cat.get((max_yr, c_name), 0.0)
                        for c_name in cat_totals
                        if by_year_cat.get((max_yr, c_name), 0.0) != 0
                    }
                    if latest_slice:
                        top_cats = [
                            k for k, _ in sorted(latest_slice.items(), key=lambda kv: abs(kv[1]), reverse=True)
                        ][:10]
                        yr_tag = f"{max_yr}*" if max_yr in partial_years else str(max_yr)
                        return {
                            "chart_type": "horizontal_bar",
                            "labels": [_smart_shorten_gov_label(c_name, 26) for c_name in top_cats],
                            "series": [
                                {
                                    "name": f"{_pretty_col_name(prim_col)} ({yr_tag})",
                                    "values": [latest_slice[c_name] for c_name in top_cats],
                                }
                            ],
                            "is_monetary": _is_monetary_col(prim_col),
                            "is_pct": _is_pct_col(prim_col),
                        }

                yr_span = f"{distinct_years[0]}-{distinct_years[-1]}"
                top_cats = ranked_cats[:10]
                return {
                    "chart_type": "horizontal_bar",
                    "labels": [_smart_shorten_gov_label(c_name, 26) for c_name in top_cats],
                    "series": [
                        {
                            "name": f"{_pretty_col_name(prim_col)} ({yr_span})",
                            "values": [cat_totals[c_name] for c_name in top_cats],
                        }
                    ],
                    "is_monetary": _is_monetary_col(prim_col),
                    "is_pct": _is_pct_col(prim_col),
                }

        # Caso 3: Série Temporal Vertical / Linha (1 linha por ano)
        is_time_series = bool(year_col and len(distinct_years) >= 2 and not label_candidates)
        if is_time_series and year_col:
            sorted_rows = sorted(clean_rows, key=lambda r: int(_to_float(r.get(year_col)) or 0))[:12]
            primary_col_preview = _pick_primary_metric(numeric_cols)
            is_stock_metric = _is_stock_or_non_additive_col(primary_col_preview) or any(
                tok in primary_col_preview.lower() for tok in ("populac", "idhm", "ideb", "per_capita", "pct_", "densidade", "area_")
            )
            labels = []
            for r in sorted_rows:
                yr_f = _to_float(r.get(year_col))
                yr_int = int(round(yr_f)) if yr_f is not None else 0
                yr_str = str(yr_int) if yr_f is not None and abs(yr_f - yr_int) < 1e-6 else str(r.get(year_col))
                if self._is_partial_year_row(r, yr_int, distinct_years, is_stock_metric):
                    labels.append(f"{yr_str}*")
                else:
                    labels.append(yr_str)

            # Par comparativo clássico (LOA vs Arrecadado/Executado) OU par genérico de mesma unidade e ordem de grandeza compatível
            loa_col = next((c for c in numeric_cols if "orcamento_previsto" in c.lower() or "previsaoatualizada" in c.lower() or "dotacao" in c.lower()), None)
            real_col = next(
                (
                    c for c in numeric_cols
                    if c != loa_col and (
                        "liquidada" in c.lower()
                        or "liquidado" in c.lower()
                        or "arrecadada" in c.lower()
                        or "realizado" in c.lower()
                        or "despesa_empenhada" in c.lower()
                        or "empenhado" in c.lower()
                        or "despesa_paga" in c.lower()
                    )
                ),
                None,
            )

            pair_cols: Optional[Tuple[str, str]] = None
            if loa_col and real_col and loa_col != real_col:
                pair_cols = (loa_col, real_col)
            elif len(numeric_cols) >= 2 and requested_type != "vertical_bar":
                non_pct = [c for c in numeric_cols if not _is_pct_col(c)]
                if len(non_pct) >= 2:
                    c1, c2 = non_pct[0], non_pct[1]
                    if _is_monetary_col(c1) == _is_monetary_col(c2):
                        m1 = max([abs(float(_to_float(r.get(c1)) or 0.0)) for r in sorted_rows] + [0.0])
                        m2 = max([abs(float(_to_float(r.get(c2)) or 0.0)) for r in sorted_rows] + [0.0])
                        if m1 > 0 and m2 > 0 and (0.25 <= (m1 / m2) <= 4.0):
                            pair_cols = (c1, c2)

            if pair_cols and not (requested_type == "vertical_bar"):
                c1, c2 = pair_cols
                v1 = [float(_to_float(r.get(c1)) or 0.0) for r in sorted_rows]
                v2 = [float(_to_float(r.get(c2)) or 0.0) for r in sorted_rows]
                if max([abs(x) for x in (v1 + v2)] + [0.0]) <= 0:
                    return None
                ctype_out = "line" if (wants_line and len(labels) >= 3) else "grouped_vertical_bar"
                return {
                    "chart_type": ctype_out,
                    "labels": labels,
                    "series": [
                        {"name": _pretty_col_name(c1), "values": v1},
                        {"name": _pretty_col_name(c2), "values": v2},
                    ],
                    "is_monetary": _is_monetary_col(c1),
                    "is_pct": _is_pct_col(c1),
                }

            primary_col = _pick_primary_metric(numeric_cols)
            valid_ts_pairs = [
                (lbl, float(_to_float(r.get(primary_col))))
                for lbl, r in zip(labels, sorted_rows)
                if r.get(primary_col) is not None and _to_float(r.get(primary_col)) is not None
            ]
            if len(valid_ts_pairs) >= 2:
                labels = [p[0] for p in valid_ts_pairs]
                vals = [p[1] for p in valid_ts_pairs]
            else:
                vals = [float(_to_float(r.get(primary_col)) or 0.0) for r in sorted_rows]
            if max([abs(v) for v in vals] + [0.0]) <= 0:
                return None
            ctype_out = "line" if (wants_line and len(labels) >= 3) else "vertical_bar"
            return {
                "chart_type": ctype_out,
                "labels": labels,
                "series": [{"name": _pretty_col_name(primary_col), "values": vals}],
                "is_monetary": _is_monetary_col(primary_col),
                "is_pct": _is_pct_col(primary_col),
            }

        # Caso 3b: Série Mensal/Competência + Categoria (ex.: Gasolina vs Diesel mês a mês, ou Comissionados vs Efetivos de Jan a Out)
        primary_col = _pick_primary_metric(numeric_cols)
        month_col = next(
            (
                c for c in cols
                if c != primary_col and re.search(r"(^mes$|^mes_|_mes$|num_mes|competencia|seq_dat_pagamento)", c.lower())
            ),
            None,
        )
        if month_col and month_col != primary_col and label_candidates:
            cat_m_col = label_candidates[0]
            def _fmt_month_tok(raw_m: Any) -> str:
                s = str(raw_m or "").strip()
                if re.fullmatch(r"\d{8}", s):
                    return f"{s[4:6]}/{s[2:4]}"
                if re.fullmatch(r"\d{6}", s):
                    return f"{s[4:6]}/{s[2:4]}"
                mf = _to_float(raw_m)
                if mf is not None and 1 <= int(round(mf)) <= 12:
                    return f"Mês {int(round(mf)):02d}"
                return s[:10]

            m_order: List[str] = []
            by_m_cat: Dict[Tuple[str, str], float] = {}
            cats_seen: Dict[str, float] = {}
            for r in clean_rows:
                mtok = _fmt_month_tok(r.get(month_col))
                ctok = str(r.get(cat_m_col) or "N/I").strip()
                mv = float(_to_float(r.get(primary_col)) or 0.0)
                if not mtok:
                    continue
                if mtok not in m_order:
                    m_order.append(mtok)
                by_m_cat[(mtok, ctok)] = by_m_cat.get((mtok, ctok), 0.0) + mv
                cats_seen[ctok] = cats_seen.get(ctok, 0.0) + abs(mv)

            ranked_m_cats = [k for k, v in sorted(cats_seen.items(), key=lambda kv: kv[1], reverse=True) if v > 0]
            if len(m_order) >= 2 and 1 <= len(ranked_m_cats) <= 2:
                m_slice = m_order[:12]
                return {
                    "chart_type": "line" if (wants_line and len(m_slice) >= 3) else ("grouped_vertical_bar" if len(ranked_m_cats) == 2 else "vertical_bar"),
                    "labels": m_slice,
                    "series": [
                        {
                            "name": _smart_shorten_gov_label(cn, 24),
                            "values": [by_m_cat.get((m_lbl, cn), 0.0) for m_lbl in m_slice],
                        }
                        for cn in ranked_m_cats
                    ],
                    "is_monetary": _is_monetary_col(primary_col),
                    "is_pct": _is_pct_col(primary_col),
                }

        # Caso 4: Ranking ou distribuição categórica -> Horizontal Bar Chart
        lbl_col = label_candidates[0] if label_candidates else (year_col if year_col and year_col != primary_col else None)
        if not lbl_col or lbl_col == primary_col:
            other_cols = [c for c in cols if c != primary_col]
            if not other_cols:
                return None
            lbl_col = other_cols[0]

        top_rows = clean_rows[:10]
        raw_labels = [str(r.get(lbl_col) or "N/I").strip() for r in top_rows]
        if month_col and len(set(raw_labels)) < len(raw_labels):
            labels = [
                _smart_shorten_gov_label(f"{r.get(month_col)} · {lbl_v}", 26)
                for r, lbl_v in zip(top_rows, raw_labels)
            ]
        else:
            labels = [_smart_shorten_gov_label(lbl_v, 26) for lbl_v in raw_labels]
        values = [float(_to_float(r.get(primary_col)) or 0.0) for r in top_rows]
        if max([abs(v) for v in values] + [0.0]) <= 0:
            return None
        return {
            "chart_type": "vertical_bar" if (requested_type == "vertical_bar" and len(labels) <= 6) else "horizontal_bar",
            "labels": labels,
            "series": [{"name": _pretty_col_name(primary_col), "values": values}],
            "is_monetary": _is_monetary_col(primary_col),
            "is_pct": _is_pct_col(primary_col),
        }

    def generate_chart(
        self,
        rows: Sequence[Dict[str, Any]],
        title: str = "",
        subtitle: str = "Dados públicos oficiais auditados",
        municipality: str = "",
        question: str = "",
        prefer_native_renderer: bool = True,
        chart_spec: Optional[Dict[str, Any]] = None,
    ) -> Optional[ChartArtifact]:
        spec = self.extract_chart_series(rows, question=question, chart_spec=chart_spec)
        if not spec or not spec.get("labels") or not spec.get("series"):
            return None

        eff_muni = (municipality or getattr(self, "default_municipality_label", "Prefeitura Municipal de Caeté/MG")).strip()
        city_tag = f"{getattr(self, 'municipality_name', 'Caeté')}/{getattr(self, 'state_uf', 'MG')}"
        spec_title = str((chart_spec or {}).get("title") or (chart_spec or {}).get("chart_title") or "").strip()
        s_names = [s["name"] for s in spec["series"]]
        clean_title = _derive_editorial_title(
            question=question,
            explicit_title=spec_title or title,
            series_names=s_names,
            city_tag=city_tag,
        )
        spec_sub = str((chart_spec or {}).get("subtitle") or "").strip()
        clean_sub = _truncate_words(spec_sub or subtitle or f"Fonte: Base Auditada SIM ({city_tag})", 76)
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
        has_partial_star, starred_years = _extract_starred_years(spec["labels"], spec["series"])
        if starred_years:
            partial_note = f" (*{'/'.join(starred_years)}: exercicio em execucao parcial acumulada)."
        elif has_partial_star:
            partial_note = " (*exercicio em execucao parcial acumulada)."
        else:
            partial_note = "."
        caption = (
            f"Grafico institucional gerado pelo SIM ({municipality}): {clean_title} - "
            f"Metrica(s): {', '.join(s_names)}{partial_note}"
        )
        slug_city = re.sub(r"[^a-z0-9]+", "_", _ascii_clean_upper(str(getattr(self, "municipality_name", "sim"))).lower()).strip("_") or "sim"

        return ChartArtifact(
            png_bytes=png_bytes,
            base64_png=b64,
            mime_type="image/png",
            filename=f"grafico_sim_{slug_city}.png",
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
                ax.set_ylim(0, max_v * 1.34 if max_v > 0 else 1.0)
                ax.grid(axis="y", linestyle="--", alpha=0.35, color="#CBD5E1")
                ax.legend(frameon=False, fontsize=9, loc="upper left", ncol=2)
            else:
                x_idx = list(range(len(labels)))
                vals = series[0]["values"]
                max_v = max(vals) if vals else 1.0
                bars = ax.bar(x_idx, vals, width=0.55, color=colors[0])
                ax.set_xticks(x_idx)
                ax.set_xticklabels(labels, fontsize=9, rotation=12 if len(labels) > 5 else 0, color="#0F172A")
                ax.set_ylim(0, max_v * 1.24 if max_v > 0 else 1.0)
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
            partial_footer_tag = _build_partial_footer_tag(labels, series)
            footer_txt = (
                f"SIM - Sistema de Inteligencia Municipal | {municipality}"
                + (f" | {partial_footer_tag}" if partial_footer_tag else "")
            )
            fig.text(
                0.04,
                0.015,
                footer_txt,
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

        # Cabeçalho institucional gov.br azul-marinho escuro (#0F172A) com quebra limpa em até 2 linhas
        title_lines = _wrap_words(title, max_chars_per_line=72, max_lines=2)
        header_h = 98 if len(title_lines) >= 2 else 86
        canvas.fill_rect(0, 0, width, header_h, (15, 23, 42))
        canvas.fill_rect(0, header_h, width, header_h + 4, (2, 132, 199))
        if len(title_lines) >= 2:
            canvas.draw_text(28, 14, title_lines[0], (255, 255, 255), scale=2, align="left")
            canvas.draw_text(28, 36, title_lines[1], (255, 255, 255), scale=2, align="left")
            canvas.draw_text(28, 64, _truncate_words(subtitle, 74), (186, 230, 253), scale=2, align="left")
        else:
            t_single = title_lines[0] if title_lines else "PAINEL ANALITICO OFICIAL"
            canvas.draw_text(28, 20, t_single, (255, 255, 255), scale=2, align="left")
            canvas.draw_text(28, 52, _truncate_words(subtitle, 74), (186, 230, 253), scale=2, align="left")

        # Card branco central
        card_x0, card_y0, card_x1, card_y1 = 24, header_h + 18, width - 24, height - 42
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
            s_label = _smart_shorten_gov_label(str(s_obj["name"]), 30)
            canvas.draw_text(leg_x + 26, card_y0 + 14, s_label, (30, 41, 59), scale=2, align="left")
            leg_x += 26 + len(s_label) * 12 + 28

        if ctype == "horizontal_bar":
            n = max(1, len(labels))
            vals = series[0]["values"]
            min_raw = min([0.0] + list(vals))
            max_raw = max([0.0] + list(vals))
            span_v = (max_raw - min_raw) if (max_raw - min_raw) > 0 else 1.0
            plot_x0 = card_x0 + 308
            plot_x1 = card_x1 - 176
            plot_y0 = card_y0 + 50
            plot_y1 = card_y1 - 22
            avail_h = max(60, plot_y1 - plot_y0)
            plot_w = max(80, plot_x1 - plot_x0)
            zero_x = plot_x0 + int((abs(min_raw) / span_v) * plot_w) if min_raw < 0 else plot_x0
            slot_h = avail_h // n
            bar_h = max(14, min(32, int(slot_h * 0.64)))

            for g in range(5):
                gx = plot_x0 + int(plot_w * (g / 4.0))
                canvas.fill_rect(gx, plot_y0, gx + 1, plot_y1, (226, 232, 240))
            if min_raw < 0:
                canvas.fill_rect(zero_x, plot_y0, zero_x + 2, plot_y1, (148, 163, 184))

            for idx, (lbl, val) in enumerate(zip(labels, vals)):
                cy = plot_y0 + idx * slot_h + (slot_h - bar_h) // 2
                short_lbl = _smart_shorten_gov_label(str(lbl), 24)
                canvas.draw_text(plot_x0 - 12, cy + max(0, (bar_h - 14) // 2), short_lbl, (15, 23, 42), scale=2, align="right")
                bw = int((abs(val) / span_v) * plot_w)
                v_txt = _format_value_compact(val, is_mon, is_pct)
                v_scale = 2 if len(v_txt) <= 13 else 1
                v_y = cy + max(0, (bar_h - (7 * v_scale)) // 2)
                if val >= 0:
                    bw = max(3, bw) if val > 0 else 0
                    canvas.fill_rect(zero_x, cy, zero_x + bw, cy + bar_h, self.PALETTE_RGB[0])
                    canvas.draw_text(zero_x + bw + 10, v_y, v_txt, (30, 58, 138), scale=v_scale, align="left")
                else:
                    bw = max(3, bw)
                    canvas.fill_rect(zero_x - bw, cy, zero_x, cy + bar_h, (220, 38, 38))
                    canvas.draw_text(zero_x + 10, v_y, v_txt, (185, 28, 28), scale=v_scale, align="left")
        elif ctype == "line":
            n = max(1, len(labels))
            n_series = min(2, len(series))
            raw_vals = [v for s in series[:n_series] for v in s["values"]]
            min_raw = min([0.0] + raw_vals)
            max_raw = max([0.0] + raw_vals)
            span_v = (max_raw - min_raw) if (max_raw - min_raw) > 0 else 1.0
            plot_x0 = card_x0 + 125
            plot_x1 = card_x1 - 42
            plot_y0 = card_y0 + 70
            plot_y1 = card_y1 - 48
            plot_h = max(80, plot_y1 - plot_y0)
            plot_w = max(120, plot_x1 - plot_x0)

            for g in range(5):
                frac = g / 4.0
                gy = plot_y1 - int(plot_h * frac)
                canvas.fill_rect(plot_x0, gy, plot_x1, gy + 1, (203, 213, 225) if g == 0 else (226, 232, 240))
                yv = min_raw + span_v * frac
                canvas.draw_text(plot_x0 - 10, gy - 7, _format_value_compact(yv, is_mon, is_pct), (100, 116, 139), scale=1, align="right")
            if min_raw < 0 < max_raw:
                zero_y = plot_y1 - int(((0.0 - min_raw) / span_v) * plot_h)
                canvas.fill_rect(plot_x0, zero_y, plot_x1, zero_y + 2, (148, 163, 184))

            slot_w = plot_w // n
            xs = [plot_x0 + i * slot_w + slot_w // 2 for i in range(n)]
            for s_i in range(n_series):
                col_rgb = self.PALETTE_RGB[s_i % len(self.PALETTE_RGB)]
                vals_s = series[s_i]["values"]
                ys = [plot_y1 - int(((v - min_raw) / span_v) * plot_h) for v in vals_s]
                for i in range(len(xs) - 1):
                    canvas.draw_line(xs[i], ys[i], xs[i + 1], ys[i + 1], col_rgb, thickness=3)
                for i, (px, py, val) in enumerate(zip(xs, ys, vals_s)):
                    pt_rgb = (220, 38, 38) if (n_series == 1 and val < 0) else col_rgb
                    canvas.fill_rect(px - 5, py - 5, px + 5, py + 5, pt_rgb)
                    canvas.fill_rect(px - 2, py - 2, px + 2, py + 2, (255, 255, 255))
                    v_txt = _format_value_compact(val, is_mon, is_pct)
                    v_scale = 2 if (n_series == 1 and slot_w >= len(v_txt) * 12 + 12) else 1
                    off_y = -20 if s_i == 0 else 10
                    if py + off_y < card_y0 + 42:
                        off_y = 10
                    canvas.draw_text(px, py + off_y, v_txt, pt_rgb, scale=v_scale, align="center")

            for i, lbl in enumerate(labels):
                short_lbl = _smart_shorten_gov_label(str(lbl), 11)
                canvas.draw_text(xs[i], plot_y1 + 12, short_lbl, (15, 23, 42), scale=2 if n <= 8 else 1, align="center")
        else:
            n = max(1, len(labels))
            n_series = min(2, len(series))
            raw_vals = [v for s in series[:n_series] for v in s["values"]]
            min_raw = min([0.0] + raw_vals)
            max_raw = max([0.0] + raw_vals)
            span_v = (max_raw - min_raw) if (max_raw - min_raw) > 0 else 1.0
            plot_x0 = card_x0 + 125
            plot_x1 = card_x1 - 36
            plot_y0 = card_y0 + (80 if n_series == 2 else 70)
            plot_y1 = card_y1 - 48
            plot_h = max(80, plot_y1 - plot_y0)
            plot_w = max(120, plot_x1 - plot_x0)
            zero_y = plot_y1 - int(((0.0 - min_raw) / span_v) * plot_h)

            for g in range(5):
                frac = g / 4.0
                gy = plot_y1 - int(plot_h * frac)
                canvas.fill_rect(plot_x0, gy, plot_x1, gy + 1, (203, 213, 225) if g == 0 else (226, 232, 240))
                yv = min_raw + span_v * frac
                canvas.draw_text(plot_x0 - 10, gy - 7, _format_value_compact(yv, is_mon, is_pct), (100, 116, 139), scale=1, align="right")
            if min_raw < 0:
                canvas.fill_rect(plot_x0, zero_y, plot_x1, zero_y + 2, (148, 163, 184))

            slot_w = plot_w // n
            for i, lbl in enumerate(labels):
                group_cx = plot_x0 + i * slot_w + slot_w // 2
                if n_series == 2:
                    bw = max(14, min(42, int(slot_w * 0.34)))
                    y_anchors: List[int] = []
                    for s_i in range(2):
                        val = series[s_i]["values"][i]
                        bh = int((abs(val) / span_v) * plot_h)
                        if val >= 0:
                            y_anchors.append(zero_y - (max(2, bh) if val > 0 else 0))
                        else:
                            y_anchors.append(min(plot_y1, zero_y + max(2, bh)))
                    close_bars = abs(y_anchors[0] - y_anchors[1]) < 24
                    for s_i in range(2):
                        val = series[s_i]["values"][i]
                        bx0 = (group_cx - bw - 4) if s_i == 0 else (group_cx + 4)
                        bh = int((abs(val) / span_v) * plot_h)
                        v_txt = _format_value_compact(val, is_mon, is_pct)
                        stagger = (s_i * 18) if close_bars else 0
                        if val >= 0:
                            by0 = zero_y - (max(2, bh) if val > 0 else 0)
                            canvas.fill_rect(bx0, by0, bx0 + bw, zero_y, self.PALETTE_RGB[s_i])
                            lbl_y = max(card_y0 + 46, by0 - 12 - stagger)
                            canvas.draw_text(bx0 + bw // 2, lbl_y, v_txt, self.PALETTE_RGB[s_i], scale=1, align="center")
                        else:
                            by1 = min(plot_y1, zero_y + max(2, bh))
                            canvas.fill_rect(bx0, zero_y, bx0 + bw, by1, (220, 38, 38))
                            lbl_y = min(plot_y1 - 8, by1 + 4 + stagger)
                            canvas.draw_text(bx0 + bw // 2, lbl_y, v_txt, (185, 28, 28), scale=1, align="center")
                else:
                    bw = max(18, min(64, int(slot_w * 0.56)))
                    val = series[0]["values"][i]
                    bh = int((abs(val) / span_v) * plot_h)
                    bx0 = group_cx - bw // 2
                    v_txt = _format_value_compact(val, is_mon, is_pct)
                    v_scale = 2 if (n <= 7 and slot_w >= len(v_txt) * 12 + 10) else 1
                    v_off = 18 if v_scale == 2 else 11
                    if val >= 0:
                        by0 = zero_y - (max(2, bh) if val > 0 else 0)
                        canvas.fill_rect(bx0, by0, bx0 + bw, zero_y, self.PALETTE_RGB[0])
                        canvas.draw_text(group_cx, max(card_y0 + 42, by0 - v_off), v_txt, (30, 58, 138), scale=v_scale, align="center")
                    else:
                        by1 = min(plot_y1, zero_y + max(2, bh))
                        canvas.fill_rect(bx0, zero_y, bx0 + bw, by1, (220, 38, 38))
                        lbl_y = by1 + 5 if (by1 + 18) <= plot_y1 else max(card_y0 + 42, zero_y - v_off)
                        canvas.draw_text(group_cx, lbl_y, v_txt, (185, 28, 28), scale=v_scale, align="center")

                short_lbl = _smart_shorten_gov_label(str(lbl), 11 if n <= 8 else 9)
                canvas.draw_text(group_cx, plot_y1 + 12, short_lbl, (15, 23, 42), scale=2 if n <= 8 else 1, align="center")

        # Rodapé institucional dinâmico: extrai os anos reais marcados com '*' (ex: *2026 ou *2025/2026)
        partial_footer_tag = _build_partial_footer_tag(labels, series)
        footer = (
            f"SIM - TRANSPARENCIA PUBLICA | {municipality} | {partial_footer_tag}"
            if partial_footer_tag
            else f"SIM - TRANSPARENCIA PUBLICA | {municipality} | BASE OFICIAL AUDITADA"
        )
        canvas.draw_text(28, height - 24, _truncate_words(footer, 145), (71, 85, 105), scale=1, align="left")
        return canvas.to_png_bytes()
