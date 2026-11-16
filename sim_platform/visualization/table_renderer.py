"""
Renderizador Institucional de Tabelas Visuais (PNG TrueType para WhatsApp/WAHA + HTML GovTech para Web).

Projetado sob padrão gov.br (SIM v4.3):
- Preserva 100% intacto o `_NativePNGCanvas` retrô para gráficos em `chart_generator.py`.
- Renderiza cartões PNG nítidos em alta resolução via Pillow (`PIL`) com suporte completo a UTF-8
  e acentuação portuguesa (`ç`, `ã`, `é`, `ê`, `ó`, `º`), cabeçalho institucional azul-marinho,
  linhas zebradas, alinhamento numérico à direita e linha determinística de `TOTAL GERAL` em `Decimal`.
- Guardião geométrico automático: seleciona até 6 colunas mais relevantes, limita a exibição visual
  a até 25 linhas por imagem (com nota explícita caso haja mais linhas) e quebra textos longos sem
  sobreposição.
"""

from __future__ import annotations

import base64
import html
import io
import math
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from PIL import Image, ImageDraw, ImageFont

_PERCENTAGE_COL_RE = re.compile(
    r"(^pct_|_pct$|percent|proporc|^taxa_|_taxa$|aliquota|cobertura_.*_pct|minimo_constitucional|limite_.*_pct)",
    re.IGNORECASE,
)

_NON_ADDITIVE_COL_RE = re.compile(
    r"(^ano|ano_|_ano$|num_ano|exercicio|competencia|ultima_competencia|^mes$|^mes_|_mes$|seq_|^cod_|_cod$|^id_|^id$|_id$|"
    r"quadrimestre|trimestre|bimestre|semestre|edicao|ordem|ranking|posicao|"
    r"cpf|cnpj|doc_credor|documento|nis|socios|qtd_socios|"
    r"placa|pct_|percent|proporc|taxa|aliquota|media|medio|avg|mediana|per_capita|por_aluno|por_hab|"
    r"densidade|area_territorial|km2|ideb|idhm|idh|isf_m|indice|nota_capag|limite_|minimo_|"
    r"telefone|cep|^num_|^nro_|^numero_|_numero$|convenio|edital|portaria|decreto|protocolo|processo|contrato_id)",
    re.IGNORECASE,
)

_YEAR_OR_CODE_COL_RE = re.compile(
    r"(^ano|ano_|_ano$|num_ano|exercicio|competencia|ultima_competencia|^mes$|^mes_|_mes$|seq_dat_pagamento|"
    r"quadrimestre|trimestre|bimestre|semestre|edicao|ordem|ranking|posicao|"
    r"^id$|^id_|_id$|^cod_|_cod$|^seq_|^num_|^nro_|^numero_|_numero$|"
    r"num_contrato|nro_contrato|numero_contrato|num_convenio|numero_convenio|num_licitacao|num_processo|num_empenho|numero_ato|protocolo)",
    re.IGNORECASE,
)

_MULTI_YEAR_STOCK_COL_RE = re.compile(
    r"(populacao|habitantes|qtd_escolas|qtd_leitos|qtd_ubs|qtd_estabelecimentos|"
    r"total_servidores|qtd_servidores|qtd_beneficiados|beneficiarios|familias|pessoas|socios|"
    r"total_matriculas|qtd_veiculos|total_veiculos)",
    re.IGNORECASE,
)

_MONETARY_COL_RE = re.compile(
    r"(^vlr_|_vlr|valor|total_pago|pago_|empenhad|liquidad|dotacao|orcamento|receita|despesa|"
    r"remuneracao|rmn_|brut|liquid|desconto|aditivo|repasse|arrecada|saldo|montante|custo|gasto|pib)",
    re.IGNORECASE,
)

_COUNT_COL_RE = re.compile(
    r"(^qtd_|_qtd|quantidade|registros_|total_servidores|total_contratos|total_empenhos|total_veiculos|"
    r"matriculas|escolas|leitos|populacao|beneficiarios|familias|contratos|empenhos|pagamentos)",
    re.IGNORECASE,
)

_LOW_PRIORITY_INTERNAL_COLS = {
    "seq_orgao",
    "seq_dim_pessoa",
    "seq_contrato",
    "seq_empenho",
    "seq_veiculo",
    "cod_orgao_norm",
    "administracao_tipo",
    "dsc_tipo_pag",
    "doc_credor_raiz",
}

_FRIENDLY_HEADER_MAP: Dict[str, str] = {
    "ano_referencia": "Ano",
    "ano": "Ano",
    "ano_exercicio": "Exercício",
    "exercicio": "Exercício",
    "num_exercicio": "Exercício",
    "num_anoexercicio": "Exercício",
    "nom_servidor": "Servidor / Agente",
    "dsc_cargo": "Cargo / Função",
    "cargo_politico": "Cargo Político",
    "dsc_lotacao": "Secretaria / Lotação",
    "vlr_rmn_bruta": "Remuneração Bruta",
    "vlr_rmn_liquida": "Remuneração Líquida",
    "vlr_descontos": "Descontos",
    "nom_credor": "Credor / Fornecedor",
    "nom_contratado": "Contratado",
    "num_doc_credor": "CNPJ / CPF",
    "num_cpf": "CPF",
    "num_cnpj": "CNPJ",
    "pago_exercicio": "Pago no Exercício",
    "total_pago": "Total Pago",
    "total_pago_historico": "Total Pago Histórico",
    "total_empenhado": "Total Empenhado",
    "total_liquidado": "Total Liquidado",
    "dotacao_inicial_loa": "Dotação Inicial (LOA)",
    "dotacao_atualizada": "Dotação Atualizada",
    "despesa_empenhada": "Despesa Empenhada",
    "despesa_liquidada": "Despesa Liquidada",
    "despesa_paga": "Despesa Paga",
    "orcamento_previsto_loa": "Orçamento Previsto (LOA)",
    "orcamento_atualizado_loa": "Orçamento Atualizado",
    "receita_arrecadada_realizada": "Receita Arrecadada",
    "receita_arrecadada_propria_orgao": "Receita Própria",
    "nom_orgao": "Órgão Municipal",
    "esfera_poder": "Poder",
    "natureza_credor": "Natureza",
    "qtd_pagamentos": "Qtd. Pagamentos",
    "qtd_empenhos": "Qtd. Empenhos",
    "qtd_aditivos": "Aditivos",
    "vlr_original_contrato": "Valor Original",
    "vlr_global_atualizado": "Valor Atualizado",
    "dsc_funcao": "Função Orçamentária",
    "seq_dat_pagamento": "Competência",
}


@dataclass
class TableArtifact:
    image_base64: str
    mime_type: str
    caption: str
    html: str
    displayed_rows: int
    total_rows: int
    columns_rendered: List[str]
    totals_by_column: Dict[str, str]


def _load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates: List[str] = []
    if bold:
        candidates.extend(
            [
                "C:/Windows/Fonts/segoeuib.ttf",
                "C:/Windows/Fonts/arialbd.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            ]
        )
    else:
        candidates.extend(
            [
                "C:/Windows/Fonts/segoeui.ttf",
                "C:/Windows/Fonts/arial.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            ]
        )
    for fp in candidates:
        if Path(fp).exists():
            try:
                return ImageFont.truetype(fp, size=size)
            except Exception:
                continue
    try:
        return ImageFont.load_default(size=size)
    except TypeError:
        return ImageFont.load_default()


def _to_decimal(val: Any) -> Optional[Decimal]:
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, Decimal):
        if val.is_nan() or val.is_infinite():
            return None
        return val
    if isinstance(val, int):
        return Decimal(val)
    if isinstance(val, float):
        if not math.isfinite(val):
            return None
        return Decimal(str(round(val, 4)))
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        # Evita tratar strings de documento (00123456000199) como número somável
        if len(s) >= 11 and s.isdigit() and s.startswith("0"):
            return None
        if re.fullmatch(r"-?\d+(\.\d+)?", s):
            try:
                return Decimal(s)
            except InvalidOperation:
                return None
    return None


def format_brl_decimal(val: Decimal) -> str:
    q = val.quantize(Decimal("0.01"))
    sign = "-" if q < 0 else ""
    q_abs = abs(q)
    int_part = int(q_abs)
    frac_part = int((q_abs - Decimal(int_part)) * 100)
    int_fmt = f"{int_part:,}".replace(",", ".")
    return f"{sign}R$ {int_fmt},{frac_part:02d}"


def format_int_pt(val: Decimal) -> str:
    int_val = int(val.to_integral_value())
    return f"{int_val:,}".replace(",", ".")


def _friendly_col_label(col_name: str) -> str:
    low = col_name.lower().strip()
    if low in _FRIENDLY_HEADER_MAP:
        return _FRIENDLY_HEADER_MAP[low]
    cleaned = re.sub(r"^(vlr_|dsc_|nom_|num_|cod_|qtd_|ind_)", "", low)
    words = [w for w in cleaned.replace("_", " ").split() if w]
    if not words:
        return col_name
    acronyms = {"cpf", "cnpj", "loa", "lrf", "rcl", "ideb", "ibge", "sus", "ubs", "qsa", "saae", "inss", "fgts"}
    out = [w.upper() if w in acronyms else w.capitalize() for w in words]
    return " ".join(out)


def _classify_column(col_name: str, rows: Sequence[Dict[str, Any]]) -> str:
    """Retorna 'monetary', 'non_additive_monetary', 'percentage', 'integer_additive', 'year_or_code', ou 'text'."""
    low = col_name.lower().strip()
    non_null_vals = [r.get(col_name) for r in rows if r.get(col_name) is not None and str(r.get(col_name)).strip() != ""]
    if not non_null_vals:
        return "text"

    if re.search(r"(cpf|cnpj|doc_credor|documento|nis|cod_ibge)", low):
        return "text"
    if _YEAR_OR_CODE_COL_RE.search(low):
        return "year_or_code"

    decs = [_to_decimal(v) for v in non_null_vals]
    valid_decs = [d for d in decs if d is not None]
    if (
        valid_decs
        and len(valid_decs) == len(non_null_vals)
        and re.search(r"(ano|exerc|perio)", low)
        and all(d == d.to_integral_value() and Decimal(1990) <= d <= Decimal(2100) for d in valid_decs)
    ):
        return "year_or_code"

    numeric_ratio = len(valid_decs) / max(1, len(non_null_vals))
    if numeric_ratio < 0.8:
        return "text"

    if _PERCENTAGE_COL_RE.search(low):
        return "percentage"
    if _NON_ADDITIVE_COL_RE.search(low):
        if _MONETARY_COL_RE.search(low) or "per_capita" in low or "por_aluno" in low:
            return "non_additive_monetary"
        return "year_or_code"
    if _MONETARY_COL_RE.search(low):
        return "monetary"
    if _COUNT_COL_RE.search(low):
        return "integer_additive"

    # Se todos têm casas decimais ou valores grandes típicos de moeda e não é código/ano
    has_decimals = any(isinstance(v, float) or (isinstance(v, Decimal) and v != v.to_integral_value()) for v in non_null_vals)
    if has_decimals:
        return "monetary"
    return "integer_additive"


def _format_cell_value(val: Any, col_kind: str, col_name: str) -> str:
    if val is None:
        return "—"
    if col_kind in ("monetary", "non_additive_monetary"):
        d = _to_decimal(val)
        if d is not None:
            return format_brl_decimal(d)
    elif col_kind == "percentage":
        d = _to_decimal(val)
        if d is not None:
            s = f"{float(d):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            return f"{s}%"
    elif col_kind == "integer_additive":
        d = _to_decimal(val)
        if d is not None:
            if d == d.to_integral_value():
                return format_int_pt(d)
            s = f"{float(d):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            return s
    elif col_kind == "year_or_code":
        s = str(val).strip()
        if re.fullmatch(r"\d+\.0+", s):
            s = s.split(".")[0]
        if col_name.lower() == "seq_dat_pagamento" and len(s) == 8 and s.isdigit():
            return f"{s[4:6]}/{s[:4]}"
        return s

    s = str(val).strip()
    # Formata CNPJ de 14 dígitos limpos para leitura institucional
    if len(s) == 14 and s.isdigit() and re.search(r"(cnpj|doc_credor|documento)", col_name.lower()):
        return f"{s[:2]}.{s[2:5]}.{s[5:8]}/{s[8:12]}-{s[12:]}"
    return s or "—"


def _select_best_columns(columns: Sequence[str], rows: Sequence[Dict[str, Any]], max_cols: int = 6) -> List[str]:
    non_empty_cols = [
        c for c in columns
        if any(r.get(c) is not None and str(r.get(c)).strip() not in ("", "None") for r in rows)
    ]
    if not non_empty_cols:
        return list(columns[:max_cols])
    if len(non_empty_cols) <= max_cols:
        return non_empty_cols

    prioritized = [c for c in non_empty_cols if c.lower() not in _LOW_PRIORITY_INTERNAL_COLS]
    if len(prioritized) <= max_cols:
        return prioritized
    return prioritized[:max_cols]


def _wrap_cell_text(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    max_px: int,
    max_lines: int = 2,
) -> List[str]:
    text = re.sub(r"\s+", " ", (text or "").strip()) or "—"
    if draw.textlength(text, font=font) <= max_px:
        return [text]

    words = text.split(" ")
    lines: List[str] = []
    cur = ""
    for w in words:
        cand = f"{cur} {w}".strip() if cur else w
        if draw.textlength(cand, font=font) <= max_px:
            cur = cand
        else:
            if cur:
                lines.append(cur)
                cur = w
            else:
                # Palavra única maior que a largura da coluna
                cut = w
                while len(cut) > 4 and draw.textlength(cut + "…", font=font) > max_px:
                    cut = cut[:-1]
                lines.append(cut + "…")
                cur = ""
            if len(lines) >= max_lines:
                break

    if cur and len(lines) < max_lines:
        lines.append(cur)
    elif cur and lines:
        last = lines[-1]
        while len(last) > 4 and draw.textlength(last + "…", font=font) > max_px:
            last = last[:-1]
        lines[-1] = last.rstrip(".,; ") + "…"

    # Garantir que nenhuma linha ultrapasse max_px
    for idx, ln in enumerate(lines):
        while len(ln) > 4 and draw.textlength(ln, font=font) > max_px:
            ln = ln[:-2] + "…"
        lines[idx] = ln
    return lines[:max_lines]


class InstitutionalTableRenderer:
    """
    Renderiza tabelas analíticas em cartão PNG institucional nítido (Pillow TrueType)
    e HTML responsivo gov.br, com linha opcional determinística de `TOTAL GERAL`.
    """

    MAX_VISUAL_ROWS = 25
    MAX_VISUAL_COLS = 6

    def __init__(
        self,
        municipality_name: str = "Caeté",
        state_uf: str = "MG",
    ) -> None:
        self.municipality_name = municipality_name
        self.state_uf = state_uf

    def build_clean_title(self, question: str) -> str:
        q = re.sub(r"\s+", " ", (question or "").strip()).rstrip("?.!:")
        q = re.sub(
            r"^(?:por\s+favor[,\s]*|me\s+(?:gere|traga|mostre|da|dê|envie|liste|faca|faça)\s+|"
            r"(?:gere|monte|crie|exiba|mostre|apresente|liste)\s+(?:uma\s+|um\s+)?(?:tabela|quadro|planilha)\s+"
            r"(?:com\s+|de\s+|dos\s+|das\s+|sobre\s+)?)",
            "",
            q,
            flags=re.IGNORECASE,
        ).strip()
        if not q:
            return f"Demonstrativo Oficial — {self.municipality_name}/{self.state_uf}"
        title = q[0].upper() + q[1:]
        if len(title) > 74:
            cut_t = title[:71]
            if " " in cut_t:
                cut_t = cut_t.rsplit(" ", 1)[0]
            title = cut_t.rstrip(" ,;:-") + "…"
        return title

    def can_render(self, rows: Sequence[Dict[str, Any]], columns: Optional[Sequence[str]] = None) -> bool:
        if not rows or len(rows) < 1:
            return False
        cols = list(columns) if columns else list(rows[0].keys())
        if len(cols) < 2 and len(rows) < 2:
            return False
        return True

    def render(
        self,
        rows: Sequence[Dict[str, Any]],
        columns: Optional[Sequence[str]] = None,
        title: Optional[str] = None,
        subtitle: Optional[str] = None,
        include_totals: bool = True,
        truncated_by_security_limit: bool = False,
    ) -> Optional[TableArtifact]:
        if not self.can_render(rows, columns):
            return None

        raw_cols = list(columns) if columns else list(rows[0].keys())
        sel_cols = _select_best_columns(raw_cols, rows, max_cols=self.MAX_VISUAL_COLS)
        if not sel_cols:
            return None

        total_rows_count = len(rows)
        vis_rows = list(rows[: self.MAX_VISUAL_ROWS])
        was_visually_sliced = total_rows_count > len(vis_rows)

        col_kinds = {c: _classify_column(c, rows) for c in sel_cols}
        headers = {c: _friendly_col_label(c) for c in sel_cols}

        # Evitar somar salários de múltiplos meses distintos se a query trouxer série mensal de folha
        distinct_pay_months = {
            str(r.get("seq_dat_pagamento"))
            for r in rows
            if r.get("seq_dat_pagamento") is not None
        }
        has_multi_month_payroll = len(distinct_pay_months) > 1 and any(
            "rmn_" in c.lower() or "remuneracao" in c.lower() for c in sel_cols
        )

        year_cols_detected = [
            c for c in raw_cols
            if re.search(r"(^ano|ano_|_ano$|num_ano|exercicio)", c.lower())
        ]
        distinct_years_in_rows: set[str] = set()
        for yc in year_cols_detected:
            for r in rows:
                yv = r.get(yc)
                if yv is not None and str(yv).strip() != "":
                    distinct_years_in_rows.add(str(yv).strip().split(".")[0])
        has_multiple_years = len(distinct_years_in_rows) > 1

        # Soma determinística Decimal: se houve corte visual (was_visually_sliced) ou limite de query (truncated_by_security_limit),
        # soma estritamente vis_rows e rotula como TOTAL PARCIAL (EXIBIDO) (Invariante F5.3)
        is_partial_slice = bool(truncated_by_security_limit or was_visually_sliced)
        totals_formatted: Dict[str, str] = {}
        if include_totals and len(vis_rows) >= 2 and not has_multi_month_payroll:
            sum_scope = vis_rows if is_partial_slice else rows
            for c in sel_cols:
                kind = col_kinds[c]
                if has_multiple_years and _MULTI_YEAR_STOCK_COL_RE.search(c.lower()):
                    continue
                if kind in ("monetary", "integer_additive"):
                    acc = Decimal("0.00")
                    valid_n = 0
                    for r in sum_scope:
                        d = _to_decimal(r.get(c))
                        if d is not None:
                            acc += d
                            valid_n += 1
                    if valid_n >= 2:
                        totals_formatted[c] = (
                            format_brl_decimal(acc) if kind == "monetary" else format_int_pt(acc)
                        )

        formatted_matrix: List[List[str]] = []
        for r in vis_rows:
            formatted_matrix.append([_format_cell_value(r.get(c), col_kinds[c], c) for c in sel_cols])

        clean_title = (
            self.build_clean_title(title)
            if title
            else f"Demonstrativo Oficial — {self.municipality_name}/{self.state_uf}"
        ).strip()
        clean_sub = (
            subtitle
            or f"Prefeitura Municipal de {self.municipality_name}/{self.state_uf} · Portal SIM de Dados Abertos"
        ).strip()

        footer_parts: List[str] = [
            f"Fonte: Base Auditável SIM — {self.municipality_name}/{self.state_uf}"
        ]
        if was_visually_sliced:
            footer_parts.append(
                f"Exibindo os {len(vis_rows)} primeiros registros de {total_rows_count} apurados"
            )
            if totals_formatted:
                footer_parts.append(f"Total Parcial referente aos {len(vis_rows)} registros exibidos")
        else:
            footer_parts.append(f"{total_rows_count} registro(s)")
        footer_text = " · ".join(footer_parts)

        totals_label = (
            "TOTAL PARCIAL (EXIBIDO)"
            if is_partial_slice
            else "TOTAL GERAL"
        )

        png_bytes = self._render_png_bytes(
            sel_cols=sel_cols,
            headers=headers,
            col_kinds=col_kinds,
            formatted_matrix=formatted_matrix,
            totals_formatted=totals_formatted,
            totals_label=totals_label,
            title=clean_title,
            subtitle=clean_sub,
            footer_text=footer_text,
            was_visually_sliced=was_visually_sliced,
            total_rows_count=total_rows_count,
        )
        b64 = base64.b64encode(png_bytes).decode("ascii")
        html_str = self._build_html_table(
            sel_cols=sel_cols,
            headers=headers,
            col_kinds=col_kinds,
            formatted_matrix=formatted_matrix,
            totals_formatted=totals_formatted,
            totals_label=totals_label,
            title=clean_title,
            footer_text=footer_text,
        )
        caption = f"{clean_title} ({len(vis_rows)} de {total_rows_count} registros)" if was_visually_sliced else f"{clean_title} ({total_rows_count} registros)"

        return TableArtifact(
            image_base64=b64,
            mime_type="image/png",
            caption=caption,
            html=html_str,
            displayed_rows=len(vis_rows),
            total_rows=total_rows_count,
            columns_rendered=sel_cols,
            totals_by_column=totals_formatted,
        )

    def _render_png_bytes(
        self,
        sel_cols: List[str],
        headers: Dict[str, str],
        col_kinds: Dict[str, str],
        formatted_matrix: List[List[str]],
        totals_formatted: Dict[str, str],
        totals_label: str,
        title: str,
        subtitle: str,
        footer_text: str,
        was_visually_sliced: bool,
        total_rows_count: int,
    ) -> bytes:
        f_title = _load_font(22, bold=True)
        f_sub = _load_font(14, bold=False)
        f_hdr = _load_font(15, bold=True)
        f_cell = _load_font(15, bold=False)
        f_tot = _load_font(15, bold=True)
        f_foot = _load_font(13, bold=False)

        dummy = Image.new("RGB", (10, 10), (255, 255, 255))
        d_measure = ImageDraw.Draw(dummy)

        pad_x = 16
        outer_margin = 24
        col_widths: List[int] = []
        for idx, c in enumerate(sel_cols):
            kind = col_kinds[c]
            hdr_w = int(d_measure.textlength(headers[c], font=f_hdr)) + pad_x * 2 + 8
            cell_max_w = 0
            for row_vals in formatted_matrix:
                w = int(d_measure.textlength(row_vals[idx], font=f_cell)) + pad_x * 2 + 6
                if w > cell_max_w:
                    cell_max_w = w
            tot_w = 0
            if c in totals_formatted:
                tot_w = int(d_measure.textlength(totals_formatted[c], font=f_tot)) + pad_x * 2 + 10
            elif idx == 0 and totals_formatted:
                tot_w = int(d_measure.textlength(totals_label, font=f_tot)) + pad_x * 2 + 10

            ideal = max(hdr_w, cell_max_w, tot_w, 105)
            cap = 220 if kind in ("monetary", "non_additive_monetary", "percentage", "integer_additive", "year_or_code") else 370
            col_widths.append(min(ideal, cap))

        min_table_w = 820
        cur_table_w = sum(col_widths)
        if cur_table_w < min_table_w and col_widths:
            extra = min_table_w - cur_table_w
            text_indices = [i for i, c in enumerate(sel_cols) if col_kinds[c] == "text"]
            targets = text_indices if text_indices else list(range(len(col_widths)))
            add_each = extra // len(targets)
            for i in targets:
                col_widths[i] += add_each
            col_widths[targets[0]] += extra - add_each * len(targets)

        table_w = sum(col_widths)
        img_w = table_w + outer_margin * 2

        # Pré-calcular quebras de linha das células e cabeçalhos
        wrapped_headers: List[List[str]] = [
            _wrap_cell_text(d_measure, headers[c], f_hdr, col_widths[i] - pad_x * 2, max_lines=2)
            for i, c in enumerate(sel_cols)
        ]
        hdr_lines_max = max(len(lines) for lines in wrapped_headers)
        hdr_h = 22 + hdr_lines_max * 20

        wrapped_rows: List[Tuple[List[List[str]], int]] = []
        line_h = 20
        for row_vals in formatted_matrix:
            r_wrap: List[List[str]] = []
            max_ln = 1
            for i, val_s in enumerate(row_vals):
                max_l = 1 if col_kinds[sel_cols[i]] in ("monetary", "non_additive_monetary", "percentage", "integer_additive", "year_or_code") else 2
                w_lines = _wrap_cell_text(d_measure, val_s, f_cell, col_widths[i] - pad_x * 2, max_lines=max_l)
                r_wrap.append(w_lines)
                if len(w_lines) > max_ln:
                    max_ln = len(w_lines)
            row_h = 18 + max_ln * line_h
            wrapped_rows.append((r_wrap, row_h))

        max_meta_text_w = max(200, table_w - 48)
        title_lines = _wrap_cell_text(d_measure, title, f_title, max_meta_text_w, max_lines=2)
        subtitle_lines = _wrap_cell_text(d_measure, subtitle, f_sub, max_meta_text_w, max_lines=2)
        footer_lines = _wrap_cell_text(d_measure, footer_text, f_foot, max_meta_text_w, max_lines=2)

        title_line_h = 26
        sub_line_h = 19
        foot_line_h = 18

        banner_h = max(84, 14 + len(title_lines) * title_line_h + 6 + len(subtitle_lines) * sub_line_h + 14)
        tot_h = 44 if totals_formatted else 0
        foot_h = 44 if len(footer_lines) <= 1 else (20 + len(footer_lines) * foot_line_h)
        body_h = sum(rh for _, rh in wrapped_rows)
        img_h = outer_margin * 2 + banner_h + hdr_h + body_h + tot_h + foot_h

        # Paleta Institucional gov.br
        bg_page = (244, 247, 251)
        navy_gov = (7, 29, 65)        # #071D41
        blue_gov = (19, 81, 180)      # #1351B4
        hdr_bg = (229, 238, 250)      # #E5EEFA
        zebra_even = (255, 255, 255)
        zebra_odd = (248, 250, 253)
        tot_bg = (220, 232, 249)
        border_col = (209, 218, 230)
        text_dark = (22, 34, 53)
        text_muted = (88, 102, 126)

        img = Image.new("RGB", (img_w, img_h), bg_page)
        draw = ImageDraw.Draw(img)

        x0 = outer_margin
        y0 = outer_margin
        x1 = img_w - outer_margin

        # Sombra sutil + Cartão branco
        draw.rectangle([x0 + 2, y0 + 3, x1 + 2, img_h - outer_margin + 3], fill=(218, 225, 235))
        draw.rectangle([x0, y0, x1, img_h - outer_margin], fill=(255, 255, 255), outline=border_col, width=1)

        # Faixa superior institucional azul-marinho
        draw.rectangle([x0, y0, x1, y0 + banner_h], fill=navy_gov)
        draw.rectangle([x0, y0 + banner_h - 4, x1, y0 + banner_h], fill=blue_gov)

        ty_banner = y0 + 14
        for t_ln in title_lines:
            draw.text((x0 + 18, ty_banner), t_ln, font=f_title, fill=(255, 255, 255))
            ty_banner += title_line_h
        ty_banner += 6
        for s_ln in subtitle_lines:
            draw.text((x0 + 18, ty_banner), s_ln, font=f_sub, fill=(197, 214, 242))
            ty_banner += sub_line_h

        # Cabeçalho das colunas
        cur_y = y0 + banner_h
        draw.rectangle([x0, cur_y, x1, cur_y + hdr_h], fill=hdr_bg)
        draw.line([x0, cur_y + hdr_h, x1, cur_y + hdr_h], fill=blue_gov, width=2)

        cur_x = x0
        for i, c in enumerate(sel_cols):
            cw = col_widths[i]
            right_align = col_kinds[c] in ("monetary", "non_additive_monetary", "percentage", "integer_additive")
            h_lines = wrapped_headers[i]
            block_h = len(h_lines) * 20
            ty = cur_y + (hdr_h - block_h) // 2
            for ln in h_lines:
                tw = draw.textlength(ln, font=f_hdr)
                tx = (cur_x + cw - pad_x - tw) if right_align else (cur_x + pad_x)
                draw.text((tx, ty), ln, font=f_hdr, fill=navy_gov)
                ty += 20
            if i > 0:
                draw.line([cur_x, cur_y + 6, cur_x, cur_y + hdr_h - 6], fill=border_col, width=1)
            cur_x += cw

        cur_y += hdr_h

        # Corpo da tabela (linhas zebradas)
        for r_idx, (r_wrap, rh) in enumerate(wrapped_rows):
            row_fill = zebra_even if (r_idx % 2 == 0) else zebra_odd
            draw.rectangle([x0, cur_y, x1, cur_y + rh], fill=row_fill)
            draw.line([x0, cur_y + rh, x1, cur_y + rh], fill=border_col, width=1)

            cur_x = x0
            for i, c in enumerate(sel_cols):
                cw = col_widths[i]
                right_align = col_kinds[c] in ("monetary", "non_additive_monetary", "percentage", "integer_additive")
                c_lines = r_wrap[i]
                block_h = len(c_lines) * line_h
                ty = cur_y + (rh - block_h) // 2
                for ln in c_lines:
                    tw = draw.textlength(ln, font=f_cell)
                    tx = (cur_x + cw - pad_x - tw) if right_align else (cur_x + pad_x)
                    draw.text((tx, ty), ln, font=f_cell, fill=text_dark)
                    ty += line_h
                if i > 0:
                    draw.line([cur_x, cur_y, cur_x, cur_y + rh], fill=(235, 240, 247), width=1)
                cur_x += cw
            cur_y += rh

        # Linha determinística de TOTAL GERAL
        if totals_formatted:
            draw.rectangle([x0, cur_y, x1, cur_y + tot_h], fill=tot_bg)
            draw.line([x0, cur_y, x1, cur_y], fill=blue_gov, width=2)
            draw.line([x0, cur_y + tot_h, x1, cur_y + tot_h], fill=navy_gov, width=2)
            cur_x = x0
            label_placed = False
            for i, c in enumerate(sel_cols):
                cw = col_widths[i]
                if c in totals_formatted:
                    val_s = totals_formatted[c]
                    tw = draw.textlength(val_s, font=f_tot)
                    tx = cur_x + cw - pad_x - tw
                    draw.text((tx, cur_y + 12), val_s, font=f_tot, fill=navy_gov)
                elif not label_placed:
                    draw.text((cur_x + pad_x, cur_y + 12), totals_label, font=f_tot, fill=navy_gov)
                    label_placed = True
                cur_x += cw
            cur_y += tot_h

        # Rodapé institucional de rastreabilidade
        draw.rectangle([x0, cur_y, x1, cur_y + foot_h], fill=(248, 250, 253))
        fy_foot = cur_y + max(8, (foot_h - len(footer_lines) * foot_line_h) // 2)
        for f_ln in footer_lines:
            draw.text((x0 + 18, fy_foot), f_ln, font=f_foot, fill=text_muted)
            fy_foot += foot_line_h

        # Contorno externo final
        draw.rectangle([x0, y0, x1, img_h - outer_margin], outline=border_col, width=1)

        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        return buf.getvalue()

    def _build_html_table(
        self,
        sel_cols: List[str],
        headers: Dict[str, str],
        col_kinds: Dict[str, str],
        formatted_matrix: List[List[str]],
        totals_formatted: Dict[str, str],
        totals_label: str,
        title: str,
        footer_text: str,
    ) -> str:
        ths: List[str] = []
        for c in sel_cols:
            align = "right" if col_kinds[c] in ("monetary", "non_additive_monetary", "percentage", "integer_additive") else "left"
            ths.append(
                f'<th style="padding:9px 12px;text-align:{align};background:#E5EEFA;color:#071D41;'
                f'border-bottom:2px solid #1351B4;font-weight:700;font-size:12px;white-space:nowrap;">'
                f"{html.escape(headers[c])}</th>"
            )

        trs: List[str] = []
        for idx, row_vals in enumerate(formatted_matrix):
            bg = "#FFFFFF" if idx % 2 == 0 else "#F8FAFD"
            tds: List[str] = []
            for i, c in enumerate(sel_cols):
                align = "right" if col_kinds[c] in ("monetary", "non_additive_monetary", "percentage", "integer_additive") else "left"
                fw = "600" if col_kinds[c] in ("monetary", "non_additive_monetary") else "400"
                tds.append(
                    f'<td style="padding:8px 12px;text-align:{align};border-bottom:1px solid #E5EAF2;'
                    f'color:#162235;font-size:12.5px;font-weight:{fw};">{html.escape(row_vals[i])}</td>'
                )
            trs.append(f'<tr style="background:{bg};">{"".join(tds)}</tr>')

        tfoot_html = ""
        if totals_formatted:
            tf_cells: List[str] = []
            placed = False
            for c in sel_cols:
                if c in totals_formatted:
                    tf_cells.append(
                        f'<td style="padding:9px 12px;text-align:right;background:#DCE8F9;color:#071D41;'
                        f'font-weight:700;font-size:12.5px;border-top:2px solid #1351B4;">'
                        f"{html.escape(totals_formatted[c])}</td>"
                    )
                elif not placed:
                    tf_cells.append(
                        f'<td style="padding:9px 12px;text-align:left;background:#DCE8F9;color:#071D41;'
                        f'font-weight:700;font-size:12.5px;border-top:2px solid #1351B4;">{html.escape(totals_label)}</td>'
                    )
                    placed = True
                else:
                    tf_cells.append(
                        '<td style="padding:9px 12px;background:#DCE8F9;border-top:2px solid #1351B4;"></td>'
                    )
            tfoot_html = f"<tfoot><tr>{''.join(tf_cells)}</tr></tfoot>"

        return (
            '<div class="sim-gov-table-card" style="margin:10px 0;border:1px solid #D1DAE6;border-radius:8px;'
            'overflow:hidden;background:#FFFFFF;box-shadow:0 1px 4px rgba(7,29,65,0.06);">'
            '<div style="background:#071D41;color:#FFFFFF;padding:10px 14px;border-bottom:3px solid #1351B4;'
            'font-weight:700;font-size:13px;">'
            f"{html.escape(title)}</div>"
            '<div style="overflow-x:auto;">'
            '<table style="width:100%;border-collapse:collapse;font-family:\'Rawline\',\'Segoe UI\',sans-serif;">'
            f"<thead><tr>{''.join(ths)}</tr></thead>"
            f"<tbody>{''.join(trs)}</tbody>"
            f"{tfoot_html}"
            "</table></div>"
            '<div style="padding:7px 14px;background:#F8FAFD;color:#58667E;font-size:11px;'
            'border-top:1px solid #E5EAF2;">'
            f"{html.escape(footer_text)}</div></div>"
        )
