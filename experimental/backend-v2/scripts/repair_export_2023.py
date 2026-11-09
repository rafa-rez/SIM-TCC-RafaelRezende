#!/usr/bin/env python3
"""Repara o export 2023 (colunas deslocadas por '|' embutido na fonte de recurso).

Causa: descrições de fonte de recurso da EC 123/2022 contêm o caractere '|'
(ex.: "... ART. 5º | INCISO IV | EC Nº 123/2022"), que o export não escapou.
As colunas seguintes deslocam e as últimas são truncadas.

Reparo: para cada linha com vlr_* não-numérico, funde k campos a partir da
coluna de fonte de recurso (menor k que torna todas as vlr_* seguintes
numéricas) e preenche os k campos truncados no fim com vazio (NULL).

NÃO altera os dados v1: escreve em --out (padrão: dados/database_fix2023/),
copiando apenas os arquivos de 2023 afetados.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

AVALIACAO_ROOT = Path(__file__).resolve().parents[3] / "avaliacao"

# tabela (sufixo do arquivo) -> nome da coluna de fonte de recurso
FONTE_COL = {
    "contrato.creditoContrato": "dsc_fonte_recurso",
    "despesa.despesa": "dsc_fonterecurso",
    "despesa.liquidacaoFonte": "dsc_fonte_recurso",
    "despesa.pagamento": "dsc_fonte_recurso",
    "empenho.empenhoFonte": "dsc_fonte_recurso",
    "empenho.movFonteRsp": "dsc_fonte_recurso",
    "licitacao.recLicitacao": "dsc_fonte_recurso",
    "receita.receita": "dsc_fonterecurso",
}

MAX_MERGE = 4


def is_num(s: str) -> bool:
    s = s.strip()
    if s == "":
        return True  # vazio é aceitável em colunas de valor
    try:
        float(s)
        return True
    except ValueError:
        return False


def row_ok(fields: list[str], vlr_idx: list[int]) -> bool:
    return all(i < len(fields) and is_num(fields[i]) for i in vlr_idx)


def repair_line(fields: list[str], fonte_idx: int, vlr_idx: list[int], n_cols: int):
    """Tenta fundir k campos na coluna de fonte; retorna (linha_reparada, k) ou (None, 0)."""
    for k in range(1, MAX_MERGE + 1):
        # Junta com "/" (NUNCA com "|", que é o delimitador do arquivo).
        merged = (
            fields[:fonte_idx]
            + [" / ".join(f.strip() for f in fields[fonte_idx : fonte_idx + k + 1])]
            + fields[fonte_idx + k + 1 :]
        )
        merged = merged + [""] * (n_cols - len(merged))
        if len(merged) == n_cols and row_ok(merged, vlr_idx):
            return merged, k
    return None, 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", default=str(AVALIACAO_ROOT / "dados" / "database"))
    parser.add_argument("--out", default=str(AVALIACAO_ROOT / "dados" / "database_fix2023"))
    args = parser.parse_args()

    src = Path(args.src)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    total_fix = total_fail = 0
    for sufixo, fonte_col in FONTE_COL.items():
        matches = sorted(src.glob(f"2023.*.{sufixo}.csv"))
        for path in matches:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
            header = lines[0].split("|")
            n_cols = len(header)
            try:
                fonte_idx = header.index(fonte_col)
            except ValueError:
                print(f"AVISO: {path.name} sem coluna {fonte_col}; pulado.")
                continue
            vlr_idx = [i for i, h in enumerate(header) if h.startswith("vlr_")]

            fixed, failed = 0, 0
            new_lines = [lines[0]]
            for line in lines[1:]:
                fields = line.split("|")
                if len(fields) == n_cols and row_ok(fields, vlr_idx):
                    new_lines.append(line)
                    continue
                repaired, k = repair_line(fields, fonte_idx, vlr_idx, n_cols)
                if repaired:
                    new_lines.append("|".join(repaired))
                    fixed += 1
                else:
                    new_lines.append(line)  # mantém original para inspeção
                    failed += 1

            (out / path.name).write_text("\n".join(new_lines) + "\n", encoding="utf-8")
            total_fix += fixed
            total_fail += failed
            print(f"{path.name}: {fixed} reparadas, {failed} irreparáveis")

    # Reparo de remuneracao.municipio (2023–2025): ';' / '|' embutido em dsc_req_crg desloca cod_municipio (3110004) em +1 ou +2
    for path in sorted(src.glob("*remuneracao*municipio.csv")):
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        if not lines:
            continue
        header = lines[0].split("|")
        n_cols = len(header)
        if "dsc_req_crg" not in header or "cod_municipio" not in header:
            continue
        req_idx = header.index("dsc_req_crg")
        mun_idx = header.index("cod_municipio")
        fixed, failed = 0, 0
        new_lines = [lines[0]]
        for line in lines[1:]:
            fields = line.split("|")
            if len(fields) == n_cols and fields[mun_idx].strip() == "3110004":
                new_lines.append(line)
                continue
            repaired_row = None
            for k in (1, 2, 3):
                merged = (
                    fields[:req_idx]
                    + [" / ".join(f.strip() for f in fields[req_idx : req_idx + k + 1])]
                    + fields[req_idx + k + 1 :]
                )
                merged = merged + [""] * (n_cols - len(merged))
                if len(merged) == n_cols and merged[mun_idx].strip() == "3110004":
                    repaired_row = merged
                    break
            if repaired_row is not None:
                new_lines.append("|".join(repaired_row))
                fixed += 1
            else:
                new_lines.append(line)
                failed += 1
        if fixed > 0:
            (out / path.name).write_text("\n".join(new_lines) + "\n", encoding="utf-8")
            total_fix += fixed
            total_fail += failed
            print(f"{path.name}: {fixed} contracheques reparados, {failed} irreparáveis")

    print(f"\nTotal: {total_fix} linhas reparadas | {total_fail} não reparadas | saída: {out}")
    if total_fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
