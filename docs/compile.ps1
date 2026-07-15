param([switch]$All)
$ErrorActionPreference = "Stop"
$env:PATH = "D:\texlive\2026\bin\windows;" + $env:PATH
$Root = Split-Path $PSScriptRoot -Parent
$Repo = Split-Path $Root -Parent

function Build-Artigo {
    Set-Location "$Root\artigo"
    pdflatex -interaction=nonstopmode sim_artigo_sbc_v1.tex | Out-Null
    bibtex sim_artigo_sbc_v1 | Out-Null
    pdflatex -interaction=nonstopmode sim_artigo_sbc_v1.tex | Out-Null
    pdflatex -interaction=nonstopmode sim_artigo_sbc_v1.tex | Out-Null
    Write-Host "OK: docs/artigo/sim_artigo_sbc_v1.pdf"
}

function Build-Monografia {
  if (-not (Test-Path "$Repo\template-ufla\templufla.cls")) {
    Write-Error "template-ufla/ nao encontrado. Monografia requer template UFLA local."
  }
  Set-Location "$Repo\template-ufla"
  pdflatex -interaction=nonstopmode templufla_main.tex | Out-Null
  bibtex templufla_main | Out-Null
  pdflatex -interaction=nonstopmode templufla_main.tex | Out-Null
  pdflatex -interaction=nonstopmode templufla_main.tex | Out-Null
  Copy-Item "$Repo\template-ufla\templufla_main.pdf" "$Root\monografia\SIM_monografia_v1.pdf" -Force
  Write-Host "OK: docs/monografia/SIM_monografia_v1.pdf"
}

if ($All -or $PSBoundParameters.Count -eq 0) {
  Build-Monografia
  Build-Artigo
} else {
  Build-Artigo
}
