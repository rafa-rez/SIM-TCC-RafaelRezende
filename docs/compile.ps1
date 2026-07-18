param([switch]$ArtigoOnly)
$ErrorActionPreference = "Stop"
$env:PATH = "D:\texlive\2026\bin\windows;" + $env:PATH
$Repo = Split-Path $PSScriptRoot -Parent
$Template = Join-Path $Repo "template-ufla"
$DocsMono = Join-Path $PSScriptRoot "monografia"
$DocsArtigo = Join-Path $PSScriptRoot "artigo"

function Sync-Monografia {
    if (-not (Test-Path $Template)) {
        Write-Error "template-ufla/ nao encontrado em $Repo"
    }
    Copy-Item "$DocsMono\secoes\*" "$Template\secoes\" -Recurse -Force
    Copy-Item "$DocsMono\figuras\*" "$Template\figuras\" -Recurse -Force -ErrorAction SilentlyContinue
    Copy-Item "$DocsMono\templufla_main.tex" "$Template\templufla_main.tex" -Force
    Copy-Item "$PSScriptRoot\referencias\referencias_sim.bib" "$Template\refbib.bib" -Force
    Write-Host "Sincronizado: docs/monografia -> template-ufla/"
}

function Build-Artigo {
    Set-Location $DocsArtigo
    pdflatex -interaction=nonstopmode sim_artigo_sbc_v1.tex | Out-Null
    bibtex sim_artigo_sbc_v1 | Out-Null
    pdflatex -interaction=nonstopmode sim_artigo_sbc_v1.tex | Out-Null
    pdflatex -interaction=nonstopmode sim_artigo_sbc_v1.tex | Out-Null
    Write-Host "OK: docs/artigo/sim_artigo_sbc_v1.pdf"
}

function Build-Monografia {
    Sync-Monografia
    Set-Location $Template
    pdflatex -interaction=nonstopmode templufla_main.tex | Out-Null
    bibtex templufla_main | Out-Null
    pdflatex -interaction=nonstopmode templufla_main.tex | Out-Null
    pdflatex -interaction=nonstopmode templufla_main.tex | Out-Null
    Copy-Item "$Template\templufla_main.pdf" "$DocsMono\SIM_monografia_v1.pdf" -Force
    Write-Host "OK: docs/monografia/SIM_monografia_v1.pdf"
}

if ($ArtigoOnly) {
    Build-Artigo
} else {
    Build-Monografia
    Build-Artigo
}
