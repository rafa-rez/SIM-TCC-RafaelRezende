param([switch]$ArtigoOnly)
$ErrorActionPreference = "Stop"
$env:PATH = "D:\texlive\2026\bin\windows;" + $env:PATH
$Repo = Split-Path $PSScriptRoot -Parent
$Template = Join-Path $Repo "template-ufla"
$DocsMono = Join-Path $PSScriptRoot "monografia"
$DocsArtigo = Join-Path $PSScriptRoot "artigo"

function Invoke-TeX {
    param(
        [Parameter(Mandatory)][string]$Exe,
        [Parameter(ValueFromRemainingArguments = $true)][string[]]$Args
    )
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & $Exe @Args *> $null
    $code = $LASTEXITCODE
    $ErrorActionPreference = $prev
    if ($code -ne 0) {
        throw "Comando falhou (exit $code): $Exe $($Args -join ' ')"
    }
}

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
    Push-Location $DocsArtigo
    try {
        Invoke-TeX pdflatex -interaction=nonstopmode sim_artigo_sbc_v1.tex
        Invoke-TeX bibtex sim_artigo_sbc_v1
        Invoke-TeX pdflatex -interaction=nonstopmode sim_artigo_sbc_v1.tex
        Invoke-TeX pdflatex -interaction=nonstopmode sim_artigo_sbc_v1.tex
        Write-Host "OK: docs/artigo/sim_artigo_sbc_v1.pdf"
    } finally {
        Pop-Location
    }
}

function Build-Monografia {
    Sync-Monografia
    Push-Location $Template
    try {
        Invoke-TeX pdflatex -interaction=nonstopmode templufla_main.tex
        Invoke-TeX bibtex templufla_main
        Invoke-TeX pdflatex -interaction=nonstopmode templufla_main.tex
        Invoke-TeX pdflatex -interaction=nonstopmode templufla_main.tex
        Copy-Item "$Template\templufla_main.pdf" "$DocsMono\SIM_monografia_v1.pdf" -Force
        Write-Host "OK: docs/monografia/SIM_monografia_v1.pdf"
    } finally {
        Pop-Location
    }
}

if ($ArtigoOnly) {
    Build-Artigo
} else {
    Build-Monografia
    Build-Artigo
}
