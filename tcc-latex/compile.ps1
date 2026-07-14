# Compila main.tex -> main.pdf (Windows)
param(
    [switch]$UseDocker,
    [switch]$UseMiktex
)

$ErrorActionPreference = "Continue"
$Root = $PSScriptRoot
Set-Location $Root

function Compile-Docker {
    Write-Host ">> Compilando via Docker (texlive/texlive)..."
    docker run --rm `
        -v "${Root}:/work" `
        -w /work `
        texlive/texlive:latest `
        bash -c "pdflatex -interaction=nonstopmode main.tex; pdflatex -interaction=nonstopmode main.tex"
}

function Compile-Miktex {
    $bin = "$env:LOCALAPPDATA\Programs\MiKTeX\miktex\bin\x64"
    if (Test-Path $bin) { $env:Path = "$bin;" + $env:Path }

    $pdflatex = Get-Command pdflatex -ErrorAction SilentlyContinue
    if (-not $pdflatex) { throw "pdflatex nao encontrado" }

    # Evita bloqueio por atualizacao pendente
    initexmf --set-config-value [MPM]AutoInstall=1 2>$null
    initexmf --set-config-value [MPM]LastUserUpdateCheck=9999999999 2>$null

    Write-Host ">> Compilando com MiKTeX: $($pdflatex.Source)"
    & $pdflatex -interaction=nonstopmode main.tex
    & $pdflatex -interaction=nonstopmode main.tex
}

if ($UseDocker) {
    Compile-Docker
} elseif ($UseMiktex) {
    Compile-Miktex
} else {
    try {
        if (Get-Command docker -ErrorAction SilentlyContinue) {
            Compile-Docker
        } else {
            Compile-Miktex
        }
    } catch {
        Write-Warning "Docker falhou, tentando MiKTeX..."
        Compile-Miktex
    }
}

if (Test-Path "$Root\main.pdf") {
    $info = Get-Item "$Root\main.pdf"
    Write-Host ""
    Write-Host "OK PDF gerado: $($info.FullName)"
    Write-Host "   Paginas aprox.: ver main.log | Tamanho: $([math]::Round($info.Length/1KB)) KB"
} else {
    Write-Error "PDF nao gerado. Verifique main.log"
    exit 1
}
