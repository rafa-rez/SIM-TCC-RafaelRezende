# Overleaf Community Edition — setup local (Docker)

Requer: Docker Desktop em execução.

## Instalação rápida (Windows + Git Bash ou WSL)

```bash
cd tcc-latex
git clone https://github.com/overleaf/toolkit.git overleaf-toolkit
cd overleaf-toolkit
bin/init
```

Edite `config/overleaf.rc` se quiser porta diferente de 80:

```
SHARELATEX_LISTEN_IP=127.0.0.1
SHARELATEX_PORT=8090
```

Inicie:

```bash
bin/up -d
```

Acesse: http://localhost:8090 (ou porta configurada)

## Importar o TCC

1. Crie novo projeto no Overleaf local
2. Faça upload da pasta `tcc-latex/` (main.tex, secoes/, macros/)
3. **Para formato UFLA oficial:** substitua `main.tex` pelo template `template-ufla-overleaf` do repositório e copie os capítulos de `secoes/`

## TeXLive completo no container (recomendado)

```bash
cd overleaf-toolkit
bin/shell
tlmgr install scheme-full
```

## Compilação sem Overleaf (mais rápido para PDF)

```powershell
cd tcc-latex
.\compile.ps1           # MiKTeX local
.\compile.ps1 -UseDocker  # força Docker texlive
```

PDF de saída: `tcc-latex/main.pdf`

## VS Code (opcional)

Extensão **LaTeX Workshop** apontando para `tcc-latex/main.tex` com recipe `pdflatex`.
