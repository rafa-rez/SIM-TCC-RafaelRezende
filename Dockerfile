# --- ESTÁGIO 1: O Fornecedor ---
FROM alpine:3.20 AS builder
RUN apk add --no-cache apk-tools-static

# --- ESTÁGIO 2: O n8n Final ---
FROM n8nio/n8n:latest

USER root

# 1. Copia o apk estático
COPY --from=builder /sbin/apk.static /sbin/apk

# 2. Atualiza pacotes base e instala Python, Chromium e Dependências do Sistema
RUN /sbin/apk upgrade --no-cache && \
    /sbin/apk add --no-cache \
    python3 \
    py3-pip \
    chromium \
    chromium-chromedriver \
    bash \
    git \
    build-base \
    linux-headers \
    python3-dev \
    libpq-dev  
    # libpq-dev é necessário para compilar adaptadores postgres se precisar

# 3. Instala Selenium, Pandas, LIBS DE IA e DRIVER POSTGRES (psycopg2)
# Adicionei 'psycopg2-binary' para permitir conexão Python -> SQL
RUN pip3 install selenium pandas llama-parse llama-index-core openai python-dotenv psycopg2-binary --break-system-packages

USER node