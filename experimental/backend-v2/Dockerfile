FROM python:3.11-slim

WORKDIR /app

# Dependências pinadas na imagem: boot passa de ~3 min (pip a cada restart)
# para segundos, e elimina quebra silenciosa por versão nova de biblioteca.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
