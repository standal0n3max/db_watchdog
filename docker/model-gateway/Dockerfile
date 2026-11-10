# model-gateway: единственная точка, где живёт ключ OpenRouter.
# Подключён одновременно к внешней сети и к внутренней сети задач.
# Task-container получает только внутренний адрес и не видит ключ.

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

RUN pip install --no-cache-dir "httpx>=0.27" "uvicorn>=0.30" "starlette>=0.37"

COPY docker/model-gateway/gateway.py /app/gateway.py

EXPOSE 8080
# Ключ приходит переменной OPENROUTER_API_KEY в рантайме, в образ не входит.
CMD ["uvicorn", "gateway:app", "--host", "0.0.0.0", "--port", "8080"]
