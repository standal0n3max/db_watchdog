# Локальный стек Langfuse v4

Стек нужен хостовому оркестратору benchmark: `db_pipeline` публикует трассы по OTLP
на `LANGFUSE_URL` (по умолчанию `http://localhost:3100`), сам агент и task-контейнеры
Langfuse не видят.

## Состав

| Сервис | Образ | Назначение |
|---|---|---|
| `langfuse-web` | `docker.langfuse.com/langfuse/langfuse:4` | API и UI, приём OTLP |
| `langfuse-worker` | `docker.langfuse.com/langfuse/langfuse-worker:4` | асинхронная запись событий |
| `clickhouse` | `clickhouse/clickhouse-server:25.12` | хранилище observations |
| `minio` | `cgr.dev/chainguard/minio` | S3 для event/media upload |
| `redis` | `redis:7` | очереди BullMQ |
| `postgres` | `postgres:17` | метаданные, ключи, пользователи |

Все порты привязаны к `127.0.0.1`. Стек автономен: своя compose-сеть
(`db-pipeline-langfuse`), внешних сетей нет.

## Порты

| Порт | Сервис |
|---|---|
| `3100` | langfuse-web (UI и OTLP) |
| `3030` | langfuse-worker |
| `18123` / `19000` | clickhouse HTTP / native |
| `9090` / `19091` | minio API / console |
| `6379` | redis |
| `15432` | postgres |

## Запуск

```bash
cp docker/langfuse/.env.example docker/langfuse/.env
# заполнить секреты: POSTGRES_PASSWORD, CLICKHOUSE_PASSWORD, REDIS_AUTH,
# MINIO_ROOT_PASSWORD, NEXTAUTH_SECRET, SALT, ENCRYPTION_KEY и LANGFUSE_INIT_*
make langfuse-up        # up -d --wait, дожидается здоровья сервисов
make langfuse-health    # GET /api/public/health
make langfuse-logs      # хвост логов web и worker
make langfuse-down      # остановить, данные сохранить
make langfuse-reset     # ОПАСНО: down -v, удаляет тома
```

## Ключи проекта

`LANGFUSE_INIT_PROJECT_PUBLIC_KEY` и `LANGFUSE_INIT_PROJECT_SECRET_KEY` создают
проект при первом старте. Те же значения должны лежать в корневом `.env` как
`LANGFUSE_PUBLIC_KEY` и `LANGFUSE_SECRET_KEY` — их использует `db_pipeline`.

## Два env-файла

- `docker/langfuse/.env` — секреты стека (postgres, clickhouse, redis, minio,
  nextauth, ключи проекта). Читается только Makefile через `--env-file`.
- корневой `.env` — окружение benchmark (OpenRouter, `LANGFUSE_URL`,
  `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY`).

Разделение не даёт секретам стека попасть в окружение benchmark-прогона.

## Ограничения

`SALT` и `ENCRYPTION_KEY` после первого старта менять нельзя: сохранённые в базе
ключи перестанут расшифровываться. Данные живут в томах `db-pipeline-langfuse_*`.

Не поднимайте два стека Langfuse одновременно: порты пересекаются. Перед
`make langfuse-up` остановите прежний стек (`docker compose ... down` в его каталоге).
