# Трассировка Langfuse v4 через OTLP

Трасса публикуется на `/api/public/otel/v1/traces` с Basic auth и заголовком
`x-langfuse-ingestion-version: 4`. Deprecated trace ingestion не используется.

## Что попадает в трассу

- Корневой run span: имя `<func> · <mode>`, вердикт и metadata.
- `harness:verdict` и по одному span на каждый шаг harness:
  `prepare/prebuild/hide/agent/restore/build/install/test`.
- `opencode:generation` на каждый вызов модели: model, usage details (input,
  output, reasoning), cost details и текст ответа.
- `opencode:tool:<имя>` на каждый инструмент с input/output/status.

## Разбор JSONL OpenCode

OpenCode запускается с `--format json --standalone`. Каждый raw event
сохраняется. События группируются по `messageID`:

- `step_finish` даёт tokens/cost/reason;
- `tool_use` — полные input/output/status/time;
- `text` — ответ модели.

Критерий полноты: количество локальных generation/tool/harness events совпадает
с observations в trace, у всех завершённых вызовов есть время, input/output и
tokens, а trace доступен по сохранённому URL.

## Переменные окружения

`LANGFUSE_URL` (для текущей установки `http://localhost:3100`),
`LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY`. Значения берутся из окружения;
корневой `.env` загружается только на границе CLI (`load_environment`,
`override=False`), так что экспортированные переменные имеют приоритет.

## doctor

`python -m db_pipeline.benchmark doctor` проверяет:

- доступность Docker;
- наличие переменных Langfuse;
- health `/api/public/health` и валидность ключей `/api/public/projects`;
- probe trace (`--no-probe` отключает).

Секреты и auth-заголовки в payload не попадают.

## Локальный стек

Стек Langfuse v4 входит в репозиторий: `docker/langfuse/docker-compose.yml`
поднимает web, worker, clickhouse, minio, redis и postgres в своей compose-сети
(`db-pipeline-langfuse`). Внешних сетей нет, наружу открыт только web на
`127.0.0.1:3100`. Справка — [docker/langfuse/README.md](../docker/langfuse/README.md).

Цели Makefile:

```bash
cp docker/langfuse/.env.example docker/langfuse/.env
make langfuse-up        # up -d --wait
make langfuse-down      # остановить, данные сохранить
make langfuse-health    # GET /api/public/health
make langfuse-logs      # хвост логов web и worker
make langfuse-reset     # ОПАСНО: down -v
```

Порты: `3100` (web/OTLP), `3030` (worker), `18123`/`19000` (clickhouse),
`9090`/`19091` (minio), `6379` (redis), `15432` (postgres).

Ключи проекта создаются при первом старте из `LANGFUSE_INIT_PROJECT_PUBLIC_KEY`
и `LANGFUSE_INIT_PROJECT_SECRET_KEY` файла `docker/langfuse/.env`. Те же значения
нужны в корневом `.env` как `LANGFUSE_PUBLIC_KEY` и `LANGFUSE_SECRET_KEY`.

Два env-файла: `docker/langfuse/.env` держит секреты стека (postgres, clickhouse,
redis, minio, nextauth, ключи проекта), корневой `.env` — окружение benchmark.
Секреты стека в окружение прогона не попадают.

`SALT` и `ENCRYPTION_KEY` после первого старта менять нельзя: сохранённые ключи
перестанут расшифровываться. Два стека одновременно не поднимать, порты
пересекаются. `doctor` проверяет health, валидность ключей на `/api/public/projects`
и публикует probe trace, поэтому работает и без поднятого pg-gym.
