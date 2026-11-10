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

Секреты и auth-заголовки в payload не попадают. Сам стек Langfuse в этот
репозиторий не входит: он поднимается отдельно, а `db_pipeline` только
публикует трассы по OTLP.
