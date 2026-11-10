# Происхождение каркаса benchmark

Этот файл фиксирует, какие идеи и поведение перенесены в `db_pipeline` из стенда `pg-gym`, а что написано заново. Репозиторий `pg-gym` берётся как читаемый источник поведения, а не как зависимость: код `postgres_gym` не импортируется и не устанавливается.

## Что взято как поведение

- `pg-gym/src/postgres_gym/core/runner.py` — порядок одного прогона: подготовка дерева, применение мутации, prebuild/install, скрытие тестов, обрыв истории, фаза агента, восстановление тестов, gates, build/install, targeted и полный regression, verdict. В `db_pipeline` этому соответствует `benchmark/runner.py`.
- `pg-gym/src/postgres_gym/core/tree.py` — обрыв истории: замена `.git` на пустой bare-репозиторий с одним baseline-коммитом, применение патча текстом, вычисление изменённых файлов и diff. В `db_pipeline` этому соответствует `benchmark/container.py` и `benchmark/postgres.py`.
- `pg-gym/src/postgres_gym/execution/docker.py` — создание контейнера через `docker create`, передача payload через stdin, `docker cp` для результатов, гарантированное удаление контейнера, ограничения CPU/memory/pids, allowlist переменных окружения. В `db_pipeline` этому соответствует `benchmark/container.py`.
- `pg-gym/suites/commit/src/suite.py` — протокол commit-suite: публичная задача (`area`, `goal`, `contract`, `graded_by`), приватный oracle (`sha`, `parent`, `patch`, `test_files`, `reference`, `test`), скрытие тестовых `.sql`/`.out`/`.spec`, форма промпта. В `db_pipeline` этому соответствует `benchmark/tasks.py` и конвертер в `benchmark/convert.py`.
- `pg-gym/src/postgres_gym/core/scoring.py` — gates (непустой source diff, запрет правок тестов) и исход тестов (func_ok, noregress_ok, verdict). В `db_pipeline` этому соответствует `benchmark/security.py` и `benchmark/runner.py`.

## Что написано заново

- CLI `python -m db_pipeline.benchmark` с командами `convert`, `doctor`, `build`, `run`, `smoke`, `graph-index`.
- Конвертер задач watchdog → commit-suite поверх полей `TaskInstance`.
- Трассировка Langfuse v4 через OTLP HTTP с разбором JSONL OpenCode.
- Свой граф и MCP-сервер с временной отсечкой по `base_sha`.
- Собственные Dockerfile, model-gateway и compose.

## Границы

- В git входят Python-код, Dockerfile/Compose, промпты, конфигурационные примеры и тесты. `data/`, индексы графа, исходники PostgreSQL, журналы запусков и секреты остаются локальными.
- На этом этапе поддерживается только PostgreSQL. Хостовый путь `opencode_run.py` больше не участвует в benchmark-прогоне; сбор (harvesting) остаётся работоспособным.
