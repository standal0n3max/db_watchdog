# Отчёт факта: исправление графа и парный smoke (2026-11-11)

Файл фиксирует состояние на момент остановки пользователем. Коммит не делался.
Отчёт составлен по фактическим артефактам: `data/runs/postgres__*.json`,
`data/graph/postgres.sqlite`, логам сборки образа и probe. Ничего не выдумано;
незавершённое отмечено явно.

## Что было сделано

### 1. Дефекты графа: исправлено 6 из 6

Все правки — в `src/db_pipeline/benchmark/graph/` (`index.py`, `tools.py`,
`store.py`, `server.py`), плюс тесты в `tests/test_benchmark_graph.py`.

| # | Дефект | Статус | Что сделано |
|---|--------|--------|-------------|
| 1 | `_FUNC_RE` не находит определения PG (тип на предыдущей строке) | исправлено | Введены `_DEF_RE` (`^\s*([A-Za-z_]\w*)\s*\([^;{}]*\)\s*\{`) и `_DECL_RE` для прототипов; определение предпочтительнее прототипа; тело функции ограничивается балансом скобок (`_find_body_end`) |
| 2 | `find_code_symbols` отдаёт прототип (`LIKE`+`LIMIT`, `INSERT OR IGNORE`), нет диапазона строк | исправлено | Сначала точное совпадение `name = ?`, затем экранированная подстрока; дедуп по `(path, name)` с выбором свежей ревизии ≤ cutoff; определение выше прототипа; ответ содержит `line`, `end_line`, `kind`, `path`, `revision`, `sha256` |
| 3 | `read_versioned_source.offset` — байты, а не строки | исправлено | Добавлены `start_line`/`end_line` (1-based, приоритетнее); байтовый `offset`/`limit` сохранён для обратной совместимости; в ответе `start_line`, `end_line`, `total_lines`, `byte_span` |
| 4 | Индексируются только функции (kind=function 100%) | исправлено | Индексируются `struct`, `union`, `enum`, `typedef`, `macro` в `.c` и `.h`; `add_symbol` пишет `end_line` |
| 5 | `search_git_history`/`_fts_query` склеивают токены через `AND`; тела кода не в FTS | исправлено | `_fts_query` строит `OR` по токенам с префиксами (`"tok"*`), ранжирование `bm25`; в FTS пишутся тела символов (`kind='code'`) |
| 6 | `_flush_hunk` пишет весь дифф файла одной записью | исправлено | Парсинг `@@ -a,b +c,d @@`, каждый hunk — отдельная запись с `old_start/old_count/new_start/new_count` |

### 2. Тесты и статические проверки

- `uv run python -m pytest tests/test_benchmark_graph.py -q` — 24 passed (было 9, добавлены тесты на определение vs прототип, тип/структуру, чтение по строкам, FTS по естественному языку, реальные hunks).
- `uv run python -m pytest tests/ -q` — 115 passed, 5 skipped.
- `uv run mypy` (src+tests+dags) — Success, 65 files.
- `uv run pyright` — 4 ошибки, все в файлах, которых не касалась эта сессия (`tests/test_benchmark_opencode.py`, `tests/test_benchmark_security.py`, private usage). Изменённые файлы графа — 0 ошибок.

### 3. Индекс перестроен

Старый индекс сохранён как `data/graph/postgres.sqlite.prefix-backup`
(221 413 376 байт, до правок). Новый построен командой
`uv run python -m db_pipeline.benchmark graph-index --union --per-base 50`
(~97 с), размер 460 660 736 байт. Состав: commits 250, revisions 22 070,
blobs 6 080, hunks 2 354, symbols 112 979.

Проверки после перестройки:

- `get_expression_sortgroupref`: строки определений 3206/3231/3311/3318 (по одному на каждую из 5 баз); прототип `line=108` больше не выдаётся (0 записей).
- Символы не-function: 31 995 (`macro` 22 512, `struct` 7 564, `enum` 1 339, `typedef` 543, `union` 37). `JsonConstructorExpr` → struct, primnodes.h:1730; `int4mul` → function, int.c:849; `to_char` → серия `*_to_char`/`DCH_to_char`.
- FTS: `"eager"* OR "aggregation"* OR "grouping"* OR "cross"* OR "type"*` → 23 718 строк; `"to_char"* OR "int4"* OR "overflow"*` → 3 319 строк. В `search`: `code` 112 979, `hunk` 2 354, `message` 250, `symbol` 112 979.
- Hunks: 2 354 записи, у 2 328 заданы ненулевые диапазоны (вместо прежних 700 склеенных).

### 4. Образ собран, probe пройден

- `uv run python -m db_pipeline.benchmark build` — образы task/graph/gateway собраны, `db-pipeline-graph-postgres:local` пересобран (COPY src).
- `uv run python -m db_pipeline.benchmark probe` — `ok: true`. Gateway: health ok, allowlist ok, отказ вне allowlist 403, completion 200. Граф: 3 инструмента, `find_code_symbols` вернул macro `PARALLEL_VACUUM_KEY_SHARED` с `end_line`; `read_versioned_source` вернул фрагмент со `start_line`/`end_line`; `search_git_history` вернул `kind='code'`; отказ по `head_sha` — `error: cutoff`. Cleanup: gateway/network/graph removed.

### 5. Smoke: 4 прогона завершились, остановлен пользователем

Запущен `uv run python -m db_pipeline.benchmark smoke`, остановлен по команде
пользователя. Полного `data/reports/smoke-postgres.json` нет: smoke не успел
дописать сводный отчёт. Успели завершиться 4 прогона (новые run records в
`data/runs/`, время 00:16–00:27):

| Задача | Режим | verdict | mcp_calls | diff_size | check.failed | gates |
|--------|-------|---------|-----------|-----------|--------------|-------|
| d764d61af316 | bare | FAIL | 0 | 14 | `['eager_aggregate']` | ok |
| d764d61af316 | graph | PASS | 2 | 26 | — | ok |
| d8444163d22e | graph | PASS | 7 | 34 | — | ok |
| d8444163d22e | bare | PASS | 0 | 29 | — | ok |

После правок завершилась первая пара (d764d61af316, bare+graph) и вторая
(d8444163d22e, graph+bare). Заметное отличие от прогонов до фиксов: graph
теперь PASS на d764d61af316 (ранее FAIL из-за правки не в том месте) и PASS на
d8444163d22e (ранее FAIL из-за правки скрытых тестов). Оставшиеся задачи
(96ab9a990eed, 3b120b1e94dd, 9b917a93116e) в этом запуске не выполнялись.

### 6. Очистка

Процессы `db_pipeline.benchmark` завершены, контейнеры `bench-*` удалены,
сеть `bench-internal-*` удалена. Smoke не перезапускался.

## Что не сделано

- Полный парный smoke 5×2 не завершён: нет сводного `data/reports/smoke-postgres.json`.
- Пятая задача `9b917a93116e` в новых прогонах не выполнялась.
- Коммит и пуш не делались (по требованию).
