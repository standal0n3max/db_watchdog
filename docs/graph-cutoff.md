# Граф и отсечка

Один файл `data/graph/graph.sqlite`. Факты ключуются `commit_oid` и `blob_oid`.
Все семь MCP-инструментов читают граф только через `GraphView(base_sha, head_sha)`:
видимы предки базы, включая её саму, и невидимы `head_sha` и его потомки.
Файл открывается `mode=ro&immutable=1`. Пустой ответ пишет причину `not_found`
или `outside_cutoff`.

Сборка: `uv run python -m db_pipeline.benchmark graph-build`. Стадии
`extract → trees → symbols → derive → validate` пропускают уже известные oid.
`--fetch` делает `git fetch origin master`. Окно ханков — последние 50 предков
каждой базы. Байты исходников лежат в таблице `blobs` (zlib), git-клон в
контейнер задачи не монтируется.

## Инструменты

Сервер всегда отдаёт все семь. В описаниях инструменты названы так, как их
видит OpenCode: `graph_<имя>`. Блок `cutoff` есть один раз на ответ.

- `find_code_symbols` — символы дерева текущей базы, revision = base.
- `read_versioned_source` — blob из дерева базы или из `file_change` окна.
- `search_git_history` — FTS по сообщениям видимых коммитов и ханкам окна.
- `symbol_history` — как менялся символ, от свежих коммитов к старым.
- `tests_to_code` — co-change на запросе по видимым `file_change` (вес 1/n,
  коммиты больше 8 путей отброшены).
- `similar_fixes` — FTS по сообщениям видимых коммитов.
- `find_error_origin` — `error_strings` текущего дерева, не чужой базы.

Ответ компактный: без SHA-256 и пустых полей, `revision` — 12 символов, cutoff один раз на ответ. Дефолтный `limit` — 10.

## Отсечка

- Множество видимых коммитов строится по таблице `parents` от `base_sha`.
- `head_sha` и потомки вычитаются и никогда не выдаются.
- Часы коммитов не монотонны, поэтому членство решает предковство, а не timestamp.
  `base_time` остаётся в метаданных ответа.

## Сессия

На graph-run в контейнер копируется один `graph.sqlite` потоковым `docker cp`
(`container.copy_in`). Переменная окружения одна: `BENCH_GRAPH_DB`. Гейт
изоляции task-контейнера не менялся: клон не монтируется.

## Проверки

- `graph-coverage` пишет `data/reports/graph-coverage.json`: структура 299 баз
  и живой вызов семи инструментов, все revision внутри отсечки.
- `graph-leak-audit` и `graph-canary`: membership, prior-twin по `patch_id`,
  канарейки эталона и отказ по `head_sha`.
- Двойная сборка с пустой базы даёт одинаковые хэши таблиц.

## Подключение к OpenCode

MCP подключается как remote Streamable HTTP (`http://graph:8765/mcp`) с
увеличенным timeout. Эффективный OpenCode-конфиг генерируется на каждый run:
bare-конфиг MCP не содержит, graph-конфиг подключает сервер графа; hash конфига
пишется в manifest. В graph-промпт до первого шага кладётся сводка графа и
имена `graph_*`. Вердикт тестов не переписывается, если агент сам не вызвал MCP:
для graph-руки интеграция требует доставленную сводку и отсечку. Свои вызовы
агента пишутся в `agent_mcp_calls`.

Абзац про историю имеет два варианта формулировки (`--prompt-variant`):

- `directive` — «Graph tools are available»: инструменты названы, вызов до
  правки не требуется; текст рамки пишется в manifest как `graph_frame`;
- `situational` — «Start editing as soon as you have localized the cause»:
  инструменты доступны по необходимости, правку можно начинать раньше.

Вариант пишется в manifest (`prompt_variant`) и в run record, поэтому эффект
формулировки промпта не смешивается с эффектом состава инструментов при разборе
отчёта.

Дешёвый E2E probe (`benchmark probe`) запускает MCP-клиент внутри самого
graph-container: `initialize`, `list_tools`, по одному вызову каждого
инструмента и отказ по `head_sha` текущей задачи.
