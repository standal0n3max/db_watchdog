# Smoke: критерии до запуска

Критерии фиксируются до прогона, чтобы не подгонять оценку постфактум.

## Набор задач

Пять разных задач: `postgres__d764d61af316`, `postgres__d8444163d22e`,
`postgres__96ab9a990eed`, `postgres__3b120b1e94dd`, `postgres__9b917a93116e`.
Первая используется для isolation acceptance; пятая отдельно проверяет генерацию
`contract` для точного текста ошибок.

## Условия

- Для каждой задачи две независимые одноразовые сессии: `bare` и `graph`.
  Итого 10 scored runs.
- Одинаковые model/temperature/steps/timeouts, порядок условий чередуется,
  reuse сессий запрещён.
- Ноль обращений к MCP в graph-run — ошибка интеграции, а не результат.
- На каждый smoke поднимается одна внутренняя сеть и один model-gateway; на
  каждый graph-run — отдельный одноразовый graph-container с копией union-индекса.
- До десяти прогонов выполняется дешёвый E2E probe: gateway completion,
  MCP `initialize`/`list_tools`, по одному вызову каждого инструмента и
  cutoff-refusal по `head_sha` контрольной задачи.

## Что проверяется для каждого run

- контейнер удалён;
- trace опубликован;
- hidden tests восстановлены только после агента;
- verdict рассчитан;
- graph-run сделал хотя бы один MCP-вызов.

## Отчёт

`data/reports/smoke-postgres.json` содержит pass@1, gates, test outcomes,
latency, tokens, tool counts, MCP calls, effective base/head, ссылки Langfuse
и отчёт cleanup. Run records immutable: каждый прогон пишется отдельным файлом
в `data/runs/` с session/run ID и не перезаписывает предыдущие. На пяти задачах
выводится только работоспособность E2E, не статистическая значимость эффекта
графа.
