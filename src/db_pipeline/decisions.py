from __future__ import annotations

from pathlib import Path

from beartype import beartype

_HEADER = """# Решения реализации

Поля JSON задачи: `instance_id`, `repository`, `license`, `base_sha`, `statement`, `statement_source`, `test_patch`, `gold_patch`, `fail_to_pass`, `pass_to_pass`, `oracle`, `reject_reason`. Дополнительно пишутся `head_sha` и `pr_number`.

`pass_to_pass` пустой: оракул запускает только тесты из `test_patch`, полный регресс не гоняется.

`statement_source` — `issue`, `pr` или `commit_message`.

Префиксы кода: `src/` и `extension/core_functions/` (встроенные функции ядра лежат там, не в отдельном репозитории расширений). Префиксы тестов: `test/sql/`, `test/optimizer/`, `test/api/`, `test/common/`, `test/issues/`. Сгенерированные ответы TPC-H и TPC-DS отсекаются отдельно.

Три кандидата оракула, не больше одной сборки на PR: 26419, 26057, 26460. Первый закреплён в `repos.toml`.

Слитый PR DuckDB на `main` — merge-коммит с двумя родителями и обычным заголовком. Фильтр считает мусором только сообщение, которое начинается с `Merge`. База для diff — первый родитель.

`cryptography` в окружении сборщика зафиксирован на 42.0.8, как в constraints Airflow 2.11. Версия 50.0.2 не имеет колеса macOS x86_64, а локальный CPython 3.11 собран под x86_64.

Клон: `--filter=blob:none --single-branch --branch main --depth 200`, при необходимости `git fetch --deepen`. Полный клон обрывался по сети.

Airflow: свой `AIRFLOW_HOME` в `data/airflow`, SequentialExecutor, SQLite, UI на порту 8088. Порты 5432 и 8123 не используются. Расписание пустое.

OpenCode запускается процессом рядом с контейнером. Ключ читается из `OPENROUTER_API_KEY` и в образ, репозиторий и `data/` не пишется. Модель `deepseek/deepseek-v4-flash`, температура 0, лимит шагов 50, таймаут 1800 с.

"""


@beartype
def ensure_decisions(data: Path) -> None:
    path = data / "decisions.md"
    if path.exists():
        return
    path.write_text(_HEADER, encoding="utf-8")


@beartype
def append_decision(data: Path, text: str) -> None:
    ensure_decisions(data)
    path = data / "decisions.md"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(text)
        if not text.endswith("\n"):
            handle.write("\n")
