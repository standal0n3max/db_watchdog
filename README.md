# db_pipeline

Локальный сборщик задач по ядру СУБД. Это компонент автопополнения: он готовит экземпляры, которые позже кладутся в тот же pg-gym. Отдельный бенчмарк здесь не заводится.

Включены три ядра: DuckDB, PostgreSQL и SQLite. У каждого свой DAG и свой образ инструментов.

Общий образ на три ядра не собирается, потому что у ядер разные компиляторы, зависимости и программы проверки. DuckDB собирается через CMake и Ninja. PostgreSQL нуждается в bison, flex и ICU. SQLite гоняет тесты через `testfixture` и Tcl. Команда сборки и команда теста тоже свои. Стенд потом оценивает ядра по отдельности, поэтому инструментальный слой сборщика остаётся раздельным.

Одновременно собирается одно ядро. У Docker на этой машине около 7,6 ГиБ, параллельная сборка DuckDB уже убивалась по памяти, поэтому компиляция идёт с `-j1`. Сами образы при этом маленькие: в них нет исходников конкретного sha и нет ключа модели. Дерево монтируется снаружи.

Сборщик и стенд оценки агентов остаются разными компонентами. Образ, в котором сборщик проверяет коммит, и образ, в котором стенд прогоняет агента, живут отдельно. У pg-gym свой образ PostgreSQL уже есть, и прогон агента на стенде идёт в нём.

План DuckDB: [plans/2026-10-10_mvp_пайплайн_duckdb.md](../plans/2026-10-10_mvp_пайплайн_duckdb.md). План PostgreSQL и SQLite: [plans/2026-10-10_postgres_sqlite_пайплайн.md](../plans/2026-10-10_postgres_sqlite_пайплайн.md). Сбор новых SQL-коммитов: [plans/2026-10-18_сбор_новых_sql_коммитов.md](../plans/2026-10-18_сбор_новых_sql_коммитов.md). Сбор коммитов ядра: [plans/2026-10-19_сбор_коммитов_ядра.md](../plans/2026-10-19_сбор_коммитов_ядра.md).

## Что делает цепочка

Три DAG на TaskFlow API Airflow 2.11: `duckdb_pipeline`, `postgres_pipeline`, `sqlite_pipeline`. Стадии — одни и те же функции пакета, граф передаёт id ядра.

```mermaid
flowchart LR
  watch[watchRepos] --> fetch[fetchChange]
  fetch --> filt[filterCandidate]
  filt --> task[compileTask]
  task --> oracle[verifyEnvironment]
  oracle --> agent[runOpenCode]
  agent --> record[writeRecord]
```



Закреплённый запуск берёт один заранее записанный кандидат. `--harvest` идёт по истории и останавливается, когда оракул принял `--accept` задач.

1. `watchRepos` в закреплённом запуске делает `git fetch` включённой строки `repos.toml` и берёт кандидата из конфига: PR 26419 у DuckDB, следующий образец pg-gym у PostgreSQL, закреплённый sha у SQLite. `--harvest` этот шаг заменяет обходом `git log` первой родительской линии, от новых коммитов к старым. У DuckDB в окно попадают коммиты `origin/main`, в теме которых есть `(#номер)`. У PostgreSQL и SQLite — коммиты `master`. История добирается `git fetch --deepen`, пока не набрано `--scan` коммитов, которых ещё нет в журнале. Курсор лежит в `data/cursors/<core>.json` и отмечает последний уже просмотренный sha: следующее окно начинается сразу после него.
2. `fetchChange` достаёт родительский sha, diff, сообщение, номер PR и постановку. Источник постановки пишется в `statement_source`: `issue`, `pr` или `commit_message`. Текст issue читается с GitHub, если ссылка есть. Документация в условие не подмешивается с текущего HEAD. В `--harvest` постановка PostgreSQL и SQLite — сообщение коммита. У DuckDB по-прежнему issue, описание PR или сообщение коммита.
3. `filterCandidate` отсекает merge-сообщение, revert, только документацию, форматирование и переименование без тестов, массовое обновление сгенерированных ответов TPC-H/TPC-DS и изменение без одновременной правки кода и тестов. Повторный sha не получает вторую запись. В `--harvest` код берётся из префиксов ядра в `repos.toml`, сторона теста не расширяется. Коммит PostgreSQL без изменённого `expected/*.out` получает `no_oracle_test` и не собирается. Запись журнала с причиной ровно `нет правки SQL-кода` и без запуска оракула для harvest не считается известной и разбирается снова. Принятый оракулом sha второй JSON не получает.
4. `compileTask` режет diff коммита между родителем и самим коммитом. У DuckDB и SQLite рез идёт по префиксам из `repos.toml`: тесты попадают в `test_patch`, код — в скрытый `gold_patch`. В закреплённом запуске PostgreSQL рез идёт по спискам `test_files` и `solution` образца, потому что и код, и регресс лежат под `src/`. В `--harvest` всё под `src/test/regress/` попадает в `test_patch`, остальной diff коммита — в `gold_patch`. Имя теста берётся из `expected/<имя>.out`.
5. `verifyEnvironment` поднимает контейнер из шаблона своего ядра: `docker/duckdb.Dockerfile`, `docker/postgres.Dockerfile` или `docker/sqlite.Dockerfile`. В образе только инструменты сборки. Исходники конкретного `base_sha` монтируются снаружи. Экземпляр остаётся, если тесты из задачи падают до эталона и проходят после него.
6. `runOpenCode` в закреплённом запуске стартует OpenCode процессом рядом с контейнером. В образ ключ не кладётся. Температура 0, лимит шагов 50, таймаут 1800 с. Пустой diff записывается как pass ложь. Если оракул экземпляр не принял, эта стадия пропускается. `--harvest` эту стадию не вызывает: экземпляр для датасета готов после оракула.
7. `writeRecord` пишет JSON задачи и JSON запуска отдельно. Повторный прогон того же sha не создаёт второй файл задачи.

Если фильтр или оракул отвергают изменение, цепочка всё равно доходит до `writeRecord`. Причина и лог лежат в журнале. Необработанное исключение посередине таким завершением не считается.

## Экземпляр

Экземпляр — одна задача датасета, по которой потом можно поднять дерево, показать агенту условие и проверить его diff. Это один sha или один PR после фильтра и разреза. Файл лежит в `data/tasks/<instance_id>.json`. Прогон агента в этот файл не входит: он пишется отдельно в `data/runs/`.

Поля ниже стенд уже умеет потреблять. Имена не переименовываются.


| Поле               | Смысл                                                    |
| ------------------ | -------------------------------------------------------- |
| `instance_id`      | `duckdb__duckdb-<номер PR>` или `<репозиторий>-<sha>`    |
| `repository`       | репозиторий ядра из `repos.toml`                         |
| `license`          | лицензия из `repos.toml`                                 |
| `base_sha`         | родитель коммита, на нём собирается дерево               |
| `statement`        | постановка                                               |
| `statement_source` | `issue`, `pr` или `commit_message`                       |
| `test_patch`       | патч тестов, агенту отдаётся                             |
| `gold_patch`       | эталон кода, агенту не показывается                      |
| `fail_to_pass`     | тесты, которые должны упасть до эталона и пройти после   |
| `pass_to_pass`     | пустой список: полный регресс в этот проход не входит    |
| `oracle`           | имя проверки ядра: `unittest`, `pg_regress` или `testfixture` |
| `reject_reason`    | пусто, если оракул принял экземпляр                      |


В задаче дополнительно лежат `head_sha` и `pr_number`. В запуске: `model`, `harness`, `temperature`, `pass`, `empty_diff`, `duration_seconds`, `step_limit`, `timeout_seconds`.

Закреплённый экземпляр DuckDB — `duckdb__duckdb-26419`, issue 26401. Тест `test/sql/subquery/exists/test_issue_26401.test` падает до эталона (`0 <> 1`) и проходит после. Логи: `data/oracle/fea4498ea5e7ba7687aab2619417eed9139960eb/`.

Первые принятые `--harvest` экземпляры, каждый не из закреплённого списка: `sqlite__sqlite-3bce1260e7ef` (`test/date.test`), `duckdb__duckdb-25817` (`test/sql/function/list/lambdas/reduce_captured_columns.test`), `postgres__postgres-1a846a555afb` (регресс `foreign_key`).

## Что видит агент

Агенту отдаются постановка и `test_patch`. `gold_patch` хранится для проверки и в промпт не входит. На `base_sha` сначала накладывается тестовый патч: эти тесты должны падать. Затем накладывается эталон кода: те же тесты должны проходить. После попытки агента тестовые файлы возвращаются к патчу задачи, чтобы закрыть тест правкой самого теста было нельзя.

`test_patch` — это кусок diff между родителем и коммитом, в котором остались только тесты. У DuckDB и SQLite граница берётся из префиксов `repos.toml`. В закреплённом запуске PostgreSQL граница берётся из списков `test_files` и `solution` готового образца. В `--harvest` граница — каталог `src/test/regress/`. Код ядра уходит в `gold_patch`.

Так устроен SWE-bench и SWE-rebench: проверяющий тест агенту виден, исправление скрыто. Готовый diff кода в условие не входит. Тест, по которому агента оценивают, в условие входит.

С pg-gym совпадают состав источников и набор полей, которые стенд потом может прочитать. Набор `commit` в pg-gym на время работы агента снимает регрессионный тест с диска и оставляет `goal`, область и имена файлов, которыми его будут оценивать. Текст этого правила лежит в `suites/commit/src/suite.py`. JSON этого сборщика в pg-gym пока не импортируется. При импорте нужно явно выбрать видимость теста: показать `test_patch`, как в этом сборщике, или спрятать тест, как в наборе `commit`.

## Оракул

Оракул — программа, которая решает, годится ли коммит в датасет и верен ли ответ агента. Поле `oracle` называет эту программу, чтобы задачи трёх ядер можно было хранить рядом и проверять правильной командой.

| Ядро | Поле `oracle` | Чем проверяется |
|---|---|---|
| DuckDB | `unittest` | бинарник тестов ядра, только файлы из `fail_to_pass` |
| PostgreSQL | `pg_regress` | в закреплённом запуске тест из образца; в `--harvest` имя из изменённого `expected/*.out` |
| SQLite | `testfixture` | TCL-тест `test/*.test` |

Чужой оракул на чужое ядро не ставится: падение `pg_regress` и падение `testfixture` устроены по-разному. Коммит остаётся в наборе, если тесты падают на базе с тестовым патчем и проходят после скрытого эталона. Та же проверка потом отличает удачный diff агента от неудачного. OpenCode запускается только после того, как оракул экземпляр принял.

## Конфиг источников

`repos.toml`. Watchdog смотрит только строки с `enabled = true`.


| id       | Репозиторий                | Лицензия           | Оракул        | Сейчас                      |
| -------- | -------------------------- | ------------------ | ------------- | --------------------------- |
| duckdb   | github.com/duckdb/duckdb   | MIT                | `unittest`    | закреплён PR 26419; `--harvest` читает `origin/main` |
| postgres | зеркало git.postgresql.org | PostgreSQL License | `pg_regress`  | закреплённый запуск читает образцы pg-gym; `--harvest` читает `master` |
| sqlite   | github.com/sqlite/sqlite   | public domain      | `testfixture` | закреплён sha `bfb858c596f6`; `--harvest` читает `master` |


Префиксы кода DuckDB: `src/`, `extension/core_functions/`. Встроенные функции ядра лежат во втором пути, это не отдельный репозиторий расширений. Префиксы тестов: `test/sql/`, `test/optimizer/`, `test/api/`, `test/common/`, `test/issues/`. Каталоги `extension/tpch/dbgen/answers/` и `extension/tpcds/dsdgen/answers/` считаются массовой генерацией и отсекаются.

Узкий SQL-срез снят. Для `--harvest` код — это ядро, тесты остаются прежними. DuckDB: `src/` или `extension/core_functions/` и `test/sql/`. Каталоги `test/optimizer/` и `test/api/` в harvest не открываются. PostgreSQL: `src/backend/` и `src/test/regress/` с изменённым `expected/*.out`. `contrib` и `src/bin` кодом ядра не считаются. SQLite: любой файл `src/`, кроме амальгамы `sqlite3.c` и `sqlite3.h`, и `test/*.test`. Каталог `ext/` кодом ядра не считается.

Команда сборки и теста заморожены после первой успешной сборки, а не взяты из статьи:

```text
CMAKE_BUILD_PARALLEL_LEVEL=1 make release DISABLE_SHELL=1 DISABLE_PARQUET=1 CMAKE_VARS_BUILD=-DDUCKDB_RELEASE_LINK_JOBS=1
build/release/test/unittest {test}
```

Параллельная unity-сборка на этой машине убивается по памяти: у Docker около 7.6 ГиБ. Сборка без unity этим GCC не компилируется (`incomplete type` в `partitioned_sort.cpp`). Поэтому один компилятор, один линкер, parquet выключен. Расширения извне не клонируются.

Кандидаты оракула, не больше одной содержательной сборки на PR: 26419, затем 26057, затем 26460. Первый принят, два других не собирались.

## Каталог

```text
repos.toml                 три ядра, лицензии, префиксы, команды
docker/duckdb.Dockerfile   инструменты сборки DuckDB, без исходников и без секрета
docker/postgres.Dockerfile инструменты сборки PostgreSQL
docker/sqlite.Dockerfile   инструменты сборки SQLite и testfixture
dags/duckdb_pipeline.py    DAG DuckDB
dags/postgres_pipeline.py  DAG PostgreSQL
dags/sqlite_pipeline.py    DAG SQLite
prompts/opencode_system.txt
prompts/opencode.json      температура 0 и 50 шагов, ключ не хранится
src/db_pipeline/           стадии и модель экземпляра
tests/                     фильтр на 20 изменениях, SQL-двадцатки, разрез патча, журнал
constraints/               constraints Airflow 2.11 для Python 3.11
data/                      в gitignore: клон, журнал, JSON, логи, AIRFLOW_HOME
```

`data/` не коммитится. Туда попадают:

- `data/clones/<ядро>` — частичный клон; у DuckDB глубина из конфига 200, `--harvest` при необходимости углубляет историю;
- `data/cursors/<ядро>.json` — последний просмотренный sha обхода;
- `data/journal/<sha>.json` — одна запись на sha;
- `data/tasks/<instance_id>.json` — экземпляр;
- `data/runs/<instance_id>.json` — прогон;
- `data/oracle/<sha>/` — логи сборки, «до» и «после»;
- `data/filter_sample.jsonl` — журнал фильтра на 20 изменениях;
- `data/decisions.md` — фактические команды и ошибки среды;
- `data/airflow/` — свой отдельный Airflow.



## Окружение

Python 3.11, зависимости через `uv`, виртуальное окружение в `.venv`. Airflow ставится с официальными constraints, иначе разрешение зависимостей уходит в сторону. Пакет `cryptography` остаётся на 42.0.8 из этих constraints: у 50.0.2 нет колеса macOS x86_64, а локальный CPython 3.11 собран под x86_64.

```bash
cd НИР/db_pipeline
UV_CONSTRAINT=constraints/airflow-2.11.0-3.11.txt uv sync
```

Проверки пакета:

```bash
uv run python -m unittest discover -s tests
uv run mypy
uv run pyright
```

Фильтр на размеченной выборке заново пишет `data/filter_sample.jsonl`. Метки в `tests/fixtures/labeled_changes.json` после просмотра решения фильтра не меняются. Если совпадение падает ниже 16 из 20, правятся префиксы в `repos.toml`, а не разметка. Расхождения пишутся в `data/filter_notes.md`. Сейчас совпадение 20 из 20. То же правило для двадцаток ядра `tests/fixtures/labeled_sql_duckdb.json`, `labeled_sql_postgres.json` и `labeled_sql_sqlite.json`: на каждом ядре 20 из 20, заметки `data/<ядро>_sql_filter_notes.md`. Эти двадцатки фиксируют правило «код ядра и прежний тест». Узкий срез трёх файлов SQLite, `adt/` и только `core_functions/` больше не является критерием.

## Запуск

Ручной. Расписание в `repos.toml` пустое: для недели его достаточно не включать.

Airflow исполнитель — SequentialExecutor, база — SQLite внутри `data/airflow`, интерфейс — порт 8088.

```bash
export AIRFLOW_HOME="$PWD/data/airflow"
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8088
uv run airflow db migrate
uv run airflow dags list
uv run airflow dags test duckdb_pipeline 2026-10-10
```

Тот же проход без планировщика проводит одно ядро до журнала. Повтор на том же sha останавливается на фильтре и не создаёт второй JSON задачи. DAG остаётся этим закреплённым проходом.

```bash
uv run python -m db_pipeline --core duckdb
uv run python -m db_pipeline --core postgres
uv run python -m db_pipeline --core sqlite
```

Закреплённый PostgreSQL читает три JSON из `suites/commit/data/samples`. Постановка — поле `goal` такого образца. После трёх отказов оракула поиск останавливается. SQLite в этом запуске берёт закреплённый sha, постановка — сообщение коммита.

Обход коммитов ядра — отдельный флаг. История идёт от новых к старым. `--scan` задаёт, сколько коммитов смотреть в одном окне, `--accept` — сколько задач должен принять оракул за это окно. `--oracle-budget` ограничивает число сборок за окно; 0 оставляет сборки без потолка. OpenCode в этом режиме не запускается. Повтор того же sha второй JSON не создаёт, курсор сдвигается, следующий запуск продолжает историю.

```bash
uv run python -m db_pipeline --core sqlite --harvest --scan 40 --accept 1
uv run python -m db_pipeline --core duckdb --harvest --scan 40 --accept 1
uv run python -m db_pipeline --core postgres --harvest --scan 40 --accept 1
```

Несколько ядер идут одной командой, по порядку флагов `--core`. Следующее ядро стартует, когда предыдущее добрало `--goal` или история кончилась. Перед каждым окном программа считает в `data/journal` записи с `oracle_status=accepted` этого ядра. В окно уходит недостача до `--goal`, уже лежащие задачи входят в этот счёт. Число 300 в код не вшито. Без `--goal` или с `--goal 0` очередь идёт до конца истории. Ошибка сбора останавливает очередь. Одно ядро без `--goal` по-прежнему смотрит одно окно и слушает `--accept`.

```bash
uv run python -m db_pipeline --harvest --core sqlite --core duckdb --core postgres --goal 300 --scan 5000
```

Образ инструментов, если его ещё нет:

```bash
docker build -t db-pipeline-duckdb-tools:local -f docker/duckdb.Dockerfile docker
```



## OpenCode

Модель `deepseek/deepseek-v4-flash`, обвязка `opencode`, температура 0. Ключ читается из переменной `OPENROUTER_API_KEY` в момент прогона. В Dockerfile, в git и в `data/` он не записывается. OpenCode стоит на хосте и правит смонтированное дерево. После прогона тестовые файлы возвращаются к патчу задачи, чтобы агент не мог закрыть тест правкой самого теста. Затем оракул пересобирает дерево и гоняет `fail_to_pass`.

Пустой diff — ошибка модели: в поле `pass` пишется ложь. Один такой прогон уже записан для `duckdb__duckdb-26419`: модель отработала около 22 минут, код выхода 0, рабочего diff после прогона не осталось, поэтому pass ложь. Установленный CLI не принимает `--auto`; процесс запускается с `--dangerously-skip-permissions`. Пять повторов, как в оригинальном SWE-rebench, в этот проход не входят.

## Чего в этом компоненте нет

Импорт JSON в pg-gym, повтор 104 задач, Langfuse, граф, пять повторов и языковой отбор.
