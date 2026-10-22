"""Вход сборщика без планировщика Airflow.

Флаги:

- ``--core`` — id ядра (``duckdb``, ``postgres``, ``sqlite``). Флаг можно повторять. По умолчанию ``duckdb``. Несколько ядер задаются только вместе с ``--harvest``.
- ``--harvest`` — обход истории от новых коммитов к старым, без закреплённого образца и без OpenCode.
- ``--scan`` — сколько новых коммитов смотреть в одном окне. По умолчанию 40.
- ``--accept`` — сколько задач должен принять оракул за одно окно. По умолчанию 1.
- ``--goal`` — сколько принятых задач держать в журнале на ядро. Только вместе с ``--harvest``. ``0`` и отсутствие флага — до конца истории.
- ``--oracle-budget`` — потолок сборок за окно. ``0`` значит без потолка.
- ``--reverify`` — заново прогнать оракул по уже собранным JSON PostgreSQL. Только для ``postgres`` и не вместе с ``--harvest``.

Одно ядро без ``--goal`` смотрит одно окно и слушает ``--accept``. Очередь по порядку флагов ``--core`` сама листает окна, пока ядро не добрало ``--goal`` или история не кончилась. ``harvest_failed`` — это сбой подготовки окна, он и исчерпание ``--oracle-budget`` останавливают всю очередь. Ошибка одного коммита очередь не останавливает.
"""

from __future__ import annotations

import argparse
import json
from typing import cast

from beartype import beartype

from db_pipeline.journal import accepted_count
from db_pipeline.paths import resolve_data
from db_pipeline.stages import StageResult, reverify_postgres, run_all, run_harvest


@beartype
def next_window_accept(accepted: int, goal: int | None, scan: int) -> int | None:
    # None — ядро уже добрало потолок. Иначе --accept следующего окна.
    # Без потолка окно не останавливается раньше, чем кончатся просмотренные коммиты:
    # принять можно не больше, чем --scan.
    if goal is None or goal <= 0:
        return scan
    if accepted >= goal:
        return None
    return goal - accepted


@beartype
def harvest_cores(cores: list[str], scan: int, goal: int | None, oracle_budget: int) -> None:
    # Ядра идут по порядку. Следующее начинается, когда текущее добрало goal
    # или история кончилась. Ошибка подготовки окна останавливает всю очередь.
    root = resolve_data(None)
    for core in cores:
        while True:
            accepted = accepted_count(root, core)
            accept = next_window_accept(accepted, goal, scan)
            if accept is None:
                print(f"{core}: принято {goal}", flush=True)
                break
            if goal is None or goal <= 0:
                print(f"{core}: окно {scan}, до конца истории", flush=True)
            else:
                print(f"{core}: осталось {accept}", flush=True)
            state = run_harvest(
                root,
                core_id=core,
                scan=scan,
                accept=accept,
                oracle_budget=oracle_budget,
            )
            _print_state(state)
            if _sequence_stopped(state):
                return
            if state["reason_code"] == "empty_window":
                print(f"{core}: история кончилась", flush=True)
                break


@beartype
def _sequence_stopped(state: StageResult) -> bool:
    code = state["reason_code"]
    if code == "harvest_failed":
        raise SystemExit(1)
    return code == "oracle_budget"


@beartype
def _print_state(state: StageResult) -> None:
    print(json.dumps(state, ensure_ascii=False, indent=2), flush=True)


@beartype
def _cores_of(values: object) -> list[str]:
    if values is None:
        return ["duckdb"]
    if not isinstance(values, list):
        raise SystemExit("ядро задаётся строкой")
    cores: list[str] = []
    items = cast(list[object], values)
    for item in items:
        if not isinstance(item, str) or item == "":
            raise SystemExit("ядро задаётся строкой")
        cores.append(item)
    if len(cores) == 0:
        return ["duckdb"]
    return cores


@beartype
def main() -> None:
    parser = argparse.ArgumentParser(description="Один проход сборщика для ядра из repos.toml")
    parser.add_argument(
        "--core",
        action="append",
        dest="cores",
        help="id ядра: duckdb, postgres или sqlite; повтори флаг для очереди",
    )
    parser.add_argument(
        "--reverify",
        action="store_true",
        help="повторить оракул по уже собранным JSON PostgreSQL, без нового образца",
    )
    parser.add_argument(
        "--harvest",
        action="store_true",
        help="обойти новые SQL-коммиты, без закреплённого образца и без OpenCode",
    )
    parser.add_argument("--scan", type=int, default=40, help="сколько новых коммитов смотреть в одном окне")
    parser.add_argument("--accept", type=int, default=1, help="сколько задач должен принять оракул за одно окно")
    parser.add_argument(
        "--goal",
        type=int,
        default=None,
        help="сколько принятых задач держать в журнале на ядро; 0 — до конца истории",
    )
    parser.add_argument(
        "--oracle-budget",
        type=int,
        default=0,
        help="потолок сборок за окно, 0 значит без потолка",
    )
    args = parser.parse_args()
    cores = _cores_of(args.cores)
    if args.reverify is True and args.harvest is True:
        raise SystemExit("--reverify и --harvest вместе не запускаются")
    if args.reverify is True and cores != ["postgres"]:
        raise SystemExit("--reverify только для ядра postgres")
    if len(cores) > 1 and args.harvest is not True:
        raise SystemExit("несколько ядер задаются только вместе с --harvest")
    goal = args.goal
    if goal is not None and args.harvest is not True:
        raise SystemExit("--goal задаётся только вместе с --harvest")
    if isinstance(goal, int) and goal < 0:
        raise SystemExit("--goal не отрицательный")
    scan = args.scan
    accept = args.accept
    oracle_budget = args.oracle_budget
    if not isinstance(scan, int) or not isinstance(accept, int) or not isinstance(oracle_budget, int):
        raise SystemExit("--scan, --accept и --oracle-budget задаются числами")
    if not isinstance(goal, int) and goal is not None:
        raise SystemExit("--goal задаётся числом")
    if args.harvest is True and (scan < 1 or accept < 1 or oracle_budget < 0):
        raise SystemExit("--scan и --accept больше нуля, --oracle-budget не отрицательный")
    sequence = args.harvest is True and (len(cores) > 1 or goal is not None)
    if args.reverify is True:
        state = reverify_postgres()
    elif sequence:
        harvest_cores(cores, scan, goal if isinstance(goal, int) else None, oracle_budget)
        return
    elif args.harvest is True:
        state = run_harvest(
            core_id=cores[0],
            scan=scan,
            accept=accept,
            oracle_budget=oracle_budget,
        )
    else:
        state = run_all(core_id=cores[0])
    _print_state(state)


if __name__ == "__main__":
    main()
