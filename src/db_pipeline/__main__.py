from __future__ import annotations

import argparse
import json

from beartype import beartype

from db_pipeline.stages import reverify_postgres, run_all, run_harvest


@beartype
def main() -> None:
    parser = argparse.ArgumentParser(description="Один проход сборщика для ядра из repos.toml")
    parser.add_argument("--core", default="duckdb", help="id ядра: duckdb, postgres или sqlite")
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
    parser.add_argument("--scan", type=int, default=40, help="сколько новых коммитов смотреть")
    parser.add_argument("--accept", type=int, default=1, help="сколько задач должен принять оракул")
    parser.add_argument(
        "--oracle-budget",
        type=int,
        default=0,
        help="потолок сборок за запуск, 0 значит без потолка",
    )
    args = parser.parse_args()
    core_id = args.core
    if not isinstance(core_id, str):
        raise SystemExit("ядро задаётся строкой")
    if args.reverify is True and args.harvest is True:
        raise SystemExit("--reverify и --harvest вместе не запускаются")
    if args.reverify is True and core_id != "postgres":
        raise SystemExit("--reverify только для ядра postgres")
    scan = args.scan
    accept = args.accept
    oracle_budget = args.oracle_budget
    if not isinstance(scan, int) or not isinstance(accept, int) or not isinstance(oracle_budget, int):
        raise SystemExit("--scan, --accept и --oracle-budget задаются числами")
    if args.harvest is True and (scan < 1 or accept < 1 or oracle_budget < 0):
        raise SystemExit("--scan и --accept больше нуля, --oracle-budget не отрицательный")
    if args.reverify is True:
        state = reverify_postgres()
    elif args.harvest is True:
        state = run_harvest(
            core_id=core_id,
            scan=scan,
            accept=accept,
            oracle_budget=oracle_budget,
        )
    else:
        state = run_all(core_id=core_id)
    print(json.dumps(state, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
