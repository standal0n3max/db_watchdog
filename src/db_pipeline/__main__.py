from __future__ import annotations

import argparse
import json

from beartype import beartype

from db_pipeline.stages import reverify_postgres, run_all


@beartype
def main() -> None:
    parser = argparse.ArgumentParser(description="Один проход сборщика для ядра из repos.toml")
    parser.add_argument("--core", default="duckdb", help="id ядра: duckdb, postgres или sqlite")
    parser.add_argument(
        "--reverify",
        action="store_true",
        help="повторить оракул по уже собранным JSON PostgreSQL, без нового образца",
    )
    args = parser.parse_args()
    core_id = args.core
    if not isinstance(core_id, str):
        raise SystemExit("ядро задаётся строкой")
    if args.reverify is True and core_id != "postgres":
        raise SystemExit("--reverify только для ядра postgres")
    if args.reverify is True:
        state = reverify_postgres()
    else:
        state = run_all(core_id=core_id)
    print(json.dumps(state, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
