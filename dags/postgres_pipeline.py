# pyright: reportUnknownVariableType=false
"""Закреплённый DAG PostgreSQL: следующий образец pg-gym, расписание выключено.

Граф TaskFlow вызывает те же стадии пакета. ``watchRepos`` передаёт ``core_id="postgres"``. Обход истории этим DAG не запускается: он идёт командой ``--harvest``.

``cast`` не меняет значение. Airflow подставляет XCom, а проверка типов видит словарь ``StageResult``.
"""
from __future__ import annotations

from typing import cast

import pendulum
from airflow.decorators import dag, task

from db_pipeline.stages import (
    StageResult,
    compile_stage,
    fetch_change,
    filter_stage,
    opencode_stage,
    verify_stage,
    watch_repos,
    write_record,
)


@dag(
    dag_id="postgres_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 10, tz="UTC"),
    catchup=False,
    tags=["postgres"],
)
def postgres_pipeline() -> None:
    """Закреплённый проход PostgreSQL по образцу pg-gym. Обход истории — команда --harvest."""

    @task
    def watchRepos() -> StageResult:
        return watch_repos(core_id="postgres")

    @task
    def fetchChange(payload: StageResult) -> StageResult:
        return fetch_change(payload)

    @task
    def filterCandidate(payload: StageResult) -> StageResult:
        return filter_stage(payload)

    @task
    def compileTask(payload: StageResult) -> StageResult:
        return compile_stage(payload)

    @task
    def verifyEnvironment(payload: StageResult) -> StageResult:
        return verify_stage(payload)

    @task
    def runOpenCode(payload: StageResult) -> StageResult:
        return opencode_stage(payload)

    @task
    def writeRecord(payload: StageResult) -> StageResult:
        return write_record(payload)

    watched = cast(StageResult, watchRepos())
    fetched = cast(StageResult, fetchChange(watched))
    filtered = cast(StageResult, filterCandidate(fetched))
    compiled = cast(StageResult, compileTask(filtered))
    verified = cast(StageResult, verifyEnvironment(compiled))
    ran = cast(StageResult, runOpenCode(verified))
    writeRecord(ran)


postgres_pipeline()
