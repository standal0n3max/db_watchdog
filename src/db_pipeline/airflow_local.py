"""Окружение локального Airflow для ручного прогона DAG.

``local_airflow_env(root)`` возвращает словарь: ``AIRFLOW_HOME`` в ``data/airflow``, SequentialExecutor, примеры выключены, DAG из ``dags/``, база SQLite, порт ``WEB_PORT`` (8088). ``root`` по умолчанию — корень пакета.
"""

from __future__ import annotations

from pathlib import Path

from beartype import beartype

from db_pipeline.paths import project_root

WEB_PORT = "8088"


@beartype
def local_airflow_env(root: Path | None = None) -> dict[str, str]:
    project = project_root() if root is None else root
    home = project / "data" / "airflow"
    database = home / "airflow.db"
    return {
        "AIRFLOW_HOME": str(home),
        "AIRFLOW__CORE__EXECUTOR": "SequentialExecutor",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__DAGS_FOLDER": str(project / "dags"),
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{database}",
        "AIRFLOW__WEBSERVER__WEB_SERVER_PORT": WEB_PORT,
    }
