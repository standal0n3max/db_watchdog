"""Загрузка корневого ``.env`` на границе CLI.

Читается только ``project_root() / ".env"``: сторонние ``.env`` не подхватываются.
``override=False`` означает, что уже экспортированные переменные окружения имеют
приоритет и не перезаписываются. Отсутствующий файл не является ошибкой, а
значения секретов не логируются.

Загрузчик вызывается только в CLI-границах, а не при импорте модулей: у
низкоуровневых модулей не должно быть side effects.
"""

from __future__ import annotations

from pathlib import Path

from beartype import beartype
from dotenv import load_dotenv

from db_pipeline.paths import project_root

ENV_FILE_NAME = ".env"


@beartype
def env_file_path() -> Path:
    """Путь к корневому ``.env`` независимо от текущего каталога."""
    return project_root() / ENV_FILE_NAME


@beartype
def load_environment() -> bool:
    """Загружает корневой ``.env`` без перезаписи экспортированных переменных.

    Возвращает ``True``, если файл есть и из него что-то применилось. Отсутствие
    файла — не ошибка: возвращается ``False``. Значения не логируются.
    """
    path = env_file_path()
    if not path.is_file():
        return False
    return bool(load_dotenv(dotenv_path=path, override=False))
