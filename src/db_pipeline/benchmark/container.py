"""Одноразовый task-container без bind mounts.

Контейнер создаётся через ``docker create`` с ``--init -i``, payload и архив
дерева передаются через stdin/``docker cp``. Родительские каталоги, ``data/``,
клон и ``gold_patch`` никогда не монтируются: ``docker inspect`` должен
показать ``Mounts == []``.

Перед запуском агента дерево обрывается по истории, тесты скрываются, а затем
возвращаются. Секреты модели живут только в model-gateway, task-container их
не получает. Ниже — обёртка над ``docker`` и передача полезной нагрузки.
"""

from __future__ import annotations

import io
import json
import os
import subprocess
import tarfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

from beartype import beartype

from db_pipeline.errors import PipelineError

DEFAULT_CPUS = "4"
DEFAULT_MEMORY = "8g"
DEFAULT_PIDS = "1024"


@beartype
@dataclass(frozen=True)
class ContainerSpec:
    image: str
    workspace: str = "/workspace/postgres"
    network: str = ""
    cpus: str = DEFAULT_CPUS
    memory: str = DEFAULT_MEMORY
    pids_limit: str = DEFAULT_PIDS
    entrypoint: str = ""
    user: str = ""
    network_alias: str = ""
    ports: dict[str, str] = field(default_factory=dict[str, str])
    labels: dict[str, str] = field(default_factory=dict[str, str])
    environment: dict[str, str] = field(default_factory=dict[str, str])


@beartype
@dataclass(frozen=True)
class ContainerRun:
    returncode: int
    stdout: str
    stderr: str
    container_id: str | None = None


@beartype
def docker_command(*args: str, context: str = "") -> list[str]:
    command = ["docker"]
    if context != "":
        command += ["--context", context]
    return [*command, *args]


@beartype
def docker_available(context: str = "") -> str | None:
    try:
        result = subprocess.run(
            docker_command("version", "--format", "{{.Server.Version}}", context=context),
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return f"docker недоступен: {exc}"
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        return detail or "демон docker не отвечает"
    return None


@beartype
def create_command(spec: ContainerSpec, name: str, context: str = "") -> list[str]:
    command = docker_command(
        "create",
        "--init",
        "-i",
        "--name",
        name,
        "--workdir",
        spec.workspace,
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--pids-limit",
        spec.pids_limit,
        "--cpus",
        spec.cpus,
        "--memory",
        spec.memory,
        context=context,
    )
    if spec.network != "":
        command += ["--network", spec.network]
    if spec.network_alias != "":
        command += ["--network-alias", spec.network_alias]
    for host_port, container_port in spec.ports.items():
        if host_port in ("", "0"):
            command += ["--publish", container_port]
        else:
            command += ["--publish", f"{host_port}:{container_port}"]
    if spec.user != "":
        command += ["--user", spec.user]
    for key, value in spec.labels.items():
        command += ["--label", f"{key}={value}"]
    for key, value in spec.environment.items():
        command += ["--env", f"{key}={value}"]
    if spec.entrypoint != "":
        command += ["--entrypoint", spec.entrypoint]
    return command


@beartype
def create_container(spec: ContainerSpec, name: str | None = None, context: str = "") -> str:
    container_name = name or f"bench-postgres-{uuid.uuid4().hex[:12]}"
    result = subprocess.run(
        [*create_command(spec, container_name, context), spec.image],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise PipelineError("docker_create", detail or "docker create не удался")
    return result.stdout.strip()


@beartype
def inspect_mounts(container_id: str, context: str = "") -> list[object]:
    result = subprocess.run(
        docker_command(
            "inspect",
            container_id,
            "--format",
            "{{json .Mounts}}",
            context=context,
        ),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise PipelineError("docker_inspect", result.stderr.strip() or "inspect не удался")
    parsed: object = json.loads(result.stdout.strip() or "[]")
    if not isinstance(parsed, list):
        raise PipelineError("docker_inspect", "Mounts не список")
    mounts: list[object] = list(cast("list[object]", parsed))
    return mounts


@beartype
def inspect_running_user(container_id: str, context: str = "") -> str:
    script = "id -u"
    result = subprocess.run(
        docker_command("exec", "-i", container_id, "sh", "-c", script, context=context),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip()


@beartype
def copy_in(container_id: str, source: Path, target: str, context: str = "") -> None:
    result = subprocess.run(
        docker_command("cp", str(source), f"{container_id}:{target}", context=context),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise PipelineError("docker_cp", detail or f"docker cp {source} не удался")


@beartype
def write_in(container_id: str, path: str, data: bytes, context: str = "") -> None:
    script = 'mkdir -p "$(dirname "$1")" && cat > "$1"'
    result = subprocess.run(
        docker_command(
            "exec", "-i", container_id, "sh", "-c", script, "sh", path, context=context
        ),
        input=data,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", "replace").strip()
        raise PipelineError("docker_exec", detail or f"не удалось записать {path}")


@beartype
def start_container(
    container_id: str,
    payload: bytes,
    timeout: int,
    context: str = "",
    echo: bool = False,
) -> ContainerRun:
    proc = subprocess.Popen(
        docker_command("start", "-a", "-i", container_id, context=context),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    output = bytearray()

    def drain() -> None:
        assert proc.stdout is not None
        for line in iter(proc.stdout.readline, b""):
            output.extend(line)
            if echo:
                print(line.decode("utf-8", "replace"), end="", flush=True)

    reader = threading.Thread(target=drain, daemon=True)
    reader.start()
    try:
        assert proc.stdin is not None
        proc.stdin.write(payload)
        proc.stdin.close()
        code = proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        subprocess.run(
            docker_command("kill", container_id, context=context),
            capture_output=True,
            check=False,
        )
        proc.wait(timeout=30)
        code = 124
    finally:
        reader.join(timeout=30)
    text = bytes(output).decode("utf-8", "replace")
    return ContainerRun(code, text, "" if code != 124 else "container timeout", container_id)


@beartype
def start_detached(container_id: str, context: str = "") -> None:
    """Запускает контейнер в фоне; оркестратор дальше работает через ``exec``."""
    result = subprocess.run(
        docker_command("start", container_id, context=context),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise PipelineError("docker_start", detail or "docker start не удался")


@beartype
def wait_ready(container_id: str, marker: str, timeout: int = 600, context: str = "") -> bool:
    """Ждёт появления файла-маркера: entrypoint распаковывает архив в фоне."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = subprocess.run(
            docker_command(
                "exec", "-i", container_id, "sh", "-lc", f"test -f {marker}", context=context
            ),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return True
        time.sleep(1)
    return False


@beartype
def remove_container(container_id: str, context: str = "") -> None:
    subprocess.run(
        docker_command("rm", "-f", container_id, context=context),
        capture_output=True,
        check=False,
    )


@beartype
def remove_container_verified(container_id: str, context: str = "") -> bool:
    """Удаляет контейнер и подтверждает, что он больше не существует."""
    remove_container(container_id, context=context)
    return not container_exists(container_id, context=context)


@beartype
def copy_out(container_id: str, source: str, target: Path, context: str = "") -> bool:
    target.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        docker_command("cp", f"{container_id}:{source}", str(target), context=context),
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


@beartype
def archive_directory(root: Path, names: list[str] | None = None) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        if names is None:
            tar.add(root, arcname=".")
        else:
            for name in names:
                path = root / name
                if path.exists():
                    tar.add(path, arcname=name)
    return buffer.getvalue()


@beartype
def env_flag(name: str) -> bool:
    return os.environ.get(name, "0") == "1"


@beartype
def network_create(name: str, internal: bool = True, context: str = "") -> None:
    """Создаёт Docker-сеть. Внутренняя сеть не имеет внешнего egress."""
    args = ["network", "create"]
    if internal:
        args += ["--internal"]
    args.append(name)
    result = subprocess.run(
        docker_command(*args, context=context),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise PipelineError("docker_network", detail or f"network create {name} не удался")


@beartype
def network_connect(
    network: str,
    container_id: str,
    alias: str = "",
    context: str = "",
) -> None:
    """Подключает контейнер к сети, при необходимости с alias."""
    args = ["network", "connect"]
    if alias != "":
        args += ["--alias", alias]
    args += [network, container_id]
    result = subprocess.run(
        docker_command(*args, context=context),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise PipelineError("docker_network", detail or f"connect {container_id} не удался")


@beartype
def network_exists(name: str, context: str = "") -> bool:
    result = subprocess.run(
        docker_command(
            "network", "ls", "--filter", f"name=^{name}$", "--format", "{{.Name}}",
            context=context,
        ),
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip() == name


@beartype
def network_remove(name: str, context: str = "") -> None:
    subprocess.run(
        docker_command("network", "rm", name, context=context),
        capture_output=True,
        check=False,
    )


@beartype
def container_running(container_id: str, context: str = "") -> bool:
    result = subprocess.run(
        docker_command("inspect", "-f", "{{.State.Running}}", container_id, context=context),
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip() == "true"


@beartype
def container_exists(container_id: str, context: str = "") -> bool:
    result = subprocess.run(
        docker_command("inspect", "-f", "{{.Id}}", container_id, context=context),
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


@beartype
def container_ip(container_id: str, network: str, context: str = "") -> str:
    result = subprocess.run(
        docker_command(
            "inspect",
            "-f",
            f'{{{{index .NetworkSettings.Networks "{network}" "IPAddress"}}}}',
            container_id,
            context=context,
        ),
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip()


@beartype
def published_port(container_id: str, container_port: str, context: str = "") -> str:
    """Возвращает host-порт для опубликованного порта контейнера."""
    result = subprocess.run(
        docker_command(
            "inspect",
            "-f",
            f'{{{{(index (index .NetworkSettings.Ports "{container_port}/tcp") 0).HostPort}}}}',
            container_id,
            context=context,
        ),
        capture_output=True,
        text=True,
        check=False,
    )
    value = result.stdout.strip()
    return "" if value in ("", "<no value>") else value


@beartype
def wait_http_ready(
    url: str,
    timeout: int = 120,
    interval: float = 1.0,
) -> bool:
    """Ждёт HTTP 200 по адресу: используется как readiness gateway и графа."""
    import urllib.error
    import urllib.request

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                if response.status == 200:
                    return True
        except (urllib.error.URLError, OSError, ValueError):
            pass
        time.sleep(interval)
    return False
