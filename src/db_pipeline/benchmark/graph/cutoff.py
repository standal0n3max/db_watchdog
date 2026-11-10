"""Временная отсечка графа.

Cutoff задаётся одновременно ``base_sha`` и его commit time. Любая возвращаемая
реvision обязана быть предком ``base_sha``; одной проверки timestamp мало, потому
что часы коммитов не монотонны по времени.

``head_sha``, его патч и потомки никогда не выдаются инструментами. Проверка
предковства идёт по списку reachable-коммитов, вычисленному от ``base_sha``.
"""

from __future__ import annotations

from dataclasses import dataclass

from beartype import beartype


@beartype
@dataclass(frozen=True)
class Cutoff:
    base_sha: str
    base_time: int
    reachable: frozenset[str]

    @property
    def head_sha(self) -> str:
        return ""

    def allows(self, sha: str, commit_time: int) -> bool:
        if sha == "":
            return False
        if sha not in self.reachable:
            return False
        return commit_time <= self.base_time


@beartype
def build_cutoff(
    base_sha: str,
    base_time: int,
    reachable: set[str],
    forbidden_head: str = "",
) -> Cutoff:
    allowed = {sha for sha in reachable if sha != forbidden_head}
    return Cutoff(base_sha=base_sha, base_time=base_time, reachable=frozenset(allowed))


@beartype
def refusal(reason: str) -> dict[str, object]:
    return {"error": "cutoff", "reason": reason, "results": []}
