"""Human-curated causal DAG specifications."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence


def _unique_text(values: Iterable[Any]) -> tuple[str, ...]:
    out: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in out:
            out.append(text)
    return tuple(out)


def _normalize_edges(edges: Iterable[Sequence[Any]]) -> tuple[tuple[str, str], ...]:
    out: list[tuple[str, str]] = []
    for edge in edges:
        if len(edge) != 2:
            raise ValueError("DAG edges must contain exactly two endpoints")
        src = str(edge[0] or "").strip()
        dst = str(edge[1] or "").strip()
        if not src or not dst:
            raise ValueError("DAG edge endpoints must be non-empty")
        if src == dst:
            raise ValueError("DAG self-edges are not allowed")
        pair = (src, dst)
        if pair not in out:
            out.append(pair)
    return tuple(out)


@dataclass(frozen=True)
class CausalDAG:
    """JSON-serializable curated DAG used by the DoWhy runner."""

    name: str
    nodes: tuple[str, ...]
    edges: tuple[tuple[str, str], ...]
    treatment: str
    outcome: str
    confounders: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        name = str(self.name or "").strip()
        treatment = str(self.treatment or "").strip()
        outcome = str(self.outcome or "").strip()
        if not name:
            raise ValueError("DAG name is required")
        if not treatment:
            raise ValueError("DAG treatment is required")
        if not outcome:
            raise ValueError("DAG outcome is required")
        if treatment == outcome:
            raise ValueError("DAG treatment and outcome must differ")

        confounders = _unique_text(self.confounders)
        edges = _normalize_edges(self.edges)
        nodes = _unique_text([*self.nodes, treatment, outcome, *confounders, *(v for edge in edges for v in edge)])
        node_set = set(nodes)
        for src, dst in edges:
            if src not in node_set or dst not in node_set:
                raise ValueError("DAG edge endpoint is not declared as a node")
        _reject_cycles(nodes, edges)

        object.__setattr__(self, "name", name)
        object.__setattr__(self, "treatment", treatment)
        object.__setattr__(self, "outcome", outcome)
        object.__setattr__(self, "confounders", confounders)
        object.__setattr__(self, "nodes", nodes)
        object.__setattr__(self, "edges", edges)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": str(self.name),
            "nodes": list(self.nodes),
            "edges": [[src, dst] for src, dst in self.edges],
            "treatment": str(self.treatment),
            "outcome": str(self.outcome),
            "confounders": list(self.confounders),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "CausalDAG":
        return cls(
            name=str(payload.get("name") or ""),
            nodes=tuple(payload.get("nodes") or ()),
            edges=tuple(tuple(edge) for edge in (payload.get("edges") or ())),
            treatment=str(payload.get("treatment") or ""),
            outcome=str(payload.get("outcome") or ""),
            confounders=tuple(payload.get("confounders") or ()),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, payload: str) -> "CausalDAG":
        parsed = json.loads(str(payload or "{}"))
        if not isinstance(parsed, Mapping):
            raise ValueError("DAG JSON must decode to an object")
        return cls.from_dict(parsed)

    def to_dot(self) -> str:
        lines = ["digraph {"]
        for node in self.nodes:
            lines.append(f'  "{_dot_escape(node)}";')
        for src, dst in self.edges:
            lines.append(f'  "{_dot_escape(src)}" -> "{_dot_escape(dst)}";')
        lines.append("}")
        return "\n".join(lines)


def _dot_escape(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def _reject_cycles(nodes: Sequence[str], edges: Sequence[tuple[str, str]]) -> None:
    outgoing: dict[str, list[str]] = {node: [] for node in nodes}
    indegree: dict[str, int] = {node: 0 for node in nodes}
    for src, dst in edges:
        outgoing[src].append(dst)
        indegree[dst] += 1
    queue = [node for node in nodes if indegree[node] == 0]
    visited = 0
    while queue:
        node = queue.pop(0)
        visited += 1
        for dst in outgoing[node]:
            indegree[dst] -= 1
            if indegree[dst] == 0:
                queue.append(dst)
    if visited != len(nodes):
        raise ValueError("DAG contains a cycle")
