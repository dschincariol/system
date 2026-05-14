"""Artifact reference value objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class ArtifactRef:
    sha256: str
    size: int
    content_type: str
    kind: str
    created_ts: datetime
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def uri(self) -> str:
        return f"artifact:{self.sha256}"

    def to_metadata(self) -> dict[str, Any]:
        return {
            "sha256": str(self.sha256),
            "size_bytes": int(self.size),
            "content_type": str(self.content_type),
            "kind": str(self.kind),
            "created_ts": self.created_ts.astimezone(timezone.utc).isoformat(),
            "metadata": dict(self.metadata or {}),
        }


__all__ = ["ArtifactRef"]
