"""Secret loading facade for runtime services."""

from __future__ import annotations

from services.secrets.loader import SecretNotAvailable, load_secret

__all__ = ["SecretNotAvailable", "load_secret"]
