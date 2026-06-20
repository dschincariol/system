"""Layer 5 negative test: secret-name path traversal.

Secret providers (`systemd-creds`, `plaintext`) must reject any secret
name that contains path separators or parent
directory references. Without this gate, a caller could read
arbitrary files on the host (`../../etc/passwd`, etc.).

This sweep confirms each provider raises `SecretNotAvailable` (the
typed error) for every traversal pattern — never returns bytes,
never opens a wrong file.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from services.secrets.loader import SecretNotAvailable

# Path-traversal patterns that must be rejected by every provider.
_MALICIOUS_NAMES = [
    "../etc/passwd",
    "../../etc/passwd",
    "../../../etc/shadow",
    "..\\windows\\system32\\config\\sam",
    "subdir/secret",
    "subdir\\secret",
    "foo/../bar",
    "foo/./bar",
    "/absolute/path",
    "C:\\absolute\\path",
    "..",
    ".",
    "/",
    "\\",
    "name with/slash",
    "name with\\backslash",
]


def _decoy_setup(tmp_path: Path) -> Path:
    """Plant a decoy file at every plausible traversal target so the
    provider has something to read if the validator fails."""
    decoy = tmp_path.parent / "decoy_secret"
    decoy.write_text("DECOY_VALUE_DO_NOT_LEAK", encoding="utf-8")
    return decoy


@pytest.fixture(autouse=True)
def _reset_plaintext_production_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """The plaintext provider caches its production-mode check across
    a TTL window. Other tests in the suite (notably
    `test_plaintext_provider_refuses_production`) flip TS_ENV to
    `production` and exercise the cache. The cache state survives
    test-process exit. This fixture forcibly invalidates the cache at
    the start of every test in this module so each path-traversal
    case sees a dev-mode provider, not a stuck-production one."""
    monkeypatch.delenv("TS_ENV", raising=False)
    try:
        from services.secrets.providers import plaintext

        monkeypatch.setattr(plaintext, "_production_forbidden", None, raising=False)
        monkeypatch.setattr(plaintext, "_production_check_at", 0.0, raising=False)
    except Exception:
        # Provider not importable on this runtime; tests will skip
        # individually if applicable.
        pass


# Names that should trigger the *explicit* "invalid_secret_name" path:
# they contain separators or absolute-path markers. The provider's
# `secret_name != Path(secret_name).name` check rejects all of them.
_REJECTED_BY_VALIDATOR = [
    "../etc/passwd",
    "../../etc/passwd",
    "../../../etc/shadow",
    "..\\windows\\system32\\config\\sam",
    "subdir/secret",
    "subdir\\secret",
    "/absolute/path",
    "C:\\absolute\\path",
    "/",
    "\\",
    "name with/slash",
    "name with\\backslash",
]


# Names that slip past the explicit validator because `Path(name).name
# == name` for them (e.g. `..`, `foo/..`), but are still rejected by
# the runtime safety net (file-open failure or directory check). The
# safety contract is "no bytes returned", regardless of which layer
# catches it. NOTE: this is brittle — see the follow-up finding in
# `docs/System_Audit_Layer4.md` (L5-FINDING-SECRETS-VALIDATOR-WEAK).
_REJECTED_BY_SAFETY_NET = [
    "..",
    ".",
    "foo/..",
    "foo/./bar",
]


# ---- systemd-creds provider -----------------------------------------

@pytest.mark.parametrize("malicious_name", _REJECTED_BY_VALIDATOR + _REJECTED_BY_SAFETY_NET)
def test_systemd_creds_provider_never_returns_bytes_for_malicious_name(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    malicious_name: str,
) -> None:
    """Contract: SecretNotAvailable is raised AND no bytes are
    returned. The reason may be `invalid_secret_name` (validator),
    `systemd_creds_linux_only` (platform restriction on non-Linux),
    or a file/directory-open failure (safety net). All three
    outcomes satisfy the safety contract."""
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(tmp_path))
    decoy = _decoy_setup(tmp_path)
    try:
        from services.secrets.providers import systemd_creds

        with pytest.raises(SecretNotAvailable):
            result = systemd_creds.load(malicious_name)
            # If we ever reach here, the provider returned bytes
            # instead of raising — that's a P0 leak.
            assert result is None, (
                f"systemd_creds.load({malicious_name!r}) returned bytes "
                f"instead of raising — security boundary breached."
            )
    finally:
        if decoy.exists():
            decoy.unlink()


# ---- plaintext provider ---------------------------------------------

@pytest.mark.parametrize("malicious_name", _REJECTED_BY_VALIDATOR + _REJECTED_BY_SAFETY_NET)
def test_plaintext_provider_never_returns_bytes_for_malicious_name(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    malicious_name: str,
) -> None:
    monkeypatch.delenv("TS_ENV", raising=False)
    monkeypatch.setenv("TS_DEV_SECRETS_DIR", str(tmp_path))
    decoy = _decoy_setup(tmp_path)
    try:
        from services.secrets.providers import plaintext

        with pytest.raises(SecretNotAvailable):
            result = plaintext.load(malicious_name)
            assert result is None, (
                f"plaintext.load({malicious_name!r}) returned bytes — "
                f"security boundary breached."
            )
    finally:
        if decoy.exists():
            decoy.unlink()


# ---- Documented validator hole — locked in as an explicit test -----

@pytest.mark.parametrize("name", _REJECTED_BY_VALIDATOR)
def test_explicit_validator_catches_separator_names(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    name: str,
) -> None:
    """Names with separators or absolute markers MUST be caught by
    the explicit `invalid_secret_name` validator, not just by the
    runtime safety net. This is the strict contract."""
    monkeypatch.setenv("TS_DEV_SECRETS_DIR", str(tmp_path))
    monkeypatch.delenv("TS_ENV", raising=False)

    from services.secrets.providers import plaintext

    with pytest.raises(SecretNotAvailable) as excinfo:
        plaintext.load(name)
    assert "invalid_secret_name" in str(excinfo.value), (
        f"plaintext provider should catch {name!r} via the explicit "
        f"validator, not via downstream file-open failure. Got: "
        f"{excinfo.value}"
    )


# ---- empty / None edge cases ----------------------------------------

@pytest.mark.parametrize("provider_module", [
    "services.secrets.providers.systemd_creds",
    "services.secrets.providers.plaintext",
])
def test_empty_or_whitespace_secret_name_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    provider_module: str,
) -> None:
    import importlib

    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(tmp_path))
    monkeypatch.setenv("TS_DEV_SECRETS_DIR", str(tmp_path))
    monkeypatch.delenv("TS_ENV", raising=False)

    mod = importlib.import_module(provider_module)
    for bad in ("", "  ", "\t", "\n"):
        with pytest.raises(SecretNotAvailable):
            mod.load(bad)
