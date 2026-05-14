# Docstring Style

This repository uses NumPy-style docstrings for Python modules, classes, and functions that form public, operator-facing, or cross-module contracts.

The goal is consistency and maintainability, not blanket churn. Prefer improving docstrings in code you are already touching instead of sweeping untouched files.

## Scope

Apply this standard to:

- public modules under `engine/`, `routes/`, `services/`, `ops/`, and `tools/`
- public classes and dataclasses with non-obvious responsibilities or invariants
- functions that are imported across modules, called by operators, exposed through APIs, or relied on by validation and runtime orchestration

It is acceptable to keep very small private helpers undocumented when the code is already obvious.

## General Rules

- Start with a one-line summary that explains purpose, not filename.
- Prefer meaningful module docstrings over placeholder headers such as `FILE: config_schema.py`.
- Keep the summary stable and factual. Avoid change-log language inside docstrings.
- Document behavior, invariants, and side effects. Do not restate the code line by line.
- Keep terminology consistent with the rest of the repo: runtime, operator, control plane, provider, source, lifecycle, execution, and governance have specific meanings here.

## Recommended Sections

Use the sections that materially help the reader.

- `Parameters`
  Use when arguments are not self-evident or when the function accepts structured request or context objects.
- `Returns`
  Use for non-trivial outputs, especially `dict` payloads, dataclasses, and tuples.
- `Raises`
  Use when callers need to understand validation failures, safety guards, or operational exceptions.
- `Yields`
  Use for generators.
- `Attributes`
  Use for dataclasses or stateful classes with important fields.
- `Notes`
  Use for invariants, safety rules, compatibility boundaries, or operational caveats.
- `Examples`
  Use sparingly when request shapes, configuration loading, or lifecycle behavior would otherwise be easy to misuse.

## Style Details

- Keep types in the docstring even when the function is type-annotated. That keeps the output compatible with NumPy-style renderers such as Napoleon.
- Prefer concise descriptions over repeating the type in prose.
- Document defaults only when they affect behavior or operator expectations.
- For `dict[str, Any]` style payloads, explain the contract keys that matter instead of enumerating every incidental field.
- For API handlers, describe the request inputs and the response contract at a high level.
- For config loaders and validators, always document failure conditions.

## Module Example

```python
def load_runtime_config() -> RuntimeConfig:
    """Load and validate the runtime environment contract.

    Returns
    -------
    RuntimeConfig
        Parsed runtime configuration derived from the current environment.

    Raises
    ------
    ConfigError
        Raised when a required variable is missing or a safety invariant is
        violated.
    """
```

This pattern fits modules such as `engine/runtime/config_schema.py`, where callers need to know both the returned object and the validation failure mode.

## API Handler Example

```python
def api_post_data_source_enable(parsed, body=None, ctx=None):
    """Enable a configured data source and reconcile its runtime lifecycle.

    Parameters
    ----------
    parsed : Any
        Parsed HTTP request object or query container.
    body : dict[str, Any] | None, optional
        Request body containing the source identifier and optional actor
        metadata.
    ctx : dict[str, Any] | None, optional
        Request context that may include the jobs manager.

    Returns
    -------
    dict[str, Any]
        API payload containing the updated source record and lifecycle result.
    """
```

This pattern fits route modules such as `routes/data_sources_routes.py` and handler modules under `engine/api/`.

## Dataclass Example

```python
@dataclass(frozen=True)
class SourceDefinition:
    """Describe one operator-managed ingestion or provider source.

    Attributes
    ----------
    source_type : str
        High-level source family used by the control plane.
    display_name : str
        Operator-facing name shown in the UI.
    job_name : str
        Runtime job associated with the source.
    credential_env : dict[str, str]
        Bootstrap environment variables that can seed stored credentials.
    setting_env : dict[str, str]
        Bootstrap environment variables that can seed stored non-secret
        settings.
    """
```

This pattern fits classes like `services.data_source_manager.SourceDefinition`, where the field meanings matter more than their Python syntax.

## What Not To Do

- Do not add large docstrings to every trivial helper just to satisfy style.
- Do not duplicate the full request or response schema in both a docstring and OpenAPI.
- Do not use docstrings as design notes or TODO lists.
- Do not preserve placeholder module docstrings when you are already editing the file for real work.
