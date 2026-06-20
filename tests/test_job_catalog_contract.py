from __future__ import annotations

from engine.api import api_jobs
from engine.runtime.job_catalog import (
    SAFETY_EXECUTION_SENSITIVE,
    SAFETY_UNAVAILABLE,
    build_job_catalog,
    build_job_catalog_entry,
)
from engine.runtime.job_registry import ALLOWED_JOBS


class _FakeJobs:
    def __init__(self) -> None:
        self.started: list[str] = []

    def start(self, name: str):
        self.started.append(str(name))
        return {"ok": True, "job": str(name), "status": "started"}


def test_job_catalog_serializes_every_registered_job_with_operator_metadata() -> None:
    rows = build_job_catalog(environ={})
    by_name = {row["name"]: row for row in rows}

    assert set(by_name) == set(ALLOWED_JOBS)
    required = {
        "id",
        "group",
        "script",
        "module",
        "mode",
        "schedule",
        "cadence_seconds",
        "stage",
        "owner_subsystem",
        "dependencies",
        "required_secrets",
        "required_secret_any",
        "required_providers",
        "safety",
        "execution_sensitivity",
        "resource_class",
        "purpose",
        "action_policy",
        "log_url",
        "last_output_url",
    }
    for row in rows:
        assert required.issubset(row)
        assert row["id"] == row["name"]
        assert row["purpose"].strip()
        assert row["action_policy"]["start"]["required_confirm"] == "JOB_ACTION"


def test_execution_sensitive_job_start_is_guarded_in_api_handler() -> None:
    fake = _FakeJobs()

    denied = api_jobs.api_post_job_start(
        {"name": "broker_apply_orders"},
        {},
        {"JOBS": fake},
    )

    assert denied["ok"] is False
    assert denied["error"] == "confirmation_required"
    assert denied["required_confirm"] == "JOB_ACTION"
    assert denied["safety"] == SAFETY_EXECUTION_SENSITIVE
    assert fake.started == []

    allowed = api_jobs.api_post_job_start(
        {"name": "broker_apply_orders"},
        {"confirmation": "JOB_ACTION", "consequence_ack": True},
        {"JOBS": fake},
    )

    assert allowed["ok"] is True
    assert fake.started == ["broker_apply_orders"]


def test_data_refresh_job_handler_remains_backward_compatible_without_direct_confirmation() -> None:
    fake = _FakeJobs()

    result = api_jobs.api_post_job_start({"name": "poll_prices"}, {}, {"JOBS": fake})

    assert result["ok"] is True
    assert fake.started == ["poll_prices"]


def test_missing_secret_marks_catalog_job_unavailable_and_blocks_start(monkeypatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    row = build_job_catalog_entry("llm_factor_discovery").to_dict()
    assert row["safety"] == SAFETY_UNAVAILABLE
    assert row["action_policy"]["start"]["enabled"] is False
    assert row["missing_prerequisites"] == [{"type": "secret", "name": "ANTHROPIC_API_KEY"}]

    denied = api_jobs.api_post_job_start(
        {"name": "llm_factor_discovery"},
        {"confirmation": "JOB_ACTION", "consequence_ack": True},
        {"JOBS": _FakeJobs()},
    )

    assert denied["ok"] is False
    assert denied["error"] == "job_unavailable"
    assert denied["missing_prerequisites"] == [{"type": "secret", "name": "ANTHROPIC_API_KEY"}]


def test_jobs_catalog_read_api_is_available_without_jobs_manager() -> None:
    payload = api_jobs.api_get_jobs_catalog({}, ctx={})

    assert payload["ok"] is True
    assert payload["status"] == "static"
    assert len(payload["jobs"]) == len(ALLOWED_JOBS)
    assert payload["catalog"] == payload["jobs"]


def test_pipeline_include_execution_requires_backend_confirmation() -> None:
    denied = api_jobs.api_post_pipeline_run(
        {"include_execution": "1"},
        {},
        {"JOBS": _FakeJobs()},
    )

    assert denied["ok"] is False
    assert denied["error"] == "confirmation_required"
    assert denied["required_confirm"] == "RUN_PIPELINE"
