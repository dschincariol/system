from __future__ import annotations

from engine.runtime import supervisor as supervisor_mod


class _Delegate:
    pass


def _job_spec(name: str) -> tuple[str, str, None, dict[str, bool]]:
    return (f"engine/runtime/jobs/{name}.py", "oneshot", None, {"execution": False})


def test_validate_graph_reports_missing_dependency(monkeypatch) -> None:
    monkeypatch.setattr(
        supervisor_mod,
        "ALLOWED_JOBS",
        {"prices": _job_spec("prices"), "signals": _job_spec("signals")},
    )
    monkeypatch.setattr(supervisor_mod, "PIPELINE_ORDER", ["prices", "signals"])
    monkeypatch.setattr(supervisor_mod, "validate_runtime_architecture", lambda **_kwargs: {"errors": []})

    sup = supervisor_mod.RuntimeSupervisor(jobs=_Delegate())
    sup._deps = {"prices": [], "signals": ["missing_job"]}

    result = sup.validate_graph(strict=True)

    assert result["ok"] is False
    assert "missing_dep:signals->missing_job" in result["errors"]


def test_validate_graph_reports_dependency_cycle(monkeypatch) -> None:
    monkeypatch.setattr(
        supervisor_mod,
        "ALLOWED_JOBS",
        {"prices": _job_spec("prices"), "signals": _job_spec("signals")},
    )
    monkeypatch.setattr(supervisor_mod, "PIPELINE_ORDER", ["prices", "signals"])
    monkeypatch.setattr(supervisor_mod, "validate_runtime_architecture", lambda **_kwargs: {"errors": []})

    sup = supervisor_mod.RuntimeSupervisor(jobs=_Delegate())
    sup._deps = {"prices": ["signals"], "signals": ["prices"]}

    result = sup.validate_graph(strict=True)

    assert result["ok"] is False
    assert any(str(error).startswith("dependency_cycle:") for error in result["errors"])


def test_topo_expand_returns_dependency_first_order(monkeypatch) -> None:
    monkeypatch.setattr(
        supervisor_mod,
        "ALLOWED_JOBS",
        {
            "prices": _job_spec("prices"),
            "events": _job_spec("events"),
            "signals": _job_spec("signals"),
            "portfolio": _job_spec("portfolio"),
        },
    )
    monkeypatch.setattr(supervisor_mod, "JOB_ORDER", ["prices", "events", "signals", "portfolio"])

    sup = supervisor_mod.RuntimeSupervisor(jobs=_Delegate())
    sup._deps = {
        "prices": [],
        "events": ["prices"],
        "signals": ["events"],
        "portfolio": ["signals"],
    }

    assert sup._topo_expand(["portfolio"], strict=True) == [
        "prices",
        "events",
        "signals",
        "portfolio",
    ]
