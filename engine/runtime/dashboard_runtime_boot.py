"""Dashboard control-plane post-bind runtime boot helpers."""

from __future__ import annotations

import os
import time
from typing import Any, Dict


def _mode() -> str:
    return str(os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower() or "safe"


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name), "")
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _safe_no_credential_runtime_mode() -> bool:
    try:
        from services.data_source_manager import safe_no_credential_market_data_mode

        return bool(safe_no_credential_market_data_mode())
    except Exception:
        return False


def run_post_bind_boot(dashboard_module, handler_ctx: Dict[str, Any]) -> None:
    jobs = dashboard_module._jobs_manager()
    supervisor = dashboard_module._runtime_supervisor()
    runtime_orchestrator = dashboard_module._runtime_orchestrator()
    safe_no_credential = _safe_no_credential_runtime_mode()

    try:
        if dashboard_module._PRIMARY_BOOTSTRAP_DONE:
            dashboard_module.log.info(
                "dashboard_server skipping lifecycle monitor boot claim under start_system supervision"
            )
        dashboard_module.start_lifecycle_monitor(
            get_health=lambda: dashboard_module.get_health_snapshot(),
            get_jobs=lambda: jobs.list_jobs(),
            get_kill_switches=dashboard_module._get_kill_switches_snapshot,
            interval_s=2.0,
            claim_booting=(not dashboard_module._PRIMARY_BOOTSTRAP_DONE),
        )
    except Exception:
        dashboard_module.log.exception("failed to start lifecycle monitor")
        raise

    if safe_no_credential and not _env_flag("MODEL_SCORING_ENABLED", False):
        dashboard_module._BOOT_DIAGNOSTICS["model_scoring"] = {
            "enabled": False,
            "ok": True,
            "skipped": True,
            "reason": "safe_no_credential_mode",
        }
        dashboard_module._publish_boot_diagnostics()
    else:
        try:
            from engine.model_scoring import start_model_scoring_service

            scoring_snapshot = start_model_scoring_service()
            dashboard_module._BOOT_DIAGNOSTICS["model_scoring"] = dict(scoring_snapshot or {})
            dashboard_module._publish_boot_diagnostics()
            if bool((scoring_snapshot or {}).get("enabled")) and not bool((scoring_snapshot or {}).get("ok")):
                raise RuntimeError(f"model_scoring_start_failed:{scoring_snapshot}")
        except Exception:
            dashboard_module.log.exception("failed to start model_scoring service")
            raise

    if not (safe_no_credential and not _env_flag("AUTO_ROLLBACK_ENABLED", False)):
        try:
            dashboard_module._start_background_thread(
                "auto_rollback_loop",
                dashboard_module.auto_rollback_loop,
                (dashboard_module.api_post_rollback, dashboard_module.write_job_history),
            )
        except Exception:
            dashboard_module.log.exception("failed to start auto_rollback_loop thread")
            raise

    try:
        preflight = dashboard_module._run_preflight_bounded()
        dashboard_module._BOOT_DIAGNOSTICS["startup_preflight"] = preflight
        dashboard_module._publish_boot_diagnostics()
        dashboard_module._update_startup_trace(
            "RUNNING",
            status="started",
            detail="post_bind_preflight",
            extra={"preflight_ok": bool(preflight.get("ok"))},
        )

        if not preflight.get("ok"):
            dashboard_module.log.error("preflight FAILED at startup")

            for note in preflight.get("notes", []):
                dashboard_module.log.error("preflight note: %s", note)

            if dashboard_module._is_timeout_only_preflight(preflight):
                dashboard_module.log.warning(
                    "startup preflight timed out with schema intact; skipping synchronous boot auto-repair"
                )
                dashboard_module._BOOT_DIAGNOSTICS["startup_repair"] = {
                    "ok": False,
                    "skipped": True,
                    "reason": "startup_preflight_timeout_only",
                    "notes": list(preflight.get("notes") or []),
                    "ts_ms": int(time.time() * 1000),
                }
                dashboard_module._publish_boot_diagnostics()
                dashboard_module._update_startup_trace(
                    "RUNNING",
                    status="ok",
                    detail="post_bind_preflight_timeout_warning",
                    extra={"notes": list(preflight.get("notes") or [])},
                )
            else:
                try:
                    dashboard_module._BOOT_DIAGNOSTICS["auto_repair_attempted"] = True
                    dashboard_module._publish_boot_diagnostics()
                    repair = dashboard_module.api_post_self_repair(
                        None,
                        None,
                        {
                            "JOBS": jobs,
                            "SUPERVISOR": supervisor,
                            "ORCHESTRATOR": runtime_orchestrator,
                            "API_HANDLERS": dashboard_module.API_HANDLERS,
                        },
                    )
                    dashboard_module._BOOT_DIAGNOSTICS["startup_repair"] = repair
                    dashboard_module._BOOT_DIAGNOSTICS["startup_preflight"] = dashboard_module._run_preflight_bounded()
                    dashboard_module._publish_boot_diagnostics()
                except Exception as repair_error:
                    dashboard_module._BOOT_DIAGNOSTICS["startup_repair"] = {
                        "ok": False,
                        "error": str(repair_error),
                    }
                    dashboard_module._publish_boot_diagnostics()
                    dashboard_module.log.exception("startup auto repair exception")

                final_preflight = dict(dashboard_module._BOOT_DIAGNOSTICS.get("startup_preflight") or preflight)
                if not bool(final_preflight.get("ok")):
                    raise RuntimeError(
                        "startup_preflight_failed:"
                        + ",".join(str(item) for item in (final_preflight.get("notes") or []))
                    )
                dashboard_module._update_startup_trace(
                    "RUNNING",
                    status="ok",
                    detail="post_bind_preflight_recovered",
                )
        else:
            dashboard_module._update_startup_trace(
                "RUNNING",
                status="ok",
                detail="post_bind_preflight_ok",
            )
            dashboard_module.log.info("preflight OK")

    except Exception as error:
        dashboard_module._record_startup_failure(
            "RUNNING",
            error,
            module="dashboard_server.run_preflight",
            file_path=dashboard_module.__file__,
        )
        dashboard_module._update_startup_trace(
            "RUNNING",
            status="failed",
            detail=f"preflight_exception:{error}",
        )
        dashboard_module._BOOT_DIAGNOSTICS["startup_preflight"] = {"ok": False, "error": str(error)}
        dashboard_module._publish_boot_diagnostics()
        dashboard_module.log.exception("preflight exception")

    validation: Dict[str, Any] = {"ok": True}
    try:
        if hasattr(supervisor, "validate_graph"):
            validation = supervisor.validate_graph(strict=True)
    except Exception:
        validation = {"ok": False, "errors": ["validate_graph_exception"]}
    dashboard_module._BOOT_DIAGNOSTICS["graph_validation"] = dict(validation or {})
    dashboard_module._publish_boot_diagnostics()

    if not validation.get("ok"):
        try:
            if dashboard_module.set_state:
                dashboard_module.set_state(dashboard_module.DEGRADED, "invalid_dependency_graph")
        except Exception as error:
            dashboard_module._warn_nonfatal(
                "DASHBOARD_SERVER_SET_STATE_FAILED",
                error,
                scope="invalid_dependency_graph",
            )
        graph_errors = validation.get("errors")
        if not isinstance(graph_errors, list):
            graph_errors = []
        raise RuntimeError(f"invalid_dependency_graph: {list(graph_errors)}")

    if not dashboard_module.ALLOWED_JOBS:
        raise RuntimeError("no_allowed_jobs_registered")

    auto_boot_daemons = dashboard_module.AUTO_BOOT_DAEMONS and (not dashboard_module._PRIMARY_BOOTSTRAP_DONE)
    ingestion_enabled = str(os.environ.get("START_INGESTION_WITH_SERVER", "1")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

    def _non_price_daemon_targets() -> list[str]:
        ordered: list[str] = []
        for name in dashboard_module.get_boot_jobs():
            try:
                meta = dashboard_module.ALLOWED_JOBS.get(name)
                if not (isinstance(meta, tuple) and len(meta) >= 3 and meta[1] == "daemon"):
                    continue
                if name == "ingestion_runtime":
                    continue
                if meta[2] == "price_feed":
                    continue
                ordered.append(name)
            except Exception as error:
                dashboard_module._warn_nonfatal(
                    "DASHBOARD_SERVER_BOOT_JOB_FILTER_FAILED",
                    error,
                    job=str(name),
                )
        return ordered

    def _price_running_now() -> bool:
        try:
            from engine.runtime.ipc import market_data_status

            snapshot = market_data_status(
                max_age_ms=int(float(os.environ.get("HEALTH_PRICES_MAX_AGE_S", "120")) * 1000.0)
            )
            if snapshot.get("ok") and snapshot.get("running"):
                return True
        except Exception as error:
            dashboard_module._warn_nonfatal("DASHBOARD_SERVER_MARKET_DATA_STATUS_FAILED", error)

        try:
            for job in jobs.list_jobs():
                if not job.get("running"):
                    continue
                if job.get("category") == "price_feed":
                    return True
                if job.get("name") in dashboard_module._OPERATOR_PRICE_JOB_CANDIDATES:
                    return True
        except Exception as error:
            dashboard_module._warn_nonfatal(
                "DASHBOARD_SERVER_LIST_JOBS_FAILED",
                error,
                scope="price_running_now",
            )
        return False

    if not auto_boot_daemons:
        dashboard_module.log.info(
            "AUTO_BOOT_DAEMONS=0 or primary bootstrap already owns runtime -> skipping duplicate daemon auto-boot"
        )
        boot_errors: list[Dict[str, Any]] = []
    else:
        static_targets = dashboard_module._dashboard_auto_boot_static_targets(
            list(dashboard_module.AUTO_BOOT_TARGETS) if dashboard_module.AUTO_BOOT_TARGETS else [],
            ingestion_enabled=ingestion_enabled,
        )
        price_candidates = dashboard_module._dashboard_auto_boot_price_candidates(
            ingestion_enabled=ingestion_enabled,
        )
        non_price_targets = [
            name
            for name in _non_price_daemon_targets()
            if name not in static_targets
        ]

        dashboard_module.log.info(
            "boot_plan mode=%s static_targets=%s price_candidates=%s non_price_targets=%s",
            _mode(),
            static_targets,
            price_candidates,
            non_price_targets,
        )

        boot_errors = []

        if not static_targets and not price_candidates and not ingestion_enabled:
            dashboard_module.log.error("No price boot candidates resolved from ALLOWED_JOBS")
            try:
                if dashboard_module.set_state:
                    dashboard_module.set_state(dashboard_module.DEGRADED, "no_price_boot_candidates")
            except Exception as error:
                dashboard_module._warn_nonfatal(
                    "DASHBOARD_SERVER_SET_STATE_FAILED",
                    error,
                    scope="no_price_boot_candidates",
                )
        elif ingestion_enabled:
            dashboard_module.log.info("isolated ingestion enabled -> skipping dashboard-side ingestion/price auto-boot")

        if non_price_targets:
            non_price_result = supervisor.deterministic_start(
                non_price_targets,
                include_deps=True,
                strict=False,
            )
            dashboard_module.log.info("SUPERVISOR non-price boot result: %s", non_price_result)
            if not non_price_result.get("ok"):
                boot_errors.append(non_price_result)

        if static_targets:
            dashboard_module.log.info("SUPERVISOR deterministic_start targets: %s", static_targets)
            result = supervisor.deterministic_start(
                static_targets,
                include_deps=True,
                strict=True,
            )
            dashboard_module.log.info("SUPERVISOR boot result: %s", result)
            if not result.get("ok"):
                boot_errors.append(result)
        else:
            for candidate in price_candidates or []:
                dashboard_module.log.info("SUPERVISOR price boot candidate: %s", candidate)
                result = supervisor.deterministic_start(
                    [candidate],
                    include_deps=True,
                    strict=False,
                )
                dashboard_module.log.info("SUPERVISOR price boot result for %s: %s", candidate, result)

                if _price_running_now():
                    break

                boot_errors.append({"candidate": candidate, "result": result})

        if not _price_running_now() and not ingestion_enabled:
            dashboard_module.log.warning("No price daemon running after boot - trying full fallback chain")
            try:
                if dashboard_module.set_state:
                    dashboard_module.set_state(dashboard_module.DEGRADED, "no_price_daemon_running")
            except Exception as error:
                dashboard_module._warn_nonfatal(
                    "DASHBOARD_SERVER_SET_STATE_FAILED",
                    error,
                    scope="no_price_daemon_running",
                )

            for candidate in price_candidates or []:
                try:
                    result = supervisor.deterministic_start(
                        [candidate],
                        include_deps=True,
                        strict=False,
                    )
                    dashboard_module.log.info("forced_price_start_result %s: %s", candidate, result)
                except Exception as error:
                    dashboard_module.log.error("forced_price_start_failed %s: %s", candidate, error)

                if _price_running_now():
                    break

        if not _price_running_now():
            if _mode() == "safe":
                dashboard_module.log.error("auto_boot_failed (SAFE) - continuing without price daemon: %s", boot_errors)
            else:
                raise RuntimeError(f"auto_boot_failed: {boot_errors}")

    auto_startup_bootstrap = dashboard_module._env_bool("AUTO_STARTUP_BOOTSTRAP", True)
    if dashboard_module._PRIMARY_BOOTSTRAP_DONE and auto_startup_bootstrap:
        dashboard_module.log.info("ENGINE_PRIMARY_BOOTSTRAP_DONE=1 -> skipping duplicate startup orchestrator")
        auto_startup_bootstrap = False

    if auto_startup_bootstrap and not dashboard_module._STARTUP_ORCHESTRATOR_THREAD_STARTED:
        dashboard_module._STARTUP_ORCHESTRATOR_THREAD_STARTED = True
        dashboard_module._BOOT_DIAGNOSTICS["startup_orchestrator"] = {
            "ok": None,
            "started": True,
            "mode": _mode(),
            "error": "",
            "ts_ms": int(time.time() * 1000),
        }
        dashboard_module._publish_boot_diagnostics()
        dashboard_module._update_startup_trace("RUNNING", status="started", detail="startup_orchestrator_begin")
        dashboard_module.log.info("startup_orchestrator_begin mode=%s", _mode())
        try:
            orchestrator = dashboard_module.StartupOrchestrator(
                jobs=jobs,
                supervisor=supervisor,
                health_fn=lambda: dashboard_module.API_HANDLERS["api_get_health"](None, handler_ctx),
                log=dashboard_module.log,
            )
            result = orchestrator.run(_mode())
            dashboard_module._BOOT_DIAGNOSTICS["startup_orchestrator"] = result
            dashboard_module._publish_boot_diagnostics()
            dashboard_module._update_startup_trace(
                "RUNNING",
                status="ok" if bool(result.get("ok")) else "failed",
                detail="startup_orchestrator_done",
                extra={"ready": bool(result.get("ready")), "ok": bool(result.get("ok"))},
            )
            dashboard_module.log.info(
                "startup_orchestrator_result ok=%s ready=%s",
                bool(result.get("ok")),
                bool(result.get("ready")),
            )
        except Exception as error:
            dashboard_module._BOOT_DIAGNOSTICS["startup_orchestrator"] = {
                "ok": False,
                "error": str(error),
            }
            dashboard_module._publish_boot_diagnostics()
            dashboard_module._record_startup_failure(
                "RUNNING",
                error,
                module="dashboard_server.startup_orchestrator",
                file_path=dashboard_module.__file__,
            )
            dashboard_module._update_startup_trace(
                "RUNNING",
                status="failed",
                detail=f"startup_orchestrator_failed:{error}",
            )
            dashboard_module.log.exception("startup_orchestrator_failed: %s", error)

    if dashboard_module.AUTO_PIPELINE:
        dashboard_module.log.info(
            "auto_pipeline enabled interval_s=%s include_execution=%s mode=%s",
            dashboard_module.AUTO_PIPELINE_INTERVAL_S,
            dashboard_module.AUTO_PIPELINE_INCLUDE_EXECUTION,
            _mode(),
        )
        if not dashboard_module._AUTO_PIPELINE_THREAD_STARTED:
            dashboard_module._AUTO_PIPELINE_THREAD_STARTED = True
            dashboard_module._start_background_thread(
                "auto_pipeline_loop",
                runtime_orchestrator.auto_pipeline_loop,
            )

    if dashboard_module.AUTO_CHALLENGER:
        dashboard_module.log.info(
            "auto_challenger enabled interval_s=%s drift_gate=%s",
            dashboard_module.AUTO_CHALLENGER_INTERVAL_S,
            dashboard_module.AUTO_CHALLENGER_MIN_DRIFT,
        )
        if not dashboard_module._AUTO_CHALLENGER_THREAD_STARTED:
            dashboard_module._AUTO_CHALLENGER_THREAD_STARTED = True
            dashboard_module._start_background_thread(
                "auto_challenger_loop",
                runtime_orchestrator.auto_challenger_loop,
            )

    if dashboard_module.AUTO_SIZE_POLICY:
        dashboard_module.log.info(
            "auto_size_policy enabled interval_s=%s",
            dashboard_module.AUTO_SIZE_POLICY_INTERVAL_S,
        )
        if not dashboard_module._AUTO_SIZE_POLICY_THREAD_STARTED:
            dashboard_module._AUTO_SIZE_POLICY_THREAD_STARTED = True
            dashboard_module._start_background_thread(
                "auto_size_policy_loop",
                runtime_orchestrator.auto_size_policy_loop,
            )


def run_post_bind_boot_safe(dashboard_module, handler_ctx: Dict[str, Any]) -> None:
    dashboard_module._BOOT_DIAGNOSTICS["post_bind_boot"] = {
        "started": True,
        "ok": None,
        "error": "",
        "ts_ms": int(time.time() * 1000),
    }
    dashboard_module._publish_boot_diagnostics()
    try:
        dashboard_module.log.info("dashboard_server_post_bind_boot_begin")

        storage_probe = {"ok": True}
        probe_fn = getattr(dashboard_module, "_dashboard_storage_readiness_probe", None)
        if callable(probe_fn):
            storage_probe = dict(probe_fn(force=True, startup=False) or {})
        if storage_probe.get("ok") is False:
            dashboard_module.log.error("dashboard_storage_unavailable_degraded: %s", storage_probe)
            dashboard_module._BOOT_DIAGNOSTICS["runtime_bootstrap"] = {
                "ok": False,
                "skipped": True,
                "reason": "runtime_storage_unavailable",
                "storage": dict(storage_probe),
                "ts_ms": int(time.time() * 1000),
            }
            dashboard_module._BOOT_DIAGNOSTICS["post_bind_boot"] = {
                "started": True,
                "ok": False,
                "error": "runtime_storage_unavailable",
                "ts_ms": int(time.time() * 1000),
            }
            dashboard_module._publish_boot_diagnostics()
            dashboard_module._update_startup_trace(
                "RUNNING",
                status="failed",
                detail="runtime_storage_unavailable",
                extra={"storage": dict(storage_probe)},
            )
            return

        dashboard_module._ensure_runtime_orchestration()
        handler_ctx["JOBS"] = dashboard_module.JOBS
        handler_ctx["SUPERVISOR"] = dashboard_module.SUPERVISOR
        handler_ctx["ORCHESTRATOR"] = dashboard_module.ORCHESTRATOR

        if dashboard_module._PRIMARY_BOOTSTRAP_DONE:
            runtime_boot = {
                "ok": True,
                "skipped": True,
                "reason": "primary_bootstrap_completed_by_start_system",
            }
        else:
            runtime_boot = dashboard_module.bootstrap_runtime(log=dashboard_module.log)
        dashboard_module._BOOT_DIAGNOSTICS["runtime_bootstrap"] = runtime_boot
        dashboard_module._publish_boot_diagnostics()

        if not isinstance(runtime_boot, dict) or not runtime_boot.get("ok"):
            dashboard_module.log.error("bootstrap_runtime_failed_non_fatal: %s", runtime_boot)
            try:
                if dashboard_module.set_state:
                    dashboard_module.set_state(dashboard_module.DEGRADED, f"bootstrap_runtime_failed:{runtime_boot}")
            except Exception as error:
                dashboard_module._warn_nonfatal(
                    "DASHBOARD_SERVER_SET_STATE_FAILED",
                    error,
                    scope="bootstrap_runtime_failed",
                )
            return

        run_post_bind_boot(dashboard_module, handler_ctx)
        dashboard_module.log.info("dashboard_server_post_bind_boot_ok")

        dashboard_module._BOOT_DIAGNOSTICS["post_bind_boot"] = {
            "started": True,
            "ok": True,
            "error": "",
            "ts_ms": int(time.time() * 1000),
        }
        dashboard_module._publish_boot_diagnostics()
    except Exception as error:
        dashboard_module._record_startup_failure(
            "RUNNING",
            error,
            module="dashboard_server._post_bind_boot",
            file_path=dashboard_module.__file__,
        )
        dashboard_module._update_startup_trace(
            "RUNNING",
            status="failed",
            detail=f"post_bind_boot_failed:{error}",
        )
        dashboard_module.log.exception("post_bind_boot_failed")
        dashboard_module._BOOT_DIAGNOSTICS["ok"] = False
        dashboard_module._BOOT_DIAGNOSTICS["post_bind_boot"] = {
            "started": True,
            "ok": False,
            "error": str(error),
            "ts_ms": int(time.time() * 1000),
        }
        dashboard_module._publish_boot_diagnostics()
        is_storage_error = False
        try:
            checker = getattr(dashboard_module, "_is_dashboard_storage_unavailable_error", None)
            is_storage_error = bool(callable(checker) and checker(error))
        except Exception:
            is_storage_error = False
        if is_storage_error:
            storage_payload = {}
            try:
                from engine.runtime.storage_pool import storage_readiness_snapshot

                storage_payload = dict(storage_readiness_snapshot() or {})
            except Exception:
                storage_payload = {
                    "checked": True,
                    "ok": False,
                    "status": "unavailable",
                    "storage": "postgres",
                    "backend": "postgres",
                    "degraded": True,
                    "detail": "runtime_storage_unavailable",
                    "error": str(error),
                    "ts_ms": int(time.time() * 1000),
                }
            dashboard_module._BOOT_DIAGNOSTICS["storage"] = dict(storage_payload)
            dashboard_module._BOOT_DIAGNOSTICS["runtime_bootstrap"] = {
                "ok": False,
                "skipped": True,
                "reason": "runtime_storage_unavailable",
                "storage": dict(storage_payload),
                "ts_ms": int(time.time() * 1000),
            }
            dashboard_module._BOOT_DIAGNOSTICS["post_bind_boot"] = {
                "started": True,
                "ok": False,
                "error": "runtime_storage_unavailable",
                "ts_ms": int(time.time() * 1000),
            }
            dashboard_module._publish_boot_diagnostics()
            dashboard_module.log.error("post_bind_boot_storage_unavailable_non_fatal: %s", error)
            return
        try:
            if dashboard_module.append_event:
                dashboard_module.append_event(
                    event_type="post_bind_boot_failed",
                    event_source="dashboard_server",
                    entity_type="runtime",
                    entity_id="dashboard_server",
                    payload={
                        "error": str(error),
                        "host": str(dashboard_module.host),
                        "port": int(dashboard_module.port),
                        "engine_mode": str(os.environ.get("ENGINE_MODE", "safe") or "safe"),
                        "ts_ms": int(time.time() * 1000),
                    },
                    ts_ms=int(time.time() * 1000),
                    best_effort=True,
                )
        except Exception as append_error:
            dashboard_module.log.exception("post_bind_boot_failed_append_event_failed: %s", append_error)
        try:
            if dashboard_module.set_state:
                dashboard_module.set_state(dashboard_module.DEGRADED, f"dashboard_bound_post_bind_boot_failed:{error}")
        except Exception:
            dashboard_module.log.exception("post_bind_boot_set_state_failed")
        dashboard_module.log.error("post_bind_boot_failed_non_fatal: %s", error)
        try:
            if dashboard_module.set_state:
                dashboard_module.set_state(dashboard_module.DEGRADED, f"post_bind_boot_failed:{error}")
        except Exception as set_state_error:
            dashboard_module._warn_nonfatal(
                "DASHBOARD_SERVER_SET_STATE_FAILED",
                set_state_error,
                scope="post_bind_boot_failed",
            )
        try:
            dashboard_module._shutdown_runtime_once(f"post_bind_boot_failed:{error}")
        except Exception as shutdown_error:
            dashboard_module._warn_nonfatal(
                "DASHBOARD_SERVER_SHUTDOWN_ON_BOOT_FAILURE_FAILED",
                shutdown_error,
            )
        try:
            if dashboard_module._HTTPD:
                dashboard_module._HTTPD.shutdown()
        except Exception as httpd_error:
            dashboard_module._warn_nonfatal(
                "DASHBOARD_SERVER_HTTPD_SHUTDOWN_ON_BOOT_FAILURE_FAILED",
                httpd_error,
            )
        raise


def launch_post_bind_runtime_threads(dashboard_module, handler_ctx: Dict[str, Any]) -> None:
    dashboard_module._start_background_thread(
        "health_cache_prewarm",
        dashboard_module._prewarm_health_cache,
        (handler_ctx,),
    )
    dashboard_module._start_background_thread(
        "post_bind_boot",
        run_post_bind_boot_safe,
        (dashboard_module, handler_ctx),
    )
