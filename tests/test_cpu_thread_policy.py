from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from engine.runtime.thread_policy import (
    BLAS_THREAD_ENV_KEYS,
    apply_cpu_thread_policy_to_env,
    cpu_thread_policy_snapshot,
)


def test_ingestion_child_env_generation_caps_thread_pools() -> None:
    env = {
        "ENGINE_SUPERVISED": "1",
        "RUNTIME_CPUS": "12",
        "TRADING_CPU_THREAD_POLICY": "auto",
    }

    policy = apply_cpu_thread_policy_to_env(
        env,
        role="ingestion_child",
        supervised_process_count=16,
    )

    assert policy["role"] == "ingestion_child"
    assert policy["cpu_threads"] == 1
    assert policy["interop_threads"] == 1
    assert env["ENGINE_SUPERVISED_PROCESS_COUNT"] == "16"
    for key in BLAS_THREAD_ENV_KEYS:
        assert env[key] == "1"
    assert env["TORCH_CPU_THREADS"] == "1"
    assert env["TORCH_INTEROP_THREADS"] == "1"


def test_role_specific_defaults_when_capacity_is_available() -> None:
    base = {
        "ENGINE_SUPERVISED": "1",
        "RUNTIME_CPUS": "16",
        "START_INGESTION_WITH_SERVER": "0",
    }

    ingestion_child = cpu_thread_policy_snapshot(
        dict(base),
        role="ingestion_child",
        supervised_process_count=2,
    )
    inference = cpu_thread_policy_snapshot(
        dict(base),
        role="inference",
        supervised_process_count=2,
    )
    training = cpu_thread_policy_snapshot(
        dict(base),
        role="training",
        supervised_process_count=2,
    )

    assert ingestion_child["cpu_threads"] == 1
    assert inference["cpu_threads"] == 2
    assert training["cpu_threads"] == 4
    assert inference["interop_threads"] == 1
    assert training["interop_threads"] == 2


def test_many_children_fall_to_one_thread_instead_of_full_cpu_count() -> None:
    env = {
        "ENGINE_SUPERVISED": "1",
        "RUNTIME_CPUS": "32",
        "TRADING_CPU_THREAD_POLICY": "auto",
    }

    policy = apply_cpu_thread_policy_to_env(
        env,
        role="ingestion_child",
        supervised_process_count=40,
    )

    assert policy["per_process_budget"] == 1
    assert policy["cpu_threads"] == 1
    assert policy["oversubscription_guarded"] is True
    assert env["OMP_NUM_THREADS"] == "1"
    assert env["MKL_NUM_THREADS"] == "1"
    assert env["OPENBLAS_NUM_THREADS"] == "1"
    assert env["NUMEXPR_NUM_THREADS"] == "1"
    assert env["TORCH_CPU_THREADS"] == "1"


def test_operator_override_is_explicit_and_applied() -> None:
    env = {
        "ENGINE_SUPERVISED": "1",
        "RUNTIME_CPUS": "32",
        "TRADING_CPU_THREAD_POLICY": "auto",
        "TRADING_CPU_THREADS_PER_PROCESS": "3",
        "TRADING_TORCH_INTEROP_THREADS_PER_PROCESS": "2",
    }

    policy = apply_cpu_thread_policy_to_env(
        env,
        role="inference",
        supervised_process_count=20,
    )

    assert policy["operator_override"] is True
    assert policy["source"] == "operator_override"
    assert env["OMP_NUM_THREADS"] == "3"
    assert env["TORCH_CPU_THREADS"] == "3"
    assert env["TORCH_INTEROP_THREADS"] == "2"


def test_manual_policy_preserves_existing_thread_env() -> None:
    env = {
        "ENGINE_SUPERVISED": "1",
        "RUNTIME_CPUS": "16",
        "TRADING_CPU_THREAD_POLICY": "manual",
        "OMP_NUM_THREADS": "7",
        "TORCH_CPU_THREADS": "6",
        "TORCH_INTEROP_THREADS": "2",
    }

    policy = apply_cpu_thread_policy_to_env(
        env,
        role="runtime",
        supervised_process_count=8,
    )

    assert policy["mode"] == "manual"
    assert policy["operator_override"] is True
    assert env["OMP_NUM_THREADS"] == "7"
    assert env["TORCH_CPU_THREADS"] == "6"
    assert env["TORCH_INTEROP_THREADS"] == "2"


def test_recorded_policy_role_is_reused_by_later_policy_calls() -> None:
    env = {
        "ENGINE_SUPERVISED": "1",
        "RUNTIME_CPUS": "16",
        "START_INGESTION_WITH_SERVER": "0",
    }

    first = apply_cpu_thread_policy_to_env(
        env,
        role="inference",
        supervised_process_count=2,
    )
    second = apply_cpu_thread_policy_to_env(
        env,
        supervised_process_count=2,
    )

    assert first["role"] == "inference"
    assert second["role"] == "inference"
    assert env["TORCH_CPU_THREADS"] == "2"


def test_supervised_python_process_gets_policy_before_user_code() -> None:
    repo = Path(__file__).resolve().parents[1]
    env = dict(os.environ)
    env.update(
        {
            "PYTHONPATH": str(repo),
            "ENGINE_SUPERVISED": "1",
            "ENGINE_PROCESS_ROLE": "ingestion_child",
            "RUNTIME_CPUS": "32",
            "ENGINE_SUPERVISED_PROCESS_COUNT": "40",
            "TRADING_CPU_THREAD_POLICY": "auto",
            "OMP_NUM_THREADS": "32",
            "TORCH_CPU_THREADS": "32",
        }
    )

    raw = subprocess.check_output(
        [
            sys.executable,
            "-c",
            (
                "import json, os; "
                "print(json.dumps({"
                "'omp': os.environ.get('OMP_NUM_THREADS'), "
                "'torch': os.environ.get('TORCH_CPU_THREADS'), "
                "'role': os.environ.get('ENGINE_CPU_THREAD_POLICY_ROLE')"
                "}, sort_keys=True))"
            ),
        ],
        cwd=str(repo),
        env=env,
        text=True,
    )

    payload = json.loads(raw.strip())
    assert payload == {"omp": "1", "role": "ingestion_child", "torch": "1"}
