from __future__ import annotations

import unittest

from engine.runtime.resource_isolation import check_resource_isolation


def _recommended_env() -> dict[str, str]:
    return {
        "ENV": "prod",
        "PREFLIGHT_CHECK_RESOURCE_LIMITS": "1",
        "TRADING_RESOURCE_HOST_CPUS": "32",
        "TRADING_RESOURCE_HOST_MEMORY": "123g",
        "TRADING_RESOURCE_MIN_HEADROOM_CPUS": "6",
        "TRADING_RESOURCE_MIN_HEADROOM_MEMORY": "24g",
        "RUNTIME_CPUS": "12",
        "RUNTIME_MEM_LIMIT": "48g",
        "RUNTIME_SHM_SIZE": "8g",
        "TIMESCALE_CPUS": "8",
        "TIMESCALE_MEM_LIMIT": "32g",
        "TIMESCALE_SHM_SIZE": "2g",
        "REDIS_CPUS": "2",
        "REDIS_MEM_LIMIT": "8g",
        "REDIS_MAXMEMORY": "6gb",
        "REDIS_MAXMEMORY_POLICY": "allkeys-lru",
        "MINIO_CPUS": "2",
        "MINIO_MEM_LIMIT": "6g",
        "OPERATOR_CPUS": "1",
        "OPERATOR_MEM_LIMIT": "2g",
        "TIMESCALE_SHARED_BUFFERS": "8GB",
        "TIMESCALE_EFFECTIVE_CACHE_SIZE": "22GB",
        "TIMESCALE_WORK_MEM": "48MB",
        "TIMESCALE_MAINTENANCE_WORK_MEM": "2GB",
        "TIMESCALE_MAX_CONNECTIONS": "100",
        "RESOURCE_SCHEDULER_GLOBAL_MAX": "2",
        "RESOURCE_SCHEDULER_EXECUTION_MAX": "1",
        "RESOURCE_SCHEDULER_INFERENCE_MAX": "1",
        "RESOURCE_SCHEDULER_TRAINING_MAX": "1",
        "RESOURCE_SCHEDULER_REPLAY_MAX": "1",
        "RESOURCE_SCHEDULER_BACKGROUND_MAX": "1",
        "MODEL_TRAIN_N_JOBS": "1",
        "MODEL_TRAIN_MAX_N_JOBS": "2",
        "LGBM_N_JOBS": "1",
        "LGBM_RANKER_N_JOBS": "1",
        "XGB_N_JOBS": "1",
        "META_LABEL_N_JOBS": "1",
        "TSFRESH_N_JOBS": "0",
        "TSFRESH_MAX_N_JOBS": "1",
        "TSFRESH_SNAPSHOT_SYMBOL_LIMIT": "100",
        "TSFRESH_SNAPSHOT_MAX_SYMBOLS": "100",
        "TSFRESH_SNAPSHOT_BATCH_SIZE": "25",
        "TSFRESH_SNAPSHOT_MAX_BATCH_SIZE": "25",
        "TUNE_N_TRIALS": "10",
        "TUNE_MAX_N_TRIALS": "10",
        "OMP_NUM_THREADS": "8",
        "MKL_NUM_THREADS": "8",
        "OPENBLAS_NUM_THREADS": "8",
        "NUMEXPR_NUM_THREADS": "8",
        "NUMEXPR_MAX_THREADS": "8",
        "VECLIB_MAXIMUM_THREADS": "8",
        "TORCH_CPU_THREADS": "8",
        "TORCH_INTEROP_THREADS": "4",
    }


class ResourceIsolationTests(unittest.TestCase):
    def test_recommended_compose_profile_passes_consistency_checks(self) -> None:
        summary = check_resource_isolation(_recommended_env())

        self.assertTrue(bool(summary.get("checked")))
        self.assertTrue(bool(summary.get("ok")))
        self.assertEqual(list(summary.get("warnings") or []), [])
        self.assertTrue(any("resource isolation ok" in item for item in list(summary.get("notes") or [])))

    def test_production_without_limits_warns_unbounded(self) -> None:
        summary = check_resource_isolation({"ENV": "prod"})

        warnings = list(summary.get("warnings") or [])
        self.assertFalse(bool(summary.get("ok")))
        self.assertTrue(any("service=runtime cpu_limit_env=RUNTIME_CPUS" in item for item in warnings))
        self.assertTrue(any("service=timescaledb memory_limit_env=TIMESCALE_MEM_LIMIT" in item for item in warnings))
        self.assertTrue(any("TRADING_RESOURCE_HOST_MEMORY is missing" in item for item in warnings))

    def test_memory_and_thread_inconsistencies_warn(self) -> None:
        env = _recommended_env()
        env.update(
            {
                "TIMESCALE_WORK_MEM": "512MB",
                "REDIS_MAXMEMORY": "8gb",
                "OMP_NUM_THREADS": "64",
                "MODEL_TRAIN_N_JOBS": "24",
            }
        )

        summary = check_resource_isolation(env)

        warnings = list(summary.get("warnings") or [])
        self.assertFalse(bool(summary.get("ok")))
        self.assertTrue(any("postgres peak memory estimate high" in item for item in warnings))
        self.assertTrue(any("redis maxmemory too close" in item for item in warnings))
        self.assertTrue(any("runtime thread default exceeds CPU limit" in item for item in warnings))
        self.assertTrue(any("runtime worker default exceeds CPU limit" in item for item in warnings))


if __name__ == "__main__":
    unittest.main()
