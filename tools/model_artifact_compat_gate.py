from __future__ import annotations

"""Load/predict compatibility gate for committed model artifacts."""

import argparse
import hashlib
import json
import os
import platform
import sys
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Any

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.artifacts.serialization import dump_pickle_artifact, load_pickle_artifact  # noqa: E402

FIXTURE_DIR = ROOT / "tests" / "fixtures" / "model_artifact_compat"
FEATURES_FILE = "features.npy"
GOLDEN_FILE = "golden_predictions.json"
SKLEARN_ARTIFACT = "sklearn_gradient_boosting.joblib"
LIGHTGBM_ARTIFACT = "lgbm_regressor.joblib"
FEATURE_IDS = (
    "base.source_credibility",
    "base.log_recency_hours",
    "base.normalized_text_len",
)
RTOL = 1e-5
ATOL = 1e-6


@dataclass(frozen=True)
class ArtifactSpec:
    key: str
    package: str
    path: str


ARTIFACTS = (
    ArtifactSpec("sklearn_gradient_boosting", "scikit-learn", SKLEARN_ARTIFACT),
    ArtifactSpec("lightgbm_regressor", "lightgbm", LIGHTGBM_ARTIFACT),
)
BASE_STACK_PACKAGES = ("numpy", "scikit-learn", "lightgbm")
VERSION_PACKAGES = ("numpy", "scikit-learn", "lightgbm", "joblib")
CPU_GATE_ENV = {
    "ASSET_MAP_USE_EQUITY_REGISTRY": "0",
    "TRADING_ACCELERATION_PROFILE": "cpu",
    "TRADING_ACCELERATOR_PROFILE": "cpu",
    "TRADING_DEPENDENCY_PROFILE": "cpu",
    "TRADING_HARDWARE_PROFILE": "cpu",
    "RUNTIME_HARDWARE_PROFILE": "cpu",
    "TORCH_DEVICE": "cpu",
    "EMBED_DEVICE": "cpu",
    "NLP_DEVICE": "cpu",
    "FINBERT_DEVICE": "cpu",
    "TS_FOUNDATION_DEVICE": "cpu",
}


def _force_cpu_gate_runtime() -> dict[str, str]:
    original = {key: str(os.environ.get(key, "")) for key in CPU_GATE_ENV}
    for key, value in CPU_GATE_ENV.items():
        os.environ[key] = value
    return original


def _runtime_versions() -> dict[str, str]:
    versions: dict[str, str] = {"python": platform.python_version()}
    for package in VERSION_PACKAGES:
        try:
            versions[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            versions[package] = "<missing>"
    return versions


def _fail(package: str, reason: str) -> int:
    print(f"model_artifact_incompat:{package}:{reason}", file=sys.stderr)
    return 1


def _json_dump(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _base_expected_versions() -> dict[str, str]:
    expected: dict[str, str] = {}
    for raw in (ROOT / "requirements-base.txt").read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if "==" not in line:
            continue
        name, version = line.split("==", 1)
        key = name.strip().replace("_", "-").lower()
        if key in BASE_STACK_PACKAGES:
            expected[key] = version.split(";", 1)[0].strip()
    return expected


def _assert_regenerate_base_stack() -> None:
    expected = _base_expected_versions()
    versions = _runtime_versions()
    mismatches = []
    for package in BASE_STACK_PACKAGES:
        want = expected.get(package, "")
        actual = versions.get(package, "")
        if not want or actual != want:
            mismatches.append(f"{package}:expected={want or '<missing>'}:actual={actual or '<missing>'}")
    if mismatches:
        raise RuntimeError("regenerate_requires_base_stack:" + ",".join(mismatches))


def _feature_matrix() -> np.ndarray:
    return np.asarray(
        [
            [0.00, 1.00, 1.00],
            [0.10, 0.75, 0.00],
            [0.25, 0.50, 0.50],
            [0.40, 0.25, 1.00],
            [0.55, 0.10, 0.00],
            [0.70, 0.35, 0.50],
            [0.85, 0.65, 1.00],
            [1.00, 0.90, 0.00],
        ],
        dtype=np.float32,
    )


def _training_matrix() -> tuple[np.ndarray, np.ndarray]:
    rows: list[list[float]] = []
    targets: list[float] = []
    for idx in range(64):
        f0 = float(idx % 9) / 8.0
        f1 = float((idx * 5) % 13) / 12.0
        f2 = 1.0 if idx % 4 in {0, 1} else 0.0
        interaction = f0 * (0.5 + f2)
        rows.append([f0, f1, f2])
        targets.append((1.65 * f0) - (0.85 * f1) + (0.35 * f2) + (0.18 * interaction))
    return np.asarray(rows, dtype=np.float32), np.asarray(targets, dtype=np.float32)


def _predict(value: Any, features: np.ndarray) -> np.ndarray:
    return np.asarray(value.predict(features), dtype=np.float64).reshape(-1)


def _build_sklearn_artifact(features: np.ndarray, target: np.ndarray, path: Path) -> Any:
    from sklearn.ensemble import GradientBoostingRegressor

    model = GradientBoostingRegressor(
        learning_rate=0.06,
        max_depth=2,
        n_estimators=32,
        random_state=17,
        subsample=1.0,
    )
    model.fit(features, target)
    dump_pickle_artifact(model, path, prefer_joblib=True)
    return load_pickle_artifact(path, prefer_joblib=True)


def _build_lightgbm_artifact(features: np.ndarray, target: np.ndarray, path: Path) -> Any:
    from engine.strategy.models.lgbm_regressor import train_lgbm_regressor

    model = train_lgbm_regressor(
        features,
        target,
        feature_ids=list(FEATURE_IDS),
        hyperparams={
            "learning_rate": 0.06,
            "min_child_samples": 2,
            "n_estimators": 32,
            "n_jobs": 1,
            "num_leaves": 7,
            "random_state": 17,
        },
        model_name="lgbm_regressor.compat_fixture",
    )
    model.save(path)
    return load_pickle_artifact(path, prefer_joblib=True)


def regenerate_fixtures(fixture_dir: Path = FIXTURE_DIR) -> int:
    _force_cpu_gate_runtime()
    try:
        _assert_regenerate_base_stack()
        fixture_dir.mkdir(parents=True, exist_ok=True)
        train_x, train_y = _training_matrix()
        features = _feature_matrix()
        features_path = fixture_dir / FEATURES_FILE
        np.save(features_path, features)

        loaded: dict[str, Any] = {
            "sklearn_gradient_boosting": _build_sklearn_artifact(
                train_x,
                train_y,
                fixture_dir / SKLEARN_ARTIFACT,
            ),
            "lightgbm_regressor": _build_lightgbm_artifact(
                train_x,
                train_y,
                fixture_dir / LIGHTGBM_ARTIFACT,
            ),
        }
        predictions = {
            key: [float(item) for item in _predict(model, features)]
            for key, model in sorted(loaded.items())
        }
        artifacts = {
            spec.key: {
                "package": spec.package,
                "path": spec.path,
                "sha256": _sha256(fixture_dir / spec.path),
            }
            for spec in ARTIFACTS
        }
        _json_dump(
            fixture_dir / GOLDEN_FILE,
            {
                "artifacts": artifacts,
                "base_stack_versions": _runtime_versions(),
                "feature_ids": list(FEATURE_IDS),
                "feature_matrix": FEATURES_FILE,
                "feature_matrix_sha256": _sha256(features_path),
                "generated_by": "tools/model_artifact_compat_gate.py --regenerate",
                "predictions": predictions,
                "schema_version": 1,
                "tolerance": {"atol": ATOL, "rtol": RTOL},
            },
        )
    except Exception as exc:
        return _fail("fixtures", f"{type(exc).__name__}:{exc}")

    print(f"model_artifact_compat_regenerated:{fixture_dir}")
    print("model_artifact_compat_versions:" + json.dumps(_runtime_versions(), sort_keys=True))
    return 0


def _load_features(path: Path) -> np.ndarray:
    if not path.exists():
        raise FileNotFoundError(path)
    features = np.load(path, allow_pickle=False)
    arr = np.asarray(features, dtype=np.float32)
    if arr.ndim != 2 or int(arr.shape[1]) != len(FEATURE_IDS):
        raise ValueError(f"feature_fixture_shape:{tuple(arr.shape)}")
    return arr


def _load_golden(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if int(payload.get("schema_version") or 0) != 1:
        raise ValueError("golden_schema_version")
    predictions = payload.get("predictions")
    if not isinstance(predictions, dict):
        raise ValueError("golden_predictions_missing")
    return payload


def _check_artifact(spec: ArtifactSpec, fixture_dir: Path, features: np.ndarray, golden: dict[str, Any]) -> int:
    artifact_path = fixture_dir / spec.path
    if not artifact_path.exists():
        return _fail(spec.package, f"missing_artifact:{artifact_path}")
    try:
        loaded = load_pickle_artifact(artifact_path, prefer_joblib=True)
    except Exception as exc:
        return _fail(spec.package, f"load_failed:{type(exc).__name__}:{exc}")

    try:
        predicted = _predict(loaded, features)
    except Exception as exc:
        return _fail(spec.package, f"predict_failed:{type(exc).__name__}:{exc}")

    expected_raw = dict(golden.get("predictions") or {}).get(spec.key)
    if expected_raw is None:
        return _fail(spec.package, f"golden_missing:{spec.key}")
    expected = np.asarray(expected_raw, dtype=np.float64).reshape(-1)
    if predicted.shape != expected.shape:
        return _fail(spec.package, f"prediction_shape:{tuple(predicted.shape)}:expected={tuple(expected.shape)}")

    try:
        np.testing.assert_allclose(predicted, expected, rtol=RTOL, atol=ATOL)
    except AssertionError as exc:
        max_abs_diff = float(np.max(np.abs(predicted - expected))) if predicted.size else 0.0
        return _fail(
            spec.package,
            "prediction_mismatch:"
            f"artifact={spec.key}:max_abs_diff={max_abs_diff:.8g}:"
            f"{str(exc).splitlines()[0]}",
        )

    max_abs_diff = float(np.max(np.abs(predicted - expected))) if predicted.size else 0.0
    print(f"model_artifact_compat_ok:{spec.key}:max_abs_diff={max_abs_diff:.8g}")
    return 0


def run_gate(fixture_dir: Path = FIXTURE_DIR, golden_path: Path | None = None) -> int:
    original_env = _force_cpu_gate_runtime()
    print(
        "model_artifact_compat_runtime_profile:"
        + json.dumps(
            {
                "effective": {key: os.environ.get(key, "") for key in CPU_GATE_ENV},
                "original": {key: value for key, value in original_env.items() if value},
            },
            sort_keys=True,
        )
    )
    print("model_artifact_compat_versions:" + json.dumps(_runtime_versions(), sort_keys=True))
    try:
        golden = _load_golden(golden_path or fixture_dir / GOLDEN_FILE)
        features_name = str(golden.get("feature_matrix") or FEATURES_FILE)
        features = _load_features(fixture_dir / features_name)
    except Exception as exc:
        return _fail("fixtures", f"{type(exc).__name__}:{exc}")

    for spec in ARTIFACTS:
        status = _check_artifact(spec, fixture_dir, features, golden)
        if status != 0:
            return status
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate committed model artifact load/predict compatibility.")
    parser.add_argument("--fixtures-dir", type=Path, default=FIXTURE_DIR)
    parser.add_argument("--golden-path", type=Path, default=None)
    parser.add_argument(
        "--regenerate",
        action="store_true",
        help="Regenerate fixtures and golden predictions. Must run on the base/CI stack.",
    )
    args = parser.parse_args(argv)

    fixture_dir = args.fixtures_dir.resolve()
    if args.regenerate:
        if args.golden_path is not None:
            return _fail("fixtures", "regenerate_rejects_golden_path_override")
        return regenerate_fixtures(fixture_dir)
    return run_gate(fixture_dir, args.golden_path.resolve() if args.golden_path is not None else None)


if __name__ == "__main__":
    raise SystemExit(main())
