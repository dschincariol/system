# Ops Scripts

The `ops/` directory contains operational utilities and offline analytics helpers.

These scripts are generally not part of the hot runtime path, but they are important for:

- offline checks and analytics
- backfills and calibration
- alerting
- maintenance and reporting

## Examples

- [check_events.py](check_events.py)
- [check_labels.py](check_labels.py)
- [check_predictions.py](check_predictions.py)
- [compute_drift.py](compute_drift.py)
- [train_model_v2.py](train_model_v2.py)
- [alerts_service.py](alerts_service.py)

## Maintenance Guidance

- Treat these as operator or analyst tools, not runtime primitives, unless they are explicitly wired into the runtime.
- If an ops script becomes operationally critical, consider promoting it into a structured engine job and documenting that move.
