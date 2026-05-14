# Ops Scripts

The `ops/` directory contains operational utilities and offline analytics helpers.

These scripts are generally not part of the hot runtime path, but they are important for:

- offline checks and analytics
- backfills and calibration
- alerting
- maintenance and reporting

## Examples

- [check_events.py](c:\Users\dschi\Documents\GitHub\Trading-System-\ops\check_events.py)
- [check_labels.py](c:\Users\dschi\Documents\GitHub\Trading-System-\ops\check_labels.py)
- [check_predictions.py](c:\Users\dschi\Documents\GitHub\Trading-System-\ops\check_predictions.py)
- [compute_drift.py](c:\Users\dschi\Documents\GitHub\Trading-System-\ops\compute_drift.py)
- [train_model_v2.py](c:\Users\dschi\Documents\GitHub\Trading-System-\ops\train_model_v2.py)
- [alerts_service.py](c:\Users\dschi\Documents\GitHub\Trading-System-\ops\alerts_service.py)

## Maintenance Guidance

- Treat these as operator or analyst tools, not runtime primitives, unless they are explicitly wired into the runtime.
- If an ops script becomes operationally critical, consider promoting it into a structured engine job and documenting that move.
