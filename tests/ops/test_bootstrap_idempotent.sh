#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if ! command -v docker >/dev/null 2>&1; then
  echo "[test_bootstrap_idempotent] docker not available; skipping"
  exit 0
fi

if ! docker info >/dev/null 2>&1; then
  echo "[test_bootstrap_idempotent] docker daemon not available; skipping"
  exit 0
fi

IMAGE="${TRADING_BOOTSTRAP_TEST_IMAGE:-debian:12}"

docker run --rm \
  -e TRADING_INSTALL_PYTHON_REQUIREMENTS=0 \
  -e TRADING_ENABLE_UFW=0 \
  -e TS_SECRET_MASTER_KEY=test-master-key \
  -e TS_SECRET_PG_PASSWORD_APP=test-app-password \
  -e TS_SECRET_PG_PASSWORD_INGEST=test-ingest-password \
  -e TS_SECRET_PG_PASSWORD_READER=test-reader-password \
  -v "${REPO_ROOT}:/workspace:ro" \
  -w /workspace \
  "${IMAGE}" \
  bash -lc '
    set -euo pipefail
    bash ops/server/bootstrap.sh
    sha256sum /etc/postgresql/16/main/conf.d/trading.conf > /tmp/trading-postgres-conf.sha256
    bash ops/server/bootstrap.sh | tee /tmp/bootstrap-second.log
    sha256sum -c /tmp/trading-postgres-conf.sha256
    if grep -q "installing packages:" /tmp/bootstrap-second.log; then
      echo "second bootstrap run installed packages" >&2
      exit 1
    fi
    bash ops/server/verify.sh
  '
