#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [ "${TRADING_ROTATE_TEST_IN_CONTAINER:-0}" != "1" ]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "[test_rotate_master_key_phases] docker not available; skipping"
    exit 0
  fi
  if ! docker info >/dev/null 2>&1; then
    echo "[test_rotate_master_key_phases] docker daemon not available; skipping"
    exit 0
  fi

  IMAGE="${TRADING_ROTATE_TEST_IMAGE:-debian:12}"
  docker run --rm \
    -e TRADING_ROTATE_TEST_IN_CONTAINER=1 \
    -v "${REPO_ROOT}:/workspace:ro" \
    -w /workspace \
    "${IMAGE}" \
    bash tests/ops/test_rotate_master_key_phases.sh
  exit $?
fi

ROTATE_SCRIPT="/workspace/ops/server/credstore/rotate_master_key.sh"
tmp_dir="$(mktemp -d)"
stub_dir="${tmp_dir}/bin"
mkdir -p "$stub_dir"

cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

cat > "${stub_dir}/openssl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [ "${1:-}" = "rand" ]; then
  printf 'new-key-%s\n' "${TRADING_ROTATE_TEST_CASE:-unknown}"
  exit 0
fi
echo "unexpected openssl invocation: $*" >&2
exit 97
EOF

cat > "${stub_dir}/systemd-creds" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
target="${@: -1}"
cat > "$target"
EOF

cat > "${stub_dir}/systemd-run" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
args="$*"
state_dir="${TRADING_ROTATE_TEST_STATE:?}"
events="${TRADING_ROTATE_TEST_EVENTS:?}"
credstore="${TRADING_CREDSTORE_DIR:?}"

if [[ "$args" == *"re_encrypt_data_sources"* ]]; then
  echo "phase_1" >> "$events"
  if [ "${TRADING_ROTATE_TEST_FAIL_PHASE:-}" = "phase_1" ]; then
    exit 41
  fi
  printf 'next\nnext\nnext\n' > "${state_dir}/rows.txt"
  echo '{"scanned": 3, "rotated": 3, "verified": 3}'
  exit 0
fi

if [[ "$args" == *"decrypt_key_name=\"master_key.next\""* ]]; then
  echo "phase_2" >> "$events"
  if [ "${TRADING_ROTATE_TEST_FAIL_PHASE:-}" = "phase_2" ]; then
    exit 42
  fi
  if grep -q '^old$' "${state_dir}/rows.txt"; then
    echo "rows still encrypted with old key" >&2
    exit 43
  fi
  echo '{"verified": 3}'
  exit 0
fi

if [[ "$args" == *"verify_data_sources_key"* ]]; then
  echo "phase_3_verify" >> "$events"
  if grep -q '^old-key$' "${credstore}/master_key.cred"; then
    echo "active master key was not swapped" >&2
    exit 44
  fi
  if grep -q '^old$' "${state_dir}/rows.txt"; then
    echo "rows still encrypted with old key" >&2
    exit 45
  fi
  echo '{"verified": 3}'
  exit 0
fi

echo "unexpected systemd-run invocation: $args" >&2
exit 98
EOF

cat > "${stub_dir}/systemctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "systemctl $*" >> "${TRADING_ROTATE_TEST_EVENTS:?}"
exit 0
EOF

chmod +x "${stub_dir}"/*

assert_file_contains() {
  local path="$1" expected="$2"
  if [ "$(cat "$path")" != "$expected" ]; then
    echo "${path}: expected ${expected}, got $(cat "$path")" >&2
    exit 1
  fi
}

assert_exit() {
  local got="$1" expected="$2" name="$3"
  if [ "$got" -ne "$expected" ]; then
    echo "${name}: expected exit ${expected}, got ${got}" >&2
    exit 1
  fi
}

run_rotation_case() {
  local name="$1" fail_phase="$2" expected_rc="$3" retention_hours="$4" expect_archive="$5"
  local case_dir="${tmp_dir}/${name}"
  mkdir -p "${case_dir}/credstore" "${case_dir}/state"
  printf 'old-key\n' > "${case_dir}/credstore/master_key.cred"
  printf 'old\nold\nold\n' > "${case_dir}/state/rows.txt"
  : > "${case_dir}/events.log"

  set +e
  PATH="${stub_dir}:$PATH" \
    TRADING_CREDSTORE_DIR="${case_dir}/credstore" \
    TRADING_APP_ROOT="/workspace" \
    TRADING_PYTHON_BIN="python" \
    TRADING_MASTER_KEY_SERVICES="trading-jobs.service trading-ingest.service" \
    TRADING_ROTATE_TEST_CASE="$name" \
    TRADING_ROTATE_TEST_STATE="${case_dir}/state" \
    TRADING_ROTATE_TEST_EVENTS="${case_dir}/events.log" \
    TRADING_ROTATE_TEST_FAIL_PHASE="$fail_phase" \
    TRADING_MASTER_KEY_ARCHIVE_RETENTION_HOURS="$retention_hours" \
    bash "$ROTATE_SCRIPT" > "${case_dir}/rotate.out" 2>&1
  rc=$?
  set -e

  assert_exit "$rc" "$expected_rc" "$name"

  if [ "$expected_rc" -ne 0 ]; then
    [ -f "${case_dir}/credstore/master_key.next.cred" ] || {
      echo "${name}: master_key.next.cred was not preserved" >&2
      exit 1
    }
    assert_file_contains "${case_dir}/credstore/master_key.cred" "old-key"
    if grep -q '^systemctl ' "${case_dir}/events.log"; then
      echo "${name}: services restarted after pre-swap failure" >&2
      exit 1
    fi
    return
  fi

  [ ! -e "${case_dir}/credstore/master_key.next.cred" ] || {
    echo "${name}: master_key.next.cred still exists after successful swap" >&2
    exit 1
  }
  assert_file_contains "${case_dir}/credstore/master_key.cred" "new-key-${name}"
  if grep -q '^old$' "${case_dir}/state/rows.txt"; then
    echo "${name}: synthetic rows did not rotate to the next key" >&2
    exit 1
  fi

  archive_file=""
  if [ -d "${case_dir}/credstore/keys/archive" ]; then
    archive_file="$(find "${case_dir}/credstore/keys/archive" -type f -name 'master_key.*.cred' | head -n 1)"
  fi
  if [ "$expect_archive" -eq 1 ]; then
    [ -n "$archive_file" ] || {
      echo "${name}: old master key was not archived" >&2
      exit 1
    }
    assert_file_contains "$archive_file" "old-key"
    [ "$(stat -c %a "$archive_file")" = "400" ] || {
      echo "${name}: archive mode is not 0400" >&2
      exit 1
    }
  elif [ -n "$archive_file" ]; then
    echo "${name}: old master key was archived despite zero retention" >&2
    exit 1
  fi
  grep -q '^phase_1$' "${case_dir}/events.log"
  grep -q '^phase_2$' "${case_dir}/events.log"
  grep -q '^phase_3_verify$' "${case_dir}/events.log"
  grep -q '^systemctl restart trading-jobs.service$' "${case_dir}/events.log"
  grep -q '^systemctl restart trading-ingest.service$' "${case_dir}/events.log"
}

run_rotation_case "phase1_fail" "phase_1" 1 72 0
run_rotation_case "phase2_fail" "phase_2" 2 72 0
run_rotation_case "success_archive" "" 0 72 1
run_rotation_case "success_purge" "" 0 0 0

echo "[test_rotate_master_key_phases] ok"
