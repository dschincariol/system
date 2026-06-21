#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SCRIPT="${REPO_ROOT}/ops/server/zfs_tuning.sh"

bash -n "$SCRIPT"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

fake_bin="${tmp_dir}/bin"
fake_log="${tmp_dir}/commands.log"
mkdir -p "$fake_bin"
: > "$fake_log"

cat > "${fake_bin}/zpool" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf 'zpool' >> "${FAKE_ZFS_LOG:?}"
printf ' %q' "$@" >> "$FAKE_ZFS_LOG"
printf '\n' >> "$FAKE_ZFS_LOG"

if [ "${1:-}" = "get" ]; then
  if [[ " $* " == *" -o value "* ]]; then
    printf '%s\n' "${FAKE_ZPOOL_AUTOTRIM:-off}"
  else
    printf 'zpool\tautotrim\t%s\tlocal\n' "${FAKE_ZPOOL_AUTOTRIM:-off}"
  fi
  exit 0
fi

if [ "${1:-}" = "set" ]; then
  exit 0
fi

exit 1
SH
chmod +x "${fake_bin}/zpool"

cat > "${fake_bin}/zfs" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf 'zfs' >> "${FAKE_ZFS_LOG:?}"
printf ' %q' "$@" >> "$FAKE_ZFS_LOG"
printf '\n' >> "$FAKE_ZFS_LOG"

dataset_exists() {
  case "$1" in
    zpool|zpool/data|zpool/docker|zpool/docker/timescaledb-pgdata) return 0 ;;
    *) return 1 ;;
  esac
}

target_value() {
  local dataset="$1" prop="$2"
  case "$prop" in
    atime) printf 'off\n' ;;
    compression)
      if [ "${FAKE_ZFS_BAD_CHILD_COMPRESSION:-0}" = "1" ] && [ "$dataset" = "zpool/docker" ]; then
        printf 'zstd\n'
      else
        printf 'lz4\n'
      fi
      ;;
    recordsize)
      [ "$dataset" = "zpool/docker/timescaledb-pgdata" ] && printf '16K\n' || printf '128K\n'
      ;;
    logbias)
      [ "$dataset" = "zpool/docker/timescaledb-pgdata" ] && printf 'throughput\n' || printf 'latency\n'
      ;;
    primarycache)
      [ "$dataset" = "zpool/docker/timescaledb-pgdata" ] && printf 'metadata\n' || printf 'all\n'
      ;;
    *) printf -- '-\n' ;;
  esac
}

old_value() {
  local dataset="$1" prop="$2"
  case "$prop" in
    atime) printf 'on\n' ;;
    compression)
      [ "$dataset" = "zpool/data" ] && printf 'gzip-4\n' || printf 'zstd\n'
      ;;
    recordsize) printf '128K\n' ;;
    logbias) printf 'latency\n' ;;
    primarycache) printf 'all\n' ;;
    *) printf -- '-\n' ;;
  esac
}

if [ "${1:-}" = "list" ]; then
  if [[ " $* " == *" -r "* ]]; then
    printf 'zpool\nzpool/data\nzpool/docker\nzpool/docker/timescaledb-pgdata\n'
    exit 0
  fi
  dataset="${@: -1}"
  if dataset_exists "$dataset"; then
    printf '%s\n' "$dataset"
    exit 0
  fi
  exit 1
fi

if [ "${1:-}" = "get" ]; then
  if [[ " $* " == *" -r "* ]]; then
    mode="${FAKE_ZFS_MODE:-old}"
    for dataset in zpool zpool/data zpool/docker zpool/docker/timescaledb-pgdata; do
      for prop in atime compression recordsize logbias primarycache; do
        if [ "$mode" = "target" ]; then
          value="$(target_value "$dataset" "$prop")"
        else
          value="$(old_value "$dataset" "$prop")"
        fi
        printf '%s\t%s\t%s\tlocal\n' "$dataset" "$prop" "$value"
      done
    done
    exit 0
  fi
  prop="${@: -2:1}"
  dataset="${@: -1}"
  if [ "${FAKE_ZFS_MODE:-old}" = "target" ]; then
    target_value "$dataset" "$prop"
  else
    old_value "$dataset" "$prop"
  fi
  exit 0
fi

if [ "${1:-}" = "set" ]; then
  exit 0
fi

exit 1
SH
chmod +x "${fake_bin}/zfs"

cat > "${fake_bin}/zdb" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf 'zdb' >> "${FAKE_ZFS_LOG:?}"
printf ' %q' "$@" >> "$FAKE_ZFS_LOG"
printf '\n' >> "$FAKE_ZFS_LOG"
cat <<EOF
MOS Configuration:
    vdev_tree:
        type: 'root'
        children[0]:
            type: 'disk'
            ashift: ${FAKE_ZFS_ASHIFT:-12}
EOF
SH
chmod +x "${fake_bin}/zdb"

run_env=(
  "PATH=${fake_bin}:$PATH"
  "FAKE_ZFS_LOG=${fake_log}"
  "TRADING_ZFS_CAPTURE_DIR=${tmp_dir}/captures"
)

apply_output="$(
  env "${run_env[@]}" bash "$SCRIPT" apply
)"

grep -Fq "setting zpool autotrim: off -> on" <<<"$apply_output"
grep -Fq "setting zpool/docker/timescaledb-pgdata primarycache: all -> metadata" <<<"$apply_output"
grep -Fq "zpool set autotrim=on zpool" "$fake_log"
grep -Fq "zfs set atime=off zpool" "$fake_log"
grep -Fxq "zfs set compression=lz4 zpool" "$fake_log"
grep -Fxq "zfs set compression=lz4 zpool/data" "$fake_log"
grep -Fxq "zfs set compression=lz4 zpool/docker" "$fake_log"
grep -Fq "zfs set recordsize=16K zpool/docker/timescaledb-pgdata" "$fake_log"
grep -Fq "zfs set logbias=throughput zpool/docker/timescaledb-pgdata" "$fake_log"
grep -Fq "zfs set compression=lz4 zpool/docker/timescaledb-pgdata" "$fake_log"
grep -Fq "zfs set primarycache=metadata zpool/docker/timescaledb-pgdata" "$fake_log"

verify_output="$(
  env "${run_env[@]}" FAKE_ZFS_MODE=target FAKE_ZPOOL_AUTOTRIM=on bash "$SCRIPT" verify
)"
grep -Fq "actual zpool on-disk ashift=12" <<<"$verify_output"
grep -Fq "verified compression policy on all existing zpool datasets" <<<"$verify_output"
grep -Fq "ZFS tuning verified" <<<"$verify_output"

set +e
compression_output="$(
  env "${run_env[@]}" FAKE_ZFS_MODE=target FAKE_ZPOOL_AUTOTRIM=on FAKE_ZFS_BAD_CHILD_COMPRESSION=1 bash "$SCRIPT" verify 2>&1
)"
compression_rc=$?
set -e

[ "$compression_rc" -ne 0 ] || {
  echo "child dataset compression mismatch verification unexpectedly passed" >&2
  exit 1
}
grep -Fq "zpool/docker compression=zstd, expected lz4" <<<"$compression_output"

set +e
ashift_output="$(
  env "${run_env[@]}" FAKE_ZFS_MODE=target FAKE_ZPOOL_AUTOTRIM=on FAKE_ZFS_ASHIFT=9 bash "$SCRIPT" verify 2>&1
)"
ashift_rc=$?
set -e

[ "$ashift_rc" -ne 0 ] || {
  echo "ashift mismatch verification unexpectedly passed" >&2
  exit 1
}
grep -Fq "ashift is immutable for existing vdevs" <<<"$ashift_output"
grep -Fq "will not destroy or recreate zpool" <<<"$ashift_output"

echo "[test_zfs_tuning] ok"
