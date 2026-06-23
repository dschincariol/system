#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SCRIPT="${REPO_ROOT}/ops/server/provision_storage_pools.sh"

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
printf 'zpool' >> "${FAKE_STORAGE_LOG:?}"
printf ' %q' "$@" >> "$FAKE_STORAGE_LOG"
printf '\n' >> "$FAKE_STORAGE_LOG"

pool_exists() {
  case "${FAKE_STORAGE_MODE:-apply}:${1:-}" in
    apply:zpool|verify:zpool|verify:dbpool|verify:auxpool) return 0 ;;
    *) return 1 ;;
  esac
}

if [ "${1:-}" = "list" ]; then
  if [[ " $* " == *" -o health "* ]]; then
    pool="${@: -1}"
    pool_exists "$pool" || exit 1
    printf 'ONLINE\n'
    exit 0
  fi
  if [[ " $* " == *" -o name "* ]]; then
    pool="${@: -1}"
    pool_exists "$pool" || exit 1
    printf '%s\n' "$pool"
    exit 0
  fi
  printf 'NAME SIZE ALLOC FREE\n'
  exit 0
fi

if [ "${1:-}" = "get" ]; then
  if [[ " $* " == *" -o value "* ]]; then
    pool="${@: -1}"
    prop="${@: -2:1}"
    [ "$prop" = "autotrim" ] || exit 1
    case "${FAKE_STORAGE_MODE:-apply}:$pool" in
      apply:zpool) printf 'off\n' ;;
      *) printf 'on\n' ;;
    esac
    exit 0
  fi
  printf 'zpool\tautotrim\ton\tlocal\n'
  printf 'dbpool\tautotrim\ton\tlocal\n'
  printf 'auxpool\tautotrim\ton\tlocal\n'
  exit 0
fi

if [ "${1:-}" = "set" ] || [ "${1:-}" = "create" ] || [ "${1:-}" = "status" ]; then
  if [ "${1:-}" = "status" ]; then
    case "${@: -1}" in
      dbpool) printf '  /dev/nvme2n1 ONLINE\n' ;;
      zpool) printf '  /dev/nvme1n1p4 ONLINE\n' ;;
      auxpool) printf '  /dev/nvme0n1 ONLINE\n' ;;
      *) printf '  /dev/nvme2n1 ONLINE\n  /dev/nvme1n1p4 ONLINE\n  /dev/nvme0n1 ONLINE\n' ;;
    esac
  fi
  exit 0
fi

exit 1
SH
chmod +x "${fake_bin}/zpool"

cat > "${fake_bin}/zfs" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf 'zfs' >> "${FAKE_STORAGE_LOG:?}"
printf ' %q' "$@" >> "$FAKE_STORAGE_LOG"
printf '\n' >> "$FAKE_STORAGE_LOG"

exists_verify() {
  case "$1" in
    zpool|zpool/trading-backups|dbpool|dbpool/data|dbpool/trading/timescaledb/data|auxpool|auxpool/trading/redis|auxpool/trading/minio|auxpool/trading/runtime/data|auxpool/trading/runtime/logs|auxpool/trading/runtime/artifact_mirror|auxpool/trading/runtime/training_datasets|auxpool/trading/offline/data|auxpool/trading/offline/artifact_mirror|auxpool/trading/offline/training_datasets) return 0 ;;
    *) return 1 ;;
  esac
}

exists_apply() {
  case "$1" in
    zpool|zpool/trading-backups) return 0 ;;
    *) return 1 ;;
  esac
}

prop_value() {
  local dataset="$1" prop="$2"
  case "$dataset:$prop" in
    zpool:atime) [ "${FAKE_STORAGE_MODE:-apply}" = "apply" ] && printf 'on\n' || printf 'off\n' ;;
    zpool/trading-backups:compression) printf 'zstd\n' ;;
    dbpool:atime|auxpool:atime|dbpool/trading/timescaledb/data:atime) printf 'off\n' ;;
    dbpool:compression|auxpool:compression|dbpool/trading/timescaledb/data:compression) printf 'lz4\n' ;;
    dbpool/trading/timescaledb/data:recordsize) printf '16K\n' ;;
    dbpool/trading/timescaledb/data:logbias) printf 'throughput\n' ;;
    dbpool/trading/timescaledb/data:primarycache) printf 'metadata\n' ;;
    *) printf -- '-\n' ;;
  esac
}

if [ "${1:-}" = "list" ]; then
  dataset="${@: -1}"
  if [ "${FAKE_STORAGE_MODE:-apply}" = "verify" ]; then
    exists_verify "$dataset" || exit 1
  else
    exists_apply "$dataset" || exit 1
  fi
  printf '%s\n' "$dataset"
  exit 0
fi

if [ "${1:-}" = "get" ]; then
  if [[ " $* " == *" -r "* ]]; then
    printf 'zpool\tatime\toff\tlocal\n'
    exit 0
  fi
  prop="${@: -2:1}"
  dataset="${@: -1}"
  prop_value "$dataset" "$prop"
  exit 0
fi

if [ "${1:-}" = "set" ] || [ "${1:-}" = "create" ]; then
  exit 0
fi

exit 1
SH
chmod +x "${fake_bin}/zfs"

cat > "${fake_bin}/zdb" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf 'zdb' >> "${FAKE_STORAGE_LOG:?}"
printf ' %q' "$@" >> "$FAKE_STORAGE_LOG"
printf '\n' >> "$FAKE_STORAGE_LOG"
cat <<EOF
MOS Configuration:
    vdev_tree:
        ashift: 12
EOF
SH
chmod +x "${fake_bin}/zdb"

cat > "${fake_bin}/lsblk" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ " $* " == *" -no PKNAME "* ]]; then
  case "${@: -1}" in
    /dev/nvme1n1p4) printf 'nvme1n1\n' ;;
    *) ;;
  esac
  exit 0
fi
exit 0
SH
chmod +x "${fake_bin}/lsblk"

cat > "${fake_bin}/readlink" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [ "${1:-}" = "-f" ]; then
  printf '%s\n' "${2:-}"
  exit 0
fi
exit 1
SH
chmod +x "${fake_bin}/readlink"

run_env=(
  "PATH=${fake_bin}:$PATH"
  "FAKE_STORAGE_LOG=${fake_log}"
  "TRADING_STORAGE_CAPTURE_DIR=${tmp_dir}/captures"
)

apply_output="$(
  env "${run_env[@]}" bash "$SCRIPT" apply --dry-run
)"

grep -Fq "dry-run: wipefs --all /dev/disk/by-id/nvme-Samsung_SSD_990_EVO_Plus_4TB_S7U8NU0YA01981P" <<<"$apply_output"
grep -Fq "dry-run: zpool create -f -o ashift=12 -o autotrim=on -O atime=off -O compression=lz4 -O mountpoint=/dbpool dbpool /dev/disk/by-id/nvme-Samsung_SSD_990_EVO_Plus_4TB_S7U8NU0YA01981P" <<<"$apply_output"
grep -Fq "dry-run: zfs create -p -o recordsize=16K -o logbias=throughput -o compression=lz4 -o atime=off -o primarycache=metadata dbpool/trading/timescaledb/data" <<<"$apply_output"
# Nested datasets MUST be created with -p so missing intermediate datasets
# (dbpool/trading, auxpool/trading/runtime, ...) are auto-created; without -p a
# real `zfs create` fails with "parent does not exist".
grep -Fq "dry-run: zfs create -p dbpool/data" <<<"$apply_output"
grep -Fq "dry-run: zfs create -p auxpool/trading/redis" <<<"$apply_output"
grep -Fq "dry-run: zfs create -p auxpool/trading/runtime/data" <<<"$apply_output"
grep -Fq "dry-run: zfs create -p auxpool/trading/offline/training_datasets" <<<"$apply_output"
grep -Fq "RECLAIM_ROLE=zfs-pool" <<<"$apply_output"
grep -Fq "dry-run: zpool create -f -o ashift=12 -o autotrim=on -O atime=off -O compression=lz4 -O mountpoint=/auxpool auxpool /dev/disk/by-id/nvme-KINGSTON_OM8TAP42048K1-A00_50026B73842ACAC7" <<<"$apply_output"
if grep -Fq "zfs set compression=lz4 zpool/trading-backups" "$fake_log"; then
  echo "provisioner attempted to downgrade backup compression" >&2
  exit 1
fi

verify_output="$(
  env "${run_env[@]}" FAKE_STORAGE_MODE=verify bash "$SCRIPT" verify
)"
grep -Fq "actual dbpool on-disk ashift=12" <<<"$verify_output"
grep -Fq "verified dbpool and zpool use separate physical devices" <<<"$verify_output"
grep -Fq "storage pools verified" <<<"$verify_output"

echo "[test_provision_storage_pools] ok"
