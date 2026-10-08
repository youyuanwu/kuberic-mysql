#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
QUAL="$ROOT/qualification/mysql-uds-observation"
RECEIPTS="$QUAL/.receipts"
MANIFEST="$QUAL/.manifest.live.toml"
FEATURE_GRAPH="$RECEIPTS/feature-graph.txt"
LOCK="$ROOT/Cargo.lock"

lock_digest() {
  sha256sum "$LOCK" | awk '{print toupper($1)}'
}

source_files() {
  {
    git -C "$ROOT" ls-files
    find "$ROOT/crates/kuberic-mysql-adapter/tests/live_support" -type f 2>/dev/null
    test ! -f "$ROOT/crates/kuberic-mysql-adapter/tests/live_mysql_8_4_11.rs" ||
      printf '%s\n' "$ROOT/crates/kuberic-mysql-adapter/tests/live_mysql_8_4_11.rs"
    printf '%s\n' "$QUAL/manifest.example.toml" "$QUAL/run-qualification.sh"
  } |
    sed "s#^$ROOT/##" |
    grep -Ev '^(\.paw/|target/|qualification/mysql-uds-observation/(oracle-mysql-8\.4\.11\.toml|local-run/|\.receipts/|\.manifest\.live\.toml))' |
    sort -u
}

source_digest() {
  (
    cd "$ROOT"
    while IFS= read -r path; do
      test -f "$path" && { printf '%s\0' "$path"; sha256sum "$path"; }
    done < <(source_files)
  ) | sha256sum | awk '{print toupper($1)}'
}

case "${1:-run}" in
  --lock-digest) lock_digest; exit 0 ;;
  --source-digest) source_digest; exit 0 ;;
  run) ;;
  *) echo "usage: $0 [run|--lock-digest|--source-digest]" >&2; exit 2 ;;
esac

rm -rf "$RECEIPTS"
mkdir -m 700 "$RECEIPTS"
trap 'rm -rf "$RECEIPTS" "$MANIFEST"' EXIT

LOCK_DIGEST="$(lock_digest)"
SOURCE_DIGEST="$(source_digest)"

receipt() {
  local id="$1" command="$2" output_digest="${3:-}"
  cat >"$RECEIPTS/$id.toml" <<EOF
schema = "kuberic.mysql.validation-receipt/v1"
command_id = "$id"
command = "$command"
success = true
lock_sha256 = "$LOCK_DIGEST"
source_sha256 = "$SOURCE_DIGEST"
output_sha256 = "$output_digest"
EOF
  chmod 600 "$RECEIPTS/$id.toml"
}

cd "$ROOT"
cargo fmt --all -- --check
receipt fmt 'cargo fmt --all -- --check'

CARGO_BUILD_JOBS=1 cargo test --locked --offline --workspace --all-features -- --test-threads=1
receipt test 'CARGO_BUILD_JOBS=1 cargo test --locked --offline --workspace --all-features -- --test-threads=1'

CARGO_BUILD_JOBS=1 cargo clippy --locked --offline --workspace --all-targets --all-features -- -D warnings
receipt clippy 'CARGO_BUILD_JOBS=1 cargo clippy --locked --offline --workspace --all-targets --all-features -- -D warnings'

CARGO_BUILD_JOBS=1 cargo tree --locked --offline -e features -p kuberic-mysql-adapter >"$FEATURE_GRAPH"
receipt feature-graph 'CARGO_BUILD_JOBS=1 cargo tree --locked --offline -e features -p kuberic-mysql-adapter' "$(sha256sum "$FEATURE_GRAPH" | awk '{print toupper($1)}')"

cat >"$MANIFEST" <<EOF
mysqld_path = "/usr/sbin/mysqld"
apparmor_exec_path = "/usr/bin/aa-exec"
package_name = "mysql-community-server-core"
package_version = "8.4.11-1ubuntu24.04"
apt_repository_host = "repo.mysql.com"
apt_repository_component = "mysql-8.4-lts"
expected_mysqld_sha256 = "5319F31BABC80A5B438C4C4E87292539818A3F0D737CFF6733FF8AAE7D801058"
qualification_root = "$QUAL/local-run"
output_record_path = "$QUAL/oracle-mysql-8.4.11.toml"
validation_receipt_dir = "$RECEIPTS"
feature_graph_path = "$FEATURE_GRAPH"
startup_timeout_ms = 30000
observation_timeout_ms = 5000
cleanup_timeout_ms = 10000
EOF
chmod 600 "$MANIFEST"

KUBERIC_MYSQL_8_4_11_MANIFEST="$MANIFEST" CARGO_BUILD_JOBS=1 \
  cargo test --locked --offline -p kuberic-mysql-adapter \
  --features live-mysql-8-4-11 --test live_mysql_8_4_11 \
  qualify_oracle_mysql_8_4_11 -- --ignored --exact --test-threads=1
