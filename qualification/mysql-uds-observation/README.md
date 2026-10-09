# Oracle MySQL 8.4.11 UDS qualification

This required repository gate qualifies the read-only observer against one exact installed
Oracle MySQL Community Server 8.4.11. It never starts or uses the system MySQL
service.

## Prerequisites

- Ubuntu/Linux x86-64 with the `mysql` service stopped.
- Oracle APT package
  `mysql-community-server-core=8.4.11-1ubuntu24.04`, installed from
  `repo.mysql.com` and the `mysql-8.4-lts` component.
- `/usr/bin/aa-exec`, `dpkg-query`, `dpkg`, `apt-cache`, `ldd`, and the pinned
  Rust 1.98.1 toolchain.
- The repository-pinned `cargo-nextest`, installed with
  `scripts/install_nextest.sh`.
- No reusable MySQL credential. The fixture creates deterministic, ephemeral
  local setup, observer, denied-permission, and recovery accounts for that
  isolated run.

The Rust live test owns the exact package identity, executable paths, timeouts,
and repository-local qualification paths.

## Run

```bash
cargo fmt --all -- --check
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --workspace --profile ci
CARGO_BUILD_JOBS=1 cargo test --locked --offline --doc --workspace
CARGO_BUILD_JOBS=1 cargo clippy --locked --offline --workspace --all-targets -- -D warnings
```

The workspace test runs both required live gates. To rerun them individually:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-adapter --test live_mysql_8_4_11 \
  -E 'test(=qualify_oracle_mysql_8_4_11)'
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-service --test live_mysql_8_4_11 \
  -E 'test(=one_fresh_owned_instance_lifecycle)'
```

The repository CI performs the same run for every pull request and every push
to `main` on a pinned Ubuntu 24.04 runner after installing the exact Oracle
package.

Run the standard commands sequentially. The nextest workspace run performs
both live Oracle gates before doctests and warnings-denied Clippy.

## Fixture lifecycle

The gate verifies package ownership, exact installed/candidate version, dpkg
integrity, APT source stanza, runtime libraries, and native product fields.
Before creating fixture state it rejects an active
`mysql.service` or any foreign `mysqld` process. It pins `/usr/bin/aa-exec`,
verifies its package integrity, and confirms through
`/proc/<pid>/exe` that the launched child became the verified
`/usr/sbin/mysqld`. It creates owner-only (`0700`) project-local work, runtime,
and data directories.

The child uses a private UDS, `skip_networking=ON`, and a one-member Group
Replication loopback transport. The gate initializes fresh data, provisions
ephemeral accounts, runs native and deterministic scenarios, sends TERM/KILL
only to the recorded child, reaps it, and removes the socket, data, runtime,
and work directories. Panic/drop cleanup is a final fail-closed safeguard.

## Qualification coverage

- **Native-required:** product/version, online observation, strict UDS,
  a public-observer closed/missing-UDS transport failure, authentication and
  permission distinctions, least privilege, empty/tagged/newline/multi-source
  `GTID_SUBSET` evidence with an exact scalar response of `1`, and numeric
  boundaries. Missing rows, SQL `NULL`, `0`, or any other scalar are ineligible.
- **Oracle-owned:** exact immutable commit, tag, URL, path, source digest,
  range, and derivation for absent, never-started, stopped, and recovering
  representations.
- **Synthetic:** malformed/null/empty/schema, duplicate/contradictory,
  coherence-change, and timing failures.

Expected failures are named prerequisite, package, provenance, initialization,
launch, account/state, origin, or cleanup errors. An unmet prerequisite is a
failed qualification, never a skipped pass.

## Compatibility policy

The claim covers only Oracle MySQL 8.4.11 and this installed package.
The qualified GTID maximum is `9223372036854775806`; the server rejected
`9223372036854775807` specifically with MySQL error `1772` and SQLSTATE
`HY000`; any other response requires specification review. Any package,
executable, patch, product, platform, query
shape, client graph, or fixture-source change requires a new runner result,
review, and explicit compatibility update.
