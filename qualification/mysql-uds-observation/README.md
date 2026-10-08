# Oracle MySQL 8.4.11 UDS qualification

This opt-in gate qualifies the read-only observer against one exact installed
Oracle MySQL Community Server 8.4.11. It never starts or uses the system MySQL
service.

## Prerequisites

- Ubuntu/Linux x86-64 with the `mysql` service stopped.
- Oracle APT package
  `mysql-community-server-core=8.4.11-1ubuntu24.04`, installed from
  `repo.mysql.com` and the `mysql-8.4-lts` component.
- `/usr/sbin/mysqld` with SHA-256
  `5319F31BABC80A5B438C4C4E87292539818A3F0D737CFF6733FF8AAE7D801058`.
- `/usr/bin/aa-exec`, `dpkg-query`, `dpkg`, `apt-cache`, `ldd`, `sha256sum`,
  and the pinned Rust 1.98.1 toolchain.
- No reusable MySQL credential. The fixture creates deterministic, ephemeral
  local setup, observer, denied-permission, and recovery accounts for that
  isolated run.

The documented manifest fields are in
[`manifest.example.toml`](manifest.example.toml). Paths must be absolute;
qualification, receipt, feature-graph, and output paths must remain beneath
this repository's `qualification/` tree.

## Run

```bash
./qualification/mysql-uds-observation/run-qualification.sh
```

The runner sequentially performs locked, offline, low-concurrency formatting,
workspace tests, warnings-denied Clippy, and the effective Cargo feature-tree
capture. Each successful command produces a temporary receipt containing the
exact command, success state, current `Cargo.lock` digest, deterministic source
digest, and feature-graph digest where applicable. The live gate rejects
missing, failed, changed, or stale receipts.

## Fixture lifecycle

The gate verifies package ownership, exact installed/candidate version, dpkg
integrity, APT source stanza, executable digest, runtime libraries, and native
product fields. Before creating fixture state it rejects an active
`mysql.service` or any foreign `mysqld` process. It pins `/usr/bin/aa-exec`,
verifies its package integrity and digest, and confirms through
`/proc/<pid>/exe` that the launched child became the verified
`/usr/sbin/mysqld`. It creates owner-only (`0700`) project-local work, runtime,
and data directories.

The child uses a private UDS, `skip_networking=ON`, and a one-member Group
Replication loopback transport. The gate initializes fresh data, provisions
ephemeral accounts, runs native and deterministic scenarios, sends TERM/KILL
only to the recorded child, reaps it, and removes the socket, data, runtime,
temporary manifest, and receipts. Panic/drop cleanup is a final fail-closed
safeguard.

## Evidence and record eligibility

The canonical secret-free record is
[`oracle-mysql-8.4.11.toml`](oracle-mysql-8.4.11.toml). It is written only after
all native-required, pinned Oracle-owned, and synthetic scenarios pass and
cleanup is proven.

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

Expected failures are named prerequisite, package, digest, provenance,
initialization, launch, account/state, origin, cleanup, or output-gating
errors. An unmet prerequisite is a failed qualification, never a skipped pass.

## Compatibility policy

The claim covers only Oracle MySQL 8.4.11 and this installed package/digest.
The qualified GTID maximum is `9223372036854775806`; the server rejected
`9223372036854775807` specifically with MySQL error `1772` and SQLSTATE
`HY000`; any other response requires specification review. Any package,
executable, patch, product, platform, query
shape, client graph, or fixture-source change requires a new runner result,
canonical record, review, and explicit compatibility update.
