# kuberic-mysql

`kuberic-mysql` contains a deterministic Rust safety core, a read-only native
observer, bounded lifecycle ownership for fresh Oracle MySQL servers, and the
MySQL-owned Stage 2 integration foundation. The delivered native target is
Oracle MySQL Community Server 8.4.11 on Linux x86-64. Administration and
observation use private Unix-domain sockets; Group Replication and the bounded
application gateway use distinct loopback-only endpoints.

The [high-level design](docs/design.md) defines the intended authority,
identity, progress, lifecycle, fencing, recovery, security, testing, and staged
delivery contracts. Kuberic remains the durable lifecycle and client-access
authority; MySQL retains native Group Replication membership, transport, and
GTID history.

Lifecycle work follows the
[restart-stateless metadata proposal](docs/stateless-metadata.md): the MySQL
adapter and lifecycle component persist no private recovery metadata. Durable
orchestration evidence belongs to Kuberic's generic controller and agent
facilities, while native database facts remain in the MySQL data directory.

The delivered component shape is one MySQL-owned member-local lifecycle engine
per replica plus a narrow Kuberic-neutral adapter seam. The fixed three-process
topology manager and the new three-engine handoff fixture remain qualification
infrastructure, not production topology owners. A future Kuberic adapter must
translate stabilized public values and ordering into this seam; the repository
does not depend on `kuberic-runtime`, verify future signed build/peer
authorization, or attach private managed lifecycle capabilities.

## Status

Stage 1 is delivered in the publish-disabled `kuberic-mysql` crate's `core`
module. It provides:

- validated, typed identities and exact evidence/work bindings;
- normalized MySQL GTID sets with equal, proper-subset, proper-superset, and
  incomparable relations;
- exact typed Group Replication views;
- coherent, provenance-retaining observation outcomes; and
- a minimal authority session that rejects stale completion and keeps client
  access closed.

The `kuberic_mysql::adapter` module adds one deadline-bounded, read-only
observation attempt over a validated UDS. It verifies the exact Oracle 8.4.11
product, collects opening native state, executed GTIDs, and closing native
state on one authenticated session, and returns a typed core outcome plus a
secret-free diagnostic. Transport, authentication, permission, absence,
malformed, unsupported, stale, and incoherent results remain
machine-distinguishable.

The `kuberic_mysql::service` module owns either one fresh local process
generation or one fixed topology of exactly three such generations. The
topology manager initializes all three, starts and enrolls one designated
bootstrap member, accepts one fresh one-member view, then starts and joins the
remaining members sequentially. Each join requires an exact fresh view and a
structured GTID boundary captured from the accepted predecessor. All ordinary
client access remains closed. Qualified Oracle XCom view IDs are parsed as
`<fixed u64>:<monotonic u32>` and a join receives credit only for the same
fixed part and exactly the predecessor monotonic value plus one. Skipped,
regressed, malformed, overflowed, or changed-fixed-part views fail closed.
The manager owns one monotonic clock and one absolute attempt deadline; callers
do not supply operation ticks or per-call control/observation deadlines.
The service keeps persistent MySQL data separate
from disposable runtime files, retains and attests exact children in memory,
and contains all owned members on failure or stop. It writes no application
metadata store and has no restart, reattachment, or survivor-adoption path;
loss of in-memory ownership or attempt context requires fixture reset.
Cancelling bootstrap cannot detach a query future: bootstrap-off and proof are
attempted after every enable attempt, while cancellation synchronously contains
the exact owned topology before returning control.

The same module now exposes `MysqlReplicator`, a one-member lifecycle engine
that owns one `MysqlInstanceManager` and enforces open, newer epoch, role,
configuration, access, and terminal ordering. Same-role transitions still
require an epoch barrier. Native bootstrap, join, coherent view readback, and
primary transfer receive credit only from exact request-bound member-effect
receipts. Build and removal entry points validate current primary authority,
target identity, operation identity, and exact retries, then return a typed
unsupported outcome; generic peer build/removal execution is not delivered.
Successful close and abort retain their exact terminal outcome for idempotent
retry. Changed retries conflict, unrelated post-terminal work rejects, and an
unsettled abort remains terminal without claiming complete containment. Abort
also contains an owned member that was prepared but not opened.

Qualified scalar progress is now a production boundary. Empty structured GTID
history maps to `0`; otherwise only one untagged configured group source with
one contiguous interval beginning at `1` maps to its exact signed tail.
Complete `GtidSet` evidence remains authoritative. Foreign, tagged, gapped, or
multi-source history is typed non-projectable, and catch-up capability remains
an independent unsupported/unproved result when retained donor/binlog evidence
is unavailable.

The application endpoint is a generation-fenced plaintext loopback MySQL
Classic Protocol gateway backed by the exact private UDS. It admits one
dedicated pre-warmed `caching_sha2_password` application account, filters the
negotiated capabilities, and supports bounded query, ping, and quit exchanges.
Both read and write permission are required. Revocation atomically closes
admission and dispatch, terminates registered native backend sessions, and
completes only after proxy counts are zero and native absence is proved.
Published listeners and sessions also observe shared attempt cancellation and
the absolute work cutoff, sealing without a separate caller revoke. Every
initiated backend is accounted; a connection that never yields an exact native
ID makes containment unproved. Snapshots and receipts retain bounded
secret-free outcome summaries. Text rows accept SQL `NULL` (`0xfb`) while an
initial `0xfb` response remains rejected as `LOCAL INFILE`.
TLS, cold/full authentication, arbitrary accounts or plugins, prepared
statements, local-file transfer, compression, multi-statements, multi-results,
pooling, and general MySQL protocol support are not claimed.

A repository-owned live fixture composes three independent member-local
engines for fresh formation and one controlled handoff. The handoff freezes the
source's structured/scalar boundary only after a normally completed source
closure and a fresh post-revocation native observation. The source engine
issues a stable boundary identity binding the revoked generation, closure
receipt, transfer, newer epoch/configuration, and source terminal receipt. The
fixture proves fresh exact three-member history after schema DDL, transfers
primary, closes and reaps the exact source process, accepts
the exact two-survivor successor view and frozen history on both survivors, and
submits one combined proof before a separate target access generation can be
published. Publication performs a fresh target native view/history read, so
intervening peer loss or view change blocks serving. This is controlled
host-local qualification, not automated
failover.

A test-only native feasibility gate pins and reads back all nine effective
values on every member: `gtid_mode = ON`,
`enforce_gtid_consistency = ON`,
`group_replication_gtid_assignment_block_size = 1`,
`group_replication_view_change_uuid = AUTOMATIC`, global and workload-session
`group_replication_consistency = AFTER`,
`innodb_flush_log_at_trx_commit = 1`, `sync_binlog = 1`,
`binlog_expire_logs_seconds = 2592000`, and
`group_replication_member_expel_timeout = 5`. It qualifies one untagged
group-source, contiguous-from-1 GTID history across primary transfer, graceful
departure/rejoin, retained-data restart of every process, and binary-log
purge. Full GTID sets remain authoritative; purged sets and opaque log
inventory do not become public catch-up capability.

The gate proves a normally acknowledged singleton GTID on all members and
proves it survives abrupt primary SIGKILL, a coherent two-survivor view, and
restart of every participant. A deliberately blocked secondary applier exposed
the exact transaction GTID; dropping the client before response produced an
ambiguous client outcome whose GTID was then proved on all members. The gate
did not establish an authoritative current-source-`AFTER` wait surface, so
secondary loss during that wait remains unproved and no member fault is
injected. Account lock, password
replacement, and recovery-privilege removal do not terminate established
native participation; even after explicit Group Replication stop and password
replacement, the native `START GROUP_REPLICATION` command still completes.
Because the caught-up target does not prove donor authentication, predecessor
recovery-credential acceptance remains unproved. Native peer-session
revocation and serving cutover therefore remain ineligible.

The original five-argument `MysqlInstanceConfig::new(mysqld, launcher,
data_root, scratch_root, timeouts)` remains the single-instance constructor and
retains its prior profile. Fixed-topology callers must use the explicit
`MysqlInstanceConfig::new_topology_member(..., topology, member_index,
timeouts)` path; topology management rejects single-instance profiles before
creating any root. It also rejects duplicate or cross-member overlapping data,
scratch, socket, PID, configuration, log, and runtime paths before mutation.

The second and only other workspace package, `kuberic-mysql-tests`, owns the
integration contracts, protocol simulators, fixtures, shared test
infrastructure, and live Oracle qualification targets.

The design requires structured GTID and native-view evidence, exact
process/storage ownership, generation closure, source terminal evidence, and
fresh successor reconciliation before target write publication. Later
automated-failover stages additionally require independent old-primary
fencing. Native `PRIMARY` role or MySQL read-only variables alone do not grant
or fence writes.

Native `PRIMARY`, `read_only`, and `super_read_only` values are evidence only.
They never open read or write access and do not prove fencing. The delivered
Stage 2 foundation still excludes the Kuberic adapter, effective signed
build/peer admission, generic build/removal completion, automated failover,
Clone/reseed, replacement or destructive repair, restart continuation or
survivor adoption, TLS, cross-host operation, containers, Kubernetes, and
production availability, durability, performance, or security claims.

The live gate qualifies the exact installed Oracle MySQL 8.4.11 package.
Any other patch requires an explicit compatibility update.

See [the design's staged delivery and validation
plan](docs/design.md#staged-delivery) for the evidence required before any
future support claim.

## Prerequisites

Repository development and CI require Oracle MySQL Community Server 8.4.11 on
Linux x86-64. The supported package is
`mysql-community-server-core=8.4.11-1ubuntu24.04` from `repo.mysql.com`'s
`mysql-8.4-lts` component. `/usr/sbin/mysqld` and `/usr/bin/aa-exec` must be
installed, and the system `mysql.service` must remain stopped. Install the
repository-pinned, checksum-verified `cargo-nextest` binary with:

```bash
scripts/install_nextest.sh
```

## Validation

The standard workspace test starts isolated MySQL fixtures and runs the
observer, single-process lifecycle, and three-member bootstrap/join gates:

```bash
cargo fmt --all -- --check
CARGO_BUILD_JOBS=1 cargo check --locked --offline -p kuberic-mysql --lib
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --workspace --profile ci
CARGO_BUILD_JOBS=1 cargo test --locked --offline --doc --workspace
CARGO_BUILD_JOBS=1 cargo clippy --locked --offline --workspace --all-targets -- -D warnings
```

The bounded service's deterministic contracts can be run without launching
MySQL:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests \
  --test service_config_contract --test service_process_contract \
  --test service_topology_contract
```

The Stage 2 member-local lifecycle, gateway, and handoff contracts can be run
without launching MySQL:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests \
  --test core_progress_contract \
  --test service_member_transition_contract \
  --test service_proxy_contract \
  --test service_replicator_contract
```

The bounded real-server gateway compatibility gate can be rerun independently:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests --test service_proxy_live_mysql_8_4_11
```

The fresh formation and controlled-handoff gate can be rerun independently:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests --test service_replicator_live_mysql_8_4_11 \
  -E 'test(=controlled_handoff_closes_source_before_target_publication)'
```

The native observer gate can be rerun independently:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests --test adapter_live_mysql_8_4_11 \
  -E 'test(=qualify_oracle_mysql_8_4_11)'
```

The single-process lifecycle gate can be rerun independently:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests --test service_live_mysql_8_4_11 \
  -E 'test(=one_fresh_owned_instance_lifecycle)'
```

The fresh three-member bootstrap/join gate can be rerun independently:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests \
  --test service_group_replication_live_mysql_8_4_11 \
  -E 'test(=three_fresh_members_bootstrap_join_and_cleanup)'
```

The native feasibility gates can be selected independently:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql --lib \
  -E 'test(=service::qualification::tests::qualify_profile_gtid_transitions)'

CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql --lib \
  -E 'test(/service::qualification::tests::qualify_peer_.*/)'

CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql --lib \
  -E 'test(=service::qualification::tests::qualify_normal_acknowledgement) | test(=service::qualification::tests::qualify_ambiguous_disconnect) | test(=service::qualification::tests::qualify_secondary_loss_during_after) | test(=service::qualification::tests::qualify_post_ack_primary_crash_restart)'
```

The live tests launch only isolated project-local fixture children, require all
scenarios and cleanup to pass, and remove temporary data, runtime files, and
sockets.

GitHub Actions installs the same exact Oracle package and pinned nextest binary
on Ubuntu 24.04 and runs the standard workspace gate for every pull request and
every push to `main`.
