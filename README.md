# kuberic-mysql

`kuberic-mysql` contains a deterministic Rust safety core, a read-only native
observer, bounded lifecycle ownership for fresh Oracle MySQL servers, the
MySQL-owned Stage 2 integration foundation, and a bounded feature-gated
Kuberic Public V1 adapter. The delivered native target is Oracle MySQL
Community Server 8.4.11 on Linux x86-64. Administration and observation use
private Unix-domain sockets; Group Replication and the bounded application
gateway use distinct loopback-only endpoints.

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
per replica plus a narrow adapter seam. The fixed three-process topology
manager and the three-engine handoff fixture remain qualification
infrastructure, not production topology owners. Enabling
`kuberic-public-v1` compiles the public adapter against `kuberic-runtime`
pinned exactly to commit
`ceec5887ad97d71222117610428ce715b782777e`; no private managed lifecycle,
observation, access, or receipt capability is attached.

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
also contains an owned member that was prepared but not opened. Exact retries
of a completed access publication return the retained endpoint and outcome
before one-time handoff prerequisites are reconsidered.

Qualified scalar progress is now a production boundary. Empty structured GTID
history maps to `0`; otherwise only one untagged configured group source with
one contiguous interval beginning at `1` maps to its exact signed tail.
Complete `GtidSet` evidence remains authoritative. Foreign, tagged, gapped, or
multi-source history is typed non-projectable, and catch-up capability remains
an independent unsupported/unproved result when retained donor/binlog evidence
is unavailable.

The `kuberic-public-v1` feature exports
`MysqlPublicV1Service`, `MysqlPublicV1ReplicatorFactory`, and
`MysqlPublicV1Replicator` as the pinned Public V1
`StatefulServiceReplica`/`ReplicatorFactory`/`Replicator`/
`PrimaryReplicator`/`StateProvider` bundle. The service passes the exact same
driver allocation as the factory's required progress-only provider; the
factory verifies pointer identity and still returns no `StateReplicator`.
After successful `Replicator::open`, the adapter retains `0` only from the
explicit `OpenMode::New` + freshly initialized empty owner + closed-access
proof; the pinned host can therefore refresh applied and committed progress
before role/configuration. Unknown pre-configuration progress still fails
closed. Later `current_progress` retains its exact qualified scalar and
`last_committed_lsn` returns that same observation, while provider epoch,
copy-state, and data-loss surfaces validate exactly or fail closed without
application bytes or private durable state. It supports only fresh
`OpenMode::New` ownership,
an initial one-member local-primary bootstrap, scalar progress publication,
same-role epoch transitions, and already-formed planned-swap translation.
The adapter converts the bound local replica/process session plus runtime
epoch, role, remote-peer configuration, progress, and access values into
independent MySQL fences. Public configuration callbacks contain only remote
`ReplicaInformation`; the local target is synthesized from the separately
enrolled MySQL process/session/storage/endpoint authority. A fresh singleton
therefore has an empty peer list.
Its versioned non-secret replication endpoint carries only the native member
UUID, SQL member address, and Group Replication address.

Kuberic callback order is adapted without weakening the native engine order.
Ordinary formed members complete Epoch, then Role, then Configuration; role is
observed before configuration credit. A controller-composed source swap runs
Revoke, Epoch, refreshed Configuration staging, CatchUp, Role/native transfer,
then application Role, without inventing a `previous` callback value. Catch-up
captures a fresh qualified local boundary on every wait while a current engine
configuration exists; only the intentional post-epoch gap reuses its cached
pre-epoch boundary. Before a newer epoch mutates the engine, any prepared or
published proxy generation is physically revoked through the native-backed
containment path. Exact configuration refreshes update peer progress without
another native configuration effect. If that accepted local configuration is
Primary, identical current or catch-up replay also retries the separate
fixed-port proxy publication obligation; an already published generation is
an idempotent success. A target's pre-source
Epoch -> Primary Role -> Configuration sequence is retained as fenced intent:
promotion remains pending until the later native view actually reports
Primary, so absent external transfer automated failover stays closed. If the
source swap changes the target natively before its ordinary Epoch callback,
the pre-epoch capture accepts a request-bound exact-membership view/progress
observation without requiring the obsolete recorded Secondary role. Ordinary
progress reads remain role-strict. Exact old-role retries after a newer epoch
are stale.

The application Primary callback returns the fixed proxy address before the
standalone Configuration program runs. Successful local-primary configuration
completion therefore prepares and publishes that listener. Effective access
still requires the retained Kuberic factory context to report both read and
write `Granted`: the proxy checks before accepting a session, before every
command dispatch, and again before forwarding every response packet. A denial
closes only that session/command, suppresses post-effect success, and leaves
the generation available for a later runtime regrant. Application `None` or
secondary callbacks retain explicit generation revocation behavior.

This bounded adapter deliberately rejects `OpenMode::Existing`, fresh
secondary admission, Build/reseed, Remove, data-loss acceptance,
non-projectable or unproved catch-up, and automated/unplanned failover. It does
not claim Kuberic-created three-member formation: the only fresh public
formation is one local Primary; a multi-member configuration must already be
formed by supported native work outside this stage.

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
ID makes containment unproved. The obligation is armed before polling the UDS
connect, so cancellation or future drop after server acceptance cannot produce
vacuous absence. The proxy retains a shared synchronously closable listener;
the accept task only polls nonblocking accepts and never exclusively owns the
socket. Seal, drop, revoke, and synchronous abort close that listener
immediately. Synchronous abort then completes native process containment before
returning, but does not claim that an already-aborted Tokio task was joined
synchronously. Dropping an in-flight service Open cancels and settles its
opening state so Close cannot wait indefinitely. Snapshots and receipts retain
bounded secret-free outcome summaries. Text rows accept SQL `NULL` (`0xfb`) while an
initial `0xfb` response remains rejected as `LOCAL INFILE`.
TLS, cold/full authentication, arbitrary accounts or plugins, prepared
statements, local-file transfer, compression, multi-statements, multi-results,
pooling, and general MySQL protocol support are not claimed.

A repository-owned live fixture composes three independent member-local
engines for fresh formation and one controlled handoff. The handoff freezes the
source's structured/scalar boundary only after a normally completed source
closure and a fresh post-revocation native observation. The source engine
issues a stable boundary identity binding the revoked generation, closure
receipt, transfer, newer epoch/configuration, and source terminal receipt.
Rearming or publishing a later source generation invalidates that boundary;
an obsolete eligible closure cannot authorize transfer or target publication.
The fixture proves fresh exact three-member history after schema DDL, transfers
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
bounded adapter still excludes effective signed build/peer execution, generic
build/removal completion, automated failover, Clone/reseed, replacement or
destructive repair, restart continuation or survivor adoption, TLS, cross-host
operation, containers, Kubernetes, and production availability, durability,
performance, or security claims.

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

The pinned Kuberic Public V1 API and exported translation/factory/service
contracts can be checked without launching MySQL:

```bash
CARGO_BUILD_JOBS=1 cargo check --locked --offline \
  -p kuberic-mysql --lib --features kuberic-public-v1
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests --test kuberic_public_v1_contract
CARGO_BUILD_JOBS=1 cargo clippy --locked --offline \
  -p kuberic-mysql --lib --features kuberic-public-v1 -- -D warnings
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
