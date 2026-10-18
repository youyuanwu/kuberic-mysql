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
infrastructure, not production topology owners. The normal crate surface
includes the public adapter against `kuberic-runtime` pinned exactly to commit
`be4f83b43567c59f48472025d55dd731efecc4c6`; no private managed lifecycle,
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
metadata store and has no owner-loss recovery, reattachment, or survivor-adoption path;
loss of in-memory ownership or attempt context requires fixture reset.
Cancelling bootstrap cannot detach a query future: bootstrap-off and proof are
attempted after every enable attempt, while cancellation synchronously contains
the exact owned topology before returning control.

The first bounded Stage 3 [closed safe-reopen primitive](docs/mysql-safe-reopen.md)
is available separately as `MysqlStoppedReopen::capture` and `reopen`. It
requires a continuously retained exact owner, a quiet graceful stop, preserved
native logs, matching complete structured GTIDs and explicitly renewed generic
caller authority. It starts a distinct attested child with SQL TCP/X/group
startup disabled and returns only closed observation/containment. No-proof
capture still rejects every issued application/proxy capability. The opt-in
`prepare_reopenable_proxy` → `close_for_reopen` →
`capture_with_proxy_closure` path additionally supports terminal surrender of
one exact independent production proxy, including published and unpublished
owners. Stale shared handles cannot rearm or republish; direct/generic issuance
and Build TCP remain unqualified. It restores no role, epoch/configuration, commitment,
donor capability or access, and does not enable hosted `Existing`. Lost-owner,
abrupt and lost-log recovery remain unsupported; no private durable authority
is added.

The same module now exposes `MysqlReplicator`, a one-member lifecycle engine
that owns one `MysqlInstanceManager` and enforces open, newer epoch, role,
configuration, access, and terminal ordering. Same-role transitions still
require an epoch barrier. Native bootstrap, join, coherent view readback, and
primary transfer receive credit only from exact request-bound member-effect
receipts. Its ordinary generic Build and removal entry points validate current
primary authority, target identity, operation identity, and exact retries,
then return a typed unsupported outcome; the bounded Public V1 custom
coordinators described below own fresh-secondary Build and completed-idle
retirement instead.
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

The crate exports `MysqlPublicV1Service`,
`MysqlPublicV1ReplicatorFactory`, and `MysqlPublicV1Replicator` as the pinned Public V1
`StatefulServiceReplica`/`ReplicatorFactory`/`Replicator`/
`PrimaryReplicator`/`StateProvider` bundle. The service passes the exact same
driver allocation as the factory's required progress-only provider; the
factory verifies pointer identity and still returns no `StateReplicator`.
After successful `Replicator::open`, the adapter retains `0` only from the
explicit `OpenMode::New` + freshly initialized empty owner + closed-access
proof; the pinned host can therefore refresh applied and committed progress
before role/configuration. Unknown pre-configuration progress still fails
closed. Later primary `current_progress` independently proves its frozen,
complete GTID boundary on **every applicable public voter**, with exact live
owner/configuration/session/storage/view checks. This is stronger than a write
quorum, not a relabeling of local execution or a native statistics column.
Secondary progress remains qualified local durable acceptance; it does not
establish provider commitment. `last_committed_lsn` revalidates the same
independently proved public sampling pair, without advancing it because of
intervening writes or catch-up callbacks. An unproved secondary provider read
rejects. Provider epoch input requires a scoped immediate-predecessor proof
or the separately proved fresh empty boundary; copy/data-loss methods reject.
There are no application bytes or private durable state. It supports only fresh
`OpenMode::New` ownership,
an initial one-member local-primary bootstrap, scalar progress publication,
same-role epoch transitions, fresh-secondary Build through the direct custom
replicator callback, and already-formed planned-swap translation.
The adapter converts the bound local replica/process session plus runtime
epoch, role, remote-peer configuration, progress, and access values into
independent MySQL fences. Public configuration callbacks contain only remote
`ReplicaInformation`; the local target is synthesized from the separately
enrolled MySQL process/session/storage/endpoint authority. A fresh singleton
therefore has an empty peer list.
Its versioned non-secret replication endpoint carries only the native member
UUID, SQL member address, and Group Replication address.

The custom `PrimaryReplicator::build_replica` implementation transfers one
fresh idle target through Group Replication distributed recovery. It binds the
callback to the exact source and target identities, process sessions, storage
incarnations, endpoint, epoch, configuration, Build ID, and frozen structured
GTID boundary. The target remains outside the public voting configuration and
keeps external access closed until later role and configuration callbacks.
Success requires the exact successor view, target `ONLINE`/`SECONDARY` state,
and GTID containment; Clone/reseed is not used. Dropping the callback
synchronously retires the exact target process generation and quarantines
participants when native effects may have occurred.

At the pinned runtime, active configuration descriptors may use `INVALID_LSN`
to omit current progress only when the adapter retained that field for the
exact replica identity, process session, and storage incarnation. Omitted
catch-up capability follows the same rule after first acceptance; the one
first-use exception is explicit current progress with `must_catchup=false`,
which establishes a known-unknown catch-up capability. Required unknown
catch-up fails closed. Omission never means zero and never reuses another
incarnation's observation. Idle Build input still requires both progress
fields to be `INVALID_LSN`.

An exact completed Build retry revalidates and returns its retained terminal
result. An interrupted target is not reused: partial Group Replication state
requires reset and a new Build against fresh storage. This intentionally
differs from PostgreSQL's reproducible frozen-byte retry. Cancellation closes
application admission immediately, but the temporary native recovery account
can remain technically authenticatable until its replicated `DROP USER`
commits; the Build group remains blocked until absence is proved or reset is
required.

`PrimaryReplicator::remove_replica` retires only an exactly retained,
successfully built target that is still publicly idle and nonvoting. Native
membership alone is not public voting admission. Remove verifies recovery
account/grant absence, proves the exact next native view on every survivor,
retires the target's exact process/socket/scratch generation, and invalidates
its endpoint, pending receipts, progress, and old Build completion authority.
Persistent data is **not deleted**. The source and admitted peers remain owned
and operational; active membership removal belongs to configuration updates.
An exact successful retry rechecks retained containment and live survivor
authority/account/view evidence. Unknown, activated, stale, or substituted
targets reject. Numeric replica-ID reuse is fenced for that source lifetime.

Remove uses a fresh bounded child of current lifetime owners, not the old
Build deadline. Dropping it before effects leaves the completed Build
retryable. After effects start, cancellation/error retains target ownership,
retires its exact generation and registry entry, and quarantines the group;
unproved cleanup never becomes success. That state requires reset, not a
durable recovery journal or a success-shaped retry.

Commitment qualification is limited to fresh exclusively owned processes
continuously running the enforced Oracle profile: flush-at-commit and
sync-binlog `1`, enabled binlogging/replica updates, and `ABORT_SERVER` binlog
error policy. Effective settings are read in the native bracket. An observed
violation immediately invalidates that owner lifetime, even if the rest of
the read is cancelled/fails or settings are
restored. Privileged external settings/history mutation is outside this
authority. Missing/lagging voters and unsupported configuration histories
reject; no missing evidence is replaced by zero. Exact additive Build
transitions cover both previous/proposed voter sets, including source-first
admission while peer callbacks remain outstanding. Omitted scalar input alone
cannot establish commitment or quiet-copy catch-up.

Kuberic callback order is adapted without weakening the native engine order.
Ordinary formed members complete Epoch, then Role, then Configuration; role is
observed before configuration credit. A controller-composed source swap runs
Revoke, Epoch, refreshed Configuration staging, CatchUp, Role/native transfer,
then application Role, without inventing a `previous` callback value. Catch-up
captures a fresh qualified local boundary and independent all-voter proof
while a current engine configuration exists. A cached post-epoch scalar is
not commitment authority: a Primary without an applicable native
configuration cannot complete that wait or publish committed progress.
Already-formed planned-swap translation is not a claim of qualified custom
controller cutover through that gap. Before a newer epoch mutates the engine, any prepared or
published proxy generation is physically revoked through the native-backed
containment path. Binding-preserving monotonic progress refresh, including
omissions that resolve to retained values, avoids another native configuration
effect; regressions or binding changes conflict. If that accepted local configuration is
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

This bounded adapter deliberately rejects `OpenMode::Existing`, Clone/reseed,
active-member Remove, data-loss acceptance,
non-projectable or unproved catch-up, and automated/unplanned failover. It does
not claim cross-host, TLS, Kubernetes, durable interrupted-effect recovery, or
production readiness. Its custom Build path is process-local and follows the
direct PostgreSQL example shape. Production-controller dispatch into a custom
replicator remains separately unsupported because the controller's target inbound
authority installation uses crate-private runtime control.

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
installed, `protoc` must be available for the pinned Kuberic runtime build, and
the system `mysql.service` must remain stopped. Install the
repository-pinned, checksum-verified `cargo-nextest` binary with:

```bash
scripts/install_nextest.sh
```

## Validation

The standard workspace test starts isolated MySQL fixtures and runs the
observer, single-process lifecycle, three-member bootstrap/join, and closed
retained-owner reopen gates:

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
  -p kuberic-mysql --lib
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests \
  --test kuberic_public_v1_contract \
  --test kuberic_public_v1_build_contract
CARGO_BUILD_JOBS=1 cargo clippy --locked --offline \
  -p kuberic-mysql --lib -- -D warnings
```

The serial live custom-trait gate forms one fresh Primary and builds fresh
secondaries. It covers explicit/omitted progress, independent commitment and
provider pairing, stale authority/settings/view rejection, first/second idle
Remove with retained data and surviving writers, exact retries, replacement
incarnations, and controlled Build/Remove future-drop containment:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests \
  --test kuberic_public_v1_build_live_mysql_8_4_11
```

Production-shared progress/Remove admission, retirement ledger, cleanup
checkpoint, deadline and contention reducers also run without MySQL:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql --lib -E 'test(/service::kuberic::/) | test(/idle_remove_tests/)'
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

For every pull request and every push to `main`, a GitHub Actions build job
compiles the workspace tests once with `cargo nextest archive` and uploads the
Zstandard-compressed tarball. Formatting, documentation tests, and Clippy also
run once in that job. Four separate Ubuntu 24.04 runners install the same exact
Oracle package and pinned nextest binary, download the archive, and run disjoint
hash partitions (`hash:1/4` through `hash:4/4`) in parallel without recompiling.
The `ci` profile keeps tests serial within each runner. A failed partition does
not cancel the others.

Fixture allocation, including crate-private native qualification, creates its
parents explicitly. Archive consumers do not need compilation to have created
a checkout-local `target` directory.

To reproduce one CI partition locally:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --workspace --profile ci \
  --partition hash:1/4
```

To build and reuse the same archive locally:

```bash
CARGO_BUILD_JOBS=1 cargo nextest archive --locked --offline --workspace \
  --profile ci --archive-file nextest-archive.tar.zst
cargo nextest run --archive-file nextest-archive.tar.zst \
  --workspace-remap "$PWD" --profile ci --partition hash:1/4
```
