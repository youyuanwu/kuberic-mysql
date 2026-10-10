# MySQL/Kuberic High-Level Design

This document defines the proposed safety and lifecycle contract for integrating
Oracle MySQL with Kuberic. It is an implementation design, not an implementation
claim.

## Status and Target Profile

### Current repository status

This repository delivers Stage 0 documentation, the Stage 1 deterministic
safety core, one narrow native-observation slice, and a bounded portion of
Stage 2. The publish-disabled `kuberic-mysql` crate exposes these as the
`core`, `adapter`, and `service` modules. The adapter observes exactly one
caller-selected Oracle MySQL Community Server 8.4.11 through a private
Unix-domain socket. The service owns one fresh, restart-stateless process
generation and composes exactly three fresh generations for one designated
Group Replication bootstrap followed by two sequential joins. The separate
`kuberic-mysql-tests` package owns integration contracts, shared fixtures,
protocol simulators, and live qualification. The delivered topology remains
closed to ordinary clients and does not complete Stage 2 callbacks, access
reconciliation/publication, or controlled switchover.

Future lifecycle work follows the
[restart-stateless metadata design](stateless-metadata.md): the MySQL adapter
and lifecycle component own no private durable recovery store. Durable
operation records are application-neutral Kuberic controller or generic-agent
records, not a MySQL-specific metadata file or database.

It also targets Kuberic's SF-aligned public-only contract in the sibling
repository's `service-fabric-api-semantics.md`,
`service-fabric-alignment.md`, and `stateless-default-replicator.md`. The
delivered fixed three-member manager is a native qualification boundary, not
the future production runtime shape. Production integration requires one
member-local public Replicator per replica and no private managed lifecycle,
observation, access, or receipt attachment.

Words such as **must** and **requires** below remain requirements for stages
whose delivery gates have not passed; they do not imply that broader lifecycle
integration behavior exists today.

### Delivered Oracle MySQL 8.4.11 observation slice

The core owns dependency-neutral identities, GTID sets, native snapshots,
outcomes, and Kuberic authority credit. The adapter owns UDS transport,
versioned SQL, decoding, deadlines, and diagnostics. MySQL remains authoritative
for product identity, Group Replication membership/view, native role and state,
executed GTIDs, and native read-only switches. A valid observation grants only
observation credit; Kuberic read and write access remain closed.

One authenticated session executes these exact surfaces:

1. product/version/platform and `server_uuid`;
2. opening group identity, Group Replication address, `read_only`, and
   `super_read_only`;
3. opening `replication_group_members` and the local
   `replication_group_member_stats` view;
4. `@@GLOBAL.gtid_executed`; and
5. the same local state, complete membership, and local view again.

Any opening/closing change in group, view, membership, member address, role,
state, Group Replication address, or either read-only switch rejects the result
as incoherent. GTIDs are a point sample between stable brackets, not scalar
progress. The qualified maximum sequence is `9223372036854775806`; Oracle
8.4.11 rejected `9223372036854775807`, so later patches must requalify this
boundary before changing the core contract.

The socket must be absolute, UTF-8, non-symlink, and the same socket inode at
preflight and connection. The client is configured with no TCP fallback.
Qualification also exercises the public observer against a validated socket
that is then closed and removed, proving an exact unreachable UDS result
without TCP fallback.
Observation uses one absolute deadline across validation, connect, every query
and row-consumption boundary, disconnect, and final admission. Expiry has
priority and earns zero credit; explicit teardown uses only the remaining
budget. Transport, authentication, permission, authoritative absence,
malformed evidence, unsupported product/schema/value, staleness, future timing,
and incoherence remain distinct.

The observer needs local authentication, socket access, and only `SELECT` on
`performance_schema.replication_group_members` and
`performance_schema.replication_group_member_stats`. Removing either grant is
qualified independently as permission denial. The adapter itself does not
initialize, start, stop, reap, configure, or mutate MySQL.

Qualification uses the installed Oracle
`mysql-community-server-core=8.4.11-1ubuntu24.04` package from
`repo.mysql.com`'s `mysql-8.4-lts` component. Repository development and CI
require that exact installation. The runner verifies dpkg
ownership/integrity, APT provenance, executable and library compatibility,
fixture provenance, native scenarios, and deterministic cleanup. It creates owner-only
project-local runtime/data directories, uses `aa-exec` only for the isolated
child, rejects an active system service or foreign `mysqld`, verifies the
launcher package, attests `/proc/<pid>/exe` as the package-owned
`/usr/sbin/mysqld`, disables client TCP, and permits only one-member loopback
Group Replication transport. Eligibility requires the exact upper-bound
rejection `1772/HY000` and checks the precise tagged/newline/multi-source
`GTID_SUBSET` input with an exact scalar response of `1`; no row, SQL `NULL`,
`0`, or another scalar is not accepted evidence.

### Delivered bounded process host and fresh topology chunk

`kuberic_mysql::service` validates exact executable and launcher files, absent
non-overlapping data and scratch roots, and positive operation deadlines. It
creates owner-only roots, renders runtime files under scratch, runs
`--initialize-insecure`, retains and attests the sole child and PID, waits for
the private UDS, and delegates a socket-matched request to the existing
adapter. Stop signals only the retained child, escalates within a deadline,
reaps it, proves UDS absence, removes scratch, and retains the data root.

The single-instance manager writes no application metadata, operation journal,
receipt database, state JSON, or adoption record. All lifecycle and operation
context is in memory. If that context is lost, the supported response is
fixture reset; the crate neither discovers nor adopts a survivor. TCP and
MySQL X are disabled, Group Replication remains stopped on this one-process
path, and no client address is published. Its live gate accepts the adapter's
fail-closed inactive-membership `Absent` result as the expected pre-bootstrap
observation.

The same service now composes exactly three independently configured instance
managers. It initializes all three fresh member layouts, starts and enrolls the
designated bootstrap member, proves transient bootstrap mode is disabled, and
accepts a fresh one-member view before starting the next member. The second and
third members join sequentially. Each accepted transition requires the exact
enrolled process, storage, endpoint, server and member identities; the expected
unchanged predecessors; no extra member; the required role and `ONLINE` state;
one current view; and structured GTIDs containing the captured predecessor
boundary. `RECOVERING` remains pending and grants no lifecycle credit. Oracle's
qualified XCom `VIEW_ID` is treated as `<fixed u64>:<monotonic u32>`: bootstrap
must produce a strictly parsed qualified ID, and each join must preserve the
fixed part while advancing the monotonic part by exactly one. Malformed,
overflowed, skipped, regressed, changed-fixed-part, and apparently-correct
post-churn views receive no credit.

The original five-argument `MysqlInstanceConfig::new` remains the
single-instance profile. Topology members use the explicit
`new_topology_member` constructor with `MysqlTopologyConfig` and
`MysqlMemberIndex`; the topology manager rejects any single-instance profile.
Before initialization it compares every member's data root, scratch root, and
derived configuration, UDS, PID, log, temporary, secure-file, binary-log, and
relay-log path. Equality or destructive ancestor/descendant overlap is a
configuration failure and no root is created.

One manager-owned process-monotonic clock establishes one absolute topology
attempt deadline. Default construction uses the real monotonic clock and
deterministic contracts inject a test clock. Public topology operations derive
their current time, native-control deadline, and observation deadline
internally. Clock regression, future-dated mapping, and completion at or after
the attempt deadline fail closed; control and observation cannot outlive the
attempt.

Account setup and topology mutation use a separate root session over each
currently owned UDS. Account statements run with session binary logging
disabled and restored. The observer principal receives only the two required
Performance Schema `SELECT` grants. The common recovery principal receives
only `REPLICATION SLAVE` and `CONNECTION_ADMIN`; its password is supplied to
`START GROUP_REPLICATION` from process memory. The read-only adapter supplies
accepted post-effect evidence. SQL TCP and MySQL X remain disabled and the
topology API never publishes read or write access.

Failure invalidates attempt capabilities and contains every exactly owned
member. Normal containment stops and reaps each retained child, proves UDS
absence, removes disposable scratch, and retains persistent data roots for
diagnosis until the enclosing fixture removes them. The live gate proves the
fresh three-member flow, closed client endpoints, exact identities, bootstrap
off, GTID-source restrictions, and cleanup against the pinned package.
After any bootstrap-enable attempt, the service attempts bootstrap-off and an
explicit proof even when enable or start returns an ambiguous transport or
timeout failure, preserving primary and cleanup failures together. Cancelling
the public bootstrap future never detaches a query: an armed ownership guard
synchronously contains all exact members before cancellation returns unless
bootstrap-off proof has completed.

The delivered slices make no claim for another MySQL patch or fork, Kuberic
callbacks, access reconciliation or publication, routing, switchover, failover,
fencing, Clone/reseed, replacement, destructive repair, restart continuation,
TLS, cross-host operation, containers, Kubernetes, availability, durability,
performance, or production security.

The broader host-local lifecycle target remains:

- Oracle MySQL 8.4 LTS on Linux, reusing the delivered exact 8.4.11
  observation contract until any replacement patch is independently qualified;
- one Kuberic replica incarnation per `mysqld` process and data root;
- a three-member, host-local development topology;
- GTID mode and single-primary Group Replication;
- Kuberic as durable lifecycle and client-access authority; and
- Group Replication as MySQL's membership, transport, and transaction-history
  authority.

The three members on one host provide deterministic development and integration
coverage. They do not demonstrate production fault-domain isolation. The
delivered service refuses products, versions, platforms, or topology modes
outside its pinned profile.

### Proof-of-concept boundary

The delivered Stage 1 core remains intentionally server-free. It proves the
pure identity, GTID-set, view, observation, and stale-authority contracts
without starting or contacting MySQL. The separately delivered adapter
contacts one exact server. The bounded service owns individual fresh local
processes and now proves the fresh three-member bootstrap/join portion of the
Stage 2 PoC. It does not complete the callback, access, handoff, or resumability
portions and does not claim a production lifecycle.

The first executable **server-integration** milestone is the Stage 2 PoC. Its
purpose is to prove that Kuberic can own three local `mysqld` processes, observe
Group Replication and GTID state, drive one member-local public SF-shaped
Replicator per replica, and complete a controlled primary handoff.

The PoC includes:

- three independently initialized `mysqld` processes on one Linux host;
- one private Unix-domain socket per member for adapter observation and
  administration;
- loopback TCP for Group Replication and distributed recovery;
- a single-primary group created from fresh fixture-owned data roots;
- exact process, storage, `server_uuid`, member, view, and GTID binding;
- public role, epoch, configuration, catch-up, build, and close operations with
  client access initially closed;
- one application-owned MySQL-compatible client proxy per replica while the
  raw native listener remains private;
- one controlled switchover while every member and the source runtime remain
  reachable; and
- exact source process stop/reap verification before target write publication.

The PoC explicitly defers:

- TLS, certificate authorities, certificate rotation, and network peer
  identity;
- separate observer, mutation, provisioning, and client principals;
- Clone, destructive reseed, replacement, backup, and rolling upgrade;
- crash-resumable native mutation after an ambiguous external effect;
- an independent infrastructure fence provider and continuing fence receipts;
- automated unplanned failover, force recovery, and data-loss acceptance;
- cross-host, Kubernetes, CNI, service-mesh, and production routing behavior;
  and
- production availability, durability, or security claims.

Deferred does not mean unsafe fallback. If a PoC operation encounters an
ambiguous process, native effect, GTID history, view, or authority transition,
it keeps client writes closed and requires fixture reset or operator
intervention. The PoC still preserves the foundational invariants: exact
identity, structured GTID evidence, stale-result rejection, and no write
publication from native role alone.

### Contract labels

This document uses three labels:

- **Existing Kuberic contract** describes behavior evidenced in the sibling
  Kuberic repository and cited under [Reference Comparisons](#reference-comparisons).
- **Proposed MySQL contract** is normative for future work in this repository.
- **Deferred** identifies behavior that must not be advertised until its
  delivery and test gates pass.

MySQL-native details named here must be checked against the exact pinned 8.4
release during implementation. The SF-aligned Kuberic values and sequencing
remain roadmap dependencies until implemented; this design does not treat
planned capabilities, signed authorization, or public value shapes as already
available.

## Authority and Ownership

Safety depends on keeping five independent kinds of authority distinct.

| Concern | Single owner | Owned decisions and effects | Handoff evidence |
|---|---|---|---|
| Desired topology and lifecycle | Kuberic controller | Configuration intent, replica identities, role intent, operation ordering, and retry policy | Exact configuration/epoch and callback context |
| Durable effects | Generic Kuberic agent and runtime lifecycle host | Process/storage ownership, generic operation records, callback fencing, cancellation, access reconciliation, and restart reconstruction | Exact process session, generic operation record, and effect receipt |
| Native facts and replication transport | MySQL engine / Group Replication | Authoritative group membership, native role/state, GTID and recovery facts, replication transport, and execution of accepted native operations | Native state exposed by the pinned MySQL profile |
| Native integration and proof | MySQL adapter and public Replicator | Authorized SQL/native mutation, coherent observation collection, and interpretation sufficient to complete or reject an exact public operation | Exact public call completion or typed public error under the current runtime operation fence |
| Independent containment | Infrastructure fence provider, after the PoC | Terminating or isolating the exact old process or host independently of that `mysqld` and its service runtime | Durable, verifiable, exact-incarnation fence receipt |

Kuberic authority is not Group Replication membership. A member may be
`PRIMARY` in a native view while Kuberic write access is closed. Conversely, a
Kuberic role request does not prove that MySQL has reached a safe native state.
Only the Kuberic controller chooses desired lifecycle state; only the durable
runtime applies authorized effects; only the MySQL engine supplies
authoritative native history, membership, role, and transport facts; only the
adapter issues authorized native requests and turns fresh facts into typed
proof. The PoC supports only a reachable, controlled source whose exact
`mysqld` process can be stopped and reaped before target write publication.
Later automated failover requires an independent fence provider to certify that
an unreachable old incarnation cannot continue serving writes.

### Controller, runtime, adapter, and fence boundary

The controller:

- assigns exact replica incarnations and configurations;
- sequences build, removal, catch-up, role, switchover, and failover intent;
- does not execute SQL, manage MySQL files, or interpret flattened GTID text;
- consumes typed outcomes, not unqualified “healthy” booleans; and
- fails closed when evidence is stale, incoherent, unauthorized, or missing.

The runtime lifecycle host:

- owns the exact child process, files, credentials, and process session for
  each incarnation under generic Kuberic agent authority;
- validates authority immediately before and after every external effect;
- rejects completion from an old configuration, epoch, incarnation, process
  session, native view, credential generation, or operation attempt;
- owns public operation ordering, cancellation, durable completion, service
  address retention, and Kuberic read/write status; and
- reconstructs effects and access by replaying public calls whose MySQL
  implementation performs fresh native observation after restart.

The MySQL adapter:

- provides the custom stateful-service and public replication interfaces;
- maintains typed MySQL identity, GTID, Group Replication, recovery, and
  observation evidence inside the application boundary;
- performs bootstrap, join, catch-up, topology, and later repair operations
  only as work required by one exact public callback. It interprets fresh
  engine facts and returns public success or a typed public failure without
  opening or interpreting the generic agent store; and
- never grants client access from MySQL role alone.

The external fence provider is a post-PoC boundary outside both the managed
`mysqld` and the runtime process that supervises it. It must still act and
provide proof if either process is unresponsive. The PoC does not simulate that
guarantee: it refuses automated write failover when the source runtime or
process is unreachable. A process-local boolean, Kubernetes readiness result,
object deletion request, in-memory role, or database variable is not a
production fence receipt.

### SF-style public operation admission

The target Kuberic runtime follows the Service Fabric V1 ordering and evidence
model. MySQL is hosted through only the public `Replicator` and
`PrimaryReplicator` interfaces. Successful completion of an exact public call
is the runtime-visible evidence for that operation; the runtime does not fetch
a second private MySQL receipt or native snapshot to strengthen the result.

The runtime persists operation intent before invoking MySQL and owns the exact
task, cancellation, supersession, durable revision, process session, and final
access publication. MySQL owns the native work required to make the public
postcondition true. Primary-only configuration callbacks never run before the
Replicator has successfully entered primary role.

The ordered promotion path is:

1. Close Kuberic read and write status and revoke effective client access.
2. Persist the exact role/epoch operation intent.
3. Complete `Replicator::change_role(..., Primary)`.
4. Complete the explicit epoch barrier when the transition requires one.
5. Complete the application replica's primary role change and retain its
   returned service address.
6. Complete `PrimaryReplicator::on_data_loss` when the controller authorized
   possible data loss.
7. Install the exact current/previous public replica-set configuration.
8. Complete the required catch-up wait.
9. Revalidate the durable operation and grant access separately.

A secondary that remains secondary across a newer epoch still receives
`update_epoch`. That call must invalidate predecessor work and peer admission;
an unchanged native role is not an epoch barrier.

There is no atomic transaction spanning a native MySQL mutation and the agent
store. Every public operation therefore has an explicit replay disposition.
Role, epoch, settings, and exact configuration replay must converge. Ambiguous
builds retire their source/target sessions and restart with a new build
authorization. Ambiguous data-loss mutation uses the declared
`ReplaceOnAmbiguity` policy and retires the affected storage incarnation.
Close, abort, removal, and containment must be idempotent. Late completion may
not publish state for a superseding operation.

## Identity and Host-Local Ownership

### Exact identity tuple

Every operation and observation is bound to this logical tuple:

```text
(resource, partition, replica_id, incarnation, process_session, attempt,
 endpoint, data_root, server_uuid, group_name, member_id, member_address,
 kuberic_configuration, native_view, credential_generation)
```

The runtime must treat any change to a relevant field as revocation of old
work. A reused replica number, DNS name, address, port, data path, or
`server_uuid` does not inherit the former incarnation's authority, progress,
fence, or completion credit.

`server_uuid` and Group Replication member identity are native facts observed
from the server. They are not assigned as substitutes for Kuberic incarnation
or process session. A restarted process gets a fresh process session even when
it legitimately retains the same data root and `server_uuid`. A replacement
gets a fresh Kuberic incarnation and storage identity; accidental reuse of the
old native identity is an error requiring quarantine or rebuild.

### Host-local layout

The first live fixture allocates one persistent MySQL data subtree per exact
replica incarnation:

```text
<persistent-root>/<resource>/<replica-id>/<incarnation>/
  data/
```

Runtime files use a separate disposable scratch subtree:

```text
<scratch-root>/<resource>/<replica-id>/<process-session>/
  run/        # socket and non-authoritative PID metadata
  log/
  tmp/
  config/
  credentials/
```

Each member requires unique classic/MySQL-X/Group Replication ports as
applicable, socket path, PID metadata, log and temporary paths, server identity,
replication address, credentials, and data root. The implementation must derive
and persist stable allocations through generic Kuberic authority before process
creation, detect collisions, and never discover ownership by broad process-name,
port-range, or directory scans.

The generic agent commits an exact operation record before every create or
destructive action. Before start, stop, signal, erase, clone, reseed, or cleanup
the runtime revalidates:

- the exact Kuberic identity and current authority;
- the generic-agent storage binding and its ownership identity;
- the process-session handle and executable identity;
- the native `server_uuid` and group/member binding when a server is reachable;
- allocated sockets, addresses, and ports; and
- the operation attempt and destructive-work approval.

Cleanup removes only resources that the exact generic operation still owns. A
mismatched binding, process, native identity, path, or port is a
foreign-resource error, not permission to “clean up” whatever occupies the
location.

### Process ownership and orphan containment

The runtime launches `mysqld` without placing secrets in arguments, records an
exact process-session handle, and owns its normal stop/reap lifecycle. Child
helpers and sockets belong to the same incarnation. Runtime restart must
reconcile generic agent authority with operating-system and native evidence
before adopting or terminating a survivor. Adoption additionally requires
independent withdrawal or isolation of every client path and containment of
existing sessions; ownership proof alone gives no effective-access closure
credit.

Normal process ownership is sufficient only for the PoC's controlled
switchover, where the source runtime is reachable and stop/reap completion is
verified before target writes open. A `mysqld` can outlive its supervising
runtime, so automated unplanned failover remains disabled until an independent
provider can target the exact process cgroup, virtual machine, host, or network
identity and prove the old incarnation is contained.

## Runtime Integration

### SF-aligned public interface mapping

The proposed service is one member-local custom Replicator per Kuberic replica.
The delivered three-process `MysqlTopologyManager` remains a native
qualification fixture and a source of reusable validation/control primitives;
it is not the production runtime owner of three replicas.

| Kuberic surface | Proposed MySQL responsibility |
|---|---|
| `StatefulServiceReplica.open` | Validate resource/partition/storage identity, create one member-local process host and `MysqlReplicator`, select its factory, create one coherent public interface bundle, and return that bundle's control `Replicator` |
| `ReplicatorFactory.create_replicator` | Return the same control/primary object with immutable capabilities and settings; omit `StateReplicator` and `StateProvider` because MySQL owns replication, recovery, and application bytes |
| application `change_role` | Converge the application role and return the role-specific MySQL proxy service address; address retention/publication remains runtime-owned and access status remains separate |
| application `close` / `abort` | Close the service proxy first, cancel and drain exact work, then stop/reap or quarantine the owned process; abort is synchronous containment |
| `Replicator.open` | Bind the application-owned replication/control endpoint and return its published address only after exact process/native identity validation; failed open leaves no listener |
| `Replicator::change_role` | Establish the native role postcondition under the supplied epoch while access remains closed; initial primary may perform the one authorized bootstrap without requiring a prior primary configuration callback |
| `Replicator::update_epoch` | Fence predecessor callbacks, build sessions, peer authorization, and native traffic before accepting newer-epoch work, including same-role secondary transitions |
| `Replicator::close` | Cancel and drain Replicator-owned role/configuration/catch-up/build work, stop its replication/control endpoint and native peer sessions, and leave no descendant task or listener |
| `Replicator::abort` | Synchronously fence new work, invalidate peer/build authorization, stop the replication/control endpoint, and contain owned native work as far as possible |
| `current_progress` | Return only the qualified role-correct scalar projection described below; never return a configuration generation or transaction count as progress |
| `catch_up_capability` | Return the earliest exactly retained scalar boundary only when the qualified history projection and donor/binlog proof exist; otherwise return `INVALID_LSN` or a typed rebuild-required error |
| current/joint configuration | Accept only remote active secondaries, exact process sessions, `must_catchup`, signed peer authorization, and meaningful or invalid public progress; exclude the local primary and every idle/build target |
| catch-up quorum | Capture an internal structured GTID boundary and complete only when the requested public predicate is true for the exact installed configuration; initially support the conservative `All` mode |
| `build_replica` | From the primary, contact the exact idle target through the application-owned control endpoint, present its signed one-attempt build authorization, and complete only after copy/recovery plus retained replication reach the internal build boundary |
| `remove_replica` | Cancel and drain the exact build before convergently releasing source-side sessions/resources for the retired idle target; configured removal occurs through configuration exclusion first |
| `on_data_loss` | Run only under typed controller authorization, revalidate structured GTID history internally, and use `ReplaceOnAmbiguity`; unsupported or incompatible history returns rebuild-required and keeps access closed |

The custom bundle uses no private managed lifecycle, observation, access, or
receipt capability. MySQL may keep structured observations and operation
state in memory, but runtime correctness depends only on exact public call
completion plus agent-owned identity, configuration, history context, signed
authorization, and access ordering.

Graceful teardown first revokes access and cancels/drains exact outstanding
operations, then closes the Replicator, then closes the application
replica/proxy and stops or quarantines `mysqld`. A failed Replicator close is
contained through Replicator abort before application close continues.
Ungraceful teardown invokes Replicator abort before application abort, and both
abort paths are synchronous.

### Data-loss callback and replay disposition

The data-loss callback is not permission to discard transactions or force a
view. The public bundle declares the `ReplaceOnAmbiguity` data-loss replay
disposition. A retained callback result is not reinvoked. If process failure
makes the result ambiguous, the authorized replica/storage incarnation is
retired and recovery selects or builds another incarnation.

Before controlled data-loss recovery is enabled, `on_data_loss` returns an
explicit unsupported or rebuild-required error. Once enabled, it may complete
only after one coherent native observation proves the exact candidate,
accepted history context, GTID compatibility, quorum assumptions, and required
old-primary fencing. `Ok(false)` means no local correction was made; it does
not prove history compatibility. `Ok(true)` requires a fresh progress query
and invalidates incompatible peer/build evidence before access.

### Public value and authorization dependencies

The SF-aligned Kuberic design keeps the protected public method sets but adds
immutable value semantics around them:

- `ReplicatorCapabilities`, initially including catch-up-specific-quorum
  support and the data-loss replay disposition;
- `HistoryContext`, which scopes scalar progress comparison to one resource,
  protocol generation, opaque history identity, and data-loss number;
- `must_catchup` on an active remote replica description;
- `INVALID_LSN = -1` for unknown target or Replicator-owned progress;
- signed `BuildAuthorization` for one exact source/target/build attempt; and
- signed `PeerSessionAuthorization` for one exact active primary-secondary
  process-session pair and current/previous configuration.

These target values are roadmap dependencies until their Kuberic
implementation lands. MySQL must not bind production code to the current
pre-alignment value shapes or recreate missing fields through a private
runtime channel.

Two MySQL-specific compatibility gates must pass before public-only activation.

#### Scalar GTID projection

The public progress methods return `i64`, while MySQL history is a structured
GTID set. A scalar projection is eligible only for the exact qualified profile:

1. every accepted GTID has the configured Group Replication UUID as its source;
2. no transaction tag is present;
3. the normalized executed history is empty or one interval beginning at
   sequence `1`;
4. no foreign, disjoint, or unexplained history is hidden by the projection;
   a same-lineage purged prefix remains part of `gtid_executed` and does not
   invalidate applied progress; and
5. the sequence fits the qualified signed 64-bit boundary.

For an eligible non-empty history, the interval tail is the public LSN; empty
history is `0`. Any other set is public-progress-incompatible and returns
`INVALID_LSN` or rebuild-required. The controller compares this LSN only
within the same `HistoryContext`. Internally, every native safety decision
continues to use complete GTID-set containment/equality; the scalar never
replaces the set or authorizes access by itself.

The projection remains role-sensitive. A primary reports only the highest
eligible tail proven committed under the exact installed public configuration
and qualified native commit profile, never a merely local executed tail. An
idle or active secondary reports its highest contiguous eligible tail durably
applied locally. A recovering or unassigned replacement may report qualified
election progress while access remains closed, but that value is not serving
readiness.

This shape is not assumed from ordinary Group Replication defaults. The serving
configuration must pin and qualify GTID allocation and view-change behavior,
including `group_replication_gtid_assignment_block_size = 1` and an explicit
`group_replication_view_change_uuid` policy compatible with the one-source
profile. Live gates cover transactions before and after primary transfer,
member departure/restart, every accepted view change, and binlog purge. Any
valid native transition that creates a gap or second source disables the
scalar profile and therefore serving cutover.

`catch_up_capability` is separate from applied progress. It requires exact
proof of the first incrementally recoverable transaction from the qualified
retained set, derived from `gtid_executed`, `gtid_purged`, and the pinned
binary-log inventory/retention profile. Unknown purging, donor substitution,
foreign/disjoint purged history, or non-projectable retained history returns
`INVALID_LSN` or rebuild-required.

#### Native peer-session authorization

Signed Kuberic peer authorization must become an executable native fence.
Group Replication's XCom traffic does not directly consume a Kuberic signed
token, and rotating distributed-recovery credentials does not terminate
existing group communication. Before activation, one qualified design must
prove that `update_epoch`, withdrawal, close, and replacement invalidate every
predecessor primary/session:

- an application-owned authenticated control/transport endpoint may validate
  the signed authorization and mediate native join/session establishment; or
- an exact Group Replication stop/rejoin or communication-stack credential
  transition may provide the barrier, if live qualification proves old
  sessions cannot continue.

If neither design proves process-session and epoch fencing, MySQL remains
ineligible for serving cutover even when native membership appears healthy.

#### Initial quorum and commit profile

The first public-only profile advertises
`catch_up_specific_quorum = false`. Kuberic therefore uses `All` for ordinary
full catch-up and for both planned-swap waits. The MySQL profile must also pin
and live-qualify transaction acknowledgement semantics strong enough that a
successful client commit cannot outrun the public progress/catch-up contract.
`group_replication_consistency = AFTER` is the initial candidate because it is
stronger than write-quorum application, but it is not a supported setting
until the exact Oracle MySQL patch proves its commit, failure, and member-loss
behavior in the repository gate.

The qualification pins the complete durability tuple, including
`innodb_flush_log_at_trx_commit = 1`, `sync_binlog = 1`, binary-log retention,
member-expulsion behavior, and the exact online native view required for an
`All` completion. It injects commit timeout, member loss/expulsion during
`AFTER`, primary crash immediately after acknowledgement, and restart of every
participant. An acknowledged transaction must remain represented in the
required public/native history boundary or automated publication and failover
remain unavailable.

The first serving profile keeps the active three-member set fixed. Bootstrap
and build/join complete while client writes are closed, and planned
switchover changes the primary without changing membership. Serving-time
scale-up, configured-member removal, or a PC/CC transition whose old and new
member sets differ remains unsupported until live qualification proves how the
native Group Replication view transition satisfies both public configuration
commit obligations. A single current native view must not be assumed to prove
independent previous/current quorum completion.

## Native Progress and Observation

### Separate evidence domains

The adapter preserves these independent domains:

| Domain | Examples of evidence | Permitted use |
|---|---|---|
| Transaction history | Executed, received/retrieved, and purged GTID sets | Set containment, equality, divergence detection, and frozen-boundary proof |
| Native configuration | Group name, native view identity, exact member identities/addresses, roles, and states | Coherent membership and authority-admission evidence |
| Recovery | Join/distributed-recovery/clone state, donor, errors, and completion sample | Build and rejoin progress for an exact attempt |
| Applied boundary | Candidate has executed the required frozen GTID set under the same lineage/view constraints | Switchover/failover readiness |
| Retained-history capability | Donor/source history available after purging plus clone/reseed capability | Whether incremental recovery is possible or rebuild is required |
| Kuberic authority | Configuration, epoch, replica/process sessions, partition access generation | Authorization and stale-work rejection |

A GTID set is a set-valued history description, not a scalar LSN. For sets
`A` and `B`, the useful relations are equality, `A` contained in `B`, `B`
contained in `A`, or incomparable/divergent. Lexicographic text order, encoded
length, hash, transaction count, and “largest” set do not create a safe total
order. A native view identifier orders or names membership observations only
within its defined lineage; it does not prove transaction application. Neither
domain can substitute for the other.

Candidate compatibility requires explicit policy. At minimum, the required
source/quorum boundary must be contained in the candidate's executed set, any
candidate-only GTIDs must be explained by the same accepted group history, and
divergent or unknown lineage fails closed. Purged history is a capability
constraint: absence from a donor's retained set may force clone/reseed; it is
not proof that the candidate applied the transaction.

### Coherent observation bundle

Every safety decision consumes one immutable observation bundle with:

- resource, replica, incarnation, process session, endpoint, storage identity,
  `server_uuid`, group/member identity, and local authentication generation;
- Kuberic configuration/epoch, desired role, and read/write access generation;
- collection start/end monotonic timestamps and a decision deadline;
- the native view identity and exact member list sampled for the bundle;
- each member's identity, address, role, state, and reachability;
- executed/received/purged GTID sets and recovery state, with query provenance;
- read-only defense-in-depth state and currently published client surfaces;
- privilege/capability checks and the exact local UDS path, ownership, and
  connected `server_uuid`; and
- bracketing samples sufficient to detect process, identity, or view change
  during collection.

Collection is deadline-bounded. A view, identity, session, local authentication
generation, or authority change invalidates the entire bundle. Data from
different sessions, attempts, authentication generations, or native views must
not be merged into a synthetic observation.

### Observation outcomes

Outcomes are typed and fail closed:

- **absent**: the queried native object or member is authoritatively not present;
- **unreachable**: connection or transport could not be established;
- **permission denied**: authentication succeeded or was attempted, but the
  observer lacks required capability;
- **authentication failure**: the server rejects the local observer
  credentials;
- **malformed/unsupported**: values or profile fall outside the pinned decoder;
- **partial/incomplete**: only part of the required multi-query bundle was
  collected before failure or deadline;
- **stale**: collection or decision deadline expired;
- **incoherent**: bracketing samples differ, authenticated server identity
  contradicts the exact binding, or fields describe different views, sessions,
  or identities; and
- **valid**: all required fields, privileges, bindings, and freshness checks
  pass.

Silence, missing rows, permission denial, and unsupported schema are not
interchangeable. No access, lifecycle, or topology decision may use partial,
stale, or incoherent evidence.

## Bootstrap, Clone, Reseed, and Replacement

All workflows use a durable public-operation identity:

```text
(operation_kind, resource, authority, target_incarnation, source_or_donor,
 history_context, attempt, canonical_public_input)
```

Each generic operation record contains only agent-owned facts: immutable
public inputs, exact replica/process/storage identity, signed authorization,
destructive approval, cancellation/retirement state, and terminal public-call
completion. It does not contain native pre/post views, structured GTID sets,
recovery cursors, donor sessions, or a MySQL handoff receipt.

MySQL observes those native facts inside the exact public call. It returns
success only when the native postcondition is true. If a restart loses
process-local native context, Kuberic follows the operation's public replay
disposition rather than reconstructing a private workflow stage.

| Workflow | Public operation and durable agent fact | Ambiguity/restart action |
|---|---|---|
| Initial bootstrap | Exact primary-role operation for fresh storage plus its retained public completion | Replay converges only when fresh observation proves the same group/bootstrap result; otherwise remain closed and replace/reset |
| Join / distributed recovery | One signed `build_replica` input and terminal public build completion | Retire source/target sessions and reissue with a new build ID, target generation, and signature |
| Clone / reseed | Agent-owned destructive approval followed by a new empty target incarnation and public build | Never continue a lost copy cursor; inspect ownership only to contain/clean up, then build a new incarnation |
| Replacement | Old-incarnation retirement plus allocation and public build completion for a new storage/process incarnation | Keep the old incarnation fenced; never transfer its progress, peer authorization, or completion |
| Cleanup | Exact process/storage containment and idempotent resource-absence completion | Revalidate each agent-recorded resource and remove only the still-owned identity |

A native transition that drops an unapproved member, produces an unexplained
`server_uuid`, changes donor or process session, or reveals incompatible GTID
history causes the current public call to fail. It does not create a durable
native handoff record or broaden the operation's authority.

### Initial bootstrap

Bootstrap is permitted only for a new resource with no accepted native history.

1. Allocate three fresh Kuberic incarnations, storage roots, identities,
   addresses, ports, credentials, and generic operation records.
2. Initialize each data root under its exact ownership marker.
3. Start only the designated first member under closed client access.
4. Invoke the designated member's exact public primary-role operation. Its
   MySQL implementation may enable bootstrap only for this fresh input, must
   disable it immediately, and returns success only after a coherent native
   one-member group is observed.
5. Invoke one signed public `build_replica` operation for each remaining idle
   member. Each build owns its distributed-recovery and internal GTID boundary.
6. Install the exact public three-member configuration and complete the
   conservative `All` catch-up wait while writes remain closed.
7. Persist only those exact public completions. Publish access later through
   the independent runtime decision after authority and all required public
   postconditions converge.

No second member may independently bootstrap the same group. Discovery of an
existing unknown group, data, `server_uuid`, or membership is a foreign-state
error, not a reason to adopt it automatically.

### Join and distributed recovery

A join is one signed public `build_replica` attempt bound to the target,
source, `HistoryContext`, process/storage sessions, build ID, and endpoint. The
target remains client-fenced. Inside that call, MySQL requires the exact target
to appear with the expected native identity in a coherent accepted view, reach
the required online/recovery-complete state, and execute the internal required
GTID boundary. Reachability, process readiness, or membership alone is not
completion.

If required history is no longer recoverable incrementally, the adapter records
that capability result and enters an explicitly authorized clone/reseed path.

### Clone or equivalent provisioning

Clone is deferred beyond the PoC. The initial fixture creates fresh owned data
roots and uses only the pinned Group Replication join/distributed-recovery path.
If retained history is insufficient or recovery is ambiguous, the PoC tears
down the exact owned fixture and starts again rather than entering destructive
repair.

Clone is destructive to the target and requires:

- exact target incarnation, storage marker, and destructive-work approval;
- target client/process containment;
- eligible, fresh, history-compatible donor evidence;
- separately scoped provisioning credentials and a qualified secure transport;
- a new attempt if donor, target identity, authority, or accepted view changes;
  and
- post-restart re-binding of the process session and native identity.

Completion is not the clone command returning. The target must restart or
reconcile as required, present the expected storage/native binding, join the
intended group, complete recovery, and apply the internal build boundary. The
generic agent persists only the new incarnation/process/storage allocation,
signed build input, and terminal public `build_replica` completion.

### Reseed or rebuild

Reseed is a new destructive attempt, never an in-place continuation justified
only by a reused replica number. It first fences the exact target, preserves
diagnostic metadata without secrets, validates destructive approval, removes
only the owned storage root, provisions from an eligible donor, and follows the
same native completion checks as clone/join. An unexplained target-only GTID,
foreign storage marker, or identity mismatch stops the workflow.

### Replacement and cleanup

Replacement assigns a fresh incarnation, process session, storage root, and
native identity binding. The removed incarnation remains fenced and cannot
contribute votes, history claims, or receipts. Cleanup requires current removal
authority plus exact generic operation and fence evidence, and refuses changed
or foreign resources.

### Ambiguous restart after an external effect

Restart may occur after a public call mutates MySQL but before its completion is
durable. Recovery follows the declared public replay contract:

1. Keep client access closed and revalidate the exact durable operation,
   authority, process/storage incarnation, and canonical public input.
2. Exact role, epoch, settings, configuration, close, and removal calls may be
   replayed only when their implementation converges from fresh native
   observation.
3. Catch-up waits are re-evaluated for the exact retained configuration and
   mode.
4. Ambiguous builds retire their source/target sessions and restart with a new
   build ID and signed authorization.
5. Ambiguous `ReplaceOnAmbiguity` data-loss work retires the affected
   incarnation without reinvoking the callback.
6. Destructive provisioning and cleanup use only agent-owned storage/process
   authorization and idempotent containment; they never infer database
   completion from partial files.

If the public replay contract cannot establish a safe next action, the
operation fails closed and requires replacement, fixture reset, or explicit
operator recovery. The agent does not recover success by reading a private
native receipt.

## Client Access and Fencing

### Access is a reconciled effect

The runtime derives effective read and write publication from:

1. Kuberic partition read/write access status for the current access
   generation;
2. exact completion of the required public role, epoch, configuration,
   catch-up, build, and data-loss operations; and
3. runtime-owned process/session, endpoint, revision, and containment fences.

Role is necessary but not sufficient. A native `PRIMARY` with closed Kuberic
write status publishes no writable endpoint. A Kuberic primary intent with a
missing public completion or an unfenced old primary also publishes no
writable endpoint.

Access transitions are generic Kuberic effects. Closure is acknowledged only
after existing publication is withdrawn, ordinary and administrative client
paths are contained as required, relevant sessions are drained or terminated,
and the exact proxy generation is closed. Opening is acknowledged only after
the runtime revalidates current authority, exact public completions, identity,
process/session fences, source containment, and endpoint ownership immediately
before publication. It does not request a second private MySQL observation or
access receipt.

The production service address must not expose the raw `mysqld` listener
directly. The application role change returns an application-owned
MySQL-compatible proxy endpoint. The proxy checks the current Kuberic access
generation before dispatching every MySQL command, keeps the native SQL
listener private, and owns all externally reachable client sessions.
Revocation atomically rejects new command dispatch, closes the listener,
settles or terminates in-flight commands according to the fixed policy, closes
idle and transaction-holding sessions, and proves zero commands/sessions for
the revoked generation before closure completes. Only then may MySQL freeze a
source GTID boundary. An ambiguous disconnect/commit outcome leaves the
boundary and target access uncredited.

`read_only` and `super_read_only` remain a second native barrier. The proxy,
native variables, endpoint withdrawal, and exact process containment are
independent checks; no one check substitutes for the others.

### Why MySQL read-only variables are not a fence

`read_only` and `super_read_only` are useful defense-in-depth controls and
should be enabled on non-writable members. They are not the primary fence:

- privileged operational paths may be able to change the variables;
- replication/internal applier behavior is not equivalent to client-write
  containment;
- pre-existing sessions and direct addresses still exist and must be
  disconnected or isolated according to policy;
- a surviving `mysqld` retains its last state if the supervising runtime dies;
  and
- a variable value does not prove which Kuberic incarnation, process session,
  or host is actually serving an address.

At least one concrete unsafe survival case is therefore in scope: the runtime
loses authority or terminates while its old `mysqld` and direct client network
path survive. Database variables alone cannot prove that incarnation unable to
serve writes.

### Strong fence contract

For the PoC, strong fencing is deliberately limited to a controlled source:
withdraw its client endpoint, close Kuberic write access, enable MySQL
read-only defenses, stop the exact owned `mysqld`, reap it, and verify that its
PID/session and UDS are gone before target write publication. If the source
runtime cannot complete and prove those steps, the PoC leaves writes closed.

Automated failover is a later stage. Its required fence is issued by a provider
independent of the target `mysqld` and its runtime supervisor. Depending on the
deployment stage, the provider may terminate the exact process/cgroup or
isolate the exact host, network identity, storage writer, and direct-client
path. It must contain privileged and existing sessions, not just prevent new
service discovery.

A fence receipt includes:

- provider identity and verifiable provider generation;
- operation ID and canonical input signature;
- exact resource, replica, incarnation, process/host/network identity, and
  addresses fenced;
- requested action and independently observed completion;
- issue time, observation time, validity/expiry or continuing-condition
  semantics, and revocation status; and
- verifier result and non-secret provenance.

The runtime revalidates the receipt immediately before target write
publication. A receipt for another incarnation, attempt, host, or expired
condition is rejected. If the old primary is unreachable and no valid exact
receipt can be obtained, automated write failover is unavailable.

Receipt freshness and underlying containment lifetime are distinct. The old
incarnation's containment remains effective for the entire write grant that
depends on it. A receipt may expire as observation evidence and require a fresh
provider verification, but expiry must not automatically remove the underlying
process/host/network isolation. A provider that cannot retain containment
fail-closed when the target runtime or renewer disappears is not eligible for
automated writable failover.

The generic target-access record retains the fence dependency. Provider loss,
revocation, or failed revalidation closes the dependent write grant and
endpoint, but it still does not authorize fence release. Release or readmission
is a separate exact-incarnation operation owned by the fence provider and
sequenced by Kuberic:

1. Revoke the dependent target write grant, withdraw its endpoint, and drain
   relevant sessions.
2. Persist and verify that closure under current authority.
3. Establish a new configuration that explicitly authorizes the fenced
   incarnation's disposal or readmission, with fresh identity, history, and
   topology evidence.
4. Ask the provider to release only the exact fence generation and persist its
   result through the generic agent.
5. Reobserve all affected access paths before any later grant.

The provider must reject release from an old configuration, operation, or
subject identity. Runtime death, receipt expiry, or a changed service endpoint
cannot release containment implicitly.

For planned transitions, an independent network/process containment mode may
block client and administrative write paths while preserving only the
explicitly required Group Replication path. Its scope and completion must still
be exact and verifiable.

## Planned Switchover

### PoC controlled handoff

The PoC implements only a reachable-source handoff through the SF public
sequence. The initial MySQL capability advertises no catch-up-specific quorum,
so both waits use `All`:

1. Install the current/previous configuration with the target marked
   `must_catchup`.
2. Complete the first `All` catch-up wait while source writes remain granted.
3. Revoke source write status, close its proxy endpoint, and drain or terminate
   existing client sessions.
4. Freeze the coherent source executed-GTID set after closure.
5. Apply the swap epoch barrier and refreshed configuration.
6. Complete the second `All` wait at the final frozen boundary.
7. Demote the old Replicator and promote the target Replicator; the target role
   operation performs the pinned single-primary Group Replication transfer.
8. Complete the corresponding application role changes and collect a fresh
   coherent native view proving the requested primary and compatible history.
9. Stop and reap the exact old-source `mysqld`; verify its process session and
   UDS are gone.
10. Re-evaluate the exact public catch-up/progress postcondition. Inside that
    call, MySQL observes the containment-induced successor view and requires
    that only the stopped source is absent, the intended target remains
    primary, the other secondary remains online, quorum remains available, and
    both survivors contain the frozen boundary. The public three-member
    configuration remains unchanged; this is member unavailability, not a
    PC/CC membership transition.
11. Revalidate target authority, retained public completion, and source
    containment, then grant target write status and publish the proxy endpoint.

Failure at any step leaves writes closed. Restart during this sequence is not
resumed automatically in the PoC; the exact fixture is reset. The old source
may be restarted and rejoined only through a new explicit fixture operation.

### Later durable handoff

Planned switchover assumes a healthy quorum and reachable source and target.
It is a durable handoff between two exact Kuberic authority attempts, not one
callback that survives an authority change.

The **source-close attempt** runs under configuration `C_source`, which
authorizes the old primary:

1. **Authorize source closure** for exact source/target incarnations,
   operation ID, `HistoryContext`, and public swap attempt.
2. **Close source write access**, withdraw the writable endpoint, stop new
   client writes, and drain or terminate relevant sessions.
3. **Freeze a boundary** from a coherent post-drain source observation. MySQL
   retains the complete GTID set in the live operation and returns the
   qualified public LSN; the agent records only `HistoryContext`, LSN, source
   closure, and exact public operation identity. Prove no later client command
   was accepted.
4. **Drain/apply** until the exact target has executed the frozen boundary and
   remains compatible in the accepted lineage. Equality is not inferred from a
   scalar count.
5. **Contain the source** with an independently verified exact-incarnation
   client-write fence.
6. **Complete source handoff** by persisting an agent-owned record that binds
   `C_source`, source/target identities, the qualified
   `HistoryContext`/LSN boundary, source access closure, fence dependency, and
   the target-authorizing configuration proposed next. It contains no native
   view or structured GTID receipt.

The controller may then admit configuration `C_target`, which explicitly
authorizes the target's desired primary role. This is a new exact authority
attempt. Admission consumes and revalidates the source-handoff record, starts
with target access closed, and follows the SF public operation sequence. No
public completion from `C_source` is credited directly under `C_target`.

The **target-open attempt** runs under committed `C_target`:

1. **Authorize target role** for the exact target, source fence generation,
   qualified public boundary, and current public configuration/epoch.
2. **Transfer native primary role** using the pinned, validated single-primary
   Group Replication operation.
3. **Close the pre-view bundle** and collect a fresh post-transfer bundle. A
   changed native view is accepted only when the target is the requested
   primary, the exact source and allowed members have the recorded
   disposition, lineage remains compatible, and no unrelated membership or
   identity change occurred.
4. **Complete the exact public target role operation** only after MySQL has
   internally validated the pre/post views, identities, boundary, transfer
   attempt, and fresh target recovery/role evidence. The agent persists the
   exact public completion, not the private native observation.
5. **Publish target writes** only after revalidating committed `C_target`, the
   source fence dependency, qualified public boundary, target public
   completions, and source containment. Read publication is reconciled
   separately.

If Group Replication chooses or reports a different primary, the operation
stops with writes closed. An unrecorded authority, process-session, credential,
member-identity, native-view, or attempt change invalidates the relevant
attempt. The explicitly recorded `C_source` → `C_target` transition is accepted only
through its agent-owned public operation records. Native pre-view → post-view
validation remains inside the target role call. Failure after source closure
leaves writes closed. Restart reconstructs the two attempts independently and
uses their public replay dispositions; it never infers completion from desired
role.

## Unplanned Failover and Quorum Recovery

Everything in this section is deferred beyond the PoC. The PoC may observe a
native primary change, but it never converts that observation into a writable
Kuberic failover. Unexpected source loss, quorum loss, conflicting views, or an
unreachable old primary closes writes and requires fixture reset or manual
recovery.

### Quorum-confirmed history boundary

Automated failover needs a native history envelope, not an assumed “latest”
member. From one coherent accepted view, the adapter freezes a quorum of exact
surviving member sessions and collects their executed, received/retrieved,
purged, recovery, certification/commit, and lineage evidence under one
deadline. It records:

- `B_common`, the GTIDs proven executed by every participating quorum member;
- `B_seen`, the union of GTIDs proven executed or otherwise potentially
  acknowledged/durable by any participant under the pinned native profile; and
- any previously durable source/switchover boundary that must also be retained.

The required lossless boundary `B_required` includes `B_seen`, prior required
boundaries, and any received/certified transaction that the validated MySQL
commit profile says may have been acknowledged before failure. `B_common`
alone is never enough. Purged intervals require trusted lineage and
retained-history evidence; they are not treated as absent transactions.

Before this algorithm is enabled, implementation-stage validation must prove
for the exact MySQL patch, Group Replication consistency/durability settings,
and acknowledgement policy that all client-acknowledged transactions are
represented by the evidence available from the surviving quorum. If that
native invariant, the complete evidence envelope, or an old-source boundary
cannot be proven, lossless automated failover is unsupported and writes remain
closed unless an explicit, audited data-loss recovery is authorized.

Candidate histories are then ordered only by GTID containment:

1. Reject a candidate that does not contain `B_required`.
2. Reject candidates with unexplained GTIDs outside accepted group lineage.
3. Among the remaining candidates, prefer the unique candidate whose executed
   history is a strict superset of every other safe candidate.
4. If safe candidates have equal accepted history, use stable exact identity
   only as the final deterministic tie-breaker.
5. If candidates are incomparable, no unique maximal history exists, or the
   discarded-history consequence is unknown, keep writes closed. A manual
   data-loss authorization must name the GTID intervals it accepts losing and
   enters the separately gated force-recovery path.

### Eligibility and selection

Native Group Replication may elect or report a primary, but Kuberic does not
publish that member for writes until the following proposed contract passes:

- the observation bundle proves a current quorum in one coherent accepted view;
- the candidate is an exact authorized incarnation in the current
  configuration, eligible and not still recovering;
- its executed GTID set contains the derived `B_required` boundary;
- candidate-only history is explained by accepted group lineage, with no
  unexplained divergence;
- deterministic selection follows the containment ordering above, using stable
  exact identity only among equivalent accepted histories, never GTID text,
  count, or hash order;
- the candidate matches or is safely reconciled with Group Replication's
  single primary;
- the exact old primary has a current independently verified fence receipt; and
- fresh post-fence evidence still satisfies authority, quorum, history, and
  access preconditions.

The design prefers safety over availability. An ambiguous boundary, incomplete
view, incomparable history, absent privilege, unknown old incarnation, expired
fence, or disagreement between Kuberic intent and native primary closes writes.

### Old primary unreachable

Unreachability is not fencing. The controller may prepare a candidate and
collect quorum evidence, but it may not grant writes until the fence provider
contains the exact old process/host/direct-client path and supplies a verified
receipt. If the provider cannot distinguish a reused host, process, address, or
storage identity, automated failover stops for operator recovery.

### Quorum loss and conflicting views

Loss of native quorum closes writable publication and invalidates topology
mutation attempts. Quorum restoration requires a fresh coherent view and
history check; a prior primary role, cached scalar marker, or old access grant
does not reopen writes.

Conflicting views, split observations, or members with divergent histories are
quarantined from automated recovery. The controller must not merge evidence
across views to manufacture quorum.

Automatic force-membership recovery is prohibited. A break-glass force
operation, if later implemented, is manual and separately authorized. It must:

1. identify and independently fence every excluded incarnation;
2. record the exact surviving member set and histories;
3. document the accepted data-loss boundary;
4. create a new native/Kuberic recovery generation;
5. rebuild or reseed excluded members before readmission; and
6. keep client writes closed until the recovered topology is coherently
   reobserved.

Force recovery is outside the initial automated support claim.

## Restart Reconstruction

The PoC supports component replacement only when a surviving fixture host
retains the exact process and in-memory operation ownership context and proves
that no topology mutation or handoff was pending. It first verifies effective
client-access closure, then revalidates the exact owned processes, storage,
identities, view, and GTID state before reconciling Kuberic callbacks again.

If the lifecycle component and its ownership context are both lost, the PoC
cannot prove quiescence from MySQL state alone. It keeps writes closed and
requires exact fixture reset even when the resulting native state appears
healthy. A pending topology mutation, partially completed handoff, ambiguous
native effect, or unproven client-path closure also requires fixture reset; the
PoC never resumes the operation.

The resumable reconstruction contract below belongs to Stage 3 and later.

Runtime or host restart creates fresh process and observation sessions. The
replacement starts unassigned and access-closed. It receives no predecessor
role, PC/CC configuration, peer authorization, or access credit merely because
the storage and `server_uuid` are retained.

The ordered replacement-session protocol is:

1. Validate resource, storage, process, and application identity without
   applying desired access.
2. Verify effective client-path closure and retire the predecessor process
   session; safely contain any survivor that cannot be reattached under the
   qualified ownership policy.
3. Open the member-local Replicator unassigned and report that process-session
   renewal is required, including only qualified election progress under the
   retained `HistoryContext`.
4. Have the controller admit a newer configuration epoch bound to the new
   process session and issue fresh signed peer authorizations.
5. Close affected peer ingress and complete the newer `update_epoch` barrier on
   every surviving secondary before predecessor traffic can be accepted.
6. Replay the replacement role under the newer epoch. A replacement primary
   completes Replicator role, explicit epoch work, and application role in the
   normal order.
7. Install fresh PC/CC configuration and establish peers only from its new
   signed authorizations.
8. Re-evaluate required catch-up, build, or authorized data-loss work through
   public calls; MySQL performs fresh native validation internally.
9. Revalidate external fence, routing, durable revision, public completion, and
   the new process session before granting access.

A persisted “granted” access value remains desired state, not effective state.
No endpoint is restored merely because it existed before the restart.

If restart lands after a native effect but before public completion is
persisted, recovery follows the operation-specific replay disposition.
Convergent public calls may be replayed after fresh native observation;
ambiguous builds receive new sessions/authorization; ambiguous data-loss work
retires the incarnation. Partial native membership is never promoted to
durable authority implicitly.

## Security Boundaries

### Principals and least privilege

The PoC uses one fixture-scoped administrative MySQL account over each private
UDS. Its credential is read from a private file, never passed in process
arguments, logged, or persisted in operation evidence. This is a development
simplification, not the production privilege model.

Later secure and production profiles require separate principals and secret
references for:

- **observation**: read the exact server identity, Group Replication,
  transaction-history, recovery, and required capability metadata;
- **topology mutation**: start/stop/configure Group Replication and transfer
  native primary role;
- **provisioning**: initialize, clone, join, or perform distributed recovery;
- **client access**: application data access, separate from administration;
  and
- **fencing**: invoke and verify independent infrastructure containment.

The mutation principal must not silently fall back to observer credentials.
Capability checks are explicit. Permission denial is reported as denial, not as
absence of a group, member, transaction, or recovery record.

Secrets are loaded from protected files or an equivalent secret provider. They
must not appear in desired-state documents, generic operation records,
receipts, logs, status, canonical signatures, SQL text recorded for
diagnostics, process arguments, or connection strings.

### PoC transport and deferred TLS

TLS, CA management, certificate rotation, and certificate peer identity are
out of scope for the host-local PoC.

- The adapter connects to its local `mysqld` through a private UDS.
- Group Replication and distributed recovery use loopback TCP only.
- Client probes use loopback TCP and are not exposed outside the fixture host.
- Socket paths, directory ownership, permissions, connected `server_uuid`, and
  exact process identity provide the PoC transport binding.

The PoC must not expose these plaintext listeners beyond loopback or claim that
Kubernetes networking secures them. A later cross-host stage must choose and
validate a secure transport profile. Native MySQL TLS is the portable default;
a named CNI or service-mesh profile may replace or supplement it only after
proving encryption and peer identity for client, Group Replication,
distributed-recovery, and provisioning paths.

### Rotation during an operation

In the PoC, every authenticated session and observation is bound to the local
credential generation, UDS path, process session, and observed `server_uuid`.
Credential replacement or privilege loss invalidates uncommitted evidence and
requires a new local session.

Later secure profiles additionally bind sessions to CA/trust, certificate
peer-identity, and privilege generations. Expiry, rotation, revocation,
privilege loss, or peer-identity change during an observation or long-running
operation invalidates uncommitted evidence and prevents completion credit until
a fresh authenticated session revalidates the required native and authority
state.

Old sessions cannot extend old authority. Receipts record generation IDs and
verified peer/provider provenance, never secret material. Permission loss,
authentication failure, and native absence remain distinct outcomes. Safety
actions that can be proven complete may be persisted through generic effects,
but access stays closed until trust is restored and the full decision is
freshly evaluated.

## Staged Delivery

Each stage is additive. Passing a stage supports only the claims listed for that
stage.

| Stage | Entry prerequisites and deliverables | Pass evidence and condition | Supported claim | Explicit non-claims |
|---|---|---|---|---|
| 0. Design | No prior stage. Deliver this design and concise README with current/future claims separated. | Spec, plan, cross-artifact, implementation, and final reviews pass with no unresolved safety finding. | Intended contract is documented. | No executable MySQL support. |
| 1. Server-free core | Stage 0 passed. Add typed identities, GTID relations, views, observation decoding, and minimal fail-closed state machines without a server dependency. | Deterministic tests pass for malformed, stale, partial, divergent, and stale-authority cases. | Core identity/history decisions do not flatten GTIDs or accept stale work. | No process, SQL, topology mutation, TLS, or recovery automation. |
| 2. Host-local Kuberic PoC | Stage 1 passed; the SF-aligned Kuberic public values and operation sequencing are available; an exact MySQL patch and local metadata surface are pinned. Add one member-local public Replicator per replica, signed build/peer admission, qualified scalar GTID projection, an application-owned client proxy, fresh bootstrap/join, access reconciliation, and controlled switchover. | The fixture proves public role-before-configuration ordering, same-role epoch fencing, exact identity/view/GTID observation, signed build admission, `All`/`All` swap catch-up, proxy access closure, process cleanup, source stop/reap, and delayed target write publication. | Kuberic can manage a fresh three-member development group through only public SF-shaped interfaces and perform one controlled local handoff. | No specific-quorum optimization, general non-projectable GTID history, TLS, Clone/reseed, automated failover, independent fence provider, crash-resumable mutation, cross-host, or Kubernetes claim. |
| 3. Resumable lifecycle and repair | Stage 2 passed. Add generic durable effect records, ambiguous-effect recovery, Clone/reseed, replacement, cleanup recovery, and destructive approvals. | Restart and fault gates prove effects are recovered, retried only when idempotent, or left safely closed. | Host-local lifecycle survives interrupted provisioning and replacement without a MySQL-specific metadata store. | No automated writable failover or production security claim. |
| 4. Automated failover and independent fencing | Stage 3 passed; the pinned native commit invariant and an independent fence provider are validated. Add lossless-boundary derivation, continuing fence dependencies, unplanned failover, and quorum recovery. | Old-primary survival, direct-client, provider-loss, divergent-history, and quorum-loss gates pass with no premature write publication. | Controlled automated failover for the exact validated environment. | No portable cross-host or Kubernetes claim. |
| 5. Secure cross-host qualification | Stage 4 passed. Add separate principals, secure secret handling, native TLS or an explicitly qualified equivalent network profile, real fault domains, and a production-candidate fence backend. | Cross-host network, trust rotation, host loss, storage, direct-client, and fence-lifetime gates pass. | Only the named cross-host security and infrastructure profile that passed. | No generic CNI, mesh, cloud, or cross-region assumption. |
| 6. Kubernetes qualification | Stage 5 passed; named Kubernetes/provider versions, images, secrets, storage, routing, and platform fence integrations are fixed. | The Kubernetes gate matrix passes lifecycle, faults, storage reuse, routing, trust rotation, controller restart, old-primary survival, and exact fence/release scenarios. | Only the named Kubernetes, storage, network, and fence-provider matrix. | No generic Kubernetes or provider portability. |

Stages 0 and 1, the narrow Stage 2 observation slice, the single-process
restart-stateless host, and the bounded fresh three-member bootstrap/join
portion are delivered. Stage 2 public interface wiring, access reconciliation
and publication, controlled switchover, and every later row remain a delivery
contract, not a schedule or current feature list.

## Test Strategy

### Repository gates

Repository development requires the exact installed Oracle MySQL package. The
standard nextest workspace run starts isolated local fixtures and runs the
delivered observer, single-process lifecycle, and fresh three-member
bootstrap/join gates:

```bash
cargo fmt --all -- --check
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --workspace --profile ci
CARGO_BUILD_JOBS=1 cargo test --locked --offline --doc --workspace
CARGO_BUILD_JOBS=1 cargo clippy --locked --offline --workspace --all-targets -- -D warnings
```

Stage 1 covers exact identity binding and replacement, GTID parsing/
normalization and all four set relations, native-view validation, complete
observation outcome/freshness/coherence behavior, and stale authority/session
completion. Later server-free stages must additionally cover:

- purged-history recovery capability without treating it as executed history;
- public operation success/failure around the non-atomic persistence window;
- effect-completed/receipt-not-persisted restart reconciliation;
- expected join view transition validated inside public build completion,
  without a durable pre-view/post-view receipt;
- clone restart with a fresh process session, exact identity rebinding, and
  rejection of completion from the old session;
- rejection of unrelated member/view drift, unexplained native identity
  change, or an unrecorded second process restart;
- operation canonicalization, idempotent retry, destructive approval, donor
  change, target change, and stale completion;
- data-loss callback rejection before its support stage and stale evidence
  rejection after it is enabled;
- access state machines for PRIMARY-with-closed-access, stale native evidence,
  missing privilege, and invalid fence;
- planned/unplanned transition ordering and “no publish before fence”;
- local credential replacement and privilege loss during observation;
- secret redaction and receipt verification; and
- restart reconstruction with stale desired access and surviving processes.

### Mandatory host-local gates

Live host-local gates are part of the standard nextest workspace run. Missing
MySQL binaries, required plugin/profile support, ports, UDS paths, or privileges
fail with actionable prerequisite errors rather than silently skip. Later
stages apply the same rule to certificates, cross-host networking, and
fence-provider prerequisites.

The delivered bounded topology matrix includes:

- one-member negative observation cases: absence, permission denial, invalid
  local credentials, unexpected UDS/server identity, malformed metadata, and
  provenance;
- fresh three-member bootstrap, join, native recovery, and exact identity
  validation;
- closed SQL TCP and MySQL X access, exact per-member process/UDS cleanup, and
  refusal to create a private MySQL application store.

The remaining Stage 2 PoC matrix includes:

- one member-local public interface bundle per replica, with no private managed
  lifecycle, observation, access, or receipt capability;
- role-before-primary-configuration and same-role-secondary epoch ordering;
- exact `HistoryContext`-scoped scalar projection for the qualified single
  Group Replication UUID/contiguous-interval profile, with every other GTID
  shape rejected;
- signed build authorization, active peer-session authorization, invalid idle
  target progress, and build cancellation/drain before removal;
- Replicator-owned endpoint lifetime and proxy-owned client endpoint lifetime;
- planned switchover with `must_catchup`, `All`/`All` waits, source drain,
  frozen GTID boundary, containment, and delayed publication;
- source process stop/reap and UDS disappearance before target writes open;
- refusal to publish writes after unexpected source loss, quorum loss,
  conflicting/stale views, divergent histories, or ambiguous mutation; and
- cleanup after normal completion, setup failure, and cancellation.

Later host-local stages add Clone/reseed, ambiguous restart recovery,
replacement, surviving-`mysqld` faults, independent fencing, automated
failover, trust rotation, and provider-loss scenarios.

Every gate checks its product/version/profile, fixture identities, native view,
and expected outcomes directly. CI retains the command results. Live success
on one host does not establish production availability.

### Later Kubernetes and fault gates

Kubernetes tests begin only after process, fencing, routing, and controller
contracts pass below the cluster layer. They cover Pod/host death, API
partition, network partition, persistent-volume reuse, Secret rotation,
readiness versus direct-client access, controller restart, old-primary
survival, exact infrastructure fencing, and replacement. Explicit invocation
must fail on missing cluster/provider prerequisites.

No Kubernetes lifecycle or fault-tolerance claim is supported until a named
version, storage class, network model, and fence provider pass the full suite.

### Gate contracts

These gate groups make prerequisites, pass conditions, claims, and non-claims
explicit. Individual scenarios named above inherit their group's contract and
must define their exact inputs and pass conditions.

| Gate group | Prerequisites | Pass evidence and condition | Supported claim | Explicit non-claim |
|---|---|---|---|---|
| SF core | Deterministic fixtures and the pinned Rust toolchain only | The Stage 1 Cargo gate passes identity/view replacement and stale-session cases; GTID relations never use scalar order; malformed, absent, denied, partial, stale, future-dated, and incoherent evidence produces explicit outcomes | Pure identity, GTID, view, observation, and stale-authority logic matches the PoC contract | No MySQL process, query, wall-clock, SQL, or transport behavior validated |
| PoC observation | One pinned local `mysqld`, private UDS, fixture credential | Native identity/view/GTID evidence matches the exact process; permission denial, bad credentials, and wrong UDS/server identity remain distinct | Local adapter-to-MySQL observation works without TLS | No topology mutation or writable transition claim |
| Single-process host | Exact Oracle MySQL 8.4.11, `aa-exec`, fresh distinct roots | One child is initialized, launched, attested, observed as fail-closed inactive membership, stopped, reaped, and its UDS proven absent; scratch is removed and data retained | One restart-stateless local process generation with private-UDS observation | No topology, callbacks, access publication, survivor adoption, or restart continuation |
| Fresh topology bootstrap/join | Exactly three fresh owned instances, loopback Group Replication, retained in-memory attempt authority | One designated bootstrap and two sequential joins receive exact fresh identity/view/GTID credit; access stays closed; every process and scratch root is contained | Bounded fresh three-member bootstrap/join for the exact qualified local profile | No callbacks, access publication, switchover, failover, repair, survivor adoption, or restart continuation |
| PoC lifecycle and switchover | Three fresh owned instances, loopback Group Replication, SF-aligned public Kuberic values, one member-local Replicator and client proxy per host | Public conformance ordering passes; signed build/peer admission and qualified scalar progress bind exact sessions; bootstrap/join completes; source access closes and its process is stopped/reaped before target writes open; unexpected loss leaves writes closed | Controlled host-local public-only Kuberic lifecycle and one planned handoff | No specific-quorum optimization, arbitrary GTID-set progress, Clone/reseed, restart recovery, automated failover, external fence, cross-host, or production claim |
| Advanced repair and restart | Stage 3 implementation, generic durable effect records, destructive approvals | Clone/reseed/replacement and effect-before-receipt faults resume or fail closed without touching foreign state | Resumable host-local repair for the exercised profile without an application-owned metadata store | No automated writable failover |
| Advanced failover and fencing | Validated native commit invariant and independent fence provider | Only a candidate containing the required history publishes; old-primary survival, provider loss, quorum loss, and divergent histories remain fail-closed | Automated failover for the exact qualified environment | No portable infrastructure or Kubernetes claim |
| Secure cross-host and Kubernetes | Named network, TLS or qualified equivalent, separate principals, storage, routing, and fence integration | The environment-specific lifecycle, partition, trust-rotation, direct-client, restart, and replacement matrix passes | Only the exact named deployment matrix | No generic CNI, mesh, cloud, or Kubernetes portability |

## Limitations and Unsupported Modes

Unless a later validated stage says otherwise, this design does not support:

- production MySQL process ownership, writable transition, or Kuberic callback
  behavior beyond the delivered read-only observer, bounded fresh process
  ownership, and closed-access three-member bootstrap/join;
- production use or service-level objectives;
- MySQL releases outside a pinned Oracle MySQL 8.4 LTS patch;
- MariaDB, Percona Server, cloud-vendor forks, or unqualified managed services;
- non-Linux process ownership;
- multi-primary Group Replication;
- asynchronous source/replica topology as a failover substitute;
- automatic MySQL InnoDB Cluster or MySQL Router ownership;
- automatic force-membership or data-loss recovery;
- a topology other than the staged single-primary profile;
- cross-region latency, partitions, or disaster-recovery assumptions;
- cross-host safety before an exact independent fence provider is qualified;
- TLS, certificate, CA, or network peer-identity guarantees in the PoC;
- automated unplanned failover, Clone/reseed, replacement, or crash-resumable
  native mutation in the PoC;
- shared data roots, adopted foreign processes, or identity reuse;
- flattening GTID/view evidence into scalar election progress;
- Kubernetes lifecycle, routing, storage, or fault-tolerance support before its
  named qualification stage; or
- a guarantee that MySQL-native assumptions in this document are correct for an
  unpinned release.

Ambiguous authority, incomplete evidence, unsupported profile, missing
privilege, authentication failure, or missing fence proof closes access. This
may reduce availability and is intentional.

## Reference Comparisons

### Portable lessons

This design takes the following portable contracts from the reference
repositories:

- exact Kuberic incarnation, process-session, configuration, and attempt
  binding;
- controller intent separated from durable runtime effects and database-native
  proof;
- application-owned custom replication with Kuberic-owned lifecycle
  choreography;
- independent read/write partition status rather than role-derived access;
- complete native validation behind narrow public completion and qualified
  `HistoryContext`-scoped scalar progress;
- generic effect records, stale-result rejection, restart reobservation, and
  conservative access reconstruction;
- independently verifiable fencing before new write publication; and
- deterministic core contracts retained alongside mandatory live repository
  gates.

The SF-aligned public contract and transition plan are evidenced in:

- `../kuberic/docs/features/kuberic/service-fabric-api-semantics.md`;
- `../kuberic/docs/features/kuberic/service-fabric-alignment.md`;
- `../kuberic/docs/features/kuberic/stateless-default-replicator.md`;
- `../kuberic/docs/features/kuberic/replicator-boundary.md`; and
- `../kuberic/kuberic-runtime/src/replicator/mod.rs`.

These paths were researched at local Kuberic commit
`356e96f33efbfccc8761f28c2bd97e1214a7540e`.

### Database-specific mechanisms not copied

| Reference | Useful lesson | Database-specific mechanism not treated as MySQL |
|---|---|---|
| PostgreSQL | Private host owns exact effects/access while the database owns native lineage, build, replay, and promotion proof | WAL/LSN and retained-WAL scalar meanings, timelines/system identifiers, `pg_basebackup`, `pg_rewind`, receiver drainage, slots, synchronous policy, HBA/process fence, and its documented failover ordering |
| SQL Server | Separate configuration progress from data progress; type coherent observations, operations, fence receipts, privileges/TLS, and staged tests | Availability-group configuration sequence, hardened/redone/committed numeric positions, AG/recovery-fork GUIDs, `CLUSTER_TYPE = EXTERNAL`, external write lease, TDS and AG endpoint trust, and automatic seeding |

PostgreSQL evidence:

- `../kuberic/docs/features/postgres/design.md:12-225,292-351`;
- `../kuberic/examples/postgres/src/service.rs:106-193,234-278`; and
- `../kuberic/examples/postgres/src/adapter.rs:1984-2038`.

SQL Server evidence:

- `../kuberic-mssql/docs/design.md:65-305,354-414`;
- `../kuberic-mssql/docs/kuberic-progress.md:16-144,282-302`;
- `../kuberic-mssql/crates/kuberic-mssql/src/kuberic.rs:489-635,964-1065`;
  and
- `../kuberic-mssql/crates/kuberic-mssql/src/operation.rs:21-95,276-313`.

These paths were researched at local SQL Server commit
`116925a916814f12edb995d819aaff19a8ee8cd5`.

PostgreSQL's scalar WAL progress and SQL Server's configuration sequence have
different meanings from each other and from a MySQL GTID set. Their process
fences, leases, and build protocols are likewise not portable implementations.
Only the ownership, evidence-separation, exact-identity, and fail-closed
principles are reused.

### MySQL-native validation sources

Implementation must validate the proposed native contract against the pinned
Oracle MySQL 8.4 documentation for GTIDs, Group Replication, distributed
recovery, Clone, single-primary operations, security, and read-only controls,
and record exact versioned references with the implemented decoder and tests.
This design does not use the PostgreSQL or SQL Server documents as evidence of
MySQL behavior.
