# MySQL/Kuberic High-Level Design

This document defines the proposed safety and lifecycle contract for integrating
Oracle MySQL with Kuberic. It is an implementation design, not an implementation
claim.

## Status and Target Profile

### Current repository status

This repository delivers Stage 0 documentation, the Stage 1 deterministic
`kuberic-mysql-core` library, one narrow native-observation slice, and the
first bounded Stage 2 process-host chunk. The publish-disabled
`kuberic-mysql-adapter` observes exactly one caller-selected Oracle MySQL
Community Server 8.4.11 through a private Unix-domain socket. The
publish-disabled `kuberic-mysql-service` owns one fresh, restart-stateless
process generation. This does not complete the three-process Stage 2
lifecycle PoC.

Future lifecycle work follows the
[restart-stateless metadata design](stateless-metadata.md): the MySQL adapter
and lifecycle component own no private durable recovery store. Durable
operation records are application-neutral Kuberic controller or generic-agent
records, not a MySQL-specific metadata file or database.

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
`repo.mysql.com`'s `mysql-8.4-lts` component. The opt-in runner verifies dpkg
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

### Delivered bounded process-host chunk

`kuberic-mysql-service` validates exact executable and launcher files, absent
non-overlapping data and scratch roots, and positive operation deadlines. It
creates owner-only roots, renders runtime files under scratch, runs
`--initialize-insecure`, retains and attests the sole child and PID, waits for
the private UDS, and delegates a socket-matched request to the existing
adapter. Stop signals only the retained child, escalates within a deadline,
reaps it, proves UDS absence, removes scratch, and retains the data root.

The manager writes no application metadata, operation journal, receipt
database, state JSON, or adoption record. All lifecycle and operation context
is in memory. If that context is lost, the supported response is fixture reset;
the crate neither discovers nor adopts a survivor. TCP and MySQL X are
disabled, Group Replication remains stopped, and no client address is
published. The live gate accepts the adapter's fail-closed inactive-membership
`Absent` result as the expected pre-bootstrap observation.

The delivered slices make no claim for another MySQL patch or fork,
three-member lifecycle, topology mutation, failover, fencing, Clone/reseed,
TLS, cross-host operation, containers, Kubernetes, routing, restart
continuation, availability, durability, performance, or production security.

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
coverage. They do not demonstrate production fault-domain isolation. The first
implemented stage that starts MySQL must refuse products, versions, platforms,
or topology modes outside its pinned profile.

### Proof-of-concept boundary

The delivered Stage 1 core remains intentionally server-free. It proves the
pure identity, GTID-set, view, observation, and stale-authority contracts
without starting or contacting MySQL. The separately delivered adapter
contacts one exact server, and the bounded service chunk owns one fresh local
process generation. Neither completes the three-member Stage 2 PoC or claims a
production lifecycle.

The first executable **server-integration** milestone is the Stage 2 PoC. Its
purpose is to prove that Kuberic can own three local `mysqld` processes, observe
Group Replication and GTID state, drive the custom-replicator callbacks, and
complete a controlled primary handoff.

The PoC includes:

- three independently initialized `mysqld` processes on one Linux host;
- one private Unix-domain socket per member for adapter observation and
  administration;
- loopback TCP for Group Replication and distributed recovery;
- a single-primary group created from fresh fixture-owned data roots;
- exact process, storage, `server_uuid`, member, view, and GTID binding;
- Kuberic role/configuration callbacks with client access initially closed;
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
release during implementation. Where the current Kuberic callback model cannot
carry required evidence safely, this design says so rather than treating a
planned API as present.

## Authority and Ownership

Safety depends on keeping five independent kinds of authority distinct.

| Concern | Single owner | Owned decisions and effects | Handoff evidence |
|---|---|---|---|
| Desired topology and lifecycle | Kuberic controller | Configuration intent, replica identities, role intent, operation ordering, and retry policy | Exact configuration/epoch and callback context |
| Durable effects | Generic Kuberic agent and runtime lifecycle host | Process/storage ownership, generic operation records, callback fencing, cancellation, access reconciliation, and restart reconstruction | Exact process session, generic operation record, and effect receipt |
| Native facts and replication transport | MySQL engine / Group Replication | Authoritative group membership, native role/state, GTID and recovery facts, replication transport, and execution of accepted native operations | Native state exposed by the pinned MySQL profile |
| Native integration and proof | MySQL adapter | Authorized SQL/native mutation requests, coherent observation collection and interpretation, typed evidence production, and submission through private runtime protocols | Exact request context plus fresh native observation bundle or operation receipt |
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
- reconciles Kuberic read/write status with native evidence; and
- reconstructs effects and access from generic durable state plus fresh MySQL
  observation after restart.

The MySQL adapter:

- provides the custom stateful-service and replication interfaces;
- maintains typed MySQL identity, GTID, Group Replication, recovery, and
  observation evidence inside the application boundary;
- requests PoC bootstrap, join, and topology operations only under an exact
  runtime authorization; later stages add clone, repair, and recovery
  operations. It interprets fresh engine facts and submits typed evidence
  without opening or interpreting the generic agent store; and
- never grants client access from MySQL role alone.

The external fence provider is a post-PoC boundary outside both the managed
`mysqld` and the runtime process that supervises it. It must still act and
provide proof if either process is unresponsive. The PoC does not simulate that
guarantee: it refuses automated write failover when the source runtime or
process is unreachable. A process-local boolean, Kubernetes readiness result,
object deletion request, in-memory role, or database variable is not a
production fence receipt.

### Custom-authority admission

The existing Kuberic custom-replicator boundary invokes the stateful
current/joint configuration callback before candidate host authority is
durably persisted. The proposed MySQL integration must preserve the following
ordering:

1. Close Kuberic read and write projections and revoke effective client access.
2. Bind an admission attempt to the candidate configuration, epoch, exact
   replica/process sessions, and the currently observed native view.
3. Invoke the stateful native admission callback.
4. Revalidate the callback result and persist candidate host authority only if
   the attempt is still current.
5. Reobserve native state and publish access later, under a separate explicit
   authorization.

The callback is not a dry run. There is no atomic transaction spanning native
MySQL effects and the runtime authority store, and no automatic rollback.
Therefore, a failed, dropped, stale, or crash-interrupted callback that entered
native code must abort or contain its exact host session. Restart recovery must
reobserve native effects before retrying or accepting authority. Group
membership created by a partial callback is not committed Kuberic authority.

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

### Existing interface mapping

The proposed service uses the existing custom-replication shape:

| Kuberic surface | Proposed MySQL responsibility |
|---|---|
| `StatefulServiceReplica.open` | Validate resource/partition identity, load generic agent authority, open the per-incarnation process host, retain the partition, select the factory, create one coherent custom interface bundle, and return that bundle's control `Replicator` |
| `ReplicatorFactory.create_replicator` | Construct the coherent `ReplicatorInterfaces` bundle with the control replicator and optional primary interface; omit state/operation-copy interfaces because MySQL owns native transport and state transfer |
| `change_role` | Reconcile desired role with fresh native evidence; return no client address until independent access reconciliation grants it |
| `close` / `abort` | Close access first, cancel exact attempts, and stop/reap or quarantine the owned process according to the generic operation record |
| `Replicator.open` | Return the exact application-owned Group Replication address only after identity and transport configuration validation |
| `change_role` / `update_epoch` | Record new authority, revoke stale work, and schedule native reconciliation without treating the callback as promotion proof |
| `current_progress` | Return only the compatibility marker described below; submit GTID/view/recovery proof through the typed private runtime protocol |
| `catch_up_capability` | Return only a conservative compatibility marker; never claim recoverability without retained-history and donor proof |
| `PrimaryReplicator` current/joint configuration | Perform the stateful, access-closed native admission contract and validate exact member descriptions |
| catch-up quorum | Freeze and verify a native boundary under exact sessions; do not use scalar callback values as sole proof |
| build / removal | In the PoC, support only fresh bootstrap/join and reject destructive repair explicitly. Later stages add generic clone/reseed/removal effects with typed native receipts. |
| data-loss handling | Evaluate exact authority, view, quorum, GTID compatibility, and fence evidence; reject unsupported stages explicitly |

The custom bundle does not need Kuberic operation/copy replication when MySQL
owns transport and state transfer. That does not remove Kuberic lifecycle
authority or the requirement to implement the control and primary callbacks
accurately.

### Data-loss callback

The data-loss callback is not permission to discard transactions or force a
view. It accepts only an exact authority/configuration context and a coherent
observation bundle. Its result records:

- the exact candidate and old-primary incarnations;
- native view and member set;
- GTID histories compared and the required frozen boundary;
- quorum and retained-history conclusions;
- exact old-primary fence receipt;
- decision and support-stage capability; and
- observation and decision deadlines.

Before the controlled failover stage is enabled, the callback returns an
explicit unsupported result. Once enabled, it may acknowledge only the
already-defined failover contract in this document. A stale completion cannot
earn data-loss, build, role, or access credit.

### Additive runtime capabilities

The current public progress fields are scalar `i64` values. They cannot encode
a GTID set, view identity, multi-member observation, or fence receipt. A safe
full integration may require the following additive, compatibility-preserving
capabilities. They are not prerequisites for the PoC, which keeps structured
evidence only in attempt-local memory and uses conservative callback results.
Loss of that memory forces fixture reset; it does not trigger reconstruction or
continuation:

1. **Structured application progress** carrying typed lineage/history,
   configuration/view, recovery, and retained-history evidence.
2. **Coherent observation receipts** binding all samples to exact sessions,
   native view, deadlines, query provenance, and privilege generation.
3. **External fence receipts** with provider verification, exact subject,
   input signature, validity, and revocation semantics.
4. **Resumable operation status** with exact operation identity, stages,
   native/provider receipts, cancellation, and recovered-completion outcomes.

Until such APIs exist, safety-relevant evidence is either reobserved from
MySQL, supplied by current generic Kuberic authority, or retained only for the
current in-memory PoC attempt. No MySQL-specific durable fallback is permitted.
The scalar `current_progress`/`catch_up_capability` compatibility values may
carry only a runtime-assigned Kuberic configuration/admission generation. They
must never be derived from serialized GTIDs, transaction counts, view text,
hash ordering, or member count, and must never be the sole build, catch-up,
election, promotion, or access proof. A stage that requires durable structured
evidence before the generic APIs exist must return explicit unsupported rather
than persist private state or fabricate progress.

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

All workflows use a durable operation identity:

```text
(operation_kind, resource, authority, target_incarnation, source_or_donor,
 native_view, attempt, canonical_input_signature)
```

Each generic operation record contains authorization, current stage,
non-secret inputs, exact process/storage/native bindings, external effects
entered, native/provider observations, completion receipt, cancellation, and
last reconciliation time. Retries with the same canonical identity are
idempotent. A changed authority, target, donor, or input creates a new attempt;
the old result is rejected. An unrelated view or process-session change does
the same. A workflow may, however, pre-authorize a specific native view or
process-session transition that is an expected effect of that operation. Such
a transition uses the durable handoff rules below; it never makes an old
observation or callback valid in the new context.

### Durable workflow stages and expected handoffs

The generic operation record separates a logical operation from the fresh
observation or process session used to reconcile each stage. Every stage
transition persists its input binding and evidence before the next external
effect. Expected native changes are accepted only through an explicit
pre-state/post-state handoff:

1. The pre-state record names the exact operation, authority, incarnation,
   process session, native view, expected effect, and allowed post-state shape.
2. After the effect, the old bundle is closed and cannot prove the new state.
3. A fresh reconciliation session observes the post-state independently.
4. The runtime verifies that the post-state is a permitted consequence of the
   recorded effect and that authority, target, donor, and canonical inputs did
   not change.
5. The generic agent persists a handoff receipt containing both bindings, then
   advances the stage. Delayed callbacks from the pre-state remain stale.

The runtime never merges fields across the pre- and post-state views. The
handoff receipt proves continuity of the authorized operation; the fresh
post-state bundle proves current native eligibility.

| Workflow | Durable stages | Expected handoff | Completion receipt | Restart action |
|---|---|---|---|---|
| Join / distributed recovery | `Authorized` → `JoinEntered` → `MemberObserved` → `Recovering` → `BoundaryApplied` → `Complete` | `JoinEntered` pre-authorizes a view transition that adds the exact target. `MemberObserved` requires a fresh accepted view containing that target and allowed predecessor members. | Target identity, pre/post views, recovery completion, required and executed GTID boundaries, authority, and attempt | Reobserve membership and recovery. Recover a reached stage, reissue only an idempotent join under the same binding, or fail closed. |
| Clone | `Authorized` → `TargetFenced` → `DonorValidated` → `CloneEntered` → `TargetRestarted` → `IdentityRebound` → `Joined` → `BoundaryApplied` → `Complete` | `CloneEntered` pre-authorizes one target restart. `IdentityRebound` creates a fresh process session and verifies the same owned root, expected clone result, target incarnation, and permitted native identity before join reconciliation. | Donor/target, clone result, old/new process sessions, storage/native binding, join view, required and executed GTIDs, authority, and attempt | Query clone/provider and process state before replay. Recover a completed clone or restart, then continue from a fresh binding; never credit the old session. |
| Reseed / rebuild | `Authorized` → `TargetFenced` → `OwnedRootCleared` → `Provisioning` → `TargetRestarted` → `IdentityRebound` → `Joined` → `BoundaryApplied` → `Complete` | The authorization fixes the replacement storage/native expectations and permits only the recorded clear, restart, and join transitions. | Destructive approval, fence, old/new storage and process bindings, donor, views, resulting identity/history, authority, and attempt | Revalidate the fence and ownership marker, discover whether clear/provision/restart completed, and continue only from proven exact state. |
| Replacement | `Authorized` → `OldIncarnationFenced` → `NewIdentityAllocated` → `Provisioned` → `Joined` → `Complete` | The operation fixes both old and new incarnations. The new identity is not a mutation of the old binding and receives fresh process, storage, native, and view evidence. | Old fence/removal evidence plus the new incarnation's allocation, native identity, accepted view/history, authority, and attempt | Keep the old incarnation fenced; reconcile the new incarnation independently. Never transfer old progress or receipts. |
| Cleanup | `Authorized` → `TargetContained` → `OwnershipRevalidated` → `ResourcesRemoved` → `Complete` | No identity or view transition grants broader deletion rights. Each removed resource must match the authorization recorded before removal. | Exact removed resources, pre-removal ownership/process/native evidence, containment receipt, post-removal absence, authority, and attempt | Reobserve every operation-recorded resource. Record already-absent exact resources, continue exact idempotent removal, or stop on foreign/reused state. |

An expected handoff is narrow. A join view that drops an unapproved member, a
clone restart into an unexplained `server_uuid`, a second restart, a different
donor, or any authority change is unrelated drift and creates a new operation
attempt or a fail-closed recovery decision.

### Initial bootstrap

Bootstrap is permitted only for a new resource with no accepted native history.

1. Allocate three fresh Kuberic incarnations, storage roots, identities,
   addresses, ports, credentials, and generic operation records.
2. Initialize each data root under its exact ownership marker.
3. Start only the designated first member under closed client access.
4. Authorize the MySQL group-bootstrap action for that exact member and attempt.
   The bootstrap flag/action must be transient and removed immediately after a
   coherent native group is observed.
5. Join each remaining member one at a time using distributed recovery or the
   selected provisioning path.
6. Reobserve all exact members in one accepted view, verify identities and
   history, persist the bootstrap receipt, and keep writes closed.
7. Publish access only through the independent access reconciler after Kuberic
   authority and native eligibility converge.

No second member may independently bootstrap the same group. Discovery of an
existing unknown group, data, `server_uuid`, or membership is a foreign-state
error, not a reason to adopt it automatically.

### Join and distributed recovery

A join attempt binds target, donor/source set, expected group, native view, and
credentials. The target remains client-fenced. Completion requires the exact
target to appear with the expected native identity in a coherent accepted view,
reach the required online/recovery-complete state, and execute the required
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
intended group, complete recovery, and apply the frozen GTID boundary. The
generic agent then persists a receipt binding donor, target, authority, view,
attempt, and resulting history.

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

Restart may occur after an effect succeeds but before its completion receipt is
durable. Every workflow therefore has a **reobserve before reissue** branch:

1. Reconstruct the exact pending attempt and authority from the generic agent
   operation record.
2. Keep client access closed.
3. Query the native server or provider for exact target, donor, view, input
   signature, and result evidence.
4. If completion is proven, persist a recovered receipt without replaying the
   effect.
5. If non-completion is proven and the effect is idempotent under the same
   inputs, reissue it.
6. If completion, identity, authority, or safety cannot be proven, fail closed
   and require a new explicitly authorized recovery action.

This rule applies to bootstrap, join, clone, reseed, topology mutation, fencing,
process termination, and cleanup.

## Client Access and Fencing

### Access is a reconciled effect

The effective read and write service addresses are derived from both:

1. Kuberic partition read/write access status for the current access
   generation; and
2. a fresh, coherent native observation proving the exact incarnation is
   eligible for that access.

Role is necessary but not sufficient. A native `PRIMARY` with closed Kuberic
write status publishes no writable endpoint. A Kuberic primary intent with a
member still recovering, stale, divergent, outside the accepted view, or
unfenced relative to an old primary also publishes no writable endpoint.

Access transitions are generic Kuberic effects. Closure is acknowledged only
after existing publication is withdrawn, ordinary and administrative client
paths are contained as required, relevant sessions are drained or terminated,
and the exact result is reobserved. Opening is acknowledged only after fresh
authority, identity, native eligibility, fence, and routing evidence is
validated immediately before publication.

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

The PoC implements only a reachable-source handoff:

1. Bind the exact source, target, configuration, native view, and operation
   attempt while all three members are healthy.
2. Close source write access and withdraw its client endpoint.
3. Freeze the source executed GTID set after client sessions are drained.
4. Wait until the exact target has executed that boundary.
5. Invoke the pinned single-primary Group Replication transfer while source and
   target are still reachable.
6. Collect a fresh coherent view proving the target is native primary and the
   histories remain compatible.
7. Stop and reap the exact old-source `mysqld`; verify its process session and
   UDS are gone.
8. Revalidate Kuberic target authority and the fresh native view, then publish
   the target write endpoint.

Failure at any step leaves writes closed. Restart during this sequence is not
resumed automatically in the PoC; the exact fixture is reset. The old source
may be restarted and rejoined only through a new explicit fixture operation.

### Later durable handoff

Planned switchover assumes a healthy quorum and reachable source and target.
It is a durable handoff between two exact Kuberic authority attempts, not one
callback that survives an authority change.

The **source-close attempt** runs under configuration `C_source`, which
authorizes the old primary:

1. **Authorize source closure** for exact source/target incarnations, native
   pre-view, operation ID, and attempt.
2. **Close source write access**, withdraw the writable endpoint, stop new
   client writes, and drain or terminate relevant sessions.
3. **Freeze a boundary** from a coherent post-drain source observation. Record
   the executed GTID set and native pre-view and prove no later client
   transaction was accepted.
4. **Drain/apply** until the exact target has executed the frozen boundary and
   remains compatible in the accepted lineage. Equality is not inferred from a
   scalar count.
5. **Contain the source** with an independently verified exact-incarnation
   client-write fence.
6. **Complete source handoff** by persisting a receipt that binds
   `C_source`, source/target identities, frozen GTID boundary, native pre-view,
   source access closure, fence dependency, and the target-authorizing
   configuration proposed next.

The controller may then admit configuration `C_target`, which explicitly
authorizes the target's desired primary role. This is a new exact authority
attempt. Admission consumes and revalidates the source-handoff receipt, starts
with target access closed, and follows the non-atomic custom-authority contract.
No callback or receipt from `C_source` is credited directly under `C_target`.

The **target-open attempt** runs under committed `C_target`:

1. **Authorize native transfer** for the exact target, current pre-view, source
   fence generation, frozen boundary, and allowed post-state shape.
2. **Transfer native primary role** using the pinned, validated single-primary
   Group Replication operation.
3. **Close the pre-view bundle** and collect a fresh post-transfer bundle. A
   changed native view is accepted only when the target is the requested
   primary, the exact source and allowed members have the recorded
   disposition, lineage remains compatible, and no unrelated membership or
   identity change occurred.
4. **Persist the native handoff receipt** with pre/post views, exact authority,
   identities, boundary, transfer attempt, and fresh target recovery/role
   evidence.
5. **Publish target writes** only after revalidating committed `C_target`, the
   source fence dependency, frozen GTID boundary, target eligibility, and all
   handoff receipts. Read publication is reconciled separately.

If Group Replication chooses or reports a different primary, the operation
stops with writes closed. An unrecorded authority, process-session, credential,
member-identity, native-view, or attempt change invalidates the relevant
attempt. The explicitly recorded `C_source` → `C_target` and native pre-view →
post-view transitions are accepted only through their handoff receipts.
Failure after source closure leaves writes closed. Restart reconstructs the two
attempts independently and runs ambiguous-effect reconciliation; it never
infers completion from desired role.

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
replacement receives no access credit until effective client-path closure is
verified; a surviving `mysqld` is potentially serving until then.
Reconstruction joins four durable evidence sets:

- current Kuberic authority, configuration, role intent, and access generation;
- generic identity, storage, process, allocation, and operation records;
- fresh MySQL identity, Group Replication, GTID, recovery, and access-control
  observations; and
- still-verifiable external fence and routing receipts.

The reconciler:

1. validates repository resource identity and loads generic operation records
   without applying their desired access;
2. discovers only agent-recorded process/storage candidates and rejects
   foreign state;
3. verifies effective client-path closure, then assigns fresh process sessions
   or safely contains unadoptable survivors;
4. reobserves pending external effects before reissuing them;
5. rejects old callback completions and scalar progress snapshots;
6. reconciles native membership and topology with committed Kuberic authority;
7. revalidates fence, credential, trust, and routing generations; and
8. performs a new access decision from fresh evidence.

A persisted “granted” access value remains desired state, not effective state.
No endpoint is restored merely because it existed before the restart.

If restart lands between native configuration admission and Kuberic authority
persistence, the host contains the entered session, compares the exact native
effect with the candidate generic operation record, and either records a proven
recovered effect under still-current authority or requires a new admission. It
never promotes partial membership to durable authority implicitly.

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
| 2. Host-local Kuberic PoC | Stage 1 passed; an exact MySQL patch and local metadata surface are pinned. Add UDS observation, three owned local processes, fresh bootstrap/join, custom Kuberic interfaces, access reconciliation, and controlled switchover. | The fixture proves exact identity/view/GTID observation, process cleanup, Kuberic callback wiring, source stop/reap, and delayed target write publication. | Kuberic can manage a fresh three-member development group and perform one controlled local handoff. | No TLS, Clone/reseed, automated failover, independent fence provider, crash-resumable mutation, cross-host, or Kubernetes claim. |
| 3. Resumable lifecycle and repair | Stage 2 passed. Add generic durable effect records, ambiguous-effect recovery, Clone/reseed, replacement, cleanup recovery, and destructive approvals. | Restart and fault gates prove effects are recovered, retried only when idempotent, or left safely closed. | Host-local lifecycle survives interrupted provisioning and replacement without a MySQL-specific metadata store. | No automated writable failover or production security claim. |
| 4. Automated failover and independent fencing | Stage 3 passed; the pinned native commit invariant and an independent fence provider are validated. Add lossless-boundary derivation, continuing fence dependencies, unplanned failover, and quorum recovery. | Old-primary survival, direct-client, provider-loss, divergent-history, and quorum-loss gates pass with no premature write publication. | Controlled automated failover for the exact validated environment. | No portable cross-host or Kubernetes claim. |
| 5. Secure cross-host qualification | Stage 4 passed. Add separate principals, secure secret handling, native TLS or an explicitly qualified equivalent network profile, real fault domains, and a production-candidate fence backend. | Cross-host network, trust rotation, host loss, storage, direct-client, and fence-lifetime gates pass. | Only the named cross-host security and infrastructure profile that passed. | No generic CNI, mesh, cloud, or cross-region assumption. |
| 6. Kubernetes qualification | Stage 5 passed; named Kubernetes/provider versions, images, secrets, storage, routing, and platform fence integrations are fixed. | The Kubernetes gate matrix passes lifecycle, faults, storage reuse, routing, trust rotation, controller restart, old-primary survival, and exact fence/release scenarios. | Only the named Kubernetes, storage, network, and fence-provider matrix. | No generic Kubernetes or provider portability. |

Stages 0 and 1, the narrow Stage 2 observation slice, and the first
single-process restart-stateless host chunk are delivered. The full
three-member Stage 2 lifecycle and later rows remain a delivery contract, not a
schedule or current feature list.

## Test Strategy

### Ordinary server-free gates

Ordinary tests install no database, start no process or container, and require
no socket, network, or cluster. The delivered Stage 1 gate runs with:

```bash
cargo fmt --all -- --check
CARGO_BUILD_JOBS=1 cargo test --locked --offline --workspace --all-features -- --test-threads=1
CARGO_BUILD_JOBS=1 cargo clippy --locked --offline --workspace --all-targets --all-features -- -D warnings
```

Stage 1 covers exact identity binding and replacement, GTID parsing/
normalization and all four set relations, native-view validation, complete
observation outcome/freshness/coherence behavior, and stale authority/session
completion. Later server-free stages must additionally cover:

- purged-history recovery capability without treating it as executed history;
- authority callback success/failure around the non-atomic persistence window;
- effect-completed/receipt-not-persisted restart reconciliation;
- expected join view transition with a fresh post-view bundle and durable
  pre-view/post-view handoff;
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

### Explicit host-local gates

Live host-local gates are opt-in and separate from ordinary tests. When
explicitly requested, missing MySQL binaries, required plugin/profile support,
ports, UDS paths, or privileges must fail with actionable prerequisite errors
rather than silently skip. Later stages apply the same rule to certificates,
cross-host networking, and fence-provider prerequisites.

The Stage 2 PoC matrix includes:

- one-member negative observation cases: absence, permission denial, invalid
  local credentials, unexpected UDS/server identity, malformed metadata, and
  provenance;
- fresh three-member bootstrap, join, native recovery, and exact identity
  validation;
- switchover with source drain, frozen GTID boundary, containment, and delayed
  publication;
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
| PoC lifecycle and switchover | Three fresh owned instances, loopback Group Replication, Kuberic hosts | Bootstrap/join completes; callbacks retain exact identities; source access closes and its process is stopped/reaped before target writes open; unexpected loss leaves writes closed | Controlled host-local Kuberic lifecycle and one planned handoff | No Clone/reseed, restart recovery, automated failover, external fence, cross-host, or production claim |
| Advanced repair and restart | Stage 3 implementation, generic durable effect records, destructive approvals | Clone/reseed/replacement and effect-before-receipt faults resume or fail closed without touching foreign state | Resumable host-local repair for the exercised profile without an application-owned metadata store | No automated writable failover |
| Advanced failover and fencing | Validated native commit invariant and independent fence provider | Only a candidate containing the required history publishes; old-primary survival, provider loss, quorum loss, and divergent histories remain fail-closed | Automated failover for the exact qualified environment | No portable infrastructure or Kubernetes claim |
| Secure cross-host and Kubernetes | Named network, TLS or qualified equivalent, separate principals, storage, routing, and fence integration | The environment-specific lifecycle, partition, trust-rotation, direct-client, restart, and replacement matrix passes | Only the exact named deployment matrix | No generic CNI, mesh, cloud, or Kubernetes portability |

## Limitations and Unsupported Modes

Unless a later validated stage says otherwise, this design does not support:

- production MySQL process ownership, topology mutation, writable transition,
  or Kuberic callback behavior beyond the delivered read-only observer and
  bounded one-generation host;
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
- native completion evidence in addition to public scalar progress;
- generic effect records, stale-result rejection, restart reobservation, and
  conservative access reconstruction;
- independently verifiable fencing before new write publication; and
- server-free ordinary tests separated from explicit live gates.

The existing callback and custom-authority behavior is evidenced in:

- `../kuberic/kuberic-runtime/src/application.rs:23-28,67-76`;
- `../kuberic/kuberic-runtime/src/replicator/mod.rs:42-70,160-201,608-665`;
- `../kuberic/docs/features/kuberic/replicator-boundary.md:74-181`; and
- `../kuberic/kuberic-runtime/README.md:30-260`.

These paths were researched at local Kuberic commit
`301d7f364744aea4dd2513dcc8179d3588fd7dd7`.

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
