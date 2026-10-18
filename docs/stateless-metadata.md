# MySQL Restart-Stateless Metadata

## Status

Partially implemented architecture. This document defines the persistence and
restart model for MySQL lifecycle integration. The delivered bounded service
owns fresh process generations with memory-only context and separate persistent
data/disposable scratch roots. It composes both the older fixed three-member
bootstrap/join gate and a newer test-owned composition of three independent
member-local lifecycle engines. The latter publishes a bounded application
gateway and completes one controlled handoff while the same host retains exact
ownership and attempt context. The normal crate surface translates the exact
pinned public callback bundle into one such member-local
engine for fresh initial-primary bootstrap and already-formed planned
transitions. It does not claim restart continuation, durable effect recovery,
survivor adoption, Clone/reseed, production-controller custom-Build dispatch,
automated failover, repair, or Kubernetes integration.

The delivered `kuberic_mysql::adapter` module already satisfies the
application-state part of this proposal: it accepts one observation request,
keeps only attempt-local state, and returns typed evidence without opening a
durable store.

The delivered `kuberic_mysql::service` module applies the same boundary to one
process, the fixed fresh topology, and each member-local `MysqlReplicator`: it
writes no application metadata, journal, receipt database, state JSON,
workflow cursor, or adoption record. Member effect receipts, operation retry
records, access generations, proxy registries, handoff boundaries, successor
acceptances, terminal receipts, credentials, and transition credit remain in
memory. Loss of engine ownership or attempt context requires closed-access
fixture reset even if surviving MySQL state appears healthy.

The delivered adapter translates public replica/process/storage/epoch/role/
configuration/progress/access values from `kuberic-runtime` commit
`be4f83b43567c59f48472025d55dd731efecc4c6` into the one-member MySQL engine.
The fixed three-member manager and three-engine handoff fixture remain
qualification infrastructure. Runtime correctness does not depend on a
private MySQL lifecycle, observation, access, evidence, or receipt capability;
signed Build/peer authorization and durable public completion remain external.
The direct custom callback can build a fresh idle secondary while the process
retains all participants and request authority in memory; interruption after a
native effect requires reset rather than private effect recovery.

The fresh-topology manager owns its monotonic clock and one absolute attempt
deadline. Callers cannot replay, regress, or independently extend operation
time with opaque ticks or per-call deadlines. Every control and observation
deadline is derived beneath the attempt bound. Qualified XCom views are
accepted only as the exact `<fixed u64>:<monotonic u32 + 1>` successor for a
join, so foreign join/leave churn or member loss/rejoin cannot receive credit
merely because final membership looks correct.

The custom Build registry, complete retry ledger, cleanup ownership, retained
replica progress, and pending-admission state are memory-only. Active
configuration input may omit current progress only when the exact
replica/process-session/storage-incarnation observation is retained. Omitted
catch-up capability normally reuses the exact retained field; explicit current
progress with `must_catchup=false` may establish a first known-unknown
capability. Required unknown capability, first-seen omitted current progress,
and every cross-binding reuse fail closed before admission. Rejected input
does not mutate retained progress.

Independent primary commitment samples retain complete structured history,
source and participant scopes, native view and durability qualification only
in memory. Primary progress requires live durable containment on every
applicable public voter, not a saved local applied scalar. The provider
revalidates the same proved sample; it does not relabel an unproved
secondary's accepted progress. An observed invalid native durability profile
latches that fresh concrete owner unqualified until reset, even after the
setting is restored. This is not durable `HistoryContext` reconstruction or
authority over imported/restarted history.

Successful Build also retains exact target/donor native ownership and its
successor view past the child callback lifetime. Completed-idle Remove derives
a new bounded budget from current owners, proves recovery account/grant
absence and target-only native departure, then proves exact process/socket/
scratch retirement before invalidating registry, pending receipts, progress
and old Build completion authority. Persistent data is not deleted; active
voting membership still changes through configuration updates. Exact
successful retries use retained containment plus fresh survivor proofs.
Numeric replica-ID reuse is refused within that source lifetime.

Before effects, cancellation preserves retryability. After arming, failed or
dropped removal retains target ownership and a nonterminal removal record,
retires its registration, attempts bounded exact-generation containment and
quarantines the group. Unreaped ownership is retained and unproved cleanup
cannot publish success. Reset is required; none of these records becomes an
application journal, restart-adoption capability or durable recovery cursor.

Bootstrap cancellation remains restart-stateless but not uncontained. After
any bootstrap-enable attempt, bootstrap-off and proof are attempted. If the
public future is dropped while that asynchronous cleanup is incomplete, the
retained ownership guard synchronously contains the exact topology before
control returns; no query future is detached and no survivor is adopted.

The delivered native feasibility tests add one strictly test-only exception to
the one-generation fixture shape, not to the runtime persistence contract.
After a completed fresh topology transfers ownership into a crate-private test
runner, the same in-memory attempt may gracefully stop or abruptly kill an
exact retained child, retain its already-owned data and scratch roots, start
one new attested child, derive a fresh process-session binding, and rejoin it.
The runner invalidates bootstrap/join credit on entry, reserves cleanup time,
and contains all members on cancellation or mismatch. It writes no metadata
and is absent from the normal library API.

This proves selected Oracle MySQL 8.4.11 native history and crash behavior
only. It is not restart continuation: losing the runner, its exact children,
or its attempt context still requires fixture reset. It provides no survivor
discovery, adoption, durable workflow cursor, effect recovery, public receipt,
or authority after a component restart.

This design applies the ownership principle from the
[PostgreSQL restart-stateless metadata proposal](https://github.com/youyuanwu/kuberic/blob/cfc27498ce1c67336d284e7b9c51813907e2efae/docs/features/postgres/stateless-metadata.md)
to MySQL:

> The MySQL application replicator and adapter must not persist private
> recovery metadata.

They may keep observations, queues, public-operation context, connection
state, and process handles in memory. They may perform native operations
required by an exact public Replicator call and return success or a typed
public error. They must not own a metadata file, embedded database, operation
journal, receipt store, workflow cursor, fence store, or other independent
recovery authority.

Durable MySQL facts belong to the MySQL data directory. Durable orchestration
facts belong to Kuberic's generic controller and replica-agent facilities.
Scratch files are disposable.

### Delivered closed retained-owner reopen

The first Stage 3 slice adds the separate public `MysqlStoppedReopen` primitive,
not automatic `Existing` or owner-loss reconstruction. A trusted embedding
consumes an exact quiet manager into a nonserializable graceful-stop capability
and supplies accepted public generic authority plus an exact expected native
boundary. The one-use capability owns the same data and scratch/native logs;
reopen starts a fresh closed child and returns only `MysqlClosedReopened`.
Full executed/purged sets, native UUID/topology, device/inode and independent
native/generic process-session fences are validated. No predecessor role,
configuration, access, Build ledger or commitment sample is resurrected.

Native SQL/X/GR ingress, asynchronous replication and event scheduling remain
disabled. Native read-only settings are necessary but not sufficient: issuing
any application/proxy capability monotonically disqualifies the owner, even
if an idle or unpublished proxy currently has no backend sessions. Lifetime
durability disqualification is also monotonic across the stop/reopen.

This selected mode retains native binary logs on scratch. Ordinary terminal
stop still deletes scratch; its data-only remainder is not a reopen capability.
Lost logs/scratch, abrupt shutdown or lost in-memory ownership require reset.
The native-log inventory and all generic/native binding evidence remain
memory-only. Current settings do not certify an unknown predecessor lifetime.
See [closed safe reopen](mysql-safe-reopen.md) for the complete API, trusted-caller
boundary, exact upstream recovery-envelope gap and qualification commands.

## Motivation

The broader MySQL design requires exact process and storage ownership,
Group Replication bootstrap and join, access reconciliation, planned
switchover, and eventually restart-safe repair and failover. A private
MySQL-specific journal could make interrupted operations resumable, but it
would introduce a third interpretation of state:

1. Kuberic controller and generic agent authority;
2. MySQL native state in the data directory; and
3. an application-owned MySQL metadata store.

The third store would duplicate topology, process, access, GTID, and workflow
facts already owned by the first two domains. Every restart would need to
compare all three, define precedence for mismatches, and coordinate their
backup and restoration.

The restart-stateless model instead makes the MySQL component a reconciler:

- Kuberic supplies admitted intent and durable operation authority;
- MySQL supplies native identity, topology, recovery, and transaction-history
  facts;
- the component observes both and completes or rejects exact public operations;
- Kuberic durably records public operation intent and completion; and
- every replacement process starts with external access closed.

This is a component-boundary rule, not merely a directory-layout choice.
Moving a MySQL-specific journal into the data directory, a Kubernetes object,
another SQLite database, or a remote service would still violate the design.
Records required by the protocol must be generic Kuberic controller or agent
records with typed MySQL evidence.

## Goals

1. Keep the MySQL adapters and lifecycle replicator restart-stateless.
2. Make the MySQL data directory the sole local durable authority for native
   database identity, contents, GTID history, and engine-owned recovery state.
3. Make the Kuberic controller and generic agent the durable authority for
   topology, epochs, desired roles, access intent, operation ordering, process
   generations, fencing, and accepted receipts.
4. Reconstruct a restarted MySQL component from fresh native observation plus
   replayed generic authority.
5. Start every replacement process with external client access closed.
6. Preserve exact replica, storage, process-session, credential-generation,
   native-view, and operation-attempt fencing.
7. Reconcile interrupted effects from durable pre-action records and observable
   native postconditions rather than application-private workflow cursors.
8. Keep sockets, logs, generated configuration, secret projections, and
   temporary operation files on disposable scratch storage.
9. Retain structured GTID-set semantics; never replace them with scalar
   sequence comparisons or an unauthenticated digest.
10. Fail closed when Kuberic authority, native identity, operation evidence, or
    process ownership cannot be reconstructed exactly.
11. Use only the public SF-shaped Replicator bundle for runtime correctness;
    do not require a private MySQL lifecycle, observation, access, or receipt
    channel.

## Non-Goals

- Removing the generic Kuberic controller or replica-agent stores.
- Treating the MySQL data directory as sufficient write authority.
- Persisting MySQL operation state in the adapter or lifecycle component.
- Completing the generic evidence, provisional-admission, or reattachment
  protocols in this repository.
- Making Stage 2 topology mutation crash-resumable; the first PoC may reset its
  fixture after an interrupted mutation.
- Defining automated failover before the native commit invariant and an
  independent fence provider are qualified.
- Adding Clone, reseed, replacement, TLS, cross-host operation, Kubernetes, or
  production routing to the Stage 2 PoC.
- Treating `PRIMARY`, `read_only`, `super_read_only`, or Group Replication
  membership as Kuberic access authority.
- Supporting MySQL patches or forks outside an independently qualified profile.

## Authority and Storage Model

### Durable owners

| State category | Durable owner | Reconstruction source |
|---|---|---|
| Desired topology, configuration, epoch, role, access intent, and cluster-wide transition | Kuberic controller and `KubericSet.status` | Replayed controller intent and accepted status |
| Per-replica authority, storage binding, process retirement, local fences, destructive work, and exact public-operation receipts | Generic Kuberic replica agent | Generic agent store and replayed public lifecycle/configuration calls |
| Database contents, system tables, `server_uuid`, executed and purged GTIDs, binary logs, and engine recovery state | MySQL data directory | Offline validation where qualified, then fresh closed-access SQL observation |
| Current Group Replication membership, view, role, recovery state, and active settings | MySQL engine | Fresh native observation; never restored from an application cache |
| Process handles, member-effect receipts, lifecycle retry records, access generations, proxy session/backend registries, handoff proof objects, callback tasks, deadlines, and attempt-local observations | Process memory | Recreated only by a new attempt; delivered Stage 2 does not resume them |
| Sockets, PID files, logs, generated configuration, projected credential files, and temporary operation files | Scratch storage | Recreated or discarded |
| Independent old-primary containment after the PoC | External fence provider plus generic Kuberic receipt | Provider verification under current authority |

The MySQL component may interpret native evidence, but it does not become the
durable owner of that evidence. It completes a public operation only after the
native postcondition is true. Kuberic persists the exact public call
completion under its operation, authority, revision, and process-session
fences; it does not persist a stronger private MySQL observation.

### Persistent layout

The MySQL application owns no persistent metadata sibling beside the data
directory:

```text
<mysql-storage>/<resource>/<replica>/<incarnation>/
  data/
```

The generic Kuberic agent maintains its own application-neutral store through
runtime-owned facilities. The MySQL component neither knows its filesystem path
nor opens or interprets it.

Disposable files use a separate scratch root:

```text
<scratch>/<resource>/<replica>/<process-generation>/
  run/          # Unix sockets and non-authoritative PID files
  log/
  tmp/
  config/       # generated MySQL configuration
  credentials/  # projected or generated runtime credential files
  clone/        # future temporary Clone staging, if qualified
```

Loss of scratch storage may require process restart or operation retry. It must
not change admitted topology, storage incarnation, GTID obligations, fencing,
or access authority.

### State that belongs to MySQL

The following facts are reconstructed from the exact owned MySQL data root and
fresh engine observation:

- the native `server_uuid`;
- initialized system tables and database contents;
- `@@GLOBAL.gtid_executed` and `@@GLOBAL.gtid_purged`;
- binary-log and transaction-recovery state used by the qualified profile;
- persisted users, grants, and MySQL-owned credential verifiers;
- engine-owned recovery-channel and Group Replication metadata that the pinned
  profile documents as durable;
- the active configured group identity and local Group Replication address;
- current member identity, role, state, view, and recovery status; and
- active `read_only` and `super_read_only` values.

Not every item is safely available while `mysqld` is stopped. Offline files are
used only where the exact qualified profile defines a stable format. Otherwise
the runtime starts MySQL for internal administration with external access
closed and queries the value.

Native persistence does not grant Kuberic authority. A data root that appears
to be a primary, belongs to an unexpected group, contains unexplained GTIDs, or
cannot be matched to admitted storage remains fenced.

### State that belongs to Kuberic

The controller or generic agent retains:

- resource, partition, replica, incarnation, Pod, PVC, and canonical data-root
  identity;
- previous and current configurations, epoch, and authority generation;
- desired role, access generation, and topology intent;
- exact process-session retirement and storage-bound reattachment;
- allocated addresses and ports where stable allocation is required;
- credential generation without storing secret material in evidence;
- initialization, bootstrap, join, removal, switchover, and future repair
  operation IDs;
- pre-action authorization and bounded canonical input signatures;
- destructive-work preparation and installation state;
- accepted source closure, process-stop, containment, and access receipts;
- a planned-switchover `HistoryContext` and qualified public LSN boundary;
- exact public role, epoch, configuration, catch-up, build, removal,
  data-loss, close, and abort completion; and
- immutable Replicator capabilities, `HistoryContext`, signed build/peer
  authorization, and the qualified public progress values; and
- controller-visible terminal transition results.

The MySQL component receives this state only through public construction
values and lifecycle/configuration calls. It must not query the generic agent
database directly or reconstruct missing public values through a private
runtime capability.

## MySQL-Specific Reconstruction Constraints

### GTID sets are structured boundaries

MySQL history is a set of SID, optional tag, and interval components. It has
equal, proper-subset, proper-superset, and incomparable relations. A maximum
sequence number, interval count, text digest, or last observed GTID cannot
replace the complete relation.

Ordinary public-operation receipts do not persist a complete GTID set. MySQL
reobserves and compares the normalized set internally whenever a public role,
configuration, catch-up, build, or data-loss call requires history proof.
Kuberic durably retains the operation identity, `HistoryContext`, qualified
public LSN/catch-up capability, exact public completion, and process/storage
fences.

The public scalar projection is deliberately narrower than the native model.
It is valid only when the executed set is empty or consists of one untagged
interval beginning at `1` for the exact configured Group Replication UUID,
with no foreign, disjoint, or unexplained component. A same-lineage purged
prefix remains in `gtid_executed` and does not invalidate applied progress.
The interval tail is the public LSN and empty history is `0`. Any other history
is public-progress-incompatible and must return `INVALID_LSN` or
rebuild-required. The scalar is comparable only within one exact
`HistoryContext`; it never replaces complete GTID containment/equality inside
MySQL.

A primary exposes only the highest projected tail proven committed under its
exact installed public configuration and qualified commit profile. A secondary
exposes the projected tail durably applied locally. Unassigned/recovering
replacement progress may be election evidence while access remains closed; it
is never serving-readiness evidence by itself.

The serving profile must pin and qualify
`group_replication_gtid_assignment_block_size = 1` and an explicit
`group_replication_view_change_uuid` policy compatible with the one-source
projection. `catch_up_capability` is derived separately from the qualified
retained set using `gtid_executed`, `gtid_purged`, and the exact binary-log
inventory/retention profile. Purged history may preserve applied progress
while making incremental recovery unavailable.

The Oracle 8.4.11 gate verifies the configured one-source shape through the
named primary, membership, process-restart, and purge transitions. The
delivered production boundary now projects empty history or the exact
one-source contiguous tail and returns deterministic typed incompatibility for
every other parsed shape. That scalar still is not a Kuberic public completion:
future `HistoryContext` translation and durable role-correct runtime evidence
remain external. `gtid_purged` and validated binary-log filenames are kept
separate, and the opaque inventory is not treated as
`catch_up_capability`; Stage 2 returns typed unsupported or unproved.

The Stage 2 PoC may keep its structured planned-switchover boundary in memory
because it does not resume an interrupted handoff. Restart continuation is
supported only for operations whose required native history can be
reconstructed and validated from MySQL while their durable Kuberic record uses
the qualified `HistoryContext`/LSN projection. Non-projectable history remains
unsupported rather than creating a private GTID receipt. A hash alone is not
sufficient because a target must prove set inclusion.

### Group Replication views are fresh evidence

Current Group Replication membership, role, member state, and view identifiers
are not application recovery metadata. They are observed from the live engine.
After restart, the component must not restore a cached view and treat it as
current.

Kuberic durably records the public operation input and exact completion, not
the native pre/post view. During execution, MySQL closes the old observation,
collects a fresh post-effect view, and completes the public call only if that
view is the allowed result. Unrelated member loss, replacement, identity
change, or primary selection is not accepted merely because the operation
expected some view change.

### Bootstrap is an irreversible authority boundary

Enabling Group Replication bootstrap mode on the wrong member can create a
conflicting group. The generic agent must commit an exact bootstrap preparation
record before the component enables bootstrap mode. That record binds:

- the fresh storage incarnation;
- the exact configured group name and initial public replica set;
- the designated bootstrap member;
- configuration, epoch, process session, and operation ID;
- the expected empty/fresh storage condition; and
- the canonical public primary-role input.

The component disables bootstrap mode immediately after entering the native
operation and then reobserves the group. If the action completed but its
completion acknowledgement was lost, replay under the replacement-session
protocol invokes the convergent primary-role operation, which reobserves the
native state and must recognize the exact already-created group rather than
bootstrap a second group.

### Join and distributed recovery are level-triggered

A join is driven by one exact public `build_replica` operation. Its descriptor
names the target identity/session/address, build ID, `HistoryContext`, and
signed one-attempt `BuildAuthorization`. The target validates that public
authorization against its immutable trust configuration, empty/idle storage,
local epoch, process session, and role before entering native recovery.

The source-side call completes only after the target's native join, copy or
distributed recovery, and retained replication reach the internal build
boundary. The runtime persists only terminal public build completion. An
ambiguous live build is not resumed after restart: source and target sessions
are retired, and a new attempt uses a new build ID, storage/process generation,
and signed authorization. The component does not resume an
application-private stage number or persist a native copy cursor.

### Process survival is not adapter authority

`mysqld` may survive a crash or restart of the application component. A PID
file, socket, executable name, or responsive SQL endpoint does not prove that
the survivor is owned.

The generic runtime must retire the old process generation and either:

- independently withdraw or isolate every ordinary and administrative client
  path, drain or terminate existing client sessions as required, prove that
  effective access is closed, then prove that the exact process tree remains
  attached to the admitted storage incarnation and issue a fresh restricted
  reattachment capability; or
- contain and reap the survivor before starting a replacement.

Process ownership and access closure are separate proofs. A valid storage,
executable, process-tree, and native identity binding does not prove that a
surviving listener stopped accepting writes. Until effective closure is
verified, the survivor is classified as potentially serving and receives no
closure credit, reattachment capability, operation advancement, or dependent
transition credit.

The replacement process session cannot complete callbacks or native operations
for the retired session. When it reattaches to an otherwise valid running
`mysqld`, "starts closed" means the generic runtime has already verified
effective client-path closure; it is not an in-memory assumption made by the
replacement component.

### Configuration and credentials

Generated MySQL configuration is reconstructed from admitted intent and placed
on scratch storage. Authority-critical configuration is read back from MySQL
after startup.

Use of `SET PERSIST` creates native durable state in the data directory and
therefore requires an explicit qualified contract. The initial lifecycle
implementation should prefer generated owned configuration and avoid
unclassified persisted-variable mutation.

Secret values are supplied by an authorized secret provider or protected
projection. Evidence may retain principal identity and credential generation,
but never secret material. MySQL account and grant state is native data and is
reobserved; an external secret projection remains disposable input.

## Agent-Owned Public Operation Protocol

The public Kuberic replication interfaces do not expose the generic store.
The runtime and MySQL interact only through public construction values,
Replicator calls, and the partition access projection:

```text
controller commits cluster intent
    -> local agent commits exact public-operation intent
    -> runtime invokes the exact public call under one owned task
    -> MySQL performs and freshly validates the required native effect
    -> public call returns success or a typed public error
    -> runtime revalidates revision/session and commits exact call completion
    -> controller accepts reported completion
    -> runtime grants access separately when the full ordered recipe is complete
```

Intent is not current authority and cannot open access. Public values are
valid only for their exact operation, immutable inputs, authority,
`HistoryContext`, storage incarnation, and process generation. Signed
build/peer authorization cannot be substituted across sessions or epochs.

The feature-gated adapter accepts the pinned SF-aligned Kuberic values and
retains their translation into Kuberic-neutral identity, operation, epoch,
role, configuration, access-generation, progress, and terminal values only in
memory. Its callback ledger gives an exact in-process retry the same native
operation identity and rejects changed reuse. Its epoch mapping and pre-epoch
source boundary/progress/role cache are also process-local. Runtime
configuration carries exact remote peers only; the local target is synthesized
from separately bound/enrolled identity, process-session, storage, endpoint,
UDS/inode, and process authority. In-process native
effects complete only from exact request-bound receipts; those receipts bind
local authority, predecessor and successor evidence, deadline/cancellation
state, and cleanup state. They are not durable records and are not exposed as
a private Kuberic capability. Successful terminal outcomes remain in process
memory for exact retry. An unsettled abort remains terminal and is not
converted into successful containment; drop and emergency ownership stay armed
until the exact member is contained.

Loss of the adapter process loses the callback ledger, epoch mapping,
pre-epoch cache, proxy registry, and native owner. `OpenMode::Existing` is
therefore rejected rather than reconstructing authority from a data root or
replication endpoint. Kuberic may replay this stage only by retiring/resetting
the fresh attempt; no callback is credited from surviving `mysqld` state.
The delivered direct custom Build is process-local and exact-retry-bound;
production-controller custom-Build authority, Clone/reseed, and durable
continuation remain external.
MySQL must not emulate missing public fields or durable completion proof with
an application-owned store, a private runtime capability, or a compatibility
progress marker.

If the process exits, its public operation task and process-local native
context die with it. Kuberic may replay only operations whose declared replay
contract converges. Ambiguous builds and `ReplaceOnAmbiguity` data-loss work
retire the affected sessions/incarnation rather than continuing predecessor
state.

Graceful shutdown revokes access, cancels and drains exact outstanding work,
closes the Replicator and its endpoint, aborts it if close fails, then closes
the application replica/proxy and stops or quarantines `mysqld`. Ungraceful
shutdown invokes Replicator abort before application abort; both are
synchronous containment operations.

### Lost acknowledgement

Every public operation has an operation ID and canonical immutable input. If
the agent committed completion but its response was lost, replay returns the
same retained result. If the native action completed before public completion
was committed, recovery uses:

1. the committed pre-action record;
2. current controller intent;
3. retired predecessor process authority; and
4. a fresh native observation.

Role, epoch, settings, exact configuration, close, and removal replay must
converge after fresh native observation. Catch-up is re-evaluated. Ambiguous
builds use a new target attempt. Ambiguous data-loss mutation retires the
incarnation. Otherwise access remains closed and the operation requires
compensation, fixture reset, replacement, or operator recovery according to
its delivery stage.

## Startup Reconstruction

A replacement MySQL lifecycle component follows this sequence:

1. Begin with external read and write access closed.
2. Open and validate the generic agent authority.
3. Validate resource, replica, incarnation, Pod/PVC, and canonical data-root
   bindings.
4. Load admitted configuration, epoch, desired role, access intent, local
   fences, and pending generic effects.
5. Retire the predecessor process session and discard predecessor peer/build
   authorization.
6. Inspect the data root using only qualified offline surfaces.
7. Reconcile generated configuration and pre-start fences.
8. Reattach to an exactly owned survivor or start `mysqld` for internal control
   only.
9. Open the member-local Replicator unassigned and access-closed; publish only
   qualified election progress and request process-session renewal.
10. Have the controller admit a newer configuration epoch and issue fresh
    signed peer authorization for the replacement session.
11. Complete `update_epoch` barriers on surviving secondaries before replaying
    replacement role.
12. Replay role under the new epoch, then install fresh PC/CC configuration and
    peers from the new authorizations.
13. Re-evaluate required catch-up, build, or authorized data-loss work through
    public calls; MySQL performs fresh native validation internally.
14. Publish the application-owned client proxy endpoint only after current
    authority, exact public completion, source containment, and runtime
    identity/session/endpoint fences satisfy the normal access invariants.

The component does not restore a persisted local role, health Boolean, native
view, or workflow cursor.

### Authority unavailable

The MySQL data root alone never authorizes activation. If controller or generic
agent authority is unavailable, MySQL may remain stopped or run for internal
recovery with external access closed. It must not bootstrap a group, join a new
topology, transfer primary, or grant ordinary client access.

### MySQL stopped at startup

When `mysqld` is stopped, preliminary classification may use data-root
existence, ownership, `auto.cnf`, and other exact-profile offline surfaces that
have been independently qualified. Values requiring the server are confirmed
after closed-access startup. Missing offline proof produces an unknown state,
not optimistic identity or progress.

## Lifecycle Effects

### Initialization

Initialization is destructive work. Before `mysqld --initialize`, the generic
agent records the target storage incarnation, exact path, expected emptiness,
product profile, operation ID, and state `Prepared`.

The generic effect progresses through:

```text
Prepared -> Initializing -> Installed -> Validated
```

`Installed` requires successful process completion and durable required files.
`Validated` additionally requires a closed-access server observation with the
expected fresh native identity and supported product.

After interruption in `Prepared` or `Initializing`, the directory is not
assumed reusable. The generic effect decides whether exact owned cleanup and a
fresh initialization are authorized.

### Process start and stop

Process start binds the exact executable, data root, generated configuration,
socket, allocated ports, storage incarnation, and new process session. A
running socket is not a durable receipt.

Process stop completes only after the exact process is stopped and reaped and
its process-scoped socket is absent. A stop requested for another incarnation
or a reused PID is rejected.

### Group bootstrap and join

The bounded delivered Phase 2 slice uses retained in-memory attempt authority,
exact process/storage/endpoint enrollment, and fresh post-effect native
observation. Exactly one designated member may bootstrap; bootstrap mode is
proved off before observation receives credit; and the remaining two fresh
members join sequentially. Each join captures a structured GTID boundary from
the accepted predecessor view and requires the target's accepted `ONLINE`
evidence to contain it. Native membership is reobserved; the application never
writes a private member list.

The newer member-local engine expresses the same native work as exact
`MemberEffectRequest`/`MemberEffectReceipt` pairs. Each engine owns only its
local process and can credit only a receipt bound to its current operation,
epoch, configuration, authority, predecessor evidence, and final local
binding. The three-engine fixture owns orchestration and shared attempt inputs,
not peer process authority.

This is deliberately not a restart protocol. If the component, retained child
ownership, credential generation, pending effect, or topology-attempt context
is lost, the surviving topology is not discovered, adopted, or resumed. Client
access remains closed and the fixture is reset. A later resumable stage must
use the generic durable pre-action and completion protocol described above.

### Controlled switchover

The Stage 2 PoC remains deliberately non-resumable and uses the delivered
MySQL-owned intended-order sequence:

1. revoke the source access generation and prove listener closure, zero proxy
   sessions/commands, exact native backend absence, and no cancelled or
   ambiguous command;
2. have the source engine issue a stable boundary from a fresh
   post-revocation native read, binding the revoked generation and eligible
   closure receipt to the complete GTID set, qualified scalar, exact
   three-member configuration, source authority, target, remaining secondary,
   and predecessor view;
3. prove the target and remaining secondary contain the same boundary while
   their gateways remain closed;
4. apply a newer epoch barrier to all three engines;
5. demote the source through exact native transfer, promote the target, and
   complete the remaining secondary's same-role transition from matching
   member-effect receipts;
6. accept the current three-member configuration for the newer epoch;
7. close the source and obtain an exact terminal receipt proving process
   stop/reap, PID/socket/endpoint absence, and scratch removal;
8. accept the canonical two-survivor successor view and frozen history on both
   survivors;
9. submit one combined proof containing the frozen boundary, both survivor
   outcomes, and the exact source terminal receipt; and
10. publish a separate newer target access generation only after that proof is
    freshly revalidated and a publication-bound native successor view/history
    read still matches it.

If the component restarts during this PoC operation, access remains closed and
the fixture is reset. No private journal is added to resume it.

A later resumable handoff moves these facts into generic agent-owned public
operation records:

- source and target incarnations;
- source-generation closure and exact source terminal completion;
- qualified `HistoryContext`/LSN boundary;
- native pre-view and permitted post-view reconstructed internally by MySQL and
  never stored in the agent record;
- exact public catch-up, epoch, role, and application-role completion;
- source process-stop or independent fence evidence;
- both survivor acceptances or their reconstructible public equivalents; and
- target access-grant evidence.

The application reconstructs each replayable phase from those records and
fresh native postconditions. A phase that requires lost non-projectable GTID
or native-view context is replaced or remains unsupported.

### Access reconciliation

Access is an effect, not a local Boolean. Every replacement component begins
closed. Opening requires:

- current admitted authority and access generation;
- exact storage and process identity;
- exact completion of the public operations whose postconditions include
  supported MySQL identity, coherent Group Replication membership/view,
  compatible GTID history, and recovery completion;
- accepted source containment for writable transitions;
- no conflicting or incomplete generic effect; and
- successful endpoint and credential validation.

For the Public V1 path, runtime access authority binds only the exact accepted
epoch, Primary role, and configuration identity. It is not durable MySQL
metadata and is not sufficient by itself: publication performs a fresh
request-bound local native view read, revalidates role and configured
membership, and requires projectable local history. A wrong runtime value,
stale/substituted receipt, non-projectable history, or local-evidence mismatch
leaves the proxy closed. The older fixture's combined handoff proof remains a
separate in-memory path and is not serialized into Kuberic state.

The externally returned address belongs to an application-owned
MySQL-compatible proxy, not the raw `mysqld` listener. Successful
local-primary configuration completion may publish the fixed listener before a
runtime grant, but the neutral guard checks retained Public V1 read and write
statuses before session admission, before dispatching every command, and after
backend execution before forwarding each response packet. Both must be
`Granted`. Denial closes only the affected session/command, never globally
settles the generation, and suppresses success for a write fenced during
execution. Revocation still rejects new commands, closes the listener, settles
or terminates in-flight work under a fixed policy, closes
idle/transaction-holding sessions, terminates every exact registered native
backend connection, and proves zero proxy work plus native absence for that
generation before a source GTID boundary is frozen. A containment receipt
separately records cancellation and ambiguity; either condition prevents
handoff eligibility even when cleanup completes. The native listener remains
private, and `read_only`/`super_read_only` provide an independent
defense-in-depth barrier.

The listener and sessions also observe shared attempt cancellation and the
absolute work cutoff after publication. Every initiated backend is registered
as identified or unidentified before it can disappear from accounting. The
unidentified obligation is armed before polling UDS connect, so cancellation or
drop after server acceptance prevents native-absence credit. Bounded
secret-free diagnostics survive into snapshots and containment receipts.
Async preparation, revocation, retirement, and terminal cleanup keep ownership
armed across awaits. A successful account receipt is persisted before socket
validation, listener binding, or failure cleanup awaits; terminal cleanup must
remove that account or report/contain the exact member. Fixture roots are
removed only after proxy, account, session, and exact member settlement
succeeds.

Fresh-member initialization and startup run as owned blocking work outside the
async runtime thread. Dropping the async owner cancels and joins that work; a
same-runtime cancellation task can preempt the longer work cutoff while the
cleanup reserve remains available for exact-child stop and reap.

The delivered gateway supports only one loopback plaintext profile: the exact
dedicated pre-warmed `caching_sha2_password` account, the fixed capability
allowlist, query/ping/quit, one-packet messages, and bounded simple responses.
It does not terminate authentication, interpret SQL, or provide generic
protocol support. Account create/grant/remove work runs with binary logging
disabled and restored, and retained secrets are redacted and cleared.

`read_only` and `super_read_only` are defense-in-depth observations. They do
not replace service withdrawal, process containment, independent fencing, or
Kuberic access authority.

## Failure Semantics

### Application process crash

- Attempt-local observations and capabilities are discarded.
- External access is not assumed closed merely because the component exited.
- The generic runtime withdraws or isolates client paths, contains existing
  sessions as required, and verifies effective closure.
- Until closure is verified, the surviving server is potentially serving and
  no transition may advance.
- The generic agent retires the old process session.
- MySQL is reobserved under a fresh session.
- Pending effects continue only through generic operation records.

### Surviving `mysqld`

- The survivor is not adopted from PID or socket discovery.
- The runtime proves exact process-tree, storage, executable, and native
  identity ownership or contains it.
- Reattachment additionally requires independently verified client-path
  withdrawal and existing-session containment.
- A fresh process session is required for all later callback completion.
- Existing native primary state does not grant client access.

### Pod or host restart with retained storage

- Scratch files are recreated.
- Generic authority validates the retained storage identity.
- MySQL starts internally closed and is reobserved.
- Old process and callback sessions remain retired.
- Access is freshly reconciled.

### Storage replacement

A new PVC or storage identity creates a new Kuberic incarnation. Old GTID,
native identity, effect receipts, and access grants cannot authorize it. It
requires fresh initialization or an explicitly authorized future build.

### Generic agent-store failure

- Failure to read admitted authority prevents activation.
- Failure to commit pre-action evidence prevents the irreversible action.
- Failure to commit completion keeps the effect pending even if MySQL appears
  to have completed it.
- Loss of a commit acknowledgement is resolved by operation-ID replay.
- A writable instance that loses required local authority closes access rather
  than relying indefinitely on cached memory.

### Corrupt or unexplained MySQL data

Startup or native validation fails explicitly. The absence of a private
metadata file does not authorize reinitialization. Cleanup, rebuild, or
replacement requires a new exact destructive-work operation.

### Lost scratch storage

Sockets, logs, generated configuration, projections, and temporary operation
files are recreated. Incomplete temporary work may repeat. Durable database
contents, admitted authority, and accepted receipts do not change.

### Kubernetes API partition

Before Kubernetes qualification, no production behavior is claimed. The
required invariant is that new topology mutation, promotion, or access grants
cannot be derived from the MySQL data root alone. Local generic fences remain
effective while disconnected.

## Safety Invariants

1. The MySQL adapter and lifecycle component persist no private recovery state.
2. Only the MySQL data directory and generic Kuberic stores contain durable
   state used for reconstruction.
3. MySQL native state alone never grants Kuberic role or client access.
4. Kuberic authority alone never proves MySQL identity, history, or recovery.
5. Every replacement process begins with external access closed.
6. Stale process generations and public-operation tasks cannot complete work.
7. Group Replication role and read-only variables are evidence, not authority
   or independent fencing.
8. GTID histories retain structured set relations. Only the explicitly
   qualified single-source contiguous public LSN projection is permitted, and
   it never replaces internal set comparison.
9. A GTID digest alone never proves target set inclusion.
10. Native view changes are accepted only as the exact permitted result of a
    current operation.
11. Bootstrap requires durable generic pre-action authorization.
12. Irreversible effects do not begin before required generic persistence is
    acknowledged.
13. Lost acknowledgements are resolved by operation-ID replay and fresh native
    observation.
14. Interrupted initialization or future destructive work is not inferred
    complete from partial files.
15. A surviving `mysqld` is foreign until exact ownership is proven.
16. Process-session retirement does not transfer old public-call completion
    rights to the replacement session.
17. Scratch loss may repeat work but cannot change durable authority.
18. Missing, oversized, stale, incoherent, or unsupported evidence fails
    closed.
19. No MySQL-specific store duplicates controller or agent workflow state.
20. The Stage 2 PoC does not claim restart continuation merely because it uses
    the restart-stateless ownership model.
21. Member-effect and terminal receipts are process-local evidence, not durable
    restart authority or permission to adopt a survivor.
22. Access-generation closure requires both proxy-zero and exact native
    backend absence; cancellation or ambiguity cannot authorize handoff.
23. Target publication after controlled handoff requires the frozen boundary,
    both survivor acceptances, and exact source terminal proof together.

## Adoption Plan

### Phase 1: Align contracts

1. Mark the MySQL application boundary as restart-stateless.
2. Classify every planned durable fact as MySQL-native, controller-owned,
   generic-agent-owned, or disposable.
3. Define the exact public operation postconditions, `HistoryContext`-scoped
   scalar GTID projection, signed build/peer authorization, and typed public
   errors.
4. Define process-session retirement and storage-bound reattachment inputs.
5. Remove the application-owned `journal/` concept from lifecycle layout and
   terminology.

### Phase 2: Stateless Stage 2 PoC

1. Implement process ownership with memory-only process handles and generic
   authority supplied by the test host. The single-instance process host and
   fixed three-member composition are delivered.
2. Keep sockets, logs, configuration, and credentials under disposable
   fixture scratch storage. The delivered managers keep generated runtime files
   under separate per-member scratch roots.
3. Bootstrap and join three fresh members without a MySQL metadata store.
   This bounded retained-ownership slice is delivered.
4. Execute one controlled intended-order switchover with in-memory structured
   GTID/scalar context, native-backed proxy closure, exact source terminal
   containment, both survivor acceptances, combined publication proof, and
   delayed target generation publication. This is delivered in the
   repository-owned live fixture.
5. Translate the pinned Public V1 trait bundle into one fresh member owner,
   including exact public fence translation, initial-primary bootstrap,
   fresh-secondary direct custom Build, pre-epoch boundary caching,
   already-formed planned-transition callbacks, and runtime-authority
   publication with fresh local revalidation. Build uses only Group Replication
   distributed recovery and keeps membership/access admission separate.
6. Permit component replacement only while a surviving fixture host retains
   exact ownership context and proves effective closure and that no mutation is
   pending. This remains unsupported; the delivered engine does not adopt or
   resume a survivor.
7. If that host context is lost, treat the interruption as a closed-access
   fixture reset regardless of apparently healthy MySQL state.

Items 1 through 5 and the reset rule in item 7 are delivered for the bounded
host-local stage. Component replacement in item 6, production-controller
custom-Build dispatch, Clone/reseed, and restart-resumable effect continuation
remain future work.

### Phase 3: Generic durable continuation

1. Consume the SF-aligned public operation owner, value types, history context,
   signed authorization, and replay contracts.
2. Add generic intent and exact public-completion records for initialization,
   bootstrap/role, build/join, process stop, access, and switchover.
3. Reconstruct native state inside replayed public calls; do not add a private
   MySQL preparation or receipt callback.
4. Add process-session retirement and storage-bound reattachment.
5. Support restart continuation only for the qualified
   `HistoryContext`/scalar-LSN profile; retire or rebuild non-projectable
   histories.
6. Reconcile effect-completed/completion-not-committed failures according to
   each public operation's convergent, re-evaluate, new-attempt, or
   `ReplaceOnAmbiguity` disposition.

### Phase 4: Repair and advanced safety

Future Clone, reseed, replacement, automated failover, and independent fencing
reuse the same public operation and agent-owned intent/completion protocol.
They may add public value/error types, but they do not add a MySQL application
metadata store or private runtime-to-Replicator capability.

No metadata migration is required today because this repository has not
implemented a MySQL-specific durable store.

The delivered adapter makes no claim that its in-memory callback ledger,
mapped epoch ordinal, opaque replication endpoint, cached pre-epoch boundary,
proxy address, native receipt, or terminal outcome can be replayed after
process loss. It makes no claim of `OpenMode::Existing`, fresh secondary
adoption, Clone/reseed, active-member Remove, data-loss recovery, non-projectable catch-up,
automated failover, controller-dispatched custom Build, or durable interrupted
Build recovery. None of those
gaps authorizes a MySQL-private durable metadata file.

Ordinary formed lifecycle programs complete Epoch -> Role -> Configuration.
Controller-composed source swap completes Revoke -> Epoch -> refreshed
Configuration staging -> CatchUp -> Role/native transfer -> application Role,
without inventing a previous-configuration callback. Target pre-source
Epoch -> Primary Role -> Configuration is retained as fenced intent and remains
pending until a later native view proves the external transfer.
Binding-preserving monotonic progress refresh, including omissions resolving
to retained values, preserves the original native-effect identity; regressions
or binding changes conflict. A refreshed local Primary also retries its separate
fixed-port proxy publication obligation before success; already-published
state is idempotent. Catch-up with a current engine configuration captures a
fresh qualified boundary and independently proves live structured containment
on all applicable public voters; a cached pre-epoch scalar is not a commitment
certificate. A newer epoch first
physically revokes any prepared or published proxy generation. Its pre-epoch
observation validates exact authority, membership, receipt, deadline,
cancellation, and structured GTID projection while tolerating a target whose
native role already changed before the callback; ordinary progress remains
role-strict, and only a recorded source Primary retains a transfer boundary.
The exact driver also serves as a progress-only `StateProvider`: successful
fresh `OpenMode::New` retains qualified `0` only from the freshly initialized,
closed-access owner proof. Later primary `current_progress` independently
proves commitment and `last_committed_lsn` revalidates that exact paired sample
and complete participant scope, without advancing it for intervening writes
or catch-up. Secondary accepted progress alone never establishes provider
commitment. Unknown pre-configuration progress still fails. The provider exposes no copy bytes
or private durable state. Synchronous abort closes the shared listener and
contains the exact native member even when an async
callback lease is dropped; task abortion is owned but not claimed to be
synchronously joined. Dropped service Open attempts cancel and settle opening
state so Close waiters are notified.

## Validation Strategy

The architecture is accepted only when tests demonstrate:

- no MySQL component creates or opens a private durable metadata store;
- replacement processes reject predecessor public-operation tasks and signed
  authorizations and receive no access credit until effective client-path
  closure is independently verified;
- retained data roots reconstruct exact native identity and GTID evidence;
- missing generic authority keeps a healthy-looking MySQL server closed;
- delivered bootstrap and join cannot advance without the retained exact
  in-memory attempt authorization; later restart-safe bootstrap and join
  require durable generic pre-action acknowledgement;
- action-completed/acknowledgement-lost cases reobserve rather than blindly
  reissue;
- a surviving unowned `mysqld` is contained or rejected;
- a component crash with a live client session receives no closure credit until
  the client path is withdrawn or isolated and the session is contained;
- scratch deletion does not change durable authority;
- structured GTID boundaries remain lossless inside MySQL, while durable
  Kuberic progress uses only the qualified `HistoryContext`-scoped scalar
  projection and rejects every other shape;
- member-local effects receive credit only from exact request-bound receipts
  and terminal cleanup only from exact stop/reap/absence evidence;
- revocation proves zero gateway work and native backend absence, with
  cancellation and ambiguity separately preventing handoff eligibility;
- the controlled handoff keeps target access closed until both survivors
  accept the frozen boundary and the exact source terminal receipt is combined
  and revalidated;
- unexpected views, identities, histories, or oversized evidence fail closed;
- Stage 2 resets the fixture after ownership-context loss rather than inferring
  quiescence or claiming resume from MySQL state; and
- the Public V1 adapter's external contracts pin the exact runtime API,
  preserve independent replica/process/storage/endpoint fences, retain exact
  retries, reject changed/stale callbacks, accept runtime-shaped remote-only
  configurations including an empty fresh peer list, classify only fresh empty
  storage as eligible input, and reject unsupported roles, progress, and
  configurations;
- runtime-authority access publication rejects wrong epoch/role/configuration,
  non-projectable progress, and wrong local evidence while the effective guard
  rejects pre-dispatch access and suppresses post-effect success without
  preventing later regrant;
- callback-order helpers cover ordinary role-before-configuration, planned
  source staging/catch-up, stale old-role retries, synchronous proxy-bearing
  abort, and dropped-Open settlement while the legacy combined handoff-proof
  path keeps its existing contract; and
- later resumable stages recover solely from generic records plus fresh MySQL
  state.

## Open Decisions

1. The bounded generic representation and maximum accepted size for canonical
   MySQL GTID sets.
2. Whether a separately qualified native barrier can reduce how often complete
   GTID boundaries must be retained without weakening set-inclusion proof.
3. The exact generic records supplied by the upstream Kuberic
   provisional-admission and evidence protocols.
4. How the generic runtime proves retirement or ownership of a surviving
   `mysqld` process tree after host-process restart.
5. Which stable address and port allocations belong in controller status
   versus the local generic agent.
6. Whether any authority-critical variable may use `SET PERSIST` in a qualified
   profile.
7. Which offline data-root observations are stable enough to use before
   closed-access startup.
8. The disaster-recovery procedure when MySQL data survives but both
   controller and generic agent authority are lost.

## Related Documentation

- [MySQL high-level design](design.md)
- [PostgreSQL restart-stateless metadata proposal](https://github.com/youyuanwu/kuberic/blob/cfc27498ce1c67336d284e7b9c51813907e2efae/docs/features/postgres/stateless-metadata.md)
