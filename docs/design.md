# MySQL/Kuberic High-Level Design

This document defines the proposed safety and lifecycle contract for integrating
Oracle MySQL with Kuberic. It is an implementation design, not an implementation
claim.

## Status and Target Profile

### Current repository status

At the revision for which this design was written, this repository contains no
MySQL adapter, process supervisor, controller integration, deployment manifest,
or executable test fixture. The only delivered behavior in this work is
documentation. Words such as **must** and **requires** below are requirements
for future implementation; they do not mean that the behavior exists today.

The first proposed validation target is:

- Oracle MySQL 8.4 LTS on Linux, with the exact patch release pinned by a later
  implementation stage;
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

Safety depends on keeping four independent kinds of authority distinct.

| Concern | Single owner | Owned decisions and effects | Handoff evidence |
|---|---|---|---|
| Desired topology and lifecycle | Kuberic controller | Configuration intent, replica identities, role intent, operation ordering, and retry policy | Exact configuration/epoch and callback context |
| Durable effects | Runtime lifecycle host | Process/storage ownership, journals, callback fencing, cancellation, access reconciliation, and restart reconstruction | Exact process session, operation journal, and effect receipt |
| Native replication | MySQL Group Replication and the MySQL adapter | Group membership, native role/state, GTID history, recovery transport, clone/distributed-recovery proof, and native topology mutation | Fresh native observation bundle |
| Independent containment | Infrastructure fence provider | Terminating or isolating the exact old process or host independently of that `mysqld` and its service runtime | Durable, verifiable, exact-incarnation fence receipt |

Kuberic authority is not Group Replication membership. A member may be
`PRIMARY` in a native view while Kuberic write access is closed. Conversely, a
Kuberic role request does not prove that MySQL has reached a safe native state.
Only the Kuberic controller chooses desired lifecycle state; only the durable
runtime applies authorized effects; only MySQL supplies native history and
membership evidence; and only the independent fence provider may certify that
an unreachable old incarnation cannot continue serving writes.

### Controller, runtime, adapter, and fence boundary

The controller:

- assigns exact replica incarnations and configurations;
- sequences build, removal, catch-up, role, switchover, and failover intent;
- does not execute SQL, manage MySQL files, or interpret flattened GTID text;
- consumes typed outcomes, not unqualified “healthy” booleans; and
- fails closed when evidence is stale, incoherent, unauthorized, or missing.

The runtime lifecycle host:

- owns the exact child process, files, credentials, journal, and process
  session for each incarnation;
- validates authority immediately before and after every external effect;
- rejects completion from an old configuration, epoch, incarnation, process
  session, native view, credential generation, or operation attempt;
- reconciles Kuberic read/write status with native evidence; and
- reconstructs effects and access from durable state after restart.

The MySQL adapter:

- provides the custom stateful-service and replication interfaces;
- maintains typed MySQL identity, GTID, Group Replication, recovery, and
  observation evidence inside the application boundary;
- performs native bootstrap, join, clone, recovery, and topology operations
  only under an exact runtime authorization; and
- never grants client access from MySQL role alone.

The fence provider is outside both the managed `mysqld` and the runtime process
that supervises it. It must still act and provide proof if either process is
unresponsive. A process-local boolean, Kubernetes readiness result, object
deletion request, in-memory role, or database variable is not a fence receipt.

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

The first live fixture places all state under one fixture root but allocates an
isolated subtree per exact replica incarnation:

```text
<fixture-root>/<resource>/<replica-id>/<incarnation>/
  data/
  run/        # socket, pid metadata, process-session receipt
  log/
  tmp/
  config/
  credentials/
  journal/
```

Each member requires unique classic/MySQL-X/Group Replication ports as
applicable, socket path, PID metadata, log and temporary paths, server identity,
replication address, credentials, and data root. The implementation must derive
and persist allocations before process creation, detect collisions, and never
discover ownership by broad process-name, port-range, or directory scans.

The runtime writes an intent journal before every create or destructive action.
Before start, stop, signal, erase, clone, reseed, or cleanup it revalidates:

- the exact Kuberic identity and current authority;
- the journaled storage root and its ownership marker;
- the process-session handle and executable identity;
- the native `server_uuid` and group/member binding when a server is reachable;
- allocated sockets, addresses, and ports; and
- the operation attempt and destructive-work approval.

Cleanup removes only resources that the exact journal still owns. A mismatched
marker, process, native identity, path, or port is a foreign-resource error,
not permission to “clean up” whatever occupies the location.

### Process ownership and orphan containment

The runtime launches `mysqld` without placing secrets in arguments, records an
exact process-session handle, and owns its normal stop/reap lifecycle. Child
helpers and sockets belong to the same incarnation. Runtime restart must
reconcile the journal with operating-system and native evidence before adopting
or terminating a survivor.

Normal process ownership is not sufficient for failover. A `mysqld` can outlive
its supervising runtime. Automated unplanned failover therefore also requires
an independent provider that can target the exact process cgroup, virtual
machine, host, or network identity and prove the old incarnation is contained.

## Runtime Integration

### Existing interface mapping

The proposed service uses the existing custom-replication shape:

| Kuberic surface | Proposed MySQL responsibility |
|---|---|
| `StatefulServiceReplica.open` | Validate resource/partition identity, open the durable per-incarnation host, retain the partition, and return a factory-created custom interface bundle |
| `change_role` | Reconcile desired role with fresh native evidence; return no client address until independent access reconciliation grants it |
| `close` / `abort` | Close access first, cancel exact attempts, and stop/reap or quarantine the owned process according to the durable journal |
| `Replicator.open` | Return the exact application-owned Group Replication address only after identity and transport configuration validation |
| `change_role` / `update_epoch` | Record new authority, revoke stale work, and schedule native reconciliation without treating the callback as promotion proof |
| `current_progress` | Return only the compatibility marker described below; preserve GTID/view/recovery proof privately |
| `catch_up_capability` | Return only a conservative compatibility marker; never claim recoverability without retained-history and donor proof |
| `PrimaryReplicator` current/joint configuration | Perform the stateful, access-closed native admission contract and validate exact member descriptions |
| catch-up quorum | Freeze and verify a native boundary under exact sessions; do not use scalar callback values as sole proof |
| build / removal | Run journaled bootstrap/join/clone/reseed/removal workflows with typed native receipts |
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
full integration may require additive, compatibility-preserving capabilities:

1. **Structured application progress** carrying typed lineage/history,
   configuration/view, recovery, and retained-history evidence.
2. **Coherent observation receipts** binding all samples to exact sessions,
   native view, deadlines, query provenance, and privilege generation.
3. **External fence receipts** with provider verification, exact subject,
   input signature, validity, and revocation semantics.
4. **Resumable operation status** with exact operation identity, stages,
   native/provider receipts, cancellation, and recovered-completion outcomes.

Until such APIs exist, safety-relevant evidence stays in durable private
adapter state and callback results are conservative. The scalar
`current_progress`/`catch_up_capability` compatibility values may carry only a
runtime-assigned Kuberic configuration/admission generation. They must never be
derived from serialized GTIDs, transaction counts, view text, hash ordering, or
member count, and must never be the sole build, catch-up, election, promotion,
or access proof. A stage unable to meet that restriction must return explicit
unsupported rather than fabricate progress.

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
  `server_uuid`, group/member identity, and credential/trust generation;
- Kuberic configuration/epoch, desired role, and read/write access generation;
- collection start/end monotonic timestamps and a decision deadline;
- the native view identity and exact member list sampled for the bundle;
- each member's identity, address, role, state, and reachability;
- executed/received/purged GTID sets and recovery state, with query provenance;
- read-only defense-in-depth state and currently published client surfaces;
- privilege/capability checks and verified TLS peer identity; and
- bracketing samples sufficient to detect process, identity, or view change
  during collection.

Collection is deadline-bounded. A view, identity, session, credential
generation, or authority change invalidates the entire bundle. Data from
different sessions, attempts, credential generations, or native views must not
be merged into a synthetic observation.

### Observation outcomes

Outcomes are typed and fail closed:

- **absent**: the queried native object or member is authoritatively not present;
- **unreachable**: connection or transport could not be established;
- **permission denied**: authentication succeeded or was attempted, but the
  observer lacks required capability;
- **authentication/trust failure**: credentials, certificate, peer identity, or
  trust policy cannot be verified;
- **malformed/unsupported**: values or profile fall outside the pinned decoder;
- **partial/incomplete**: only part of the required multi-query bundle was
  collected before failure or deadline;
- **stale**: collection or decision deadline expired;
- **incoherent**: bracketing samples differ or fields describe different views,
  sessions, or identities; and
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

Each journal contains authorization, current stage, non-secret inputs, exact
process/storage/native bindings, external effects entered, native/provider
observations, completion receipt, cancellation, and last reconciliation time.
Retries with the same canonical identity are idempotent. A changed authority,
target, donor, view, or input creates a new attempt; the old result is rejected.

### Initial bootstrap

Bootstrap is permitted only for a new resource with no accepted native history.

1. Allocate three fresh Kuberic incarnations, storage roots, identities,
   addresses, ports, credentials, and journals.
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

Clone is destructive to the target and requires:

- exact target incarnation, storage marker, and destructive-work approval;
- target client/process containment;
- eligible, fresh, history-compatible donor evidence;
- separately scoped provisioning credentials and verified TLS;
- a new attempt if donor, target identity, authority, or accepted view changes;
  and
- post-restart re-binding of the process session and native identity.

Completion is not the clone command returning. The target must restart or
reconcile as required, present the expected storage/native binding, join the
intended group, complete recovery, and apply the frozen GTID boundary. The
journal then persists a receipt binding donor, target, authority, view, attempt,
and resulting history.

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
authority plus exact journal and fence evidence, and refuses changed or foreign
resources.

### Ambiguous restart after an external effect

Restart may occur after an effect succeeds but before its completion receipt is
durable. Every workflow therefore has a **reobserve before reissue** branch:

1. Reconstruct the exact pending attempt and authority from the journal.
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

Access transitions are journaled. Closure is acknowledged only after existing
publication is withdrawn, ordinary and administrative client paths are
contained as required, relevant sessions are drained or terminated, and the
exact result is reobserved. Opening is acknowledged only after fresh authority,
identity, native eligibility, fence, and routing evidence is validated
immediately before publication.

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

The required fence is issued by a provider independent of the target
`mysqld` and its runtime supervisor. Depending on the deployment stage, the
provider may terminate the exact process/cgroup or isolate the exact host,
network identity, storage writer, and direct-client path. It must contain
privileged and existing sessions, not just prevent new service discovery.

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

For planned transitions, an independent network/process containment mode may
block client and administrative write paths while preserving only the
explicitly required Group Replication path. Its scope and completion must still
be exact and verifiable.

## Planned Switchover

Planned switchover assumes a healthy quorum and reachable source and target.
The sequence is:

1. **Authorize** one exact operation under the current Kuberic configuration,
   source/target incarnations, accepted native view, and attempt.
2. **Close source write access** in Kuberic, withdraw the writable endpoint,
   stop new client writes, and drain or terminate relevant sessions.
3. **Freeze a boundary** by collecting a coherent post-drain source observation
   and recording its executed GTID set and native view. Recheck that no later
   client transaction was accepted.
4. **Drain/apply** until the exact target has executed the frozen source
   boundary and remains compatible in the same accepted lineage. Equality is
   not inferred from a scalar count.
5. **Contain the source** with an independently verified exact-incarnation
   client-write fence. Record the durable handoff/fence receipt.
6. **Transfer native primary role** using the pinned, validated single-primary
   Group Replication operation. If Group Replication chooses or reports a
   different primary, stop and reconcile rather than publishing the requested
   target.
7. **Reobserve** the resulting view, source containment, target identity/role,
   GTID boundary, recovery state, and Kuberic authority.
8. **Publish target writes** only after all previous receipts remain valid.
   Read publication is reconciled separately.

Any authority, process session, credential generation, member identity, native
view, or attempt change invalidates the in-flight switchover. Failure after
source closure leaves writes closed. Restart runs the ambiguous-effect
reconciliation path; it never infers completion from desired role.

## Unplanned Failover and Quorum Recovery

### Eligibility and selection

Native Group Replication may elect or report a primary, but Kuberic does not
publish that member for writes until the following proposed contract passes:

- the observation bundle proves a current quorum in one coherent accepted view;
- the candidate is an exact authorized incarnation in the current
  configuration, eligible and not still recovering;
- its executed GTID set contains the required quorum-confirmed boundary;
- candidate-only history is explained by accepted group lineage, with no
  unexplained divergence;
- deterministic selection uses only explicitly safe candidates and stable
  exact identity as a tie-breaker, not GTID text, count, or hash order;
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

Runtime or host restart creates fresh process and observation sessions. Access
starts closed. Reconstruction joins four durable evidence sets:

- current Kuberic authority, configuration, role intent, and access generation;
- identity, storage, process, allocation, and operation journals;
- fresh MySQL identity, Group Replication, GTID, recovery, and access-control
  observations; and
- still-verifiable external fence and routing receipts.

The reconciler:

1. validates repository resource identity and opens journals without applying
   their desired access;
2. discovers only journaled process/storage candidates and rejects foreign
   state;
3. assigns fresh process sessions or safely contains unadoptable survivors;
4. reobserves pending external effects before reissuing them;
5. rejects old callback completions and scalar progress snapshots;
6. reconciles native membership and topology with committed Kuberic authority;
7. revalidates fence, credential, trust, and routing generations; and
8. performs a new access decision from fresh evidence.

A persisted “granted” access value remains desired state, not effective state.
No endpoint is restored merely because it existed before the restart.

If restart lands between native configuration admission and Kuberic authority
persistence, the host contains the entered session, compares the exact native
effect with the candidate journal, and either records a proven recovered effect
under still-current authority or requires a new admission. It never promotes
partial membership to durable authority implicitly.

## Security Boundaries

### Principals and least privilege

Separate principals and secret references are required for:

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
must not appear in desired-state documents, journals, receipts, logs, status,
canonical signatures, SQL text recorded for diagnostics, process arguments,
or connection strings.

### TLS and trust boundaries

Verified TLS is required independently for:

- administrative and observation connections to each `mysqld`;
- client endpoint publication;
- Group Replication and distributed-recovery transport; and
- clone/provisioning transport where distinct.

The identity and trust policy for each boundary must be explicit. Successful
TLS on the administrative path does not prove replication-peer identity, and a
replication channel does not authorize Kuberic commands or fence-provider
actions.

### Rotation during an operation

Every authenticated session and observation is bound to non-secret credential,
CA/trust, peer-identity, and privilege generations. Expiry, rotation,
revocation, privilege loss, or peer-identity change during an observation or
long-running operation invalidates uncommitted evidence and prevents completion
credit until a fresh authenticated session revalidates the required native and
authority state.

Old sessions cannot extend old authority. Receipts record generation IDs and
verified peer/provider provenance, never secret material. Permission loss,
authentication failure, and native absence remain distinct outcomes. Safety
actions that can be proven complete may be journaled, but access stays closed
until trust is restored and the full decision is freshly evaluated.

## Staged Delivery

Each stage is additive. Passing a stage supports only the claims listed for that
stage.

| Stage | Deliverables and entry criteria | Exit evidence | Supported claim | Explicit non-claims |
|---|---|---|---|---|
| 0. Design | This document and concise README | Documentation review against specification | Intended contract is documented | No executable MySQL support |
| 1. Safety model | Typed identities, GTID-set relations, views, observations, journals, receipts, and state machines; no server required | Deterministic unit/property tests | Pure contract logic handles stale, partial, divergent, and retry cases | No process, SQL, or topology management |
| 2. Observe-only adapter | Pinned MySQL decoder, verified administrative TLS, capability checks, custom interface bundle with mutations rejected | Server-free decoder tests plus explicit one-member observation gate | Exact native identity/progress can be observed for the pinned profile | No bootstrap, writes, build, failover, or process ownership |
| 3. Host process and provisioning | Exact multi-instance manager, storage ownership, bootstrap, join, clone/reseed, restart journals | Explicit host-local build/restart/replacement gates | Development topology can be created and reconstructed on one host | No automated writable failover or production fault domains |
| 4. Access and topology transitions | Access reconciler, independent fence provider, switchover/failover/quorum rules | Host-local transition, orphan, stale-view, fence-expiry, and recovery gates | Controlled host-local transition contract for the pinned fixture | No cross-host or Kubernetes claim |
| 5. Controller integration | End-to-end Kuberic authority, callback, cancellation, and status flow | Deterministic races plus host-local controller scenarios | Kuberic-driven lifecycle for the validated host-local profile | No production or Kubernetes support |
| 6. Cross-host qualification | Real independent fault domains and fence backend | Network partition, host loss, direct-client, and storage-fence evidence | Only the exact environment that passes the qualification | No cross-region assumption |
| 7. Kubernetes qualification | Images, operator/agent integration, secrets, persistent storage, routing, and platform fence | Explicit lifecycle and fault suite on named versions/providers | Only the named Kubernetes/provider matrix | No generic Kubernetes portability |

Stage 0 is the only stage delivered by this repository change. Later rows are a
delivery contract, not a schedule or current feature list.

## Test Strategy

### Ordinary server-free gates

Ordinary tests must install no database, start no process or container, and
require no network or cluster. Future implementation should cover:

- exact identity serialization, binding, replacement, and stale-session
  rejection;
- GTID-set equality, subset, superset, incomparable/divergent, purged-history,
  and malformed-input cases without scalar sorting;
- native-view bracketing, deadline/freshness calculations, and provenance;
- partial collection after an intermediate query failure or deadline;
- authority callback success/failure around the non-atomic persistence window;
- effect-completed/receipt-not-persisted restart reconciliation;
- operation canonicalization, idempotent retry, destructive approval, donor
  change, target change, and stale completion;
- data-loss callback rejection before its support stage and stale evidence
  rejection after it is enabled;
- access state machines for PRIMARY-with-closed-access, stale native evidence,
  missing privilege, and invalid fence;
- planned/unplanned transition ordering and “no publish before fence”;
- credential, CA, peer-identity, and privilege rotation during observation and
  long-running work;
- secret redaction and receipt verification; and
- restart reconstruction with stale desired access and surviving processes.

### Explicit host-local gates

Live host-local gates are opt-in and separate from ordinary tests. When
explicitly requested, missing MySQL binaries, required plugin/profile support,
ports, certificates, privileges, or fence helper must fail with actionable
prerequisite errors rather than silently skip.

The planned matrix includes:

- one-member negative observation cases: absence, permission denial, invalid
  TLS, expired/rotated credentials, malformed metadata, and provenance;
- fresh three-member bootstrap, join, native recovery, and exact identity
  validation;
- clone/reseed, donor change, crash/restart, replacement, and foreign-resource
  refusal;
- switchover with source drain, frozen GTID boundary, containment, and delayed
  publication;
- failover with old-primary survival, direct-client attempts, exact fence
  verification, quorum loss, conflicting/stale views, and divergent histories;
- runtime death with surviving `mysqld`; and
- cleanup after normal completion, setup failure, cancellation, and
  uncatchable runtime termination.

Every gate records product/version/profile, fixture identities, native view,
non-secret evidence, expected receipts, and supported claim. Live success on one
host does not establish production availability.

### Later Kubernetes and fault gates

Kubernetes tests begin only after process, fencing, routing, and controller
contracts pass below the cluster layer. They cover Pod/host death, API
partition, network partition, persistent-volume reuse, Secret rotation,
readiness versus direct-client access, controller restart, old-primary
survival, exact infrastructure fencing, and replacement. Explicit invocation
must fail on missing cluster/provider prerequisites.

No Kubernetes lifecycle or fault-tolerance claim is supported until a named
version, storage class, network model, and fence provider pass the full suite.

## Limitations and Unsupported Modes

Unless a later validated stage says otherwise, this design does not support:

- any current executable MySQL/Kuberic behavior in this repository;
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
- journals, stale-result rejection, restart reobservation, and conservative
  access reconstruction;
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
