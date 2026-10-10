# MySQL Restart-Stateless Metadata

## Status

Partially implemented architecture. This document defines the persistence and
restart model for MySQL lifecycle integration. The delivered bounded service
owns fresh process generations with memory-only context and separate persistent
data/disposable scratch roots. It also composes exactly three fresh members for
one designated Group Replication bootstrap and two sequential joins while the
same host retains exact ownership and attempt context. It does not claim
restart continuation, access publication, switchover, repair, failover, or
Kubernetes integration.

The delivered `kuberic_mysql::adapter` module already satisfies the
application-state part of this proposal: it accepts one observation request,
keeps only attempt-local state, and returns typed evidence without opening a
durable store.

The delivered `kuberic_mysql::service` module applies the same boundary to one
process and to the fixed fresh three-member topology: it writes no application
metadata, journal, receipt database, state JSON, workflow cursor, or adoption
record. Bootstrap/join capabilities, credentials, observations, and transition
credit remain in memory. Loss of manager ownership or topology-attempt context
requires closed-access fixture reset even if surviving MySQL state appears
healthy.

This design applies the ownership principle from the
[PostgreSQL restart-stateless metadata proposal](https://github.com/youyuanwu/kuberic/blob/cfc27498ce1c67336d284e7b9c51813907e2efae/docs/features/postgres/stateless-metadata.md)
to MySQL:

> The MySQL application replicator and adapter must not persist private
> recovery metadata.

They may keep observations, queues, callback context, connection state, and
process handles in memory. They may perform authorized native operations and
submit typed evidence to Kuberic. They must not own a metadata file, embedded
database, operation journal, receipt store, workflow cursor, fence store, or
other independent recovery authority.

Durable MySQL facts belong to the MySQL data directory. Durable orchestration
facts belong to Kuberic's generic controller and replica-agent facilities.
Scratch files are disposable.

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
- the component observes both and submits typed evidence;
- Kuberic durably accepts evidence before an irreversible action advances; and
- every replacement process starts with external access closed.

This is a component-boundary rule, not merely a directory-layout choice.
Moving a MySQL-specific journal into the data directory, a Kubernetes object,
another SQLite database, or a remote service would still violate the design.
Records required by the protocol must be generic Kuberic controller or agent
records with typed MySQL evidence.

## Goals

1. Keep the MySQL adapter and future lifecycle replicator restart-stateless.
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
| Per-replica authority, storage binding, process retirement, local fences, destructive work, and effect receipts | Generic Kuberic replica agent | Generic agent store and typed private protocols |
| Database contents, system tables, `server_uuid`, executed and purged GTIDs, binary logs, and engine recovery state | MySQL data directory | Offline validation where qualified, then fresh closed-access SQL observation |
| Current Group Replication membership, view, role, recovery state, and active settings | MySQL engine | Fresh native observation; never restored from an application cache |
| Process handles, connection pools, callback tasks, deadlines, and attempt-local observations | Process memory | Recreated |
| Sockets, PID files, logs, generated configuration, projected credential files, and temporary operation files | Scratch storage | Recreated or discarded |
| Independent old-primary containment after the PoC | External fence provider plus generic Kuberic receipt | Provider verification under current authority |

The MySQL component may interpret native evidence, but it does not become the
durable owner of that evidence. It submits bounded typed evidence to Kuberic,
which validates the operation identity and authority before persistence.

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
- a planned-switchover frozen GTID boundary and its exact source observation;
- authorized native pre-view and permitted post-view transitions;
- accepted native operation completion evidence; and
- controller-visible terminal transition results.

The MySQL component accesses this state only through typed runtime capabilities
and evidence submission. It must not query the generic agent database directly.

## MySQL-Specific Reconstruction Constraints

### GTID sets are structured boundaries

MySQL history is a set of SID, optional tag, and interval components. It has
equal, proper-subset, proper-superset, and incomparable relations. A maximum
sequence number, interval count, text digest, or last observed GTID cannot
replace the complete relation.

A durable GTID receipt must therefore:

- use the canonical parser and normalized representation from
  `kuberic_mysql::core`;
- bind the exact resource, storage incarnation, `server_uuid`, group identity,
  configuration, epoch, process session, native view, and operation;
- distinguish executed, purged, required, and observed histories;
- have an explicit encoded-size and interval-count bound;
- reject truncation, lossy summaries, and scalar comparison; and
- fail closed if the evidence cannot fit the qualified bounded format.

The Stage 2 PoC may keep its planned-switchover boundary in memory because it
does not resume an interrupted handoff. Before Stage 3 claims restart
continuation, Kuberic must provide a generic bounded GTID evidence record or a
separately qualified native certificate that proves the same set relation. A
hash alone is not sufficient because a target must prove set inclusion.

### Group Replication views are fresh evidence

Current Group Replication membership, role, member state, and view identifiers
are not application recovery metadata. They are observed from the live engine.
After restart, the component must not restore a cached view and treat it as
current.

Kuberic may durably record an operation's accepted pre-view and permitted
post-view as transition evidence. A fresh observation must still prove that the
current view is the allowed result of that exact operation. Unrelated member
loss, replacement, identity change, or primary selection is not accepted merely
because the operation expected some view change.

### Bootstrap is an irreversible authority boundary

Enabling Group Replication bootstrap mode on the wrong member can create a
conflicting group. The generic agent must commit an exact bootstrap preparation
record before the component enables bootstrap mode. That record binds:

- the fresh storage incarnation and expected `server_uuid`;
- the exact group name and initial member set;
- the designated bootstrap member;
- configuration, epoch, process session, and operation ID;
- the expected empty/fresh topology condition; and
- the only allowed native post-state.

The component disables bootstrap mode immediately after entering the native
operation and then reobserves the group. If the action completed but its
completion acknowledgement was lost, a replacement process uses the generic
pre-action record and fresh native state. It never bootstraps a second group
because no application cursor survived.

### Join and distributed recovery are level-triggered

A join intent names the exact target, group, donor eligibility, expected
pre-view, required GTID boundary, and permitted post-view. The component:

1. obtains a generation-bound runtime capability;
2. submits preparation evidence to the generic agent;
3. enters the native join only after durable acknowledgement;
4. observes membership and recovery from a fresh session;
5. waits for the required GTID relation;
6. submits completion evidence; and
7. returns success only after the agent accepts that evidence.

On restart, Kuberic repeats the desired effect. The component reobserves before
reissuing the native action. It may recognize an already completed join, retry
an idempotent join under unchanged inputs, or remain closed. It does not resume
an application-private stage number.

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

## Private Generic Evidence Protocol

The public Kuberic replication interfaces need not expose the generic store.
The private hosting boundary provides generation-bound capabilities:

```text
controller commits cluster intent
    -> local agent commits exact operation authority
    -> application observes and submits preparation evidence
    -> local agent durably accepts the pre-action record
    -> application performs the authorized native or process effect
    -> application freshly observes the postcondition
    -> local agent durably accepts completion evidence
    -> callback returns
    -> controller accepts reported completion
```

Preparation evidence is not current authority and cannot open access. A
capability is valid only for its exact operation, immutable inputs, authority,
storage incarnation, and process generation.

The referenced PostgreSQL architecture describes these generic capabilities as
proposed runtime work. Until Kuberic provides them, MySQL may implement only
the non-resumable Stage 2 fixture boundary. It must not emulate missing generic
facilities with an application-owned store or claim restart-safe continuation.

If the process exits, the capability dies with it. The generic agent may issue
a continuation capability only after retiring the predecessor, validating the
same operation and immutable inputs, and determining the remaining legal phase
from durable records plus fresh MySQL observation.

### Lost acknowledgement

Every evidence submission has an operation ID and canonical input signature.
If the agent committed a record but its response was lost, retry returns the
same accepted result. If the native action completed before completion evidence
was committed, restart uses:

1. the committed pre-action record;
2. current controller intent;
3. retired predecessor process authority; and
4. a fresh native observation.

Completion may be recovered only when those facts prove the exact allowed
postcondition. Otherwise access remains closed and the operation requires
retry, compensation, fixture reset, or operator recovery according to its
delivery stage.

## Startup Reconstruction

A replacement MySQL lifecycle component follows this sequence:

1. Begin with external read and write access closed.
2. Open and validate the generic agent authority.
3. Validate resource, replica, incarnation, Pod/PVC, and canonical data-root
   bindings.
4. Load admitted configuration, epoch, desired role, access intent, local
   fences, and pending generic effects.
5. Retire the predecessor process session.
6. Inspect the data root using only qualified offline surfaces.
7. Reconcile generated configuration and pre-start fences.
8. Reattach to an exactly owned survivor or start `mysqld` for internal control
   only.
9. Collect a fresh coherent identity, Group Replication, GTID, recovery, and
   access-state observation.
10. Compare native facts with admitted topology and pending effect inputs.
11. Submit reconstruction or operation-completion evidence to the generic
    agent.
12. Reconcile desired topology and access level-triggeredly.
13. Publish an endpoint only after current authority and fresh native evidence
    satisfy the normal access invariants.

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

This is deliberately not a restart protocol. If the component, retained child
ownership, credential generation, pending effect, or topology-attempt context
is lost, the surviving topology is not discovered, adopted, or resumed. Client
access remains closed and the fixture is reset. A later resumable stage must
use the generic durable pre-action and completion protocol described above.

### Controlled switchover

The Stage 2 PoC remains deliberately non-resumable:

1. close source access and drain client work;
2. freeze a coherent source executed-GTID set;
3. wait for the exact target to contain that set;
4. transfer native primary while source and target remain reachable;
5. validate the permitted post-view and compatible histories;
6. stop and reap the exact old-source process; and
7. open target access only after fresh authority and native validation.

If the component restarts during this PoC operation, access remains closed and
the fixture is reset. No private journal is added to resume it.

A later resumable handoff moves these facts into generic operation records:

- source and target incarnations;
- source-close completion;
- frozen structured GTID boundary;
- native pre-view and permitted post-view;
- primary-transfer preparation and completion;
- source process-stop or independent fence evidence; and
- target access-grant evidence.

The application reconstructs each phase from those records and fresh native
postconditions.

### Access reconciliation

Access is an effect, not a local Boolean. Every replacement component begins
closed. Opening requires:

- current admitted authority and access generation;
- exact storage and process identity;
- fresh supported MySQL product and native identity;
- coherent accepted Group Replication membership and view;
- compatible GTID history;
- required recovery completion;
- accepted source containment for writable transitions;
- no conflicting or incomplete generic effect; and
- successful endpoint and credential validation.

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
6. Stale process generations and callback capabilities cannot complete work.
7. Group Replication role and read-only variables are evidence, not authority
   or independent fencing.
8. GTID histories retain structured set relations and are never scalarized.
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
16. Process-session retirement does not transfer old callback completion
    rights to the replacement session.
17. Scratch loss may repeat work but cannot change durable authority.
18. Missing, oversized, stale, incoherent, or unsupported evidence fails
    closed.
19. No MySQL-specific store duplicates controller or agent workflow state.
20. The Stage 2 PoC does not claim restart continuation merely because it uses
    the restart-stateless ownership model.

## Adoption Plan

### Phase 1: Align contracts

1. Mark the MySQL application boundary as restart-stateless.
2. Classify every planned durable fact as MySQL-native, controller-owned,
   generic-agent-owned, or disposable.
3. Define bounded typed MySQL evidence for identity, GTIDs, native views,
   process completion, and access effects.
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
4. Execute one controlled switchover with in-memory operation context.
5. Permit component replacement only while a surviving fixture host retains
   exact ownership context and proves effective closure and that no mutation is
   pending.
6. If that host context is lost, treat the interruption as a closed-access
   fixture reset regardless of apparently healthy MySQL state.

Items 1 through 3 and the reset rule in item 6 are delivered for the bounded
fresh topology. Controlled switchover and component replacement in items 4 and
5 remain future work.

### Phase 3: Generic durable continuation

1. Add or consume Kuberic's private typed evidence protocol.
2. Add generic pre-action and completion records for initialization, bootstrap,
   join, process stop, access, and switchover.
3. Add provisional admission for callbacks that must prepare native state
   before final authority is committed.
4. Add process-session retirement and reattachment.
5. Add bounded structured GTID receipts.
6. Reconcile effect-completed/receipt-not-committed failures from fresh MySQL
   observations.

### Phase 4: Repair and advanced safety

Future Clone, reseed, replacement, automated failover, and independent fencing
reuse the same generic protocol. They may add new typed evidence, but they do
not add a MySQL application metadata store.

No metadata migration is required today because this repository has not
implemented a MySQL-specific durable store.

## Validation Strategy

The architecture is accepted only when tests demonstrate:

- no MySQL component creates or opens a private durable metadata store;
- replacement processes reject predecessor capabilities and receive no access
  credit until effective client-path closure is independently verified;
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
- structured GTID boundaries survive generic encoding without scalarization or
  truncation;
- unexpected views, identities, histories, or oversized evidence fail closed;
- Stage 2 resets the fixture after ownership-context loss rather than inferring
  quiescence or claiming resume from MySQL state; and
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
