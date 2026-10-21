# MySQL Recovery Authority and Retained-Owner Handoff Contract

## Status and decision

**Contract-only Stage 3 deliverable; runtime integration remains blocked.**
Assessed on 2026-10-18 against dependency pin
`be4f83b43567c59f48472025d55dd731efecc4c6` (P), initially observed upstream main
`c0e52ee6ef361d86cf9e984e1f0682bc1f479013` (H, “Port unselected SF V1 copy
enumeration and reliable sending”, #169), and subsequently observed current main
`11d804425bbdbe7759dfd18208a0d73ce667a9f4` (U, “Port unselected SF V1 copy
build gates and retargeting”, #170). That assessment did not justify a repin.
The current dependency is now
`e2a47b6c631c974aa0135e81c3a7e3a439980ded`; P/H/U remain historical evidence,
not aliases for the current pin. The later
[in-process harness assessment](mysql-hosted-lifecycle.md#upstream-in-process-harness-applicability)
does not qualify this recovery contract.

The target is deliberately narrow: move an **exactly retained closed reopened
MySQL owner** into one fresh generic replicator, initially unassigned and
access-closed. The public contracts inspected at P and H do not supply the
combined accepted recovery envelope and physical-retirement/ownership handoff
needed for that target at P, H or U. Generic recovery is not absent; its existing mechanisms
must not be confused with this missing application boundary.

Today's [closed safe reopen](mysql-safe-reopen.md) remains the supported API.
[`MysqlReopenAuthority` and `MysqlReopenRenewal`](https://github.com/youyuanwu/kuberic-mysql/blob/22f7666fc51fcbefc541597a58514168ecf05ef3/crates/kuberic-mysql/src/service/reopen.rs#L23-L146)
validate complete supplied scope, **not its authentication or acceptance**.
[`MysqlClosedReopened`](https://github.com/youyuanwu/kuberic-mysql/blob/22f7666fc51fcbefc541597a58514168ecf05ef3/crates/kuberic-mysql/src/service/reopen.rs#L322-L364)
offers only observation and terminal close; there is no manager/socket/engine
extraction. Production
[`open`](https://github.com/youyuanwu/kuberic-mysql/blob/22f7666fc51fcbefc541597a58514168ecf05ef3/crates/kuberic-mysql/src/service/kuberic/service.rs#L342-L430)
accepts only `New`. This document changes none of those boundaries.

## Immutable public-contract assessment

The following is existing-source evidence, not proposed API. H links below
describe the same production routes at P: all 24 controller source files and
48 of 49 preexisting runtime source files compared byte-identically; the sole
remaining runtime-file change inserts private `mod v1;`. H adds the unselected
V1 subtree. See the [immutable comparison](https://github.com/youyuanwu/kuberic/compare/be4f83b43567c59f48472025d55dd731efecc4c6...c0e52ee6ef361d86cf9e984e1f0682bc1f479013).

The subsequent [H → U comparison](https://github.com/youyuanwu/kuberic/compare/c0e52ee6ef361d86cf9e984e1f0682bc1f479013...11d804425bbdbe7759dfd18208a0d73ce667a9f4)
is one commit: documentation/notices and private V1 copy sender/operation/module
files only. None of the controller, agent, public context, selected factory,
progress or retirement sources cited below changes. U's
[`CopySender` scope](https://github.com/youyuanwu/kuberic/blob/11d804425bbdbe7759dfd18208a0d73ce667a9f4/kuberic-runtime/src/replicator/v1/copy_sender.rs#L9-L18)
explicitly excludes full Build/role orchestration; it remains under the
[`private, factory-unregistered module`](https://github.com/youyuanwu/kuberic/blob/11d804425bbdbe7759dfd18208a0d73ce667a9f4/kuberic-runtime/src/replicator/v1/mod.rs#L1-L20).
Its copy completion ownership is not retained native-child ownership.

| Route inspected beyond the context structs | What exists, and why it is insufficient for this handoff |
|---|---|
| Controller accepted authority | Public [`PublicV1HistoryApi` / `PublicV1SessionRenewalApi`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-controller/src/cluster_api/public_v1_history.rs#L7-L64) and production [`KubeClusterApi`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-controller/src/cluster_api.rs#L2422-L2612) observe/persist/dispatch real generic authority. They do not consume a retained MySQL child or deliver it to a custom factory. |
| Authenticated agent → application Open | Agent [`execute`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/host/service.rs#L871-L1013) checks authentication, process session, resource and replica. Its [`Open` preparation](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/host/service.rs#L676-L713), [`context construction`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/host/hosting.rs#L291-L316) and [`lifecycle invocation`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/host/public_lifecycle_program.rs#L235-L278) pass ordinary Open context, not the combined accepted recovery/native-owner capability. |
| Public application/factory context | Open has identity/mode/partition at [P](https://github.com/youyuanwu/kuberic/blob/be4f83b43567c59f48472025d55dd731efecc4c6/kuberic-runtime/src/application.rs#L39-L44) and [H](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/application.rs#L39-L44). Factory context exposes identity, session, partition/access observations: [P:677–841](https://github.com/youyuanwu/kuberic/blob/be4f83b43567c59f48472025d55dd731efecc4c6/kuberic-runtime/src/replicator/mod.rs#L677-L841), [H:678–842](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/replicator/mod.rs#L678-L842). Its [replacement-progress/Build/storage-authority hooks](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/replicator/mod.rs#L743-L907) are crate-private. Neither context exposes the complete accepted envelope plus native custody. |
| Logical session renewal | [`ProcessSessionRenewalRequired`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/protocol/public_operations.rs#L1158-L1378) validates changed session/creation, same storage, newer epoch and fresh role-None/access-closed report. The production [`reconciler`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-controller/src/reconciler.rs#L233-L365) builds renewal from reports and sets `predecessor_contained: true`. That cloneable record/boolean is not a linear witness of native stop/reap or owner transfer. |
| Physical resource retirement | Production [`retire_public_v1_incarnation`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-controller/src/cluster_api.rs#L3009-L3075) really requests exact scaffolding deletion; [`deletion`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-controller/src/cluster_api.rs#L581-L633) concerns Service/Pod/PVC resources. It does not return an in-memory closed owner on retained native storage. Logical retirement, requested resource deletion and physical child custody are distinct. |
| Factory attachment and terminal lifecycle | Host [`create_replicator`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/host/hosting.rs#L1187-L1276) reserves/binds one coherent creation; ordinary [`Close/Abort`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/host/public_lifecycle_program.rs#L435-L482) and [`RunningReplica` shutdown/wait](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/host/process.rs#L562-L596) do not return retained native custody. Reservation cancellation/abort is not the missing ownership-return contract. |
| Replacement progress and selected built-in recovery | Public [`progress queries`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/replicator/traits.rs#L44-L66), [`host projection`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/host/hosting.rs#L665-L710) and selected [`Stateless` retained-progress validation](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/replicator/stateless/mod.rs#L2740-L2841) are real. They do not authorize translating matching MySQL GTIDs into public scalar or committed progress. The public selected default remains [`Stateless`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/replicator/mod.rs#L1053-L1077). |
| Operation recovery / data loss | Generic [`operation recovery`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/host/operation_recovery.rs#L11-L77) handles replay dispositions, not native adoption. Typed [`data-loss authorization`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/protocol/public_operations.rs#L1024-L1108) concerns successor history, not this same-history closed handoff. |
| Testing and newly ported substrate | Direct [`host renewal/lifecycle helpers`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/host/process.rs#L162-L559) are `testing`-gated. H's [`private V1 module`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/replicator/v1/mod.rs#L1-L19) is explicitly unselected; its [`ReplicaManager`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/replicator/v1/replica_manager.rs#L9-L19) is not native process ownership or transport authentication. Neither is a production recovery integration. |

### Search scope and limits

This assessment used read-only `gh api` source archives at P and H, followed by
U's complete comparison/file inventory and changed copy-operation patch plus
actual copy-sender/module source checks.
Discovery searched runtime/controller/examples Rust source for recovery,
replacement, renewal, retirement, handoff, ownership, adoption, Open/factory
contexts, progress and Build; it then traced the cited production calls,
exports/reexports, private hooks, feature gates, lifecycle cancellation and
alternative built-in/testing/unselected routes. It also inspected the
[`public API inventory`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/tests/source_public_api.allowlist#L157-L208)
as supporting evidence, not proof of reachability.

The result is **no combined public selected custom-MySQL route found in those
sources**, not a claim that Kuberic has no generic authority/retirement or that
unpublished embeddings cannot exist. It is source assessment, not live runtime,
native parity or production qualification. No new runtime tests were run for
this documentation-only change.

## Required public production contract

Everything below is a **future acceptance requirement**, not an available API,
proposed Rust signature, new adapter capability or completed runtime gate.
Upstream may choose its representation. A trusted host callback or unforgeable,
scoped capability may establish provenance; a public constructor, serialization,
matching struct, digest, containment boolean or arbitrary caller signature does
not. No particular cryptographic mechanism is prescribed.

### A — Accepted recovery envelope

Generic Kuberic must supply the custom application/factory an authentic
host-admitted, operation-specific envelope from its own accepted state. It must
provide a production currentness/revocation check at admission, before physical
transfer and immediately before attachment completion. Revalidation and
consumption must be ordered against concurrent revocation; a stale check followed
by an unguarded ownership move is insufficient.

| Required binding | Acceptance rule; missing/stale/substituted outcome |
|---|---|
| Resource and partition | Exact resource UID and partition, bound to the admitted replica rather than a caller-selected root; mismatch rejects before native mutation. |
| Replica and storage incarnation | Exact replica identity, storage ID and storage generation, bound to retained roots/device/inode under trusted host ownership. Same-storage transfer only; a replacement storage incarnation requires other work, not relabeling. |
| Generic history | Complete accepted `HistoryContext` (resource/protocol/history/data-loss scope), identical across this same-history handoff. Data-loss or lineage changes reject this path. Native GTIDs cannot mint it. |
| Predecessor authority | Exact generic process session, creation, epoch, configuration, operation and positive revision sealed for the retained owner. Retired/stale/nonmatching scope cannot authorize new work; retirement must match this exact predecessor. |
| Replacement reservation | Host-reserved distinct process session and fresh coherent creation identity; unchanged replica/storage/partition/history; strictly newer same-data-loss configuration epoch, different configuration and operation, and strictly higher revision than the sealed predecessor. Caller-invented or reused IDs reject. Reservation precedes use in MySQL renewal; completion/report follows actual attachment. |
| Accepted intent and freshness | Canonical immutable operation inputs and accepted revision tied to the exact old/new identities, one attempt and its cancellation/deadline. Conflicting reuse, expired/cancelled attempt, revoked/superseded intent or unavailable admission rejects; “latest known” cache is not current acceptance. |
| Access and authorization | Effective read/write ingress closed; old role, PC/CC, peer/Build authorization and access generations invalidated for this replacement. A desired persisted grant never establishes effective access. New epoch/configuration fields describe accepted scope, not installed native/generic role or configuration. |
| Native expectation binding | Independently established expected native UUID/group/server ID/addresses and **complete executed and purged GTID sets** bound to this accepted attempt. Generic host authenticates the authorization to compare; MySQL attests the actual native facts. Self-reading a boundary and declaring it accepted is insufficient. |

The required stricter local renewal relations are already structural checks in
[`reopen.rs:115–146`](https://github.com/youyuanwu/kuberic-mysql/blob/22f7666fc51fcbefc541597a58514168ecf05ef3/crates/kuberic-mysql/src/service/reopen.rs#L115-L146);
the missing step is genuine generic issuance/currentness, not more equality.
Generic storage generation and native filesystem identity, generic process
session and attested native-child session, generic creation and local observation
attempt remain separate bindings.

### B — Physical retirement and linear custody

The generic runtime must expose a public production coordination boundary for
the exact predecessor's lifecycle work, callback/peer admission, outstanding
tasks and physical ownership. It must invalidate old generic authority and
establish physical retirement **without destroying retained data/native logs**.
The MySQL-owned graceful-stop capability remains responsible for proving exact
child stop/reap; generic session retirement is not a substitute.

For a predecessor that participated in a hosted replicator, the boundary must
account for and drain/contain all old callbacks, peers and independently held
owners before new attachment. A never-hosted retained owner still needs
authentic generic admission; it must not fabricate a predecessor runtime.
No surviving process may be adopted from a PID, socket, native UUID or observed
healthy state. Closure proofs must bind the exact physical/native issuance and
the full accepted predecessor, not merely storage name or an empty session list.

Transfer must consume exactly one closed retained-owner capability directly
into exactly one host-reserved fresh creation. It must preserve private custody,
native-log inventory, monotonic durability qualification, lifetime issuance and
terminal task poison. There must be no interval with two active owners, an
unowned child, detachable cleanup or a public raw-manager/socket/engine escape.
Generic facts remain in generic Kuberic stores; native facts remain in MySQL.
No MySQL-private durable handoff receipt, journal, adoption record or cursor
may bridge this gap.

### C — Ordered ownership and failure contract

The following is required sequencing, **not executable integration today**.
Generic implementation may combine steps atomically but must preserve the
admission/custody dependencies. “Contain” always means only the exact retained
child; a failure is not proof that containment succeeded.

| Cut / stage | Sole custody and required success | Error, cancellation, drop or stale authority |
|---|---|---|
| 0. Admit and reserve | Trusted generic host authenticates A, closes old admission and reserves replacement identity without attaching/reporting success. Original manager or closed handle remains sole native owner. | No success/credit. Reject before transfer; any already begun closure stays closed. Reservation is cancelled, never reused as completed creation. |
| 1. Settle predecessor | Owning operation holds exact manager and any independent proxy. Drain old generic work/peers; satisfy effective native closure. For the supported singleton proxy path, terminal surrender and account retirement precede native inactive/read-only capture. | No fabricated closure. Task failure/timeout and unidentified backend history remain permanent poison. Owning cancellation/drop contains only the exact child, retains native evidence/logs for inspection and never blesses interrupted commands. |
| 2. Graceful retirement | `MysqlStoppedReopen` exclusively retains manager, full boundary, logs and closure provenance after successful graceful child exit/reap. Generic host separately seals exact predecessor-session retirement. | Signal/escalation, unknown owner, logs/root/history/durability mismatch or incomplete generic retirement reject. No PID-based recovery, data-only reopen or second token. |
| 3. Closed reopen | Consumed stopped capability owns a fresh attested native child; renewed A is checked against the sealed predecessor and host reservation. Full native history/identity/storage/closure is re-read. Only a closed handle results. | Existing exact-owner failure/drop semantics apply: no success capability or public credit; preserve retained data/logs, keep admission closed. No retry may erase historical poison. |
| 4. Consume and attach | Revalidate current A and physical B atomically with consumption; one owning transfer holds the closed child until the exact fresh creation's coherent interfaces take custody. No old role/configuration/provider cache is copied. | Before consumption, caller retains the closed handle. After consumption, the owning transfer must either return that same closed capability under an explicit safe contract or terminally contain the exact child; it must never return a raw manager or detach work. Ambiguous completion stays closed/unsuccessful and generic reservation is invalidated, not guessed from native state. |
| 5. Complete closed attachment | Final admission/currentness check; host reports only the actual new process/creation identity, unassigned role and effective closed access. Generic intent/completion persistence remains generic. | Revocation, failed attachment, cancellation or lost acknowledgement never yields implicit serving/progress. Generic recovery decides replay disposition; absence of trustworthy ownership requires reset, not private-record reconstruction. |

For a handle already closed-reopened, stages 1–3 are **continuously retained
provenance to verify**, not permission to repeat terminal surrender or mint a
second stop/reopen token. Future owner transfer must not retroactively authenticate
a caller-fabricated earlier renewal. Its sealed predecessor/replacement scopes
must match actual accepted generic issuance, with currentness rechecked at cut 4.

Today's owning capture/reopen/drop boundary is
[`reopen.rs:215–364`](https://github.com/youyuanwu/kuberic-mysql/blob/22f7666fc51fcbefc541597a58514168ecf05ef3/crates/kuberic-mysql/src/service/reopen.rs#L215-L364);
production proxy surrender is
[`proxy.rs:870–984`](https://github.com/youyuanwu/kuberic-mysql/blob/22f7666fc51fcbefc541597a58514168ecf05ef3/crates/kuberic-mysql/src/service/proxy.rs#L870-L984).
Ordinary terminal `close()` removes disposable scratch while retaining data;
failure/drop preserving retained roots/logs is not a reusable recovery token.
The proposed transfer error contract must explicitly choose its supported
outcome rather than assume existing factory abort returns the closed owner.

### D — Initial fresh replicator outcome

Successful future attachment grants **none** of the predecessor's role, PC/CC
configuration, peer authorization, access, public scalar progress, committed/
provider progress or donor credit. The fresh replicator is unassigned with
effective ingress closed, fresh creation/generic process identity and the
separately attested new native-child session. The former proxy remains terminal;
no new serving proxy is issued by this boundary.

Retained exact GTID equality is not public scalar eligibility. Gaps, tags and
foreign/multiple sources remain structured; they cannot be flattened into an
LSN or commitment. Later independent [election-progress qualification](mysql-election-progress.md)
and generic role/epoch/configuration/peer/access protocols are separate work.
If public host attachment/reporting requires immediate scalar/provider success,
the integration must remain blocked until a public closed/no-progress outcome
or independently qualified mechanism exists. Do not return dummy zero, cached
progress or a fabricated provider to satisfy a trait.

## Fail-closed eligibility and future qualification gates

These gates are requirements for a later integration. **They were not executed
or passed by this documentation change.** Existing primitive qualification in
[safe reopen](mysql-safe-reopen.md#qualification-and-nonclaims) does not qualify
generic production transfer.

| Gate | Required positive evidence | Negative cases that must prevent success |
|---|---|---|
| Q1: Public production admission (A) | Selected production host supplies full accepted scope through its authenticated route; custom service consumes it without private store access. | Caller-built equivalent envelope, testing-only helper, unselected module, wrong resource/partition/storage/history/operation/revision, unknown provenance. |
| Q2: Freshness and single consumption (A,C) | Currentness/revocation serialized with transfer; exact newer same-data-loss reservation consumed once and report refers to actual attachment. | Stale/revoked authority before or during any await/final admission, concurrent double consume, duplicate creation, altered canonical input, reused predecessor ID, cancellation/deadline expiry. |
| Q3: Physical predecessor retirement (B,C) | Exact old generic work/peers fenced and drained plus actual owned child graceful reap, without deleting retained native logs. | `predecessor_contained=true` alone, requested Pod/PVC deletion, unknown PID/survivor, active old callback, another manager or session's retirement evidence, escalation/signal death. |
| Q4: Ingress lifetime (B,C) | Never-issued eligible manager or genuine exact singleton terminal proxy custody, complete accept/session/backend/account settlement, inactive native SQL/X/GR/async/event paths. | Idle/unpublished/revoked proxy alone, direct/generic issuance, multiple escaped capabilities, Build TCP, unidentified backend, failed/timed-out task join, stale facade racing publish/rearm, fake-success cleanup. |
| Q5: Native identity/history/durability (A,B) | Exact retained roots/device/inode, native UUID/group/server ID/addresses, full executed/purged sets with purged subset, retained native logs/options, continuous monotonic durability and fresh child attestation. | Substituted roots/options/logs/process, changed/partial/scalarized GTIDs, missing scratch/binlogs, restored settings after disqualification, unknown predecessor lifetime, persisted settings silently enabling ingress. |
| Q6: Asynchronous custody (C) | Unpolled/awaiting/cancelled/error/dropped transfer at every cut owns and contains only its exact child; no detached task or foreign-owner interference; explicit attachment-failure outcome. | Orphan/double owner, leaked manager/socket, proof after cancellation, ambiguous success promoted from observed state, failure that deletes retained data, task poison cleared by retry. |
| Q7: Fresh closed attachment (D) | One real public custom-factory creation, unassigned and closed, no inherited credit; prior proxy remains terminal. | Old role/PC/CC/peers/access, stale progress cache, dummy scalar/provider, commitment/election/donor credit, premature Existing acceptance, endpoint publication or serving. |

Native portions of any future qualification require the existing exact Oracle
MySQL Community **8.4.11** Linux profile, one Cargo job/target tree and serial
native tests. Preserve short UDS paths (`<108` encoded bytes), absent-target
archive fixture handling, exact owned helper cleanup and foreign-process
noninterference. Native and generic positive/negative tests must exercise the
actual selected production interface; a fixture-only route cannot qualify Q1.

## Explicit integration blockers and reopening criteria

| Blocker | Exact missing boundary / criterion to reconsider |
|---|---|
| R1: Accepted-envelope delivery/currentness | Generic controller/agent acceptance exists, but public Open/factory does not deliver A with production issuance, consumption and revocation ordering. Reconsider only with immutable public selected contract and real production call path covering Q1–Q2. |
| R2: Physical retirement/retained-owner transfer | Logical renewal, resource retirement and factory abort do not supply B/C. Reconsider only with linear exact-owner custody, callback/peer drain, retained storage/log preservation and cancellation/error/drop semantics covering Q3–Q6. |
| R3: Initial no-progress custom attachment | Existing progress/report methods are not a qualification of recovered MySQL progress. Reconsider only when the production attachment can remain genuinely unassigned/closed without publishing unqualified scalar/provider data (D/Q7). R1/R2 alone do not waive this gate. |

The **separate production-controller custom-Build blocker remains separate**:
the public custom Build callback exists, but controller
[`Build` dispatch](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-controller/src/cluster_api.rs#L2849-L2892)
first installs target inbound authority via
[`agent handling`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/host/service.rs#L759-L783)
and private [`StatelessRuntimeControl`](https://github.com/youyuanwu/kuberic/blob/c0e52ee6ef361d86cf9e984e1f0682bc1f479013/kuberic-runtime/src/host/hosting.rs#L630-L653).
Fixing that dispatch is neither this deliverable nor evidence for R1–R3.

No API additions, owner extraction, repin or hosted `OpenMode::Existing` change
is authorized merely by this contract. It excludes interrupted Build/Remove
repair, Clone/reseed, crash/host/lost-owner or lost-log reconstruction, survivor
adoption, serving replay, failover, independent fencing, TLS/cross-host/
Kubernetes and production availability/durability/security claims. The bounded
contract deliverable is complete; runtime integration and the rest of Stage 3
remain undelivered.
