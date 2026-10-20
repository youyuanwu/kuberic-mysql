# One fresh MySQL member in the public host

This is a bounded host-local qualification slice, not a production deployment
or restart-recovery claim. It runs the **actual** `MysqlPublicV1Service` in
`ReplicaHost`/`RunningReplica` on Kuberic
[`dabe144670303b22b1fe13f5ced78ab57ad3f98c`](https://github.com/youyuanwu/kuberic/tree/dabe144670303b22b1fe13f5ced78ab57ad3f98c).
The target contains two explicitly separate routes: existing `hosted_*` cases
use the pin's testing facade for generic initialization/dispatch/observation;
the `authenticated::*` native cases use ordinary authenticated control RPCs
for all of those steps. Neither replaces the production service, factory,
registered provider, native MySQL, access guard or client proxy. The adapter
corrects fresh reporting and terminal ownership locally; no upstream runtime,
private engine or controller authority is patched.

**Terminal limitation:** ordinary authenticated Close and Abort return
gRPC `Unavailable: runtime is closed`, as does subsequent live GetStatus.
Tests assert these unsuccessful RPCs and independently prove native containment
before fallback cleanup. Passing this gate is **not successful terminal wire
acknowledgment**, a completion receipt, or permission to retry/recover a closed
owner. PostgreSQL parity refers to closed-driver semantics established by source
comparison, not an executed PostgreSQL wire result.

## Executable journey

The [Rust-owned hosted fixture](../crates/kuberic-mysql-tests/tests/kuberic_public_v1_hosted_live_mysql_8_4_11.rs#L277-L489)
creates a generic agent root separate from the native data/runtime roots.
Its ordinary branch never executes testing store/history initialization.
The [ordinary adapter](../crates/kuberic-mysql-tests/tests/hosted_authenticated/mod.rs#L39-L249)
starts the genuinely uninitialized host, gets authenticated status, sends
`InitializeAgentStore`, drops that client, awaits readiness and reconnects.
The initialized report supplies genuine history/storage identity and a new
process session. Native `d`/`s` remain absent throughout generic initialization.
Initializing application-path bindings then select `OpenMode::New`.
No private store is inspected and no MySQL recovery receipt database is created.
Ordinary requests are generated `AgentControlClient` RPCs encoded through the
public command serializer, not fabricated accepted envelopes.

The shared [New/formation journey](../crates/kuberic-mysql-tests/tests/kuberic_public_v1_hosted_live_mysql_8_4_11.rs#L519-L690)
establishes:

1. Actual service Open and registered Replicator Open, non-pending host-created
   creation ID, custom MySQL replication endpoint, exact replica/session, and
   closed initial access. Production registration uses the **same driver
   allocation** for the factory and its progress-only StateProvider.
   Zero provider-writer leases are additionally inspected only by the old
   testing-facade cases, not claimed as ordinary wire evidence.
2. An initial closed-access Recovery report with independently qualified `0/0`
   only for **New + freshly initialized empty owner + closed access**. A direct
   native GTID read confirms emptiness. Unknown/reopened progress is not zero.
3. The existing public `InitialPrimary` lifecycle (`NotPossible` data loss),
   whose role, implicit epoch and singleton configuration callbacks complete
   before the final projection refresh. Standalone initial Primary/epoch
   reporting is separately qualified by the real-controller gate below;
   neither route adds an API or authority bypass.
4. Actual Granted read/write status and successful application authentication,
   INSERT and SELECT through the production proxy. Native-direct application
   login and unauthorized proxy logins fail. A valid proxy command after each
   invalid login distinguishes rejection from a dead listener.
5. Supported repeated epoch/configuration and progress operations, an unchanged
   creation/session, monotonic reports and an independently sampled coherent
   applied/committed pair after real transactions. Formation itself need not
   create GTIDs; the fixture creates actual native user data before requiring
   advancement, and parses complete native GTID sets.

The fixture configures three explicit topology slots/seeds as required by the
native configuration but instantiates **one member**, not three hosts or a
failover group. It uses no direct driver call to supply a hosted result.

### Ordinary rejection and independent progress evidence

Both [ordinary terminal scenarios](../crates/kuberic-mysql-tests/tests/hosted_authenticated/mod.rs#L285-L364)
first run the full positive journey. Missing and incorrect control bearer
credentials return Unauthenticated before native roots exist; valid Open then
succeeds. Both missing/wrong-bearer probes have a five-second local deadline;
timeout is a returned scenario error that still runs awaited cleanup.
After DML, [independent sampling](../crates/kuberic-mysql-tests/tests/hosted_authenticated/mod.rs#L438-L548)
brackets ordinary progress/report with quiet native snapshots. It compares
complete parsed executed/purged GTIDs and native identity, projects only the
qualified complete history, and requires nonzero equality to both host applied
(`last`) and independently provider-sampled committed (`first`) values.
The driver/provider commitment proof is not replaced by the fixture's query.

[Stale session, durable target and authority revision requests](../crates/kuberic-mysql-tests/tests/hosted_authenticated/mod.rs#L550-L634)
each return FailedPrecondition. New operation IDs and structurally consistent
envelopes distinguish stale authority from malformed input or exact retries.
Progress requests avoid the runtime's deliberate pre-admission access fence
for mutating programs. Before/after assertions compare exact PID/start time,
path device/inodes, auto.cnf, native UUID, full executed/purged GTIDs, group
membership/view/read-only state, committed rows, and public
identity/history/storage/session/creation/progress/Granted access. Rejected
IDs must not become completed operations. Valid newer Progress and proxy
SELECT still succeed afterward. This is bounded currentness coverage, not a
claim that every stale mutating-program shape leaves access unchanged.

## Terminal and interrupted startup evidence

The ordinary Close/Abort cases begin from admitted proxy DML and proved
progress, send the same ordinary authenticated Execute RPC, and require the
exact unsuccessful terminal/status result described above. The terminal RPC has
a measured 45-second budget. **Before** service-abort/host-shutdown fallback,
tests independently check the exact native PID is absent, native SQL/proxy
listeners are closed, native identity/table data and logs remain, and an
unrelated child is still live. Scenario, shutdown/containment and foreign-child
errors are aggregated; unexpected success also returns through awaited cleanup.

The existing [testing-dispatched Close/Abort cases](../crates/kuberic-mysql-tests/tests/kuberic_public_v1_hosted_live_mysql_8_4_11.rs#L99-L183)
separately assert successful callback dispatch plus independent containment.
They are not ordinary wire acknowledgment evidence.

After terminal cleanup, asking the registered closed driver for a new live host
report explicitly fails. No cached pair or literal zero is substituted.
Testing dispatch separates callback completion from report construction. The
pin's ordinary wire Execute response constructs a report even after terminal
operations, so these tests **do not claim successful terminal wire
acknowledgment**.

The testing-dispatched [startup interruption scenario](../crates/kuberic-mysql-tests/tests/kuberic_public_v1_hosted_live_mysql_8_4_11.rs#L831-L897)
observes `/usr/sbin/mysqld` with the exact fixture defaults-file after native
initialization, excludes initializer/version processes, verifies Open is still
pending, then cancels the same production `AttemptContext`. It awaits the real
failed public Open and verifies exact-child containment and retained data/logs.
Cancelling before application Open, merely dropping an RPC client, or injecting
after Open completed would not pass this test. Initialization observation has
the existing 180-second native allowance; post-cut settlement has 45 seconds.
There is no retry-until-pass loop or fabricated generic containment witness.
Ordinary generic initialization has separate cooperative startup cancellation
and bounded settlement, including shutdown/join if readiness races cancellation;
setup and cleanup errors remain distinct.

## Native evidence retention

The [production service constructor](../crates/kuberic-mysql/src/service/kuberic/service.rs#L203-L220)
selects an in-memory retention policy. After exact owned-child termination and
root revalidation, [terminal containment](../crates/kuberic-mysql/src/service/instance.rs#L1656-L1695)
atomically renames the owned runtime root to its complete basename plus
`.retained`. It validates retained device/inode ownership and uses nonreplacing
directory-relative rename; a destination collision fails containment rather
than overwriting foreign evidence. The active runtime name really is absent.
No cleanup receipt claims that retained native files were deleted.

Failed production-service initialization after root claim also retains faulted roots for
diagnosis; unusable partial data is **not** reusable. Direct Public V1
qualification now opts into the same policy, including exact-generation Build
retirement. Default low-level manager opt-out behavior is unchanged.
The outer fixture refuses replacement and retains failed/panicking runs; a
controlled three-native-owner unwind verifies that native logs survive actual
containment, not merely the outer directory. A retained diagnostic directory is
neither a journal/adoption record nor permission to open Existing, restart an
owner, continue Build, or serve replay.

Service-level terminal qualification additionally covers failed Close followed
by Close/Abort, dropped Close followed by Abort, and two concurrent Close
callers. The exact driver remains retained rather than disappearing before an
await. If native reaping succeeds but scratch settlement fails, only the
previously attested same-generation, authority and attempt may retry physical
cleanup; owned roots are revalidated and no missing child is dereferenced.
These are mechanical containment obligations, not a private recovery journal
or successful ordinary terminal acknowledgment
([service ownership](../crates/kuberic-mysql/src/service/kuberic/service.rs#L438-L493),
[exact cleanup custody](../crates/kuberic-mysql/src/service/instance.rs#L1923-L1967)).

## Upstream in-process harness applicability

The current 2026-10-19 pin includes the public
[`kuberic_controller::test_util`](https://github.com/youyuanwu/kuberic/blob/dabe144670303b22b1fe13f5ced78ab57ad3f98c/docs/features/kuberic/in-process-testing.md)
harness behind the controller's dev-only `test-util` feature. It runs the
production reconciler and authenticated agent RPCs against real hosted
applications, with a private Kubernetes resource implementation rather than a
real cluster. It is a useful foundation, but **not a drop-in replacement for
the MySQL fixtures**.

The public
[`ApplicationFactory`](https://github.com/youyuanwu/kuberic/blob/dabe144670303b22b1fe13f5ced78ab57ad3f98c/kuberic-controller/src/test_util/application.rs#L11-L86)
is now implemented by the
[MySQL integration probe](../crates/kuberic-mysql-tests/tests/kuberic_inprocess_live_mysql_8_4_11.rs).
It constructs inert production `MysqlPublicV1Service` instances, classifies
their actual native roots and supplies owned data/runtime storage bindings.
Its async `shutdown_and_drain` calls the production service's repeatable Close
after host drain. The adapter reserves a native three-member loopback topology
and application proxy endpoints, supplies disposable credentials, and protects
each application directory. The symbolic application image does not install
or launch MySQL.

The current boundaries are concrete:

- **Fresh New Open now succeeds.** The current pin includes
  [youyuanwu/kuberic#183](https://github.com/youyuanwu/kuberic/pull/183), fixing
  [youyuanwu/kuberic#179](https://github.com/youyuanwu/kuberic/issues/179).
  Actual fresh native storage bindings select New, and the production controller
  honors the host-reported mode. The probe below executes three successful New
  Opens, not Existing/restart adoption.
- **Initial Role/Epoch reporting succeeds; multi-member bootstrap remains
  bounded.** The separate case below obtains controller acceptance of the
  standalone Primary Role and Epoch with truthful zero reports and closed
  access, then executes the exact singleton-only fresh-bootstrap rejection on
  the proposed three-member Configuration. No formation or controller Build
  success is claimed.
- The harness creates a
  [three-replica persisted set](https://github.com/youyuanwu/kuberic/blob/dabe144670303b22b1fe13f5ced78ab57ad3f98c/kuberic-controller/src/test_util/cluster.rs#L164-L192),
  not the existing single-member hosted journey. Production controller Build
  [first installs target inbound authority](https://github.com/youyuanwu/kuberic/blob/dabe144670303b22b1fe13f5ced78ab57ad3f98c/kuberic-controller/src/cluster_api.rs#L2925-L2968).
  The agent's
  [installation path](https://github.com/youyuanwu/kuberic/blob/dabe144670303b22b1fe13f5ced78ab57ad3f98c/kuberic-runtime/src/host/hosting.rs#L652-L663)
  still requires the built-in private `StatelessRuntimeControl`; MySQL's custom
  factory does not attach it, so that path returns `NotOpen` before source Build.
  The newly public harness does not remove this controller-integration gap.
  This remains source-traced evidence: the executed probe does not reach Build.
- Network isolation and armed Build-dial faults
  [require an acknowledged built-in routed engine](https://github.com/youyuanwu/kuberic/blob/dabe144670303b22b1fe13f5ced78ab57ad3f98c/kuberic-controller/src/test_util/cluster.rs#L2199-L2268).
  They reject unsupported custom engines rather than cutting MySQL's native
  Group Replication/recovery sockets.
- Contained restart reuses storage, whereas
  [MySQL service Open](../crates/kuberic-mysql/src/service/kuberic/service.rs#L341-L354)
  deliberately rejects `Existing`. Harness restart/failover controls do not
  implement retained-owner recovery or authorize native adoption.

### Executed fresh-New Open

The unignored `controller_fresh_new_open_starts_and_contains_native_members` case
uses public `TestCluster::step` and detached observations, not direct custom
callbacks, private stores, or authority-mutating handles. Both the runtime and
the new controller dev-dependency use the exact same Git revision.

The case stops after all three members have exact current-process successful
New Open receipts and replication endpoints. It verifies each receipt's New
program digest, session, history, and storage incarnation; reporting Existing
after initialization describes a future Open, not an Existing application
callback. Missing receipts, a still-New reported storage mode, and substituted
Existing Open digests reject. All members must remain unassigned with external
application access closed, before any formation or voting-configuration claim.

Each member has a live native PID and a native `auto.cnf` identity under the
originally absent owned data root. After `finish(result)`, all three exact PIDs
must be absent, SQL/group/application proxy endpoints must refuse connections,
every factory drain must be acknowledged, and the harness-owned application
roots must be removed. Repeated factory drain remains successful. PID files
are read only for assertions; process signaling remains with the actual service
owners, never a PID-file lookup. This qualifies native startup and containment
through the real controller/host path, **not formation, replication, DML, or
controller Build**.

Every normal scenario result is consumed by `finish(result)`. Successful
finalization drains the actual hosts and native services, then releases owned
fixture storage. Failure retains the harness-owned storage and native logs;
a nonreplacing `probe-diagnostics.json` preserves redacted diagnostics outside
the native application directories. It is diagnostic output, never recovery
authority. Retained generic state can contain disposable credentials and is
not uploaded or checked in. Timeouts and cleanup failures fail the case.

The test re-executes its own test binary with a private short `/tmp` storage
root and a two-worker Tokio runtime. Only the child environment's
`CARGO_TARGET_DIR` redirects harness storage; Cargo still builds in the usual
single target tree. This keeps the configured native UDS path below 108 bytes
even with the harness's UUID/PVC/application directory layout. The child unsets
`KUBERIC_WORKSPACE_TESTS`; downstream consumers must not request upstream's
repository-owned configuration.

Fresh/New dispatch is now verified, without removing storage bindings or
permitting MySQL Existing adoption. The separate case below captures the first
executed formation boundary; successful formation and the later source-only
Build-authority boundary remain unresolved. Keep the existing native and ordinary
authenticated gates. Upstream remains unchanged; the adapter's local corrections
do not supply missing controller formation/Build authority.

### Executed fresh Primary-role boundary

The unignored
`controller_fresh_primary_reports_before_exact_multimember_configuration_boundary`
case first establishes the same three exact successful New Opens, then
continues public `TestCluster::step`, without manual callback dispatch, generic
store mutation or fake built-in runtime control.

1. The controller's
   [fresh sequence](https://github.com/youyuanwu/kuberic/blob/dabe144670303b22b1fe13f5ced78ab57ad3f98c/kuberic-controller/src/evaluator/public_v1_topology.rs#L987-L1040)
   sends initial Primary Role, Epoch, then Configuration at epoch 0/1.
   Initial Primary-before-configuration is legitimate, not an upstream ordering
   defect. Both adapter reporting paths now
   [revalidate exact fresh history](../crates/kuberic-mysql/src/service/kuberic/proof.rs#L329-L398);
   successful Role and Epoch completions enter accepted controller authority.
2. All three public reports remain queryable at `0/0` with closed access.
   The pinned custom host deliberately
   [projects None/Recovery until readiness](https://github.com/youyuanwu/kuberic/blob/dabe144670303b22b1fe13f5ced78ab57ad3f98c/kuberic-runtime/src/host/hosting.rs#L571-L630),
   even though the Primary Role callback succeeded. That policy is not changed
   or confused with the native role-intent receipt.
3. The first later executed rejection is the driver's
   [singleton-only fresh bootstrap guard](../crates/kuberic-mysql/src/service/kuberic/driver.rs#L2591-L2607)
   on the proposed three-member Configuration:
   `invalid replication evidence: fresh MySQL bootstrap requires exactly the local Primary member`.
   The frozen command/transition retains exact target, session, storage,
   history, epoch, revision, digest and three-member descriptor. Accepted
   authority contains exactly Open×3 + Role + Epoch, with no admitted topology,
   configuration or Build success. Normal 294-byte public storage fences now
   pass lossless translation rather than failing an unrelated component limit.

The [gate](../crates/kuberic-mysql-tests/tests/kuberic_inprocess_live_mysql_8_4_11.rs#L510-L1005)
checks both positive receipts and negative evidence mutations. Wrong operation,
missing command, changed credit, substituted session/storage/history, missing
Epoch completion, changed progress/role/access, timeout and unrelated error
cannot qualify. Earlier native-Open RPC retries are not proof of the boundary.

The pinned public report does not include the unsuccessful configuration
completion. To observe the exact production-controller dispatch rejection, the
test intentionally calls public `finish` with a bounded diagnostic-capture
marker, then verifies the **full exact** `lastReconcileError` chain from
redacted diagnostics. The marker is not successful evidence by itself.
The finish result must be Application, not Cleanup, have zero pending task
owners, and acknowledge every factory drain. Captured PIDs and SQL/group/proxy
listeners must be absent; active runtime names disappear while native data and
logs remain. This expected-rejection fixture is retained locally, unlike the
positive Open gate's released storage. No private runtime state is altered.

Before teardown, independent owned-UDS reads prove empty native executed
history, bootstrap disabled and no active group; all proxies refuse connects.
No access is inferred from native read-only flags or an advertised endpoint.
These checks do **not** prove formation or controller Build. The earlier
overbroad role-progress rejection at base commit `4586630` is superseded by this
qualified correction, not reclassified as a required native ordering.

Singleton bootstrap, later exact secondary Build admission, the separately
unavailable public custom inbound-Build attachment, native fault routing and
Existing recovery remain independent boundaries. The latter paths are not
executed by this controller gate.

Run both native controller cases and the negative error checks with the same
mandatory MySQL prerequisites below:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests --test kuberic_inprocess_live_mysql_8_4_11
```

The existing workspace nextest archive and qualification partitions discover
this target automatically. No separate CI gate or Cargo feature is added.

### Historical fresh-Open mismatch

The original probe at `8c659536bdf31040aa13d787427ff1601e261bf5` reproduced
`agent store identity mismatch: open context changed` before application Open
or native startup. That negative assertion has been replaced by the positive
fresh-New gate above. These links preserve the historical source trace, not
the behavior at the current pin:

- [Factory storage bindings reached the real host](https://github.com/youyuanwu/kuberic/blob/8c659536bdf31040aa13d787427ff1601e261bf5/kuberic-controller/src/test_util/cluster.rs#L1136-L1147).
- [Generic initialization marked those bindings initializing](https://github.com/youyuanwu/kuberic/blob/8c659536bdf31040aa13d787427ff1601e261bf5/kuberic-runtime/src/host/service.rs#L314-L321),
  and [the host selected New](https://github.com/youyuanwu/kuberic/blob/8c659536bdf31040aa13d787427ff1601e261bf5/kuberic-runtime/src/host/service.rs#L715-L727).
- [The controller hardcoded Existing for an unopened member](https://github.com/youyuanwu/kuberic/blob/8c659536bdf31040aa13d787427ff1601e261bf5/kuberic-controller/src/evaluator/public_v1_topology.rs#L182-L189).
- [The lifecycle interpreter rejected the mode mismatch before application Open](https://github.com/youyuanwu/kuberic/blob/8c659536bdf31040aa13d787427ff1601e261bf5/kuberic-runtime/src/host/public_lifecycle_program.rs#L227-L239).

## Running the gate

Install and verify the exact Oracle MySQL Community
`mysql-community-server-core=8.4.11-1ubuntu24.04`, `/usr/sbin/mysqld`,
`/usr/bin/aa-exec`, inactive system mysql service, no foreign mysqld and no
MySQL X listener. The existing preflight additionally checks package ownership,
integrity and apt candidate/repository; missing prerequisites **fail**, not skip.
See [repository prerequisites](../README.md#prerequisites).

```bash
CARGO_BUILD_JOBS=1 cargo test --locked --offline \
  -p kuberic-mysql-tests \
  --test kuberic_public_v1_hosted_live_mysql_8_4_11 \
  -- --test-threads=1 --nocapture

# Equivalent existing serial, zero-retry nextest profile:
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests \
  --test kuberic_public_v1_hosted_live_mysql_8_4_11
```

The target contains sixteen cases, including the existing hosted/ordinary
journeys, New-only rejection, service Close failure/drop/concurrency, and
deterministic retention/negative-result/timeout checks. Neither native
route skips missing prerequisites. The runtime test dependency still enables
`testing` for the old cases; its presence is not evidence the ordinary cases
use it. Use one build job and one target tree; do not run native fixtures
concurrently.

Recorded scoped qualification passed **21 direct Build/Remove/progress**
and **16 hosted/authenticated** cases in one serial, zero-nextest-retry
selection; the complete controller target also passed separately with related
identity/ownership checks. Final-review hardening then verified the exact native
unwind checkpoint, rejection of unrelated worker panics, and both provider-copy
rejection callbacks in a focused three-case rerun. No full-native-workspace
result is inferred from these selections. The earlier reported
`remove_second_completed_idle_preserves_primary_and_admitted_secondary`
failure followed by an isolated pass remains an undiagnosed historical
observation. Its current broad-run pass does not establish the prior cause or
a clean historical/full-native-workspace baseline.

Compact roots keep UDS paths below 108 bytes. The shared retained-root helper
creates missing target parents for archive consumers but refuses an existing
fixture root instead of deleting evidence. Each native fixture prints and
preserves its root, including native logs/data; generic state remains under
`k`, native data under `d`, and retired native runtime under `s.retained`.
Only the fixture's concrete child owner signals/reaps its child. No process-name
killing or unrelated cluster mutation is used.

Independent direct custom Build, gateway, native lifecycle, and graceful
closed-reopen tests remain relevant regressions but are not relabelled as hosted
coverage. This slice does not qualify controller-driven Build, Existing/cold or
lost-owner recovery, closed-owner conversion, serving replay, Clone/reseed,
failover, independent fencing, TLS, cross-host, Kubernetes or production safety.
