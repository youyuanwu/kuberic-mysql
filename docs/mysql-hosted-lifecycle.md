# One fresh MySQL member in the public host

This is a bounded host-local qualification slice, not a production deployment
or restart-recovery claim. It runs the **actual** `MysqlPublicV1Service` in
`ReplicaHost`/`RunningReplica` on Kuberic
[`8c659536bdf31040aa13d787427ff1601e261bf5`](https://github.com/youyuanwu/kuberic/tree/8c659536bdf31040aa13d787427ff1601e261bf5).
The target contains two explicitly separate routes: existing `hosted_*` cases
use the pin's testing facade for generic initialization/dispatch/observation;
the `authenticated::*` native cases use ordinary authenticated control RPCs
for all of those steps. Neither replaces the production service, factory,
registered provider, native MySQL, access guard or client proxy. This delivery
adds executable qualification and documentation, **not a production runtime fix**.

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
   before the final projection refresh. This avoids the unconfigured
   intermediate progress boundary of standalone fresh Primary/epoch calls; it
   adds no API or authority bypass.
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
root revalidation, [terminal containment](../crates/kuberic-mysql/src/service/instance.rs#L1632-L1671)
atomically renames the owned runtime root to its complete basename plus
`.retained`. It validates retained device/inode ownership and uses nonreplacing
directory-relative rename; a destination collision fails containment rather
than overwriting foreign evidence. The active runtime name really is absent.
No cleanup receipt claims that retained native files were deleted.

Failed production-service initialization after root claim also retains faulted roots for
diagnosis; unusable partial data is **not** reusable. Default direct manager,
direct qualification, Build/Remove and graceful closed-reopen semantics are
not opted into this service-only policy. A retained diagnostic directory is
neither a journal/adoption record nor permission to open Existing, restart an
owner, continue Build, or serve replay.

## Upstream in-process harness applicability

The 2026-10-19 pin adds the public
[`kuberic_controller::test_util`](https://github.com/youyuanwu/kuberic/blob/8c659536bdf31040aa13d787427ff1601e261bf5/docs/features/kuberic/in-process-testing.md)
harness behind the controller's dev-only `test-util` feature. It runs the
production reconciler and authenticated agent RPCs against real hosted
applications, with a private Kubernetes resource implementation rather than a
real cluster. It is a useful foundation, but **not a drop-in replacement for
the MySQL fixtures**.

The public
[`ApplicationFactory`](https://github.com/youyuanwu/kuberic/blob/8c659536bdf31040aa13d787427ff1601e261bf5/kuberic-controller/src/test_util/application.rs#L11-L86)
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

- **Fresh Open fails before MySQL starts.** With actual fresh native storage
  bindings, generic initialization selects `OpenMode::New`. The production
  controller instead requests `Open(existing=true)`, which the host rejects
  as `agent store identity mismatch: open context changed`. The probe below
  executes this boundary; it is not an Existing/restart attempt.
- The harness creates a
  [three-replica persisted set](https://github.com/youyuanwu/kuberic/blob/8c659536bdf31040aa13d787427ff1601e261bf5/kuberic-controller/src/test_util/cluster.rs#L164-L192),
  not the existing single-member hosted journey. Production controller Build
  [first installs target inbound authority](https://github.com/youyuanwu/kuberic/blob/8c659536bdf31040aa13d787427ff1601e261bf5/kuberic-controller/src/cluster_api.rs#L2849-L2892).
  The agent's
  [installation path](https://github.com/youyuanwu/kuberic/blob/8c659536bdf31040aa13d787427ff1601e261bf5/kuberic-runtime/src/host/hosting.rs#L631-L641)
  still requires the built-in private `StatelessRuntimeControl`; MySQL's custom
  factory does not attach it, so that path returns `NotOpen` before source Build.
  The newly public harness does not remove this controller-integration gap.
  This remains source-traced evidence: the executed probe does not reach Build.
- Network isolation and armed Build-dial faults
  [require an acknowledged built-in routed engine](https://github.com/youyuanwu/kuberic/blob/8c659536bdf31040aa13d787427ff1601e261bf5/kuberic-controller/src/test_util/cluster.rs#L2183-L2252).
  They reject unsupported custom engines rather than cutting MySQL's native
  Group Replication/recovery sockets.
- Contained restart reuses storage, whereas
  [MySQL service Open](../crates/kuberic-mysql/src/service/kuberic/service.rs#L345-L350)
  deliberately rejects `Existing`. Harness restart/failover controls do not
  implement retained-owner recovery or authorize native adoption.

### Executed fresh-Open boundary

The unignored `controller_fresh_open_boundary_keeps_native_state_absent` case
uses public `TestCluster::step` and detached observations, not direct custom
callbacks, private stores, or authority-mutating handles. Both the runtime and
the new controller dev-dependency use the exact same Git revision.

The source chain explaining the observed failure is:

- [Factory storage bindings reach the real host](https://github.com/youyuanwu/kuberic/blob/8c659536bdf31040aa13d787427ff1601e261bf5/kuberic-controller/src/test_util/cluster.rs#L1136-L1147).
- [Generic initialization marks those bindings initializing](https://github.com/youyuanwu/kuberic/blob/8c659536bdf31040aa13d787427ff1601e261bf5/kuberic-runtime/src/host/service.rs#L314-L321),
  and [the host selects New](https://github.com/youyuanwu/kuberic/blob/8c659536bdf31040aa13d787427ff1601e261bf5/kuberic-runtime/src/host/service.rs#L715-L727).
- [The controller hardcodes Existing for an unopened member](https://github.com/youyuanwu/kuberic/blob/8c659536bdf31040aa13d787427ff1601e261bf5/kuberic-controller/src/evaluator/public_v1_topology.rs#L182-L189).
- [The lifecycle interpreter rejects the mode mismatch before application Open](https://github.com/youyuanwu/kuberic/blob/8c659536bdf31040aa13d787427ff1601e261bf5/kuberic-runtime/src/host/public_lifecycle_program.rs#L227-L239).

The case requires the exact pending controller Open command with `existing=true`,
three valid host reports without replication endpoints, no successful completion
credit, and the exact rejection above. Arbitrary errors, missing diagnostics,
deadlines, or cleanup failures do not satisfy the probe. All three native
application directories must remain empty before and after cleanup, and their
SQL, group, and application proxy endpoints must refuse connections. This proves
construction and acknowledged drain of real production services and hosts,
**not mysqld startup, formation, replication, or native-child containment**.

Every normal scenario result is consumed by `finish(result)`. The expected
scenario error deliberately retains the harness-owned storage and redacted
diagnostics; the case separately requires zero pending drain owners and a
successful factory drain for every service. A nonreplacing
`probe-diagnostics.json` is written outside the native application directories
under that retained scenario root. It is diagnostic output, never recovery
authority. Retained generic state can contain disposable credentials; it stays
local and is not uploaded or checked in.

The test re-executes its own test binary with a private short `/tmp` storage
root and a two-worker Tokio runtime. Only the child environment's
`CARGO_TARGET_DIR` redirects harness storage; Cargo still builds in the usual
single target tree. This keeps the configured native UDS path below 108 bytes
even with the harness's UUID/PVC/application directory layout. The child unsets
`KUBERIC_WORKSPACE_TESTS`; downstream consumers must not request upstream's
repository-owned configuration.

The immediate upstream prerequisite is consistent fresh/New controller dispatch
for real application storage bindings, not removal of those bindings or
permission for MySQL to adopt Existing. Once that is available, extend the
probe to actual fresh formation and the separately traced Build-authority
boundary. Keep the existing native and ordinary authenticated gates; no
upstream or MySQL production implementation is changed here.

Run the new two-case target with the same mandatory MySQL prerequisites below:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests --test kuberic_inprocess_live_mysql_8_4_11
```

The existing workspace nextest archive and qualification partitions discover
this target automatically. No separate CI gate or Cargo feature is added.

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

The target contains eleven tests: six existing cases, two ordinary native cases,
and three deterministic retained-root/negative-result/timeout regressions. Neither native
route skips missing prerequisites. The runtime test dependency still enables
`testing` for the old cases; its presence is not evidence the ordinary cases
use it. Use one build job and one target tree; do not run native fixtures
concurrently.

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
