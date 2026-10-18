# One fresh MySQL member in the public host

This is a bounded host-local qualification slice, not a production deployment
or restart-recovery claim. It runs the **actual** `MysqlPublicV1Service` in
`ReplicaHost`/`RunningReplica` on Kuberic
[`be4f83b43567c59f48472025d55dd731efecc4c6`](https://github.com/youyuanwu/kuberic/tree/be4f83b43567c59f48472025d55dd731efecc4c6).
The test dependency enables that pin's existing `testing` feature solely for
generic store/history initialization, public operation dispatch and observation.
It does not replace the production service, factory, registered provider, native
MySQL, access guard or client proxy.

## Executable journey

The [Rust-owned hosted fixture](../crates/kuberic-mysql-tests/tests/kuberic_public_v1_hosted_live_mysql_8_4_11.rs#L270-L453)
creates a generic agent root separate from the native data/runtime roots.
Initializing application-path bindings select `OpenMode::New`; generic history
and storage are initialized through the existing testing facade. No private
store is inspected and no MySQL recovery receipt database is created. This
setup is **not bearer-authenticated wire, Kubernetes/controller accepted-status,
or recovery-authority evidence**.

The positive [New/formation journey](../crates/kuberic-mysql-tests/tests/kuberic_public_v1_hosted_live_mysql_8_4_11.rs#L480-L651)
establishes:

1. Actual service Open and registered Replicator Open, non-pending host-created
   creation ID, custom MySQL replication endpoint, exact replica/session, and
   zero provider-writer leases. Production registration uses the **same driver
   allocation** for the factory and its progress-only StateProvider.
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

## Terminal and interrupted startup evidence

Separate [Close and Abort scenarios](../crates/kuberic-mysql-tests/tests/kuberic_public_v1_hosted_live_mysql_8_4_11.rs#L97-L180)
begin from the admitted, live native proxy after DML. Public terminal dispatch
must succeed within the fixture's measured 45-second terminal budget. Tests
independently check the exact native PID is absent, native SQL/proxy listeners
are closed, native identity/table data and logs remain, and an unrelated child
is still live.

After terminal cleanup, asking the registered closed driver for a new live host
report explicitly fails. No cached pair or literal zero is substituted.
Testing dispatch separates callback completion from report construction. The
pin's ordinary wire Execute response constructs a report even after terminal
operations, so these tests **do not claim successful terminal wire
acknowledgment**.

The [startup interruption scenario](../crates/kuberic-mysql-tests/tests/kuberic_public_v1_hosted_live_mysql_8_4_11.rs#L788-L897)
observes `/usr/sbin/mysqld` with the exact fixture defaults-file after native
initialization, excludes initializer/version processes, verifies Open is still
pending, then cancels the same production `AttemptContext`. It awaits the real
failed public Open and verifies exact-child containment and retained data/logs.
Cancelling before application Open, merely dropping an RPC client, or injecting
after Open completed would not pass this test. Initialization observation has
the existing 180-second native allowance; post-cut settlement has 45 seconds.
There is no retry-until-pass loop or fabricated generic containment witness.

## Native evidence retention

The [production service constructor](../crates/kuberic-mysql/src/service/kuberic/service.rs#L203-L220)
selects an in-memory retention policy. After exact owned-child termination and
root revalidation, [terminal containment](../crates/kuberic-mysql/src/service/instance.rs#L1632-L1671)
atomically renames the owned runtime root to its complete original name plus
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

## Running the gate

Install and verify the exact Oracle MySQL Community
`mysql-community-server-core=8.4.11-1ubuntu24.04`, `/usr/sbin/mysqld`,
`/usr/bin/aa-exec`, inactive system mysql service, no foreign mysqld and no
MySQL X listener. The existing preflight additionally checks package ownership,
integrity and apt candidate/repository; missing prerequisites **fail**, not skip.
See [repository prerequisites](../README.md#prerequisites).

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests \
  --test kuberic_public_v1_hosted_live_mysql_8_4_11
```

The existing profile runs native tests serially without retries. Compact roots
keep UDS paths below 108 bytes and use the shared fixture helper that creates
missing target parents for archive consumers. Each native fixture prints and
preserves its root, including native logs/data; generic state remains under
`k`, native data under `d`, and retired native runtime under `s.retained`.
Only the fixture's concrete child owner signals/reaps its child. No process-name
killing or unrelated cluster mutation is used.

Independent direct custom Build, gateway, native lifecycle, and graceful
closed-reopen tests remain relevant regressions but are not relabelled as hosted
coverage. This slice does not qualify controller-driven Build, Existing/cold or
lost-owner recovery, closed-owner conversion, serving replay, Clone/reseed,
failover, independent fencing, TLS, cross-host, Kubernetes or production safety.
