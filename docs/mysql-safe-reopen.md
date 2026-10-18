# Closed MySQL Safe Reopen

## Delivered boundary

The first bounded Stage 3 primitive reopens existing Oracle MySQL 8.4.11 data
under a **new, closed native child**, after an exactly owned quiet graceful stop.
It does not recover after loss of the adapter owner, restore serving, or make
the Public V1 service accept `OpenMode::Existing`.

The embedding must continuously retain the original `MysqlInstanceManager`
and then the noncloneable `MysqlStoppedReopen` capability. There is no
constructor from arbitrary paths, UUIDs, PID files or a caller's “contained”
boolean. No application-private durable metadata or recovery store is written.
MySQL owns native data/history; generic Kuberic owns orchestration authority.

## Preconditions and use

Use `kuberic_mysql::service::{MysqlStoppedReopen, MysqlReopenAuthority,
MysqlReopenBoundary, MysqlReopenRenewal}` with a topology-member manager:

1. Retain the exact original manager. Native SQL TCP and MySQL X must be
   disabled; Group Replication, bootstrap, asynchronous replication and the
   event scheduler must be stopped. Enable native `read_only` and
   `super_read_only`. These settings alone are not an independent fence.
2. The manager must **never have issued an application/proxy capability**.
   An idle published proxy or an unpublished-but-publishable proxy is not
   closed. Even account cleanup or prior proxy revocation does not reset this
   conservative lifetime restriction; exact proxy-to-reopen handoff is not
   implemented. The Build recovery TCP profile also rejects.
3. Obtain authentic accepted generic authority from the trusted embedding.
   Fill `MysqlReopenAuthority` with public storage/replica incarnation,
   partition, complete `HistoryContext`, process session, replicator creation,
   epoch, configuration, operation and positive revision. Supply an independently
   accepted `MysqlReopenBoundary`: native UUID, group, server ID, report address,
   group address and complete executed/purged `GtidSet`s.
4. Call `MysqlStoppedReopen::capture(manager, authority, expected, &attempt)`.
   It verifies owned process/roots, effective closure and coherent native
   evidence, then terminates/reaps only its retained child. Only a successful
   graceful exit issues the one-use capability; escalation/signal death does
   not. Data **and scratch/native binary logs** are retained.
5. Call `stopped.reopen(renewal, &attempt)`. `renewal.predecessor` must equal the
   entire sealed authority. Its replacement must keep exact replica/storage,
   partition and history; use a strictly newer same-data-loss configuration
   epoch, different process session, creation, configuration and operation,
   and strictly higher revision. These are closed-observation scope, not
   installed role/configuration or a generic runtime renewal completion.
6. The returned `MysqlClosedReopened` exposes only `observe(&attempt)` and
   terminal `close()`. Observation returns fresh `MysqlReopenEvidence` binding
   generic authority, native identity/full history, filesystem device/inode,
   independently derived new native-child session and local observation attempt.
   It provides no manager/socket extraction, bootstrap, role, configuration,
   access, Build or scalar-progress publication.

Caller values are structurally validated and exactly compared; that does **not**
authenticate arbitrary callers or replace generic Kuberic admission. The OS
account/private administration socket and privileged administrator are trusted.
No API can infer unknown past privileged history/settings mutation from current
native state.

## Validation and failure semantics

Before spawn, the retained roots/executables, absent socket/PID, exact generated
option-file contents and in-memory native-log file identity/length inventory
must still match. A PID file never authorizes process adoption or termination.
The new child is owned before polling and attested against retained child PID,
executable and exact private endpoint.

Closed startup uses command-line overrides for disabled SQL TCP, MySQL X, GR
autostart/bootstrap, replica autostart and event scheduler, with read-only and
super-read-only enabled. Persisted global-variable loading is disabled before
native startup; retained native persisted settings cannot silently enable them.
Effective settings and native identity/history are read back in two matching
inactive snapshots. The exact qualified native GTID-compression daemon is not a
client; every other foreground session must actually disappear. A bounded
quiescence wait accommodates asynchronous server-side `COM_QUIT` teardown,
without ignoring a still-present session.

Complete executed and purged sets must equal the sealed expected boundary;
purged must be a subset of executed. Tags, gaps and multiple/foreign sources
are retained as structured history. Equality here does not qualify those shapes
for the separate scalar progress contract.

Durability requires the continuously retained fresh owner's enforced native
profile and its monotonic lifetime qualification. Every observed violation
invalidates that lifetime even if settings are later restored or a subsequent
read fails. Current post-restart settings cannot qualify an unknown predecessor.
This API issues no committed/provider, election or donor/catch-up progress.

Capture/reopen errors, cancellation and dropped futures never return a
successful capability. Their retained manager contains only its exact child;
existing data is not deleted and failure drop leaves the native roots/logs for
inspection. A dropped successful closed handle also contains its child.
`close()` is deliberately terminal: ordinary containment removes disposable
scratch and retains data, without another reopen token. Losing the token or
adapter process requires reset, not reconstruction from saved evidence.

## Public Kuberic boundary and pin

Keep exact pin `be4f83b43567c59f48472025d55dd731efecc4c6` in both crates and
Cargo.lock. Comparison with immutable upstream
`1074ecaca9730e1f8c6ea6a6a89029e867c5cccd` found no needed recovery-contract
change; the later unselected SF V1 async/tonic substrate does not solve this gap.

Kuberic already owns generic durable operation intent/completion and a
[replacement-session protocol](https://github.com/youyuanwu/kuberic/blob/be4f83b43567c59f48472025d55dd731efecc4c6/kuberic-runtime/src/protocol/public_operations.rs#L1158-L1446).
Its ordinary [OpenContext](https://github.com/youyuanwu/kuberic/blob/be4f83b43567c59f48472025d55dd731efecc4c6/kuberic-runtime/src/application.rs#L13-L44)
and [factory context](https://github.com/youyuanwu/kuberic/blob/be4f83b43567c59f48472025d55dd731efecc4c6/kuberic-runtime/src/replicator/mod.rs#L677-L788)
do not supply the custom MySQL callback with a full accepted recovery envelope,
predecessor physical retirement capability and epoch/configuration/operation
scope. Generic stores are not opened privately by MySQL, and testing-only host
helpers are not a production integration. This explicit caller-owned primitive
stops before that integration boundary.

## Qualification and nonclaims

The live prerequisite is exactly Oracle MySQL Community Server 8.4.11 on Linux
x86-64, package `mysql-community-server-core=8.4.11-1ubuntu24.04`. Use the normal
project fixture, stopped system MySQL, no foreign mysqld, one Cargo target tree,
`CARGO_BUILD_JOBS=1` and serial nextest tests. The short sibling fixture roots
also work for archive consumers whose checkout has no `target/`; UDS paths
remain subject to the `<108` encoded-byte limit.

```sh
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql -E 'test(reopen) or test(durability_lifetime)'
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests --test service_reopen_live_mysql_8_4_11
```

Live checks exercise the actual production API, retained acknowledged data and
nonempty GTIDs, renewed native/generic sessions, persisted-setting suppression,
closed ports, idle published/unpublished proxy rejection, substituted roots/
native UUID/options/logs, stale authority, bad history/durability, extra sessions,
foreign PID noninterference, cancellation and dropped futures/owners.

Not supported: adapter/host-crash or lost-owner reconstruction; abrupt, data-only
or lost-native-log reopening; survivor adoption; automatic hosted Existing;
role/epoch/configuration/peer/access replay; interrupted Build/Remove recovery;
Clone/reseed; automatic failover; independent fencing; controller custom-Build
integration; TLS, cross-host or Kubernetes. No production availability,
durability, security or failover claim follows from this host-local primitive.

## Implementation references

- [Authority and structured boundary validation, reopen.rs:29–147](../crates/kuberic-mysql/src/service/reopen.rs#L29-L147).
- [Linear owner and closed handle, reopen.rs:189–302](../crates/kuberic-mysql/src/service/reopen.rs#L189-L302).
- [Closed admission and memory-only log inventory, reopen.rs:350–435](../crates/kuberic-mysql/src/service/reopen.rs#L350-L435).
- [Owned closed startup, instance.rs:324–432](../crates/kuberic-mysql/src/service/instance.rs#L324-L432)
  and [graceful-stop/reopen seams, instance.rs:1328–1389](../crates/kuberic-mysql/src/service/instance.rs#L1328-L1389).
- [Inactive native snapshots, control.rs:42–191](../crates/kuberic-mysql/src/service/control.rs#L42-L191)
  and [monotonic durability reader, control.rs:698–729](../crates/kuberic-mysql/src/service/control.rs#L698-L729).
- [Actual production-API live qualification, service_reopen_live_mysql_8_4_11.rs:182–565](../crates/kuberic-mysql-tests/tests/service_reopen_live_mysql_8_4_11.rs#L182-L565).
