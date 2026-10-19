# Closed MySQL Safe Reopen

## Delivered boundary

The bounded Stage 3 primitives reopen existing Oracle MySQL 8.4.11 data
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
2. For ordinary `capture`, the manager must **never have issued an
   application/proxy capability**.
   An idle published proxy or an unpublished-but-publishable proxy is not
   closed. Even account cleanup or prior proxy revocation does not reset this
   conservative lifetime restriction. The explicit terminal proxy handoff
   below is the only additional supported path. The Build recovery TCP profile
   always rejects.
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

## Terminal independent-proxy handoff

The supported path is an **opt-in single production issuance**, not a receipt
claim that any previously created proxy is idle or revoked:

1. With the exact retained topology-member manager, enroll native control as
   usual and call `manager.prepare_reopenable_proxy(&native_authority, address,
   credential, &attempt)`. This reserves the sole controlled issuance before
   native preparation; failure/cancellation does not erase that reservation.
   The returned proxy is independently owned and may remain unpublished, publish,
   or be retained behind cloned `Arc<tokio::sync::Mutex<MysqlClientProxy>>` facades.
2. Before terminal surrender, ordinary generation revocation uses
   `manager.revoke_reopenable_proxy(&mut proxy, generation, &attempt)`. Genuine
   complete revocation permits ordinary `proxy.rearm(...)`. Generic public
   `proxy.revoke` and `cleanup_account` reject this provenance-bearing proxy
   **before calling the supplied control**; fake-success cleanup must not erase
   historical backend obligations and launder them through rearm.
3. Obtain the trusted complete generic predecessor and an independently expected
   final executed/purged boundary. Stop external workload initiation. Call
   `proxy.close_for_reopen(manager, predecessor, &attempt)`. It synchronously
   validates exact production custody and sets the permanent shared admission/
   publication/rearm fence **before returning its future**. Awaiting that future
   joins all accept/session work, proves exact known native backends absent,
   rejects unidentified connections or abnormal task history, removes the exact
   application account and clears the retained credential.
4. Success returns `(manager, MysqlProxyReopenClosure)`. The closure has no public
   constructor, Clone or serialization and binds private owner/member/storage/
   native-process issuance plus the full generic predecessor. New issuance is
   sealed. Shared stale proxy owners cannot publish, rearm or replace their guard.
   Ordinary serving-handoff eligibility still rejects interrupted commands;
   terminal containment does not claim their success.
5. Separately establish the inactive/native read-only conditions above, **after**
   account retirement: Oracle rejects `DROP USER` under `super_read_only`.
   Surrender does not change native role/settings or stop replication for you.
   Native SQL TCP/X must have remained disabled throughout; a Build owner cannot
   substitute a proxy proof for its independent SQL listener.
6. Call `MysqlStoppedReopen::capture_with_proxy_closure(manager, closure,
   predecessor, expected, &attempt)`, then use the same renewal/reopen/observe/
   close sequence above. All native identity, complete final history, monotonic
   durability, graceful-exit, retained-log and final-admission checks still run.
   If in-flight work changed the expected history, capture rejects rather than
   blessing an interrupted write or inventing a boundary.

The lifetime issued flag is **never reset**. No-proof `capture` still rejects
even after successful terminal surrender. The qualified ingress lifetime stays
privately sealed through reopen and subsequent closed observations, never
granting a second reopen token or serving extraction. Another manager/storage/
native-process issuance, stale generation or substituted generic predecessor
cannot consume the proof. Generic authority is still trusted embedding input,
not authenticated by the adapter.

Direct socket/account capabilities, ordinary generic `MysqlClientProxy::prepare`
(including engine staged preparation), failed/partial unproven construction and
Build TCP remain conservative. A direct issuance call after controlled preparation
but before terminal sealing permanently disqualifies that lifetime. After sealing,
raw issuance rejects without invalidating its proof; additional controlled preparations reject.
There is no upgrade of an already issued generic proxy into production custody.

Cancellation, error or drop of the owning surrender future returns no proof
and contains only the manager's exact child, preserving data/native logs.
Dropping it unpolled still leaves a validly begun proxy terminal. Task join
failure/timeout is permanent proof poison, distinct from interrupted-command
ambiguity; neither retry nor rearm clears it. Unidentified native connections
cannot be waived merely because a later session snapshot is empty.

## Validation and failure semantics

Before spawn, the retained roots/executables, absent socket/PID, exact generated
option-file contents and in-memory native/diagnostic log identity/length inventory
must still match. Retained files must be regular, singly linked and owned by
the scratch owner. A PID file never authorizes process adoption or termination.
The new child is owned before polling and attested against retained child PID,
executable and exact private endpoint.

Closed reopen never truncates retained diagnostic paths. It exclusively creates
`mysqld-reopen.out` and `mysqld-reopen.err` relative to the verified scratch
directory descriptor, without following links, and verifies their file type,
owner and link count. Existing destinations reject rather than being adopted.
Both inherited stderr and MySQL's native `log-error` use the new owned `.err`
destination. These files are diagnostics only, never recovery authority.
Stable preflight symlink/hard-link substitutions of either retained or new
output destination reject without following or truncating retained data.
Concurrent privileged filesystem mutation is outside the qualified OS/private-
directory trust boundary, as it is for native data and configuration themselves.

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

Both crates and Cargo.lock now pin
`dabe144670303b22b1fe13f5ced78ab57ad3f98c`. The historical 2026-10-18 assessment
checked the former pin `be4f83b43567c59f48472025d55dd731efecc4c6` against immutable upstream
`c0e52ee6ef361d86cf9e984e1f0682bc1f479013`, then checked the complete delta to
`11d804425bbdbe7759dfd18208a0d73ce667a9f4`, including controller admission,
authenticated agent dispatch, lifecycle/factory attachment, logical renewal,
physical resource retirement and selected built-in recovery. No public combined
accepted-envelope/retained-native-owner handoff was found. The unselected SF V1
substrate does not solve this gap. The
[recovery authority contract](mysql-recovery-authority.md) records exact public
evidence, required authority/custody sequencing, failure rules and future gates;
it is a contract-only deliverable, not runtime integration. The later dependency
update does not enable hosted `Existing` or native-owner recovery.

Kuberic already owns generic durable operation intent/completion and a
[replacement-session protocol](https://github.com/youyuanwu/kuberic/blob/be4f83b43567c59f48472025d55dd731efecc4c6/kuberic-runtime/src/protocol/public_operations.rs#L1158-L1446).
At that assessment, its ordinary [OpenContext](https://github.com/youyuanwu/kuberic/blob/be4f83b43567c59f48472025d55dd731efecc4c6/kuberic-runtime/src/application.rs#L13-L44)
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
  -p kuberic-mysql --lib -E 'test(reopen) or test(durability_lifetime) or test(terminal_)'
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests --test service_reopen_live_mysql_8_4_11
```

Live checks exercise the actual production API, retained acknowledged data and
nonempty GTIDs, renewed native/generic sessions, persisted-setting suppression,
closed ports, idle published/unpublished proxy rejection, substituted roots/
native UUID/options/logs, stale authority, bad history/durability, extra sessions,
foreign PID noninterference, cancellation and dropped futures/owners.
The diagnostic-path matrix covers both retained and new stdout/error paths with
both link kinds, asserting unchanged retained sentinel bytes and native logs.
Numeric binlog sequences above the zero-leading range remain recognizable.
Terminal proxy gates additionally cover published, unpublished, revoked and
rearmed owners; acknowledged proxy writes; exact native idle/transaction and
executing `SELECT SLEEP(60)` retirement; an accepted connection waiting at the
runtime gate; racing publication and retained stale facades; native Build/direct/
no-proof rejections; wrong-manager and generic incarnation/session substitutions;
and cancelled/unpolled/pending owning-future drop while another native owner
remains live. Deterministic tests separately cover lost/failed task joins,
cleanup interruption, unidentified backends and fake lifecycle evidence.

Not supported: adapter/host-crash or lost-owner reconstruction; abrupt, data-only
or lost-native-log reopening; survivor adoption; automatic hosted Existing;
role/epoch/configuration/peer/access replay; interrupted Build/Remove recovery;
Clone/reseed; automatic failover; independent fencing; controller custom-Build
integration; TLS, cross-host or Kubernetes. No production availability,
durability, security or failover claim follows from this host-local primitive.

## Implementation references

- [Authority and structured boundary validation, reopen.rs:29–147](../crates/kuberic-mysql/src/service/reopen.rs#L29-L147).
- [Linear closure, capture and closed handle, reopen.rs:180–365](../crates/kuberic-mysql/src/service/reopen.rs#L180-L365).
- [Closed admission and memory-only log inventory, reopen.rs:414–514](../crates/kuberic-mysql/src/service/reopen.rs#L414-L514).
- [Production custody and genuine revocation, instance.rs:156–270](../crates/kuberic-mysql/src/service/instance.rs#L156-L270),
  [terminal proxy settlement, proxy.rs:870–984](../crates/kuberic-mysql/src/service/proxy.rs#L870-L984),
  [graceful-stop/reopen seams, instance.rs:1470–1530](../crates/kuberic-mysql/src/service/instance.rs#L1470-L1530),
  and [exclusive diagnostics, instance.rs:2330–2372](../crates/kuberic-mysql/src/service/instance.rs#L2330-L2372).
- [Inactive native snapshots, control.rs:42–191](../crates/kuberic-mysql/src/service/control.rs#L42-L191)
  and [monotonic durability reader, control.rs:698–729](../crates/kuberic-mysql/src/service/control.rs#L698-L729).
- [Actual terminal production-API qualification, service_reopen_live_mysql_8_4_11.rs:393–875](../crates/kuberic-mysql-tests/tests/service_reopen_live_mysql_8_4_11.rs#L393-L875).
