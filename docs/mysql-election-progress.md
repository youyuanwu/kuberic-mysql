# MySQL Election Progress Research and Decision

## Status

Accepted architectural direction for the qualified Kuberic Public V1
projection, researched on 2026-10-16. This status records the selected design;
it does not expand the lifecycle behavior delivered by the current adapter.
The historical comparison below intentionally retains its reviewed Kuberic
commit. The delivered adapter is now pinned to
`dabe144670303b22b1fe13f5ced78ab57ad3f98c` (upstream main resolved once on
2026-10-19 and frozen for both crates).

The first Stage 3 [closed retained-owner reopen](mysql-safe-reopen.md) compares
complete structured executed/purged sets, including nonprojectable shapes.
It returns no election, provider-committed or donor/catch-up scalar and does not
feed a reopened tail into the fresh-zero or all-voter commitment proof. Explicit
generic history/session renewal scope is not, by itself, progress or serving
authority.

## Question

Kuberic Public V1 requires an `i64` log sequence number (LSN) that can represent
replica progress. MySQL does not expose a PostgreSQL-style WAL LSN with the same
election meaning. The question was whether established MySQL high availability
systems elect a primary using the same projection implemented by this
repository:

```text
<configured-group-uuid>:1-N -> N
```

## Evidence Boundary

The comparison is limited to the following reviewed versions and immutable
source snapshots. It is not an exhaustive survey of every MySQL high
availability implementation.

| System | Reviewed evidence |
|---|---|
| Oracle MySQL | MySQL 8.4.11, source commit [`99960bf74fa919347e4f4e3ca47672f333d6e91f`](https://github.com/mysql/mysql-server/tree/99960bf74fa919347e4f4e3ca47672f333d6e91f) |
| Vitess | v24.0.5, source commit [`9b28c872ea20bfbe9d5ab89621473910157aa772`](https://github.com/vitessio/vitess/tree/9b28c872ea20bfbe9d5ab89621473910157aa772) |
| Orchestrator | v3.2.6, source commit [`c846d43668239cad384dc31b9255a3ade3a35001`](https://github.com/openark/orchestrator/tree/c846d43668239cad384dc31b9255a3ade3a35001) |
| MHA | v0.58, source commit [`abe11f9abb74bc5886012ff5cc9e209fb9b093c1`](https://github.com/yoshinorim/mha4mysql-manager/tree/abe11f9abb74bc5886012ff5cc9e209fb9b093c1) |
| Percona Operator for MySQL | v1.2.0, source commit [`71fec83da870c47c3d75165ca36b848ce86fdb73`](https://github.com/percona/percona-server-mysql-operator/tree/71fec83da870c47c3d75165ca36b848ce86fdb73) |
| Kuberic Public V1 | pinned source commit [`ceec5887ad97d71222117610428ce715b782777e`](https://github.com/youyuanwu/kuberic/tree/ceec5887ad97d71222117610428ce715b782777e) |

The product summaries below identify the reviewed progress or ranking
representation. They are not complete descriptions of each product's
eligibility, recovery, durability, policy, routing, and failover authority.

## Research Results

No surveyed implementation uses the exact qualified `UUID:1-N -> N` scalar as
its general primary-election position. The reviewed systems either elect
independently of replication progress, compare structured GTID positions, or
compare binary-log coordinates.

### Oracle MySQL Group Replication 8.4.11

Group Replication's automatic primary election does not rank candidates by an
LSN or GTID tail. For the reviewed version it restricts candidates to `ONLINE`
members in the exact lowest-version range, then orders that range by highest
`group_replication_member_weight` and lowest lexicographical `server_uuid`.
Members adapt to the election algorithm supported by the lowest server version
in the group, so older compatible versions can change whether weight
participates.

GTID state still affects distributed-recovery completion and membership
eligibility, but it is not an election ordering key. The elected member can
have an unapplied backlog. The default `BEFORE_ON_PRIMARY_FAILOVER` behavior
holds new transactions until that primary's own backlog has been applied,
subject to `wait_timeout`; it is not an unbounded group-wide applied barrier.
Election, recovery eligibility, and transaction application progress are
therefore separate concerns.

Sources:

- [MySQL 8.4 Reference Manual: Single-Primary Mode](https://docs.oracle.com/cd/E17952_01/mysql-8.4-en/group-replication-single-primary-mode.html)
- [MySQL 8.4 Reference Manual: Group Replication GTIDs](https://docs.oracle.com/cd/E17952_01/mysql-8.4-en/group-replication-gtids.html)
- [MySQL 8.4 Reference Manual: Consistency Guarantees](https://docs.oracle.com/cd/E17952_01/mysql-8.4-en/group-replication-configuring-consistency-guarantees.html)
- [MySQL 8.4.11 primary election implementation](https://github.com/mysql/mysql-server/blob/99960bf74fa919347e4f4e3ca47672f333d6e91f/plugin/group_replication/src/plugin_handlers/primary_election_invocation_handler.cc#L346-L481)

### Vitess v24.0.5

Vitess EmergencyReparentShard prioritizes replication position to minimize data
loss, but it retains received and executed positions as structured,
flavor-specific values. For MySQL GTID replication, containment is evaluated
over MySQL56 SID-to-interval sets. Candidate positions form a partial order:
incomparable histories can remain tied by position rather than being flattened
to one integer.

Position is not complete promotion authority. Emergency reparenting collects
and restricts candidates, waits for relay-log application, removes errant-GTID
candidates on the MySQL GTID path, applies durability policy and deterministic
tie-breakers, and verifies that the selected winner contains every other valid
position. MariaDB and file-position flavors use different representations, so
the MySQL SID and disjoint-interval conclusions are not generalized to every
Vitess flavor.

Sources:

- [Vitess replication position abstraction](https://github.com/vitessio/vitess/blob/9b28c872ea20bfbe9d5ab89621473910157aa772/go/mysql/replication/replication_position.go#L37-L97)
- [Vitess MySQL56 GTID-set containment](https://github.com/vitessio/vitess/blob/9b28c872ea20bfbe9d5ab89621473910157aa772/go/mysql/replication/mysql56_gtid_set.go#L148-L274)
- [Vitess reparent candidate sorter](https://github.com/vitessio/vitess/blob/9b28c872ea20bfbe9d5ab89621473910157aa772/go/vt/vtctl/reparentutil/reparent_sorter.go#L29-L164)
- [Vitess emergency reparent selection and validation](https://github.com/vitessio/vitess/blob/9b28c872ea20bfbe9d5ab89621473910157aa772/go/vt/vtctl/reparentutil/emergency_reparenter.go#L260-L532)
- [Vitess errant-GTID filtering](https://github.com/vitessio/vitess/blob/9b28c872ea20bfbe9d5ab89621473910157aa772/go/vt/vtctl/reparentutil/emergency_reparenter.go#L880-L1000)

### Orchestrator v3.2.6

Orchestrator initially orders failover candidates by executed binary-log
coordinates: log filename and position. Compatibility,
`log_replica_updates`, data-center preference, errant GTIDs, promotion rules,
binlog format, server version, and candidate validity can also affect the
winner.

Its GTID regrouping path uses the same candidate-selection result and then uses
GTID facilities to repoint equal or behind replicas. It does not rank
candidates by GTID-set containment or by the tail of a qualified `UUID:1-N`
interval.

Sources:

- [Orchestrator executed-coordinate sorting](https://github.com/openark/orchestrator/blob/c846d43668239cad384dc31b9255a3ade3a35001/go/inst/instance_topology.go#L1897-L1947)
- [Orchestrator candidate tie-breakers](https://github.com/openark/orchestrator/blob/c846d43668239cad384dc31b9255a3ade3a35001/go/inst/instance_utils.go#L96-L155)
- [Orchestrator candidate selection](https://github.com/openark/orchestrator/blob/c846d43668239cad384dc31b9255a3ade3a35001/go/inst/instance_topology.go#L2107-L2296)
- [Orchestrator GTID regrouping](https://github.com/openark/orchestrator/blob/c846d43668239cad384dc31b9255a3ade3a35001/go/inst/instance_topology.go#L2508-L2564)

### MHA v0.58

MHA first identifies the latest replica set by received
`Master_Log_File`/`Read_Master_Log_Pos`, then compares applied
`Relay_Master_Log_File`/`Exec_Master_Log_Pos` within that set. Explicit
candidate preferences, server eligibility, version checks, binary logging, and
replication delay also participate in new-primary selection.

GTID failover mode changes auto-positioning and how replicas are attached after
selection. It does not replace the received-then-applied coordinate selection
with a scalar GTID tail. The exact current MySQL-version support matrix for MHA
was not established by the reviewed v0.58 evidence and is not claimed here.

Sources:

- [MHA latest-set and most-advanced selection](https://github.com/yoshinorim/mha4mysql-manager/blob/abe11f9abb74bc5886012ff5cc9e209fb9b093c1/lib/MHA/ServerManager.pm#L930-L1110)
- [MHA candidate and eligibility policy](https://github.com/yoshinorim/mha4mysql-manager/blob/abe11f9abb74bc5886012ff5cc9e209fb9b093c1/lib/MHA/ServerManager.pm#L1142-L1280)
- [MHA GTID mode](https://github.com/yoshinorim/mha4mysql-manager/blob/abe11f9abb74bc5886012ff5cc9e209fb9b093c1/lib/MHA/ServerManager.pm#L1535-L1564)

### Percona Operator for MySQL v1.2.0

The reviewed operator obtains backend primary selection from MySQL Group
Replication or Orchestrator rather than defining a scalar GTID election LSN.
The operator still participates operationally: it discovers and labels the
Group Replication primary, reconciles primary routing, and can request
maintenance-driven primary moves through Orchestrator or MySQL Shell.

Sources:

- [Percona Operator v1.2.0 architecture](https://docs.percona.com/percona-operator-for-mysql/1.2.0/architecture.html)
- [Percona backend selection](https://github.com/percona/percona-server-mysql-operator/blob/71fec83da870c47c3d75165ca36b848ce86fdb73/pkg/controller/ps/controller.go#L1643-L1692)
- [Percona Group Replication primary reconciliation](https://github.com/percona/percona-server-mysql-operator/blob/71fec83da870c47c3d75165ca36b848ce86fdb73/pkg/controller/ps/mysql_primary.go#L22-L92)
- [Percona maintenance-driven primary moves](https://github.com/percona/percona-server-mysql-operator/blob/71fec83da870c47c3d75165ca36b848ce86fdb73/pkg/controller/ps/controller.go#L332-L383)

## Analysis

A general MySQL GTID history is not a scalar. A GTID set can contain multiple
source UUIDs, tags, disjoint intervals, and histories that are neither subsets
nor supersets of one another. Selecting the largest sequence number would
discard those relationships.

### Exact projection domain

Fix one exact configured, untagged source UUID `u`. The repository's qualified
domain is:

```text
D = {empty} U {u:1-N | 1 <= N <= 9_223_372_036_854_775_806}
```

Each non-empty member has exactly one normalized interval beginning at `1`,
with no hidden source, tag, gap, or second interval. For non-empty members of
this domain:

```text
u:1-A is a subset of u:1-B if and only if A <= B
```

Mapping `empty` to `0` and `u:1-N` to `N` is therefore injective and
order-preserving on `D`. MySQL documents positive GTID sequence values through
`2^63-1`; this repository accepts one less,
`9_223_372_036_854_775_806`. The reason for that one-value difference was not
established by this research.

Sources:

- [Repository GTID sequence bound](../crates/kuberic-mysql/src/core/gtid.rs)
- [Repository scalar projection](../crates/kuberic-mysql/src/core/progress.rs)
- [MySQL 8.4 GTID format and storage](https://docs.oracle.com/cd/E17952_01/mysql-8.4-en/replication-gtids-concepts.html)

### Continuous qualification

Qualification is continuous, not a one-time property. The complete GTID set
and its context must be revalidated after any lifecycle event that can replace,
clear, import, or independently create history. This includes reset, clone,
rebootstrap, imported or explicit GTIDs, transaction tags, alternate
view-change sources, and independently initialized data. Such an event does
not always make the final history non-projectable, but previous qualification
cannot be carried across it without fresh proof.

Ordinary binary-log purge does not remove same-lineage transactions from
`gtid_executed`, so it can preserve applied progress while separately making
incremental recovery unavailable.

Sources:

- [MySQL 8.4 reset semantics](https://docs.oracle.com/cd/E17952_01/mysql-8.4-en/reset-binary-logs-and-gtids.html)
- [MySQL 8.4 remote clone semantics](https://docs.oracle.com/cd/E17952_01/mysql-8.4-en/clone-plugin-remote.html)

## Decision

Keep the qualified scalar GTID projection as the Kuberic Public V1 progress
value:

- empty qualified history reports `0`;
- qualified `UUID:1-N` history reports `N`;
- foreign sources, tags, gaps, multiple intervals, or multiple sources are
  non-projectable and fail closed; and
- recovering or unassigned progress can be election evidence only while
  external access remains closed.

For election authority, a primary must expose only a qualified boundary
independently proven committed under the exact installed public configuration
and commit profile, while a secondary must expose a qualified boundary durably
applied locally. The current bounded implementation qualifies those meanings
as described below; it does not claim a general MySQL commit-watermark API.

This value is an adapter-level **qualified public LSN**. It is not an InnoDB
redo LSN, a binary-log position, or a replacement for the complete MySQL GTID
set.

## Current Delivery Boundary

An exactly owned fresh New replica may report historical zero during initial
Primary/epoch or fresh idle preparation before configuration. Both scalar and
provider reads reobserve actual empty executed **and purged** history, absent
native membership and lifetime-qualified durability, with closed engine/proxy
authority. Provider reads must match the retained scalar scope; role/epoch/
configuration/Build/access changes cannot silently retag it. Native effect
adoption and failed/cancelled fresh observation retire eligibility. This is
inherited empty history, not a newly configured quorum or a serving grant
([fresh native observation](../crates/kuberic-mysql/src/service/control.rs#L1489-L1595),
[sample scope](../crates/kuberic-mysql/src/service/kuberic/proof.rs#L329-L398)).

For formed owners, the adapter samples qualified local `gtid_executed` under exact native
authority, role, configuration, process, storage, view, and receipt checks.
That observation alone establishes applied history, not primary quorum
commitment. For a primary, a separate bounded proof holds every exact public
voter owner, proves durable containment of the **whole frozen GTID set** on
each, and revalidates the source. All applicable voters are required, even in
write-quorum mode. Missing, lagging, divergent, stale, or substituted evidence
rejects instead of returning the local tail or zero. Native-only idle members
do not count as voters.

Durability is qualified only for fresh, exclusively owned processes with a
continuous enforced profile: `innodb_flush_log_at_trx_commit=1`,
`sync_binlog=1`, enabled binary logging and replica updates, and
`binlog_error_action=ABORT_SERVER`. Effective values are sampled around
history; any observed invalid interval latches the concrete owner unqualified
for its lifetime. Restoring settings does not certify history produced during
that interval. Privileged external mutation/import/restart is outside this
authority. Periodic `TRANSACTIONS_COMMITTED_ALL_MEMBERS` statistics and
`AFTER` configuration alone are not used as commitment certificates.

`StateProvider::last_committed_lsn` re-proves the retained structured primary
sample and compares complete source/participant scopes. It returns the
original scalar even if writes have advanced history; catch-up proof cannot
overwrite that pair. A secondary's accepted/applied result is not retained as
provider commitment: without an independently proved sample the provider
explicitly rejects. Epoch input is bound to the immediate predecessor and
post-revocation boundary, with a separate fresh-empty proof, not scalar
equality under unrelated authority. Completed-idle, active-before-configuration
and admitted-secondary tests each assert the exact unproved-provider boundary;
their scalar success is not successful hosted secondary reporting.

Fresh-secondary Build freezes and proves a complete structured GTID source
boundary and uses scalar progress only as the pinned Public V1 invocation and
configuration projection. Scalar equality is never substituted for GTID-set
containment. At the delivered runtime pin, omitted active-configuration
progress reuses only an exact retained identity/process-session/storage
observation; it does not create a new commit watermark.

The supported additive Build admission proves all voters in the union of
accepted and proposed configurations. A not-yet-configured target must retain
its exact successful Build receipt, epoch and ActiveSecondary role admission;
predecessor-configured donors must retain that same transition binding.
Source-first final configuration is therefore supported without pretending
that an idle native member was already a public voter. The successful native
successor view remains an exact anchor while that authority is reused.
Quiet catch-up requires fresh live structured containment, not equal scalar
advertisements or an upstream built-in copy-prefix optimization.

More precisely, omitted current progress requires an exact retained value for
the same replica identity, process session, and storage incarnation. Omitted
catch-up capability reuses exact retained numeric or known-unknown state;
explicit current progress with `must_catchup=false` may establish known-unknown
on first acceptance. Required unknown catch-up and every missing or substituted
binding fail closed. Idle Build targets still require both fields to be
`INVALID_LSN`.

Current contract sources:

- [Kuberic replication trait contracts at the delivered pin](https://github.com/youyuanwu/kuberic/blob/dabe144670303b22b1fe13f5ced78ab57ad3f98c/kuberic-runtime/src/replicator/traits.rs)
- [Kuberic provider contracts at the delivered pin](https://github.com/youyuanwu/kuberic/blob/dabe144670303b22b1fe13f5ced78ab57ad3f98c/kuberic-runtime/src/traits.rs)
- [SF semantics and quiet-copy proof boundary](https://github.com/youyuanwu/kuberic/blob/dabe144670303b22b1fe13f5ced78ab57ad3f98c/docs/features/kuberic/service-fabric-api-semantics.md)

The adapter also does not supply durable cross-restart `HistoryContext`,
restart adoption, retained-binlog catch-up capability, arbitrary differing
PC/CC histories, or automatic failover. The public traits do not supply an
application-owned durable `HistoryContext` reconstruction channel; retained
same-lifetime native history/configuration bindings are not a substitute for
that future authority. The upstream SF port plan/source ledger is not an
implemented engine cutover or production-controller activation fix.

Sources:

- [Current Public V1 progress implementation](../crates/kuberic-mysql/src/service/kuberic/driver.rs)
- [Independent live commitment proof](../crates/kuberic-mysql/src/service/kuberic/proof.rs)
- [Staged delivery and non-claims](design.md#staged-delivery)
- [Restart reconstruction requirements](design.md#restart-reconstruction)

## Election and Safety Constraints

`HistoryContext` is the exact tuple of resource UID, protocol generation,
history ID, and data-loss number. Scalar comparison requires exact equality of
all four fields. The current adapter does not persist or reconstruct that
context across restart.

Source:
[Kuberic Public V1 `HistoryContext`](https://github.com/youyuanwu/kuberic/blob/ceec5887ad97d71222117610428ce715b782777e/kuberic-runtime/src/protocol/types.rs#L303-L337).

Kuberic must not use a bare scalar comparison as complete promotion authority.
Progress values are comparable only when all of the following are proven:

1. both values belong to the same exact `HistoryContext`;
2. both were freshly projected from the same configured Group Replication UUID
   after the most recent history-changing lifecycle event;
3. both complete GTID histories satisfy the single-source contiguous profile;
4. replica identity, process session, storage incarnation, epoch, and
   configuration still match;
5. the proposed primary has the required independently proven committed or
   lossless boundary; and
6. the old write authority is fenced before the new primary can serve.

MySQL-owned lifecycle code must continue to use complete GTID-set containment,
equality, and divergence checks for native safety decisions. The scalar is a
public ordering projection after those checks, not an alternative to them.

Automatic failover remains unsupported until the staged
[failover and fencing requirements](design.md#unplanned-failover-and-quorum-recovery)
and [delivery gates](design.md#staged-delivery) pass. These include independent
fencing, exact surviving-history derivation, quorum and divergent-history
handling, durable restart reconstruction, and direct-client containment.

`catch_up_capability` remains `INVALID_LSN` until exact donor identity,
`gtid_executed`, `gtid_purged`, and retained binary-log inventory prove a safe
incremental recovery boundary. The authoritative catch-up requirements remain
in the main [scalar projection design](design.md#scalar-gtid-projection).

## Rejected Alternatives

### Report a maximum GTID sequence from arbitrary history

Rejected because the maximum hides source identity, gaps, tags, and divergent
or incomparable histories.

### Report a binary-log filename and position

Rejected because Public V1 requires one signed integer and binary-log
coordinates are file-scoped tuples whose meaning does not provide the required
cross-member GTID lineage proof.

### Follow Group Replication's native election order

Rejected as a progress definition. Version, weight, and UUID are deterministic
placement preferences, not statements about committed or applied transaction
history.

### Treat the scalar as sufficient promotion proof

Rejected because equal or ordered numbers are safe only after lineage,
identity, configuration, complete GTID-set, lifecycle, and fencing validation.
