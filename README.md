# kuberic-mysql

`kuberic-mysql` contains a deterministic Rust safety core, a read-only native
observer, and bounded lifecycle ownership for fresh Oracle MySQL servers. The
delivered native target is Oracle MySQL Community Server 8.4.11 on Linux
x86-64. Administration and observation use caller-selected private
Unix-domain sockets; Group Replication uses distinct loopback-only endpoints.

The [high-level design](docs/design.md) defines the intended authority,
identity, progress, lifecycle, fencing, recovery, security, testing, and staged
delivery contracts. Kuberic remains the durable lifecycle and client-access
authority; MySQL retains native Group Replication membership, transport, and
GTID history.

Future lifecycle work follows the
[restart-stateless metadata proposal](docs/stateless-metadata.md): the MySQL
adapter and lifecycle component persist no private recovery metadata. Durable
orchestration evidence belongs to Kuberic's generic controller and agent
facilities, while native database facts remain in the MySQL data directory.

## Status

Stage 1 is delivered in the publish-disabled `kuberic-mysql` crate's `core`
module. It provides:

- validated, typed identities and exact evidence/work bindings;
- normalized MySQL GTID sets with equal, proper-subset, proper-superset, and
  incomparable relations;
- exact typed Group Replication views;
- coherent, provenance-retaining observation outcomes; and
- a minimal authority session that rejects stale completion and keeps client
  access closed.

The `kuberic_mysql::adapter` module adds one deadline-bounded, read-only
observation attempt over a validated UDS. It verifies the exact Oracle 8.4.11
product, collects opening native state, executed GTIDs, and closing native
state on one authenticated session, and returns a typed core outcome plus a
secret-free diagnostic. Transport, authentication, permission, absence,
malformed, unsupported, stale, and incoherent results remain
machine-distinguishable.

The `kuberic_mysql::service` module owns either one fresh local process
generation or one fixed topology of exactly three such generations. The
topology manager initializes all three, starts and enrolls one designated
bootstrap member, accepts one fresh one-member view, then starts and joins the
remaining members sequentially. Each join requires an exact fresh view and a
structured GTID boundary captured from the accepted predecessor. All ordinary
client access remains closed. Qualified Oracle XCom view IDs are parsed as
`<fixed u64>:<monotonic u32>` and a join receives credit only for the same
fixed part and exactly the predecessor monotonic value plus one. Skipped,
regressed, malformed, overflowed, or changed-fixed-part views fail closed.
The manager owns one monotonic clock and one absolute attempt deadline; callers
do not supply operation ticks or per-call control/observation deadlines.
The service keeps persistent MySQL data separate
from disposable runtime files, retains and attests exact children in memory,
and contains all owned members on failure or stop. It writes no application
metadata store and has no restart, reattachment, or survivor-adoption path;
loss of in-memory ownership or attempt context requires fixture reset.
Cancelling bootstrap cannot detach a query future: bootstrap-off and proof are
attempted after every enable attempt, while cancellation synchronously contains
the exact owned topology before returning control.

The original five-argument `MysqlInstanceConfig::new(mysqld, launcher,
data_root, scratch_root, timeouts)` remains the single-instance constructor and
retains its prior profile. Fixed-topology callers must use the explicit
`MysqlInstanceConfig::new_topology_member(..., topology, member_index,
timeouts)` path; topology management rejects single-instance profiles before
creating any root. It also rejects duplicate or cross-member overlapping data,
scratch, socket, PID, configuration, log, and runtime paths before mutation.

The second and only other workspace package, `kuberic-mysql-tests`, owns the
integration contracts, protocol simulators, fixtures, shared test
infrastructure, and live Oracle qualification targets.

The design requires structured GTID and native-view evidence, exact
process/storage ownership, and fresh reconciliation before client write
publication. Its later automated-failover stages additionally require
independent old-primary fencing. Native `PRIMARY` role or MySQL read-only
variables alone do not grant or fence writes.

Native `PRIMARY`, `read_only`, and `super_read_only` values are evidence only.
They never open Kuberic read or write access and do not prove fencing. The
delivered fresh three-member bootstrap/join slice is not the broader Stage 2
lifecycle PoC: controller callbacks, access reconciliation or publication,
routing, switchover, failover, Clone/reseed, replacement or destructive repair,
restart continuation, TLS, cross-host operation, containers, Kubernetes, and
production availability or security claims remain out of scope.

The live gate qualifies the exact installed Oracle MySQL 8.4.11 package.
Any other patch requires an explicit compatibility update.

See [the design's staged delivery and validation
plan](docs/design.md#staged-delivery) for the evidence required before any
future support claim.

## Prerequisites

Repository development and CI require Oracle MySQL Community Server 8.4.11 on
Linux x86-64. The supported package is
`mysql-community-server-core=8.4.11-1ubuntu24.04` from `repo.mysql.com`'s
`mysql-8.4-lts` component. `/usr/sbin/mysqld` and `/usr/bin/aa-exec` must be
installed, and the system `mysql.service` must remain stopped. Install the
repository-pinned, checksum-verified `cargo-nextest` binary with:

```bash
scripts/install_nextest.sh
```

## Validation

The standard workspace test starts isolated MySQL fixtures and runs the
observer, single-process lifecycle, and three-member bootstrap/join gates:

```bash
cargo fmt --all -- --check
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --workspace --profile ci
CARGO_BUILD_JOBS=1 cargo test --locked --offline --doc --workspace
CARGO_BUILD_JOBS=1 cargo clippy --locked --offline --workspace --all-targets -- -D warnings
```

The bounded service's deterministic contracts can be run without launching
MySQL:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests \
  --test service_config_contract --test service_process_contract \
  --test service_topology_contract
```

The native observer gate can be rerun independently:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests --test adapter_live_mysql_8_4_11 \
  -E 'test(=qualify_oracle_mysql_8_4_11)'
```

The single-process lifecycle gate can be rerun independently:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests --test service_live_mysql_8_4_11 \
  -E 'test(=one_fresh_owned_instance_lifecycle)'
```

The fresh three-member bootstrap/join gate can be rerun independently:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests \
  --test service_group_replication_live_mysql_8_4_11 \
  -E 'test(=three_fresh_members_bootstrap_join_and_cleanup)'
```

The live tests launch only isolated project-local fixture children, require all
scenarios and cleanup to pass, and remove temporary data, runtime files, and
sockets.

GitHub Actions installs the same exact Oracle package and pinned nextest binary
on Ubuntu 24.04 and runs the standard workspace gate for every pull request and
every push to `main`.
