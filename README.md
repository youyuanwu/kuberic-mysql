# kuberic-mysql

`kuberic-mysql` contains a deterministic Rust safety core, a read-only native
observer, and a bounded host for one exact Oracle MySQL server. The delivered
native target is Oracle MySQL Community Server 8.4.11 on Linux x86-64, reached
only through a caller-selected private Unix-domain socket.

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

The `kuberic_mysql::service` module owns one fresh local process generation. It
keeps persistent MySQL data separate from disposable
configuration, logs, PID state, temporary files, and the private socket;
retains and attests the exact child in memory; delegates observation to the
adapter; and stops, reaps, and proves socket disappearance before removing
scratch. It writes no application metadata store and has no restart,
reattachment, or survivor-adoption path. Loss of its in-memory ownership
context requires fixture reset.

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
delivered process host is not the broader Stage 2 lifecycle PoC: three-member
topology ownership, Group Replication bootstrap/join, mutation, switchover,
failover, Clone/reseed, TLS, controller callbacks, access publication, routing,
restart continuation, containers, Kubernetes, and production availability or
security claims remain out of scope.

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

The standard workspace test starts isolated MySQL fixtures and runs both the
observer qualification and single-process lifecycle gate:

```bash
cargo fmt --all -- --check
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --workspace --profile ci
CARGO_BUILD_JOBS=1 cargo test --locked --offline --doc --workspace
CARGO_BUILD_JOBS=1 cargo clippy --locked --offline --workspace --all-targets -- -D warnings
```

The bounded host's deterministic contracts can be run without launching
MySQL:

```bash
CARGO_BUILD_JOBS=1 cargo nextest run --locked --offline --profile ci \
  -p kuberic-mysql-tests \
  --test service_config_contract --test service_process_contract
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

The live tests launch only isolated project-local fixture children, require all
scenarios and cleanup to pass, and remove temporary data, runtime files, and
sockets.

GitHub Actions installs the same exact Oracle package and pinned nextest binary
on Ubuntu 24.04 and runs the standard workspace gate for every pull request and
every push to `main`.
