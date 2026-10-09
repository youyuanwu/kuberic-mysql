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

Stage 1 is delivered as the publish-disabled `kuberic-mysql-core` crate. It
provides:

- validated, typed identities and exact evidence/work bindings;
- normalized MySQL GTID sets with equal, proper-subset, proper-superset, and
  incomparable relations;
- exact typed Group Replication views;
- coherent, provenance-retaining observation outcomes; and
- a minimal authority session that rejects stale completion and keeps client
  access closed.

The publish-disabled `kuberic-mysql-adapter` crate adds one deadline-bounded,
read-only observation attempt over a validated UDS. It verifies the exact
Oracle 8.4.11 product, collects opening native state, executed GTIDs, and
closing native state on one authenticated session, and returns a typed core
outcome plus a secret-free diagnostic. Transport, authentication, permission,
absence, malformed, unsupported, stale, and incoherent results remain
machine-distinguishable.

The publish-disabled `kuberic-mysql-service` crate owns one fresh local process
generation. It keeps persistent MySQL data separate from disposable
configuration, logs, PID state, temporary files, and the private socket;
retains and attests the exact child in memory; delegates observation to the
adapter; and stops, reaps, and proves socket disappearance before removing
scratch. It writes no application metadata store and has no restart,
reattachment, or survivor-adoption path. Loss of its in-memory ownership
context requires fixture reset.

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

## Validation

The ordinary gate is deterministic and requires no MySQL installation,
process, container, socket, network, or Kubernetes resource:

```bash
cargo fmt --all -- --check
CARGO_BUILD_JOBS=1 cargo test --locked --offline --workspace --all-features -- --test-threads=1
CARGO_BUILD_JOBS=1 cargo clippy --locked --offline --workspace --all-targets --all-features -- -D warnings
```

The bounded host's deterministic contracts can be run independently:

```bash
CARGO_BUILD_JOBS=1 cargo test --locked --offline -p kuberic-mysql-service -- --test-threads=1
```

The opt-in native qualification is separate and requires the exact documented
Oracle package and stopped system service:

```bash
CARGO_BUILD_JOBS=1 cargo test --locked --offline -p kuberic-mysql-adapter \
  --features live-mysql-8-4-11 --test live_mysql_8_4_11 \
  qualify_oracle_mysql_8_4_11 -- --ignored --exact --test-threads=1
```

The corresponding single-process lifecycle gate is:

```bash
CARGO_BUILD_JOBS=1 cargo test --locked --offline -p kuberic-mysql-service \
  --features live-mysql-8-4-11 --test live_mysql_8_4_11 \
  one_fresh_owned_instance_lifecycle -- --ignored --exact --test-threads=1
```

The live test launches only its isolated project-local fixture child, requires
all scenarios and cleanup to pass, and removes temporary data, runtime files,
and socket. See the
[qualification guide](qualification/mysql-uds-observation/README.md).

GitHub Actions installs the same exact Oracle package on Ubuntu 24.04 and runs
the Cargo gates directly for every pull request and every push to `main`.
