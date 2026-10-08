# kuberic-mysql

`kuberic-mysql` contains a deterministic, server-free Rust safety core for a
future Oracle MySQL integration with Kuberic. The proposed first live target is
Oracle MySQL 8.4 LTS on Linux with three host-local members and single-primary
Group Replication.

The [high-level design](docs/design.md) defines the intended authority,
identity, progress, lifecycle, fencing, recovery, security, testing, and staged
delivery contracts. Kuberic remains the durable lifecycle and client-access
authority; MySQL retains native Group Replication membership, transport, and
GTID history.

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

This is pure in-memory domain logic. The repository still contains no MySQL
adapter, process supervisor, SQL connectivity, topology mutation, TLS,
recovery automation, controller integration, deployment assets, or live test
fixture. It makes no production, cross-host, or Kubernetes support claim.

The design requires structured GTID and native-view evidence, exact
process/storage ownership, and fresh reconciliation before client write
publication. Its later automated-failover stages additionally require
independent old-primary fencing. Native `PRIMARY` role or MySQL read-only
variables alone do not grant or fence writes.

The first PoC is intentionally host-local: the adapter uses private Unix-domain
sockets, Group Replication uses loopback networking, and TLS/certificate work
is deferred. The PoC targets fresh bootstrap/join and controlled switchover
only. Clone/reseed, independent infrastructure fencing, automated unplanned
failover, cross-host security, and Kubernetes qualification are later stages.

See [the design's staged delivery and validation
plan](docs/design.md#staged-delivery) for the evidence required before any
future support claim.

## Validation

The ordinary gate is deterministic and requires no MySQL installation,
process, container, socket, network, or Kubernetes resource:

```bash
cargo fmt --all -- --check
CARGO_BUILD_JOBS=1 cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
CARGO_BUILD_JOBS=1 cargo test --locked --workspace --all-features -- --test-threads=1
cargo tree --locked --workspace --edges normal,build,dev
```
