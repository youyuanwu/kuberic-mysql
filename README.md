# kuberic-mysql

`kuberic-mysql` contains a deterministic Rust safety core and a read-only
native observer for one exact Oracle MySQL server. The delivered native target
is Oracle MySQL Community Server 8.4.11 on Linux x86-64, reached only through a
caller-selected private Unix-domain socket.

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

The publish-disabled `kuberic-mysql-adapter` crate adds one deadline-bounded,
read-only observation attempt over a validated UDS. It verifies the exact
Oracle 8.4.11 product, collects opening native state, executed GTIDs, and
closing native state on one authenticated session, and returns a typed core
outcome plus a secret-free diagnostic. Transport, authentication, permission,
absence, malformed, unsupported, stale, and incoherent results remain
machine-distinguishable.

The design requires structured GTID and native-view evidence, exact
process/storage ownership, and fresh reconciliation before client write
publication. Its later automated-failover stages additionally require
independent old-primary fencing. Native `PRIMARY` role or MySQL read-only
variables alone do not grant or fence writes.

Native `PRIMARY`, `read_only`, and `super_read_only` values are evidence only.
They never open Kuberic read or write access and do not prove fencing. The
delivered slice is not the broader Stage 2 lifecycle PoC: production process
supervision, three-member topology ownership, mutation, switchover, failover,
Clone/reseed, TLS, controller callbacks, routing, containers, Kubernetes, and
production availability or security claims remain out of scope.

The exact installed-package qualification record is
[`qualification/mysql-uds-observation/oracle-mysql-8.4.11.toml`](qualification/mysql-uds-observation/oracle-mysql-8.4.11.toml).
Any other patch requires a new qualification run and explicit compatibility
update.

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

The opt-in native qualification is separate and requires the exact documented
Oracle package and stopped system service:

```bash
./qualification/mysql-uds-observation/run-qualification.sh
```

The runner produces current validation and feature-graph receipts, launches
only its isolated project-local fixture child, writes the canonical secret-free
record after all scenarios and cleanup pass, and removes temporary receipts,
manifest, data, runtime files, and socket. See the
[qualification guide](qualification/mysql-uds-observation/README.md).

GitHub Actions installs the same exact Oracle package on Ubuntu 24.04 and runs
this full qualification runner for every pull request and every push to
`main`.
