# kuberic-mysql

`kuberic-mysql` is a design-stage integration for managing Oracle MySQL with
Kuberic. The proposed first target is Oracle MySQL 8.4 LTS on Linux with three
host-local members and single-primary Group Replication.

The [high-level design](docs/design.md) defines the intended authority,
identity, progress, lifecycle, fencing, recovery, security, testing, and staged
delivery contracts. Kuberic remains the durable lifecycle and client-access
authority; MySQL retains native Group Replication membership, transport, and
GTID history.

## Status

This repository currently delivers documentation only. It does not contain a
MySQL adapter, process supervisor, controller integration, deployment assets,
or live test fixture, and it makes no production, cross-host, or Kubernetes
support claim.

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
