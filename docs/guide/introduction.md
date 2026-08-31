# Introduction

## The problem

You have a value that lives in process memory across every replica of a
service — a routing table, a feature-flag set, a compiled catalog, a search
index snapshot. Something outside the cluster (a build pipeline, an admin
action, a cron job) produces a new version of it, and every pod needs to
start using it at the same moment. Not eventually-consistent, not
"whichever pod polled last" — the same moment, or provably not yet.

The usual answers don't fit this shape well:

- **A shared database or cache** turns every read into a network hop and
  adds an availability dependency to your hot path. You wanted the data in
  memory precisely to avoid that.
- **Polling a config source** (S3 object, ConfigMap, etcd key) gives you
  eventual consistency with an unbounded skew window, and no atomicity
  across replicas — pod A can be serving v5 while pod B is still on v4,
  and neither knows it.
- **A pub/sub broadcast** ("hey, reload!") races against the reader's own
  reload logic. If the reload can fail partway (a bad file, a decode
  error), some replicas silently end up on the new version and others
  silently stay on the old one.

maestro is for the specific case where you need **all-or-nothing,
whole-cluster commits** of an in-memory value, driven by a single writer,
with a bounded number of readers whose identities the writer can track via
heartbeat.

## What maestro is

maestro replicates a value from one writer (the **Soloist**) to many
readers (**Players**) using a three-phase commit (3PC) over NATS. Readers
vote on whether they can accept a new version before anyone commits to it;
if any reader votes no, or fails during staging, the round aborts and every
reader keeps what it had. There is no partial rollout state that a client
can observe.

```
            ┌──────────┐  heartbeats (NATS)   ┌─────────┐
            │  Player  │ ───────────────────▶ │ Soloist │
            │  (× N)   │ ◀─────────────────── │  (× 1)  │
            └────┬─────┘     3PC (NATS)       └────┬────┘
                 │                                 │
                 │ HTTP GET /versions/{v}/files/…  │
                 ▼                                 ▼
            ┌──────────────────────────────────────┐
            │   BlobStore  (localfs / S3 / …)       │
            └──────────────────────────────────────┘
```

Two things are deliberately decoupled:

- **Control plane** (round coordination, votes, heartbeats) is small,
  typed messages over NATS via [goflux](https://github.com/foomo/goflux).
  This is the only thing 3PC coordinates.
- **Data plane** (the actual file bytes) moves through a pluggable
  [`BlobStore`](/guide/blobstore) — `localfs` ships in-box and serves
  files over a plain `http.Handler`. Swap it for S3, GCS, or anything else
  that can hand back an `io.Reader` and store a version-addressed manifest.

Your own state — the `Catalog`, the routing table, whatever Go value you're
replicating — never touches maestro's wire format. maestro moves opaque
named files (a manifest of name/hash/size); you decide what's inside them
and how to decode them in your [`StageHandler`](/guide/stagehandler).

## What maestro is not

- **Not a leader-election system.** Soloist is a `replicas=1` Deployment
  with stable DNS — one designated writer, not an elected one. If you need
  automatic writer failover, put something in front that restarts a single
  Soloist pod; maestro does not do this for you.
- **Not a general message bus.** The transport carries only maestro's own
  round/vote/heartbeat traffic. Use NATS directly, or another tool, for
  your application's other pub/sub needs.
- **Not a database.** There's no query interface, no partial reads, no
  history beyond the current and (briefly) prior version. Forward-only: a
  bad publish is fixed by the next publish, not a rollback.
- **Not for large continuous data.** Manifests are versioned snapshots.
  If your workload is a high-frequency stream, this is the wrong tool —
  reach for NATS JetStream, Kafka, or similar.

## Where to go next

- [Getting Started](/guide/getting-started) — wire a Soloist and a Player
  in a single process against an embedded NATS server.
- [The 3PC Protocol](/guide/core-concepts) — the round lifecycle, abort
  path, silent commit, and resync.
- [Implementing StageHandler](/guide/stagehandler) — the interface you
  actually write against.
- [BlobStore](/guide/blobstore) — the pluggable byte-transfer layer.
- [keel Integration](/guide/keel-integration) — wiring into a
  `foomo/keel` server, health checks, readiness semantics.
