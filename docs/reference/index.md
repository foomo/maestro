# Configuration Reference

Full API documentation lives on
[pkg.go.dev/github.com/foomo/maestro](https://pkg.go.dev/github.com/foomo/maestro) —
this page covers the tunable knobs on `player.Options` and
`soloist.Options`, since those aren't fully self-explanatory from godoc
alone. See [The 3PC Protocol](/guide/core-concepts) for what each timeout
actually gates.

## `player.Options`

| Field | Type | Default | Notes |
|---|---|---|---|
| `Transport` | `transport.Transport` | — | **Required.** Build via `transport.NewTransport(nc)`. |
| `BlobReader` | `blobstore.BlobReader` | — | **Required.** Read-only; the Player never writes. |
| `StageHandler` | `player.StageHandler` | — | **Required.** See [Implementing StageHandler](/guide/stagehandler). |
| `InstanceID` | `string` | — | Roster key. Use a stable identity (pod hostname), not a per-boot random value. |
| `HeartbeatPeriod` | `time.Duration` | `5s` | Lower = faster roster convergence, more NATS traffic. |
| `DownloadConcurrency` | `int` | `4` | Parallel blob fetches within one `PreCommit`. |
| `MeterProvider` | `metric.MeterProvider` | `otel.GetMeterProvider()` | OTel metrics. |
| `TracerProvider` | `trace.TracerProvider` | `otel.GetTracerProvider()` | OTel tracing. |
| `Logger` | `*zap.Logger` | `zap.NewNop()` | |

## `soloist.Options`

| Field | Type | Default | Notes |
|---|---|---|---|
| `Transport` | `transport.Transport` | — | **Required.** Build via `transport.NewTransport(nc)`. |
| `BlobStore` | `blobstore.BlobStore` | — | **Required.** |
| `InstanceID` | `string` | — | Not part of the protocol's identity scheme; used for logging. |
| `HeartbeatWindow` | `time.Duration` | `15s` | A player is "alive" if a heartbeat arrived within this window. Round targeting additionally requires it to be *wired* — see [The roster](/guide/core-concepts#the-roster). |
| `RosterScanTick` | `time.Duration` | `5s` | How often the resync loop checks for stale players. |
| `ResyncDebounce` | `time.Duration` | `10s` | Minimum gap between two resync rounds. |
| `CanCommitTimeout` | `time.Duration` | `10s` | Phase 1 deadline. Timeout aborts the round. |
| `StageTimeout` | `func(totalSize int64) time.Duration` | `~2× size / 10 MiB/s`, clamped `[60s, 30m]` | Phase 2 deadline. Timeout aborts the round. Override for slow `StageHandler.Stage` decode logic. |
| `DoCommitTimeout` | `time.Duration` | `10s` | Phase 3 deadline. Timeout does **not** abort — stragglers are marked dirty for the next resync. |
| `MeterProvider` | `metric.MeterProvider` | `otel.GetMeterProvider()` | OTel metrics. |
| `TracerProvider` | `trace.TracerProvider` | `otel.GetTracerProvider()` | OTel tracing. |
| `Logger` | `*zap.Logger` | `zap.NewNop()` | |

## Player state predicates

| Method | True when | Use for |
|---|---|---|
| `Wired()` | The broker has acknowledged all four round subscriptions. The player will receive the next round. | Kubernetes readiness probe. See [Keel integration](/guide/keel-integration#gate-readiness-on-wired-not-ready). |
| `Ready()` | The first `DoCommit` has succeeded — the player has activated *some* version. | "Do I have data yet?" in your own handler. Not a pod-health signal. |

Until `Wired()` is true the player heartbeats with `Heartbeat.NotWired` and
the Soloist leaves it out of round targeting, so a starting pod cannot
abort a publish for its peers.

## Errors

Sentinel errors defined in the root `maestro` package (`errors.go`),
returned wrapped via `%w` from the relevant call sites:

| Error | Meaning |
|---|---|
| `ErrNoPlayers` | No players in roster at the time an operation expected at least one. |
| `ErrAbort` | Round was aborted. |
| `ErrGenStale` | A message carried a generation token older than the current boot epoch. |
| `ErrManifestMismatch` | `Manifest.Validate()` failed — see [BlobStore](/guide/blobstore#manifest-and-version). |
| `ErrBlobstoreMismatch` | Soloist and Player disagree on blobstore kind. |
| `ErrDuplicateInstance` | Duplicate `InstanceID` observed in the roster. |
| `ErrRoundInFlight` | Another round is already running. |
| `ErrUnsafeName` | A manifest file name failed the path-safety check (`github.com/foomo/go/sec`). |

## Packages

| Package | Purpose |
|---|---|
| [`github.com/foomo/maestro`](https://pkg.go.dev/github.com/foomo/maestro) | `Version`, `Manifest`, sentinel errors — shared by both roles. |
| [`.../pkg/soloist`](https://pkg.go.dev/github.com/foomo/maestro/pkg/soloist) | Writer role: `Soloist`, `Publish`, `IngestFiles`. |
| [`.../pkg/player`](https://pkg.go.dev/github.com/foomo/maestro/pkg/player) | Reader role: `Player`, `StageHandler`, `FileSource`. |
| [`.../pkg/transport`](https://pkg.go.dev/github.com/foomo/maestro/pkg/transport) | Typed pub/sub bundle (`Transport`, `NewTransport`) over goflux+NATS. |
| [`.../pkg/blobstore`](https://pkg.go.dev/github.com/foomo/maestro/pkg/blobstore) | `BlobStore` / `BlobReader` interfaces. |
| [`.../pkg/blobstore/localfs`](https://pkg.go.dev/github.com/foomo/maestro/pkg/blobstore/localfs) | Filesystem-backed `BlobStore` + HTTP `Handler`/`Client`. |
