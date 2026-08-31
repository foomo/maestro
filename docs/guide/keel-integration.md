# keel Integration

maestro's `Soloist` and `Player` both implement the `foomo/keel`
`service.Service` interface (`Name() string` + something `keel.Server` can
`Start`/`Close`), so they wire into a keel server like any other service.
This page assumes familiarity with `foomo/keel` server setup.

## Player pod

```go
svr := keel.NewServer(
	keel.WithHTTPHealthzService(true),
	keel.WithHTTPPrometheusService(true),
)
l := svr.Logger()

nc, err := keelnats.Connect(svr, natsURL)
log.Must(l, err)

h := &myHandler{pending: make(map[maestro.Version]*Catalog)}

pl, err := player.New(player.Options{
	Logger:       l,
	Transport:    transport.NewTransport(nc),
	BlobReader:   localfs.NewClient(soloistHTTPBase),
	InstanceID:   instanceID,
	StageHandler: h,
})
log.Must(l, err)

svr.AddService(pl)  // Start blocks for service lifetime
svr.AddCloser(pl)   // Close on SIGTERM

wired := healthz.NewHealthzerFn(func(_ context.Context) error {
	if !pl.Wired() {
		return errors.New("player not wired")
	}
	return nil
})
svr.AddReadinessHealthzers(wired)
svr.AddLivenessHealthzers(wired)

svr.AddPublicHTTPService(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	c := h.Current()
	if c == nil {
		http.Error(w, "no state", http.StatusServiceUnavailable)
		return
	}
	_ = json.NewEncoder(w).Encode(c)
}))

svr.Run()
```

### Gate readiness on `Wired()`, not `Ready()`

This is the one detail worth internalizing: `Player.Wired()` flips once the
broker has **acknowledged** every round subscription — not merely once
`Subscribe` was called. It means the player is a live, reachable
participant that will receive the next round, which is exactly what the
Soloist needs before counting it as a voter.
`Player.Ready()` flips only after the **first successful `DoCommit`** —
i.e. once the player has actually activated *some* version.

Until `Wired()` is true the player heartbeats with `NotWired`, and the
Soloist deliberately leaves it out of round targeting — so a pod that is
still starting up never blocks a publish for its healthy peers.

If your readiness probe uses `Ready()`, a freshly-deployed pod stays
`NotReady` until a round completes, and a cold cluster (Soloist restarted,
no `Publish` since) leaves every player permanently `NotReady` — even
though nothing is actually broken. Gate the k8s probe on `Wired()`, and
surface "I have no data yet" through your own public HTTP handler (as
above, `503` while `Current()` is nil) instead of conflating it with pod
health.

`instanceID` should be stable across restarts of the same logical
replica — use the pod hostname. It's the key the Soloist's roster tracks;
a random ID per boot defeats the "same pod restarted" resync path
described in [The 3PC Protocol](/guide/core-concepts#resync).

## Soloist pod

```go
svr := keel.NewServer(
	keel.WithInitService(keelnatsservice.MustNewEmbeddedServer()),
	keel.WithHTTPHealthzService(true),
)
l := svr.Logger()

nc, err := keelnats.Connect(svr, keelnatsservice.DefaultEmbeddedServerURL)
log.Must(l, err)

bs, err := localfs.NewStore(localfs.Config{DataDir: "/var/lib/maestro/data"})
log.Must(l, err)

svr.AddInternalHTTPService(bs.Handler()) // players GET blobs here

sol, err := soloist.New(soloist.Options{
	Logger:     l,
	BlobStore:  bs,
	Transport:  transport.NewTransport(nc),
	InstanceID: instanceID,
})
log.Must(l, err)

svr.AddService(sol)
svr.AddCloser(sol)

ready := healthz.NewHealthzerFn(func(_ context.Context) error {
	if !sol.Ready() {
		return errors.New("soloist not ready")
	}
	return nil
})
svr.AddReadinessHealthzers(ready)
svr.AddLivenessHealthzers(ready)

// wire your own publish trigger, e.g. an internal HTTP handler:
// POST /publish -> sol.Publish(ctx, []soloist.File{...})

svr.Run()
```

`keelnatsservice.MustNewEmbeddedServer()` runs NATS *inside* the Soloist
pod — no separate NATS deployment needed for a single-cluster setup.
Players reach it over the cluster network via the Soloist's service DNS.
`Soloist.Ready()` here means "heartbeats are wired," which is the correct
gate for the Soloist (there's no equivalent to the Player's "first commit"
milestone — a Soloist with zero players is still doing its job).

`bs.Handler()` is the `http.Handler` from [BlobStore](/guide/blobstore) —
mount it as an internal (cluster-only) service, since it's how every
Player fetches file bytes.

## Deployment shape

- **Soloist**: `replicas: 1`, stable Service DNS (a plain
  `ClusterIP`/headless Service is fine — no need for per-pod identity).
  There is no leader election in this package; if you need automatic
  failover of the writer role itself, that's infrastructure you provide
  (e.g. a `StatefulSet` with an external supervisor, or simply accepting
  the brief downtime of a pod restart).
- **Player**: any `replicas: N`, standard `Deployment`. Each replica needs
  a stable `InstanceID` (pod hostname works) and network access to both
  the Soloist's NATS endpoint and its blob-serving HTTP endpoint.
- **Soloist restart**: `currentVersion` and the roster are in-memory only
  — both reset on restart. Players stay `Wired()` (they can still
  heartbeat and receive rounds) but won't reach a new `Ready()` milestone
  until the Soloist's next `Publish`. If you need non-empty state
  immediately after a Soloist restart, have your `Publish` trigger run on
  boot from a persistent source (a DB, an object store, wherever the
  authoritative data actually lives) — maestro itself does not persist
  `currentVersion` for you.
