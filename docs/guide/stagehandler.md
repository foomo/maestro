# Implementing StageHandler

`StageHandler` is the only interface application code implements against
the Player. It owns the lifecycle of whatever in-memory value you're
replicating.

```go
type StageHandler interface {
	Stage(ctx context.Context, v maestro.Version, m maestro.Manifest, src FileSource) error
	Activate(ctx context.Context, v maestro.Version) error
	Abort(ctx context.Context, v maestro.Version) error
}
```

The Player calls these three methods, in this order, for a given `v`:

1. `Stage` — always called first, once the manifest's files have been
   downloaded and hash-verified. Build your artifact and hold it as
   *pending*; do not mutate any state a reader of `Current()` might
   observe yet.
2. Either `Activate` (round succeeded) or `Abort` (round failed / was
   superseded) — exactly one of the two follows a `Stage` call, never both,
   never neither.

## Stage

```go
func (h *myHandler) Stage(ctx context.Context, v maestro.Version, m maestro.Manifest, src player.FileSource) error {
	r, err := src.Open("catalog.json")
	if err != nil {
		return err
	}
	defer r.Close()

	var c Catalog
	if err := json.NewDecoder(r).Decode(&c); err != nil {
		return err
	}

	h.mu.Lock()
	h.pending[v] = &c
	h.mu.Unlock()

	return nil
}
```

- **`src.Open(name)`** returns a stream whose bytes are already sha256
  hash-verified against the manifest as you read — a mismatch surfaces as
  a read error. Don't re-hash; if `Open`/read succeeds, the bytes are the
  ones the Soloist published under `v`. `src.List()` gives you every file
  name in the manifest, in order, if you don't know the names up front.
- **Returning an error votes against the round.** The Soloist sees this
  player's `Staged.OK = false`, aborts, and every player (including this
  one) gets `Abort` — not just this player. A decode failure on one
  replica takes down the whole rollout by design; that's what "atomic"
  means here. Don't swallow errors to avoid this — if the manifest is
  genuinely bad for this replica, every replica needs to know.
- **Keep `pending` keyed by `Version`.** A later `Stage` call for a
  *different* version (e.g. a resync round targeting a newer publish that
  raced ahead) can arrive before the current pending one is
  activated/aborted — don't assume single-slot state unless you're certain
  only one round is ever in flight for your deployment.

## Activate

```go
func (h *myHandler) Activate(ctx context.Context, v maestro.Version) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	p, ok := h.pending[v]
	delete(h.pending, v)
	if !ok {
		return fmt.Errorf("no pending for %q", v)
	}

	h.cur.Store(p)
	return nil
}
```

- Called on `DoCommit`. This is the atomic flip: promote the pending
  artifact to whatever field/pointer your readers actually consult
  (`atomic.Pointer[T]`, as above, or an equivalent single-write swap).
- If this returns an error, the Player reports `Committed.OK = false` for
  this round — but phase 3 failures don't abort other players (see
  [phase timeouts](/guide/core-concepts#phase-timeouts)). This one
  replica is now the odd one out; the Soloist marks it dirty for the next
  resync rather than trying to unwind the others.
- `Current()` (or your equivalent read accessor) is not part of the
  `StageHandler` interface — it's convention. Expose it however fits your
  type; the framework never calls it.

## Abort

```go
func (h *myHandler) Abort(ctx context.Context, v maestro.Version) error {
	h.mu.Lock()
	delete(h.pending, v)
	h.mu.Unlock()
	return nil
}
```

- Called when the round aborts (someone else voted no, or a phase timed
  out) for a version this player had staged.
- **Must be safe to call for a version that never successfully staged.**
  If `Stage` itself failed, the Player still may invoke `Abort` as
  cleanup — treat "nothing pending for `v`" as success, not an error.
- Never returns an error the Soloist acts on differently — it's logged as
  a warning and otherwise ignored. Use it purely for local cleanup
  (releasing buffers, closing temp resources), not for signaling back into
  the protocol.

## Concurrency

A single `Player` invokes `StageHandler` methods from its own internal
subscriber goroutines — `Stage`/`Activate`/`Abort` for a *given round* are
serialized by the Player, but if your process runs a Player whose
`StageHandler` is shared with other code, that other code can read
`Current()`-equivalent state concurrently with an in-flight `Activate`.
Guard the read/write with the same primitive (mutex, atomic pointer) shown
above; don't assume external synchronization.

## Testing your handler

You don't need NATS or a BlobStore to unit-test a `StageHandler` in
isolation — `Stage` only needs something satisfying `player.FileSource`:

```go
type fakeSource struct{ files map[string]string }

func (f fakeSource) Open(name string) (io.ReadCloser, error) {
	body, ok := f.files[name]
	if !ok {
		return nil, fmt.Errorf("no such file %q", name)
	}
	return io.NopCloser(strings.NewReader(body)), nil
}

func (f fakeSource) List() []string {
	names := make([]string, 0, len(f.files))
	for n := range f.files {
		names = append(names, n)
	}
	return names
}
```

For a full end-to-end test with real 3PC rounds, wire a `Soloist` and
`Player` against an in-process NATS server the way
[`integration_test.go`](https://github.com/foomo/maestro/blob/main/integration_test.go)
does.
