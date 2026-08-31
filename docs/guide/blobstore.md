# BlobStore

Control-plane traffic (rounds, votes, heartbeats) is small typed messages
over NATS. File bytes are a different concern — potentially large, and not
something you want serialized through NATS request/reply. `BlobStore`
decouples the two.

```go
type BlobStore interface {
	Writer(ctx context.Context, v maestro.Version, name string) (io.WriteCloser, error)
	Finalize(ctx context.Context, v maestro.Version, m maestro.Manifest) error
	Stat(ctx context.Context, v maestro.Version, name string) (sha256 string, size int64, err error)
	Delete(ctx context.Context, v maestro.Version) error
}

type BlobReader interface {
	Reader(ctx context.Context, v maestro.Version, name string) (io.ReadCloser, int64, error)
}
```

`BlobStore` is the writer-side surface — only the Soloist needs it.
`BlobReader` is the read-only subset the Player needs. Implementations are
free to satisfy both on one type (`localfs.Store` does); a Player only
ever needs the `BlobReader` half, which is the point of splitting them —
you can hand a Player a client that has no write capability at all.

## Manifest and Version

```go
type ManifestFile struct {
	Name string
	Hash string // sha256, hex
	Size int64
}

type Manifest struct {
	Version   Version
	Files     []ManifestFile
	TotalSize int64
}
```

`Version` is an opaque `string` — treat it as an identifier, not something
to parse. The concrete value (when produced by `soloist.IngestFiles`, which
is what `Soloist.Publish` calls internally) is the hex sha256 of the
concatenation of each file's own sha256 — content-addressed, so publishing
byte-identical input twice yields the same `Version`. Nothing else in
maestro depends on this scheme; a custom `BlobStore`/ingest path could use
any string.

## Lifecycle a BlobStore implementation must support

1. **`Writer(ctx, v, name)`** — called once per file during ingest, before
   the file's `Version` is even known (the caller stages under a temporary
   label). Returns an `io.WriteCloser`; the caller writes the full body
   and calls `Close`.
2. **`Finalize(ctx, v, m)`** — called once per publish, after every file
   has been written. `v` is the staging label used in step 1; `m.Version`
   is the final, content-addressed destination — they may differ.
   Implementations must atomically promote staged files to the
   `m.Version`-keyed destination and only then make the manifest itself
   visible. **Must be idempotent**: if the destination already has a
   manifest, succeed without error (this is what makes silent-commit-then-
   crash-then-retry safe).
3. **`Stat(ctx, v, name)`** — sha256 + size of an already-finalized file,
   computed by re-reading it. Used for out-of-band verification, not by
   the hot path.
4. **`Delete(ctx, v)`** — removes all artifacts of a finalized version.
   Not called automatically by anything in this package — wire your own
   GC policy (e.g. keep last N versions) if you need one.

The `Finalize`-writes-manifest-last ordering is the atomicity barrier: a
reader that lists a version directory mid-promotion should never see a
"finalized" version without its manifest, because manifest presence *is*
the finalized signal (see `localfs.Store.Reader`, which checks the
manifest file's existence before serving any blob).

## localfs

`pkg/blobstore/localfs` is the in-box implementation, filesystem-backed.

```go
bs, err := localfs.NewStore(localfs.Config{DataDir: "/var/lib/maestro/data"})
```

Layout under `DataDir`:

```
staging/<label>/...           # ingest working area, pre-Finalize
versions/<version>/manifest.msgp
versions/<version>/<file...>
```

### Serving to remote players

If Soloist and Player are different processes (the normal case), expose
`Store.Handler()` — a `net/http.Handler` — from the Soloist, and point
Players at it with `localfs.NewClient`:

```go
// Soloist side
svr.AddInternalHTTPService(bs.Handler()) // GET /versions/{version}/files/{name...}

// Player side
pl, _ := player.New(player.Options{
	BlobReader: localfs.NewClient("http://soloist.internal:8080"),
	// ...
})
```

`Handler()` serves via `http.ServeContent` — zero-copy sendfile where the
OS supports it, `Range` requests, conditional GET (`If-Modified-Since` /
`If-None-Match`). It returns `404` for an unfinalized version or missing
file, `400` on a path-traversal attempt in `name`.

If Soloist and Player run **in the same process** (uncommon outside tests
and the [getting-started](/guide/getting-started) example), skip the HTTP
hop and hand the Player the same `*localfs.Store` directly — it already
satisfies `BlobReader`.

### Path safety

Both `Writer` and the HTTP handler validate `name` against path traversal
via `github.com/foomo/go/sec`, returning `maestro.ErrUnsafeName` /
HTTP 400. `Manifest.Validate()` (called from `Finalize` and from the
Player's `CanCommit` handler) independently rejects absolute paths and
`..`-escaping names at the manifest level, so a hostile or buggy manifest
is caught before any download is attempted — not just at the blob layer.

## Writing your own BlobStore

Swap in S3, GCS, or anything else that gives you an `io.Writer` for
upload, an `io.Reader` for download, and somewhere durable to park the
manifest. Keep in mind:

- `Writer` may be called many times concurrently (once per file in a
  `Publish` batch) — implementations must support concurrent writers to
  distinct `(v, name)` keys.
- `Finalize` runs once per publish and must be atomic *and* idempotent —
  see above. A partial promotion that leaves the manifest visible before
  every file has landed breaks the guarantee the whole protocol depends
  on.
- The Player never calls `Writer`/`Finalize`/`Delete` — a read-only
  implementation of `BlobReader` alone is sufficient to wire a Player.
