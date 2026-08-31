# Getting Started

This walks through the smallest working setup: one Soloist, one Player, a
`localfs` BlobStore, and a NATS server, all in a single process. It mirrors
`integration_test.go` in the repo root — read that file for more scenarios
(multi-player convergence, stage rejection, player restart/resync).

## Prerequisites

```bash
go get github.com/foomo/maestro
go get github.com/nats-io/nats-server/v2
```

You need a running NATS server reachable by both roles. In production
that's typically an embedded server inside the Soloist pod (see
[keel Integration](/guide/keel-integration)); here we start one directly.

## 1. Start NATS

```go
package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/foomo/maestro"
	"github.com/foomo/maestro/pkg/blobstore/localfs"
	"github.com/foomo/maestro/pkg/player"
	"github.com/foomo/maestro/pkg/soloist"
	"github.com/foomo/maestro/pkg/transport"
	natsd "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func main() {
	ns, err := natsd.NewServer(&natsd.Options{Host: "127.0.0.1", Port: -1})
	if err != nil {
		panic(err)
	}

	go ns.Start()

	if !ns.ReadyForConnections(2 * time.Second) {
		panic("nats not ready")
	}

	defer ns.Shutdown()

	url := ns.ClientURL()
	// continued below
```

## 2. Wire the Soloist

The Soloist owns the `BlobStore` (write side). `localfs` needs only a data
directory — it stages files, then atomically promotes them on `Finalize`.

```go
	bs, err := localfs.NewStore(localfs.Config{DataDir: "/tmp/maestro-demo"})
	if err != nil {
		panic(err)
	}

	solConn, err := nats.Connect(url)
	if err != nil {
		panic(err)
	}
	defer solConn.Close()

	sol, err := soloist.New(soloist.Options{
		Transport:  transport.NewTransport(solConn),
		BlobStore:  bs,
		InstanceID: "soloist-1",
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go sol.Start(ctx) //nolint:errcheck
```

## 3. Implement StageHandler and wire the Player

The `StageHandler` is where your application logic lives — see
[Implementing StageHandler](/guide/stagehandler) for the full contract.
This one just keeps the latest file bodies in memory.

```go
	h := newMemHandler()

	playConn, err := nats.Connect(url)
	if err != nil {
		panic(err)
	}
	defer playConn.Close()

	pl, err := player.New(player.Options{
		Transport:    transport.NewTransport(playConn),
		BlobReader:   bs, // same *localfs.Store: in-process, no HTTP hop needed
		InstanceID:   "player-1",
		StageHandler: h,
	})
	if err != nil {
		panic(err)
	}

	go pl.Start(ctx) //nolint:errcheck

	for !pl.Wired() {
		time.Sleep(10 * time.Millisecond)
	}
```

::: tip Same-process BlobReader
`*localfs.Store` satisfies both `blobstore.BlobStore` (writer) and
`blobstore.BlobReader` (reader), so an in-process Player can read directly
off the Store without going through `Store.Handler()` / `localfs.NewClient`.
A Player in a different process talks to the Soloist's HTTP handler instead
— see [BlobStore](/guide/blobstore).
:::

## 4. Publish

```go
	v, err := sol.Publish(ctx, []soloist.File{
		{Name: "greeting.txt", Reader: strings.NewReader("hello, maestro")},
	})
	if err != nil {
		panic(err)
	}

	for pl.CurrentVersion() != v {
		time.Sleep(10 * time.Millisecond)
	}

	fmt.Println("player active version:", pl.CurrentVersion())
	fmt.Println("player data:", h.Current()["greeting.txt"])
}
```

## The StageHandler used above

```go
type memHandler struct {
	mu      sync.Mutex
	pending map[maestro.Version]map[string]string
	active  atomic.Pointer[map[string]string]
}

func newMemHandler() *memHandler {
	return &memHandler{pending: make(map[maestro.Version]map[string]string)}
}

func (h *memHandler) Current() map[string]string {
	if p := h.active.Load(); p != nil {
		return *p
	}
	return nil
}

func (h *memHandler) Stage(_ context.Context, v maestro.Version, _ maestro.Manifest, src player.FileSource) error {
	out := make(map[string]string)
	for _, name := range src.List() {
		r, err := src.Open(name)
		if err != nil {
			return err
		}
		body, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			return err
		}
		out[name] = string(body)
	}

	h.mu.Lock()
	h.pending[v] = out
	h.mu.Unlock()
	return nil
}

func (h *memHandler) Activate(_ context.Context, v maestro.Version) error {
	h.mu.Lock()
	out, ok := h.pending[v]
	delete(h.pending, v)
	h.mu.Unlock()

	if !ok {
		return fmt.Errorf("no pending for %q", v)
	}
	h.active.Store(&out)
	return nil
}

func (h *memHandler) Abort(_ context.Context, v maestro.Version) error {
	h.mu.Lock()
	delete(h.pending, v)
	h.mu.Unlock()
	return nil
}
```

Since only one player was in the roster when `Publish` was called (and it
had already heartbeated in), this ran a full 3PC round: `CanCommit` →
`PreCommit` (download + `Stage`) → `DoCommit` (`Activate`). If the roster
had been empty at publish time, the Soloist would have taken the
[silent-commit](/guide/core-concepts#silent-commit) path instead, and the
Player would pick it up on its next resync.

## Next

- [The 3PC Protocol](/guide/core-concepts) for what happens on the wire
  and how failures are handled.
- [keel Integration](/guide/keel-integration) to run this for real, with
  health checks and an embedded NATS server.
