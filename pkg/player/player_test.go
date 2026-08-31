package player_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	testingx "github.com/foomo/go/testing"
	"github.com/foomo/maestro"
	"github.com/foomo/maestro/internal/testutil"
	"github.com/foomo/maestro/pkg/blobstore/localfs"
	"github.com/foomo/maestro/pkg/player"
	"github.com/foomo/maestro/pkg/soloist"
	"github.com/foomo/maestro/pkg/transport"
	"github.com/nats-io/nats.go"
)

// dummyHandler keeps file bodies in a pending map keyed by version and
// promotes them to active on Activate.
type dummyHandler struct {
	mu      sync.Mutex
	pending map[maestro.Version]map[string]string
	active  atomic.Pointer[map[string]string]
}

func newDummyHandler() *dummyHandler {
	return &dummyHandler{pending: make(map[maestro.Version]map[string]string)}
}

func (h *dummyHandler) Current() map[string]string {
	if p := h.active.Load(); p != nil {
		return *p
	}

	return nil
}

func (h *dummyHandler) Stage(_ context.Context, v maestro.Version, _ maestro.Manifest, src player.FileSource) error {
	out := make(map[string]string)

	for _, name := range src.List() {
		r, err := src.Open(name)
		if err != nil {
			return err
		}

		b, err := io.ReadAll(r)
		r.Close()

		if err != nil {
			return err
		}

		out[name] = string(b)
	}

	h.mu.Lock()
	h.pending[v] = out
	h.mu.Unlock()

	return nil
}

func (h *dummyHandler) Activate(_ context.Context, v maestro.Version) error {
	h.mu.Lock()
	out, ok := h.pending[v]
	delete(h.pending, v)
	h.mu.Unlock()

	if !ok {
		return fmt.Errorf("dummy: no pending for %q", v)
	}

	h.active.Store(&out)

	return nil
}

func (h *dummyHandler) Abort(_ context.Context, v maestro.Version) error {
	h.mu.Lock()
	delete(h.pending, v)
	h.mu.Unlock()

	return nil
}

func dialNATS(t *testing.T, url string) *nats.Conn {
	t.Helper()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(nc.Close)

	return nc
}

func TestPlayerFullCycle(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	sNC := dialNATS(t, url)
	pNC := dialNATS(t, url)

	// Start soloist.
	s, err := soloist.New(soloist.Options{
		Transport:       transport.NewTransport(sNC),
		BlobStore:       bs,
		InstanceID:      "soloist-1",
		HeartbeatWindow: 5 * time.Second,
		RosterScanTick:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	sCtx := t.Context()

	go s.Start(sCtx) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return s.Ready() })

	// Start player.
	h := newDummyHandler()

	pl, err := player.New(player.Options{
		Transport:       transport.NewTransport(pNC),
		BlobReader:      bs,
		InstanceID:      "p1",
		HeartbeatPeriod: 50 * time.Millisecond,
		StageHandler:    h,
	})
	if err != nil {
		t.Fatal(err)
	}

	pCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go pl.Start(pCtx) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return pl.Wired() })
	time.Sleep(200 * time.Millisecond) // let first heartbeat land in roster

	// Publish a file via soloist.
	v, err := s.Publish(t.Context(), []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("hello")}})
	if err != nil {
		t.Fatal(err)
	}

	testingx.WaitFor(t, 5*time.Second, func() bool { return pl.Ready() && pl.CurrentVersion() == v })

	cur := h.Current()
	if cur["doc.bin"] != "hello" {
		t.Errorf("body %q", cur["doc.bin"])
	}
}

func TestPlayerCloseBlocksUntilDone(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})
	pNC := dialNATS(t, url)

	pl, err := player.New(player.Options{
		Transport:       transport.NewTransport(pNC),
		BlobReader:      bs,
		InstanceID:      "p-close-1",
		HeartbeatPeriod: 50 * time.Millisecond,
		StageHandler:    newDummyHandler(),
	})
	if err != nil {
		t.Fatal(err)
	}

	startDone := make(chan error, 1)
	go func() { startDone <- pl.Start(t.Context()) }()

	testingx.WaitFor(t, 2*time.Second, func() bool { return pl.Wired() })

	closeCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	if err := pl.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-startDone:
		if err != nil {
			t.Fatalf("Start returned %v after clean Close", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after Close")
	}
}

func TestPlayerCloseBeforeStart(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})
	pNC := dialNATS(t, url)

	pl, err := player.New(player.Options{
		Transport:    transport.NewTransport(pNC),
		BlobReader:   bs,
		InstanceID:   "p-close-2",
		StageHandler: newDummyHandler(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := pl.Close(t.Context()); err != nil {
		t.Fatalf("Close before Start: %v", err)
	}
}

func TestPlayerStartTwice(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})
	pNC := dialNATS(t, url)

	pl, err := player.New(player.Options{
		Transport:       transport.NewTransport(pNC),
		BlobReader:      bs,
		InstanceID:      "p-close-3",
		HeartbeatPeriod: 50 * time.Millisecond,
		StageHandler:    newDummyHandler(),
	})
	if err != nil {
		t.Fatal(err)
	}

	go pl.Start(t.Context()) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return pl.Wired() })

	if err := pl.Start(t.Context()); !errors.Is(err, player.ErrAlreadyStarted) {
		t.Fatalf("second Start: want ErrAlreadyStarted, got %v", err)
	}

	closeCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	_ = pl.Close(closeCtx)
}

func TestPlayerResyncFromStaleBoot(t *testing.T) {
	// Soloist commits before player connects (silent commit), then player
	// boots and should converge via resync.
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})
	sNC := dialNATS(t, url)
	pNC := dialNATS(t, url)

	s, _ := soloist.New(soloist.Options{
		Transport:       transport.NewTransport(sNC),
		BlobStore:       bs,
		InstanceID:      "soloist-2",
		HeartbeatWindow: 5 * time.Second,
		RosterScanTick:  100 * time.Millisecond,
		ResyncDebounce:  50 * time.Millisecond,
	})

	sCtx := t.Context()

	go s.Start(sCtx) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return s.Ready() })

	// Silent commit (no players in roster yet).
	v, err := s.Publish(t.Context(), []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("hi")}})
	if err != nil {
		t.Fatal(err)
	}

	// Now boot player; should converge via resync triggered by heartbeat.
	pl, _ := player.New(player.Options{
		Transport:       transport.NewTransport(pNC),
		BlobReader:      bs,
		InstanceID:      "p1",
		HeartbeatPeriod: 50 * time.Millisecond,
		StageHandler:    newDummyHandler(),
	})

	pCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go pl.Start(pCtx) //nolint:errcheck

	testingx.WaitFor(t, 5*time.Second, func() bool { return pl.CurrentVersion() == v })
}
