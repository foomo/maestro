package maestro_test

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
	"go.uber.org/zap/zaptest"
)

// capturingHandler implements player.StageHandler and records every staged
// (then activated) map. Pending entries live until Activate or Abort.
type capturingHandler struct {
	mu      sync.Mutex
	pending map[maestro.Version]map[string]string
	all     []map[string]string
	active  atomic.Pointer[map[string]string]
}

func newCapturingHandler() *capturingHandler {
	return &capturingHandler{pending: make(map[maestro.Version]map[string]string)}
}

func (h *capturingHandler) Current() map[string]string {
	if p := h.active.Load(); p != nil {
		return *p
	}

	return nil
}

func (h *capturingHandler) Stage(_ context.Context, v maestro.Version, _ maestro.Manifest, src player.FileSource) error {
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
	h.all = append(h.all, out)
	h.mu.Unlock()

	return nil
}

func (h *capturingHandler) Activate(_ context.Context, v maestro.Version) error {
	h.mu.Lock()
	out, ok := h.pending[v]
	delete(h.pending, v)
	h.mu.Unlock()

	if !ok {
		return fmt.Errorf("capturing: no pending for %q", v)
	}

	h.active.Store(&out)

	return nil
}

func (h *capturingHandler) Abort(_ context.Context, v maestro.Version) error {
	h.mu.Lock()
	delete(h.pending, v)
	h.mu.Unlock()

	return nil
}

// dialNATS opens a *nats.Conn and registers it for cleanup.
func dialNATS(t *testing.T, url string) *nats.Conn {
	t.Helper()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(nc.Close)

	return nc
}

// startSoloist starts a Soloist with fast tick options and registers cleanup.
func startSoloist(t *testing.T, url string, bs *localfs.Store) *soloist.Soloist {
	t.Helper()

	nc := dialNATS(t, url)
	tr := transport.NewTransport(nc)

	s, err := soloist.New(soloist.Options{
		Transport:        tr,
		BlobStore:        bs,
		InstanceID:       "soloist-1",
		HeartbeatWindow:  5 * time.Second,
		RosterScanTick:   100 * time.Millisecond,
		ResyncDebounce:   50 * time.Millisecond,
		CanCommitTimeout: 500 * time.Millisecond,
		DoCommitTimeout:  500 * time.Millisecond,
		Logger:           zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())

	t.Cleanup(func() { cancel() })

	go s.Start(ctx) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return s.Ready() })

	return s
}

// newPlayer constructs a Player wired to nc. Run is NOT started — caller drives.
func newPlayer(t *testing.T, nc *nats.Conn, id string, bs *localfs.Store, h player.StageHandler) *player.Player {
	t.Helper()

	tr := transport.NewTransport(nc)

	pl, err := player.New(player.Options{
		Transport:       tr,
		BlobReader:      bs,
		InstanceID:      id,
		HeartbeatPeriod: 50 * time.Millisecond,
		StageHandler:    h,
		Logger:          zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}

	return pl
}

// startPlayer constructs a Player, wires it, and registers cleanup.
func startPlayer(t *testing.T, url, id string, bs *localfs.Store, h player.StageHandler) *player.Player {
	t.Helper()

	nc := dialNATS(t, url)
	pl := newPlayer(t, nc, id, bs, h)

	ctx, cancel := context.WithCancel(t.Context())

	t.Cleanup(func() { cancel() })

	go pl.Start(ctx) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return pl.Wired() })

	return pl
}

// TestThreePlayersConvergeOnPublish verifies that three players started in the
// same process all reach the same Version and body after a single Publish.
func TestThreePlayersConvergeOnPublish(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	s := startSoloist(t, url, bs)
	h1, h2, h3 := newCapturingHandler(), newCapturingHandler(), newCapturingHandler()
	p1 := startPlayer(t, url, "p1", bs, h1)
	p2 := startPlayer(t, url, "p2", bs, h2)
	p3 := startPlayer(t, url, "p3", bs, h3)

	time.Sleep(250 * time.Millisecond) // let all three heartbeats land in roster

	v, err := s.Publish(t.Context(), []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("hello-three")}})
	if err != nil {
		t.Fatal(err)
	}

	testingx.WaitFor(t, 5*time.Second, func() bool {
		return p1.CurrentVersion() == v && p2.CurrentVersion() == v && p3.CurrentVersion() == v
	})

	for i, p := range []*player.Player{p1, p2, p3} {
		if !p.Ready() {
			t.Errorf("player not Ready after converge")
		}

		h := []*capturingHandler{h1, h2, h3}[i]

		cur := h.Current()
		if cur == nil || cur["doc.bin"] != "hello-three" {
			t.Errorf("player %d has unexpected body: %+v", i, cur)
		}
	}
}

// rejectAfterN returns an error from Stage on the Nth invocation and beyond
// (1-indexed), delegating earlier calls to wrapped.
type rejectAfterN struct {
	n       int
	count   atomic.Int32
	wrapped player.StageHandler
}

func (r *rejectAfterN) Stage(ctx context.Context, v maestro.Version, m maestro.Manifest, src player.FileSource) error {
	if int(r.count.Add(1)) >= r.n {
		return errors.New("rejected by test handler")
	}

	return r.wrapped.Stage(ctx, v, m, src)
}

func (r *rejectAfterN) Activate(ctx context.Context, v maestro.Version) error {
	return r.wrapped.Activate(ctx, v)
}

func (r *rejectAfterN) Abort(ctx context.Context, v maestro.Version) error {
	return r.wrapped.Abort(ctx, v)
}

// TestStageErrorAbortsRoundLeavesPriorVersion verifies that when a player's
// StageHandler returns an error, the soloist's Publish returns an error and
// the player remains on the prior committed version.
func TestStageErrorAbortsRoundLeavesPriorVersion(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	s := startSoloist(t, url, bs)
	h := newCapturingHandler()
	rej := &rejectAfterN{n: 2, wrapped: h}
	p := startPlayer(t, url, "p1", bs, rej)

	time.Sleep(200 * time.Millisecond) // let heartbeat land

	v1, err := s.Publish(t.Context(), []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("first")}})
	if err != nil {
		t.Fatal(err)
	}

	testingx.WaitFor(t, 3*time.Second, func() bool { return p.CurrentVersion() == v1 })

	_, err = s.Publish(t.Context(), []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("second")}})
	if err == nil {
		t.Fatal("expected soloist.Publish to fail when player rejects stage")
	}

	if p.CurrentVersion() != v1 {
		t.Errorf("player drifted: got %q want %q", p.CurrentVersion(), v1)
	}
}

// TestPlayerGracefulCloseExcludesFromNextRound verifies that a player which
// departs via Close (not a crash) is dropped from the soloist's roster
// immediately, so the next Publish does not wait out CanCommitTimeout for it
// and does not require soloist.Options.AllowPartialCommit to succeed.
func TestPlayerGracefulCloseExcludesFromNextRound(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	s := startSoloist(t, url, bs)
	h1, h2 := newCapturingHandler(), newCapturingHandler()

	nc1 := dialNATS(t, url)
	pl1 := newPlayer(t, nc1, "p1", bs, h1)
	ctx1, cancel1 := context.WithCancel(t.Context())
	go pl1.Start(ctx1) //nolint:errcheck
	testingx.WaitFor(t, 2*time.Second, func() bool { return pl1.Wired() })

	p2 := startPlayer(t, url, "p2", bs, h2)

	time.Sleep(200 * time.Millisecond) // let both heartbeats land in roster

	if err := pl1.Close(t.Context()); err != nil {
		t.Fatalf("pl1.Close: %v", err)
	}

	cancel1()

	// Close's Leaving heartbeat is a fire-and-forget NATS publish: Close
	// returns once the local client accepts it, not once the soloist's
	// subscriber has processed it. Retry Publish until that lands in the
	// roster instead of asserting on a fixed sleep — a round that still
	// includes p1 costs a full CanCommitTimeout (strict Wait requires every
	// expected voter, so p2 answering immediately does not shortcut it), but
	// the Leaving heartbeat normally lands well within that, so this
	// converges in one or two attempts rather than actually exhausting the
	// WaitFor budget.
	var (
		v   maestro.Version
		err error
	)

	testingx.WaitFor(t, 5*time.Second, func() bool {
		v, err = s.Publish(t.Context(), []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("after-leave")}})
		return err == nil
	})

	if err != nil {
		t.Fatalf("Publish after graceful departure never succeeded: %v", err)
	}

	testingx.WaitFor(t, 3*time.Second, func() bool { return p2.CurrentVersion() == v })

	if !p2.Ready() {
		t.Error("p2 not Ready after publish")
	}
}

// TestPlayerRestartResyncsToCurrent verifies that a fresh player instance
// resyncs to the soloist's current version without carrying over in-memory
// state from a prior session.
func TestPlayerRestartResyncsToCurrent(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	s := startSoloist(t, url, bs)

	v, err := s.Publish(t.Context(), []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("restart")}})
	if err != nil {
		t.Fatal(err)
	}

	// First player session — boots after the commit, must resync.
	h1 := newCapturingHandler()
	nc1 := dialNATS(t, url)
	pl1 := newPlayer(t, nc1, "p1", bs, h1)

	ctx1, cancel1 := context.WithCancel(t.Context())
	go pl1.Start(ctx1) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return pl1.Wired() })
	testingx.WaitFor(t, 3*time.Second, func() bool { return pl1.CurrentVersion() == v })

	if !pl1.Ready() {
		t.Fatal("pl1 not Ready after first resync")
	}

	cancel1()

	time.Sleep(150 * time.Millisecond)

	// Second player session — fresh in-memory state, same InstanceID.
	h2 := newCapturingHandler()
	pl2 := startPlayer(t, url, "p1", bs, h2)

	testingx.WaitFor(t, 3*time.Second, func() bool { return pl2.CurrentVersion() == v })

	if !pl2.Ready() {
		t.Error("restarted player not Ready after resync")
	}

	cur := h2.Current()
	if cur == nil || cur["doc.bin"] != "restart" {
		t.Errorf("restarted player has unexpected body: %+v", cur)
	}
}
