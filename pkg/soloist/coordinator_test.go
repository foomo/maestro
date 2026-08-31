package soloist_test

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	testingx "github.com/foomo/go/testing"
	"github.com/foomo/goflux"
	"github.com/foomo/maestro"
	"github.com/foomo/maestro/internal/testutil"
	"github.com/foomo/maestro/pkg/blobstore/localfs"
	"github.com/foomo/maestro/pkg/soloist"
	"github.com/foomo/maestro/pkg/transport"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

// startSoloistForTest wires a soloist with a fresh *nats.Conn + transport bundle.
// The connection is closed via t.Cleanup.
func startSoloistForTest(t *testing.T, url string, base soloist.Options) *soloist.Soloist {
	t.Helper()

	nc, err := nats.Connect(url)
	require.NoError(t, err)

	t.Cleanup(nc.Close)

	base.Transport = transport.NewTransport(nc)

	s, err := soloist.New(base)
	require.NoError(t, err)

	return s
}

// fakePlayer drives the player side of the protocol over the goflux NATS
// transport. It responds OK to every phase and emits heartbeats so the
// soloist's roster sees it.
type fakePlayer struct {
	instanceID  string
	nc          *nats.Conn
	tr          transport.Transport
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	currentVer  maestro.Version
	currentVerM sync.Mutex
}

func newFakePlayer(t *testing.T, url, id string) *fakePlayer {
	t.Helper()

	nc, err := nats.Connect(url)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	p := &fakePlayer{
		instanceID: id,
		nc:         nc,
		tr:         transport.NewTransport(nc),
		cancel:     cancel,
	}

	// Heartbeat loop
	p.wg.Go(func() {
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				p.currentVerM.Lock()
				ver := p.currentVer
				p.currentVerM.Unlock()

				_ = p.tr.Heartbeat.Publish(context.Background(), p.tr.Subjects.PlayerHeartbeat(), transport.Heartbeat{
					InstanceID:     id,
					CurrentVersion: ver,
				})
			}
		}
	})

	// CanCommit -> Vote OK
	p.wg.Go(func() {
		_ = p.tr.CanCommit.Subscribe(ctx, p.tr.Subjects.RoundCanCommitWildcard(), func(_ context.Context, m goflux.Message[transport.CanCommit]) error {
			rid := p.tr.Subjects.RIDFromSubject(m.Subject)
			_ = p.tr.Vote.Publish(context.Background(), p.tr.Subjects.RoundVote(rid),
				transport.Vote{RoundID: rid, InstanceID: id, OK: true})

			return nil
		})
	})

	// PreCommit -> Staged OK
	p.wg.Go(func() {
		_ = p.tr.PreCommit.Subscribe(ctx, p.tr.Subjects.RoundPreCommitWildcard(), func(_ context.Context, m goflux.Message[transport.PreCommit]) error {
			rid := p.tr.Subjects.RIDFromSubject(m.Subject)
			_ = p.tr.Staged.Publish(context.Background(), p.tr.Subjects.RoundStaged(rid),
				transport.Staged{RoundID: rid, InstanceID: id, OK: true})

			return nil
		})
	})

	// DoCommit -> Committed OK + record current version
	p.wg.Go(func() {
		_ = p.tr.DoCommit.Subscribe(ctx, p.tr.Subjects.RoundDoCommitWildcard(), func(_ context.Context, m goflux.Message[transport.DoCommit]) error {
			rid := p.tr.Subjects.RIDFromSubject(m.Subject)

			p.currentVerM.Lock()
			p.currentVer = m.Payload.Target
			p.currentVerM.Unlock()

			_ = p.tr.Committed.Publish(context.Background(), p.tr.Subjects.RoundCommitted(rid),
				transport.Committed{RoundID: rid, InstanceID: id, OK: true})

			return nil
		})
	})

	return p
}

func (p *fakePlayer) CurrentVersion() maestro.Version {
	p.currentVerM.Lock()
	defer p.currentVerM.Unlock()

	return p.currentVer
}

func (p *fakePlayer) Close() {
	p.cancel()
	p.wg.Wait()
	p.nc.Close()
}

// ---- Tests ---------------------------------------------------------------

func TestSoloistCloseBlocksUntilDone(t *testing.T) {
	url := testutil.StartNATS(t)

	bs, err := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})
	require.NoError(t, err)

	s := startSoloistForTest(t, url, soloist.Options{
		BlobStore:      bs,
		RosterScanTick: 50 * time.Millisecond,
	})

	startDone := make(chan error, 1)
	go func() { startDone <- s.Start(t.Context()) }()

	testingx.WaitFor(t, 2*time.Second, func() bool { return s.Ready() })

	closeCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	require.NoError(t, s.Close(closeCtx))

	select {
	case err := <-startDone:
		require.NoError(t, err, "Start should return nil after clean Close")
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after Close")
	}
}

func TestSoloistCloseBeforeStart(t *testing.T) {
	url := testutil.StartNATS(t)

	bs, err := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})
	require.NoError(t, err)

	s := startSoloistForTest(t, url, soloist.Options{BlobStore: bs})

	require.NoError(t, s.Close(t.Context()))
}

func TestSoloistStartTwice(t *testing.T) {
	url := testutil.StartNATS(t)

	bs, err := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})
	require.NoError(t, err)

	s := startSoloistForTest(t, url, soloist.Options{
		BlobStore:      bs,
		RosterScanTick: 50 * time.Millisecond,
	})

	go s.Start(t.Context()) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return s.Ready() })

	err = s.Start(t.Context())
	require.ErrorIs(t, err, soloist.ErrAlreadyStarted)

	closeCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	_ = s.Close(closeCtx)
}

func TestSoloistSilentCommitWhenRosterEmpty(t *testing.T) {
	url := testutil.StartNATS(t)

	bs, err := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})
	require.NoError(t, err)

	s := startSoloistForTest(t, url, soloist.Options{BlobStore: bs})

	ctx := t.Context()

	go s.Start(ctx) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return s.Ready() })

	v, err := s.Publish(ctx, []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("hello")}})
	require.NoError(t, err)

	if v == "" {
		t.Fatal("empty version")
	}

	if s.Current() != v {
		t.Errorf("Current() %q != %q", s.Current(), v)
	}
}

func TestSoloistHappyPathWithPlayer(t *testing.T) {
	url := testutil.StartNATS(t)

	bs, err := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})
	require.NoError(t, err)

	player := newFakePlayer(t, url, "player-1")
	defer player.Close()

	s := startSoloistForTest(t, url, soloist.Options{
		BlobStore:        bs,
		HeartbeatWindow:  5 * time.Second,
		CanCommitTimeout: 3 * time.Second,
		DoCommitTimeout:  3 * time.Second,
	})

	ctx := t.Context()

	go s.Start(ctx) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return s.Ready() })

	time.Sleep(200 * time.Millisecond)

	v, err := s.Publish(ctx, []soloist.File{{Name: "data.bin", Reader: strings.NewReader("payload")}})
	require.NoError(t, err)

	if s.Current() != v {
		t.Errorf("Current() %q != %q", s.Current(), v)
	}

	testingx.WaitFor(t, 2*time.Second, func() bool {
		return player.CurrentVersion() == v
	})
}

func TestSoloistTriggersResyncForStalePlayer(t *testing.T) {
	url := testutil.StartNATS(t)

	bs, err := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})
	require.NoError(t, err)

	s := startSoloistForTest(t, url, soloist.Options{
		BlobStore:        bs,
		HeartbeatWindow:  5 * time.Second,
		RosterScanTick:   150 * time.Millisecond,
		ResyncDebounce:   100 * time.Millisecond,
		CanCommitTimeout: 3 * time.Second,
		DoCommitTimeout:  3 * time.Second,
	})

	ctx := t.Context()

	go s.Start(ctx) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return s.Ready() })

	v, err := s.Publish(ctx, []soloist.File{{Name: "init.bin", Reader: strings.NewReader("initial")}})
	require.NoError(t, err)

	player := newFakePlayer(t, url, "player-stale")
	defer player.Close()

	testingx.WaitFor(t, 4*time.Second, func() bool {
		return player.CurrentVersion() == v
	})
}

func TestSoloistPublishMultipleFiles(t *testing.T) {
	url := testutil.StartNATS(t)

	bs, err := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})
	require.NoError(t, err)

	player := newFakePlayer(t, url, "player-mf")
	defer player.Close()

	s := startSoloistForTest(t, url, soloist.Options{
		BlobStore:        bs,
		HeartbeatWindow:  5 * time.Second,
		CanCommitTimeout: 3 * time.Second,
		DoCommitTimeout:  3 * time.Second,
	})

	ctx := t.Context()

	go s.Start(ctx) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return s.Ready() })
	time.Sleep(200 * time.Millisecond)

	v, err := s.Publish(ctx, []soloist.File{
		{Name: "a.bin", Reader: strings.NewReader("alpha")},
		{Name: "b.bin", Reader: strings.NewReader("beta")},
	})
	require.NoError(t, err)

	if v == "" {
		t.Fatal("empty version")
	}

	if s.Current() != v {
		t.Errorf("Current() %q != %q", s.Current(), v)
	}

	testingx.WaitFor(t, 2*time.Second, func() bool { return player.CurrentVersion() == v })

	for name, want := range map[string]string{"a.bin": "alpha", "b.bin": "beta"} {
		rc, _, err := bs.Reader(ctx, v, name)
		if err != nil {
			t.Fatalf("Reader %s: %v", name, err)
		}

		got, err := io.ReadAll(rc)
		rc.Close()

		if err != nil {
			t.Fatal(err)
		}

		if string(got) != want {
			t.Errorf("%s: got %q want %q", name, got, want)
		}
	}
}

func TestSoloistPublishEmptyRejected(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	s := startSoloistForTest(t, url, soloist.Options{BlobStore: bs})

	if _, err := s.Publish(context.Background(), nil); err == nil {
		t.Fatal("expected error for empty files slice")
	}
}

// Ensure goflux import is used.
var _ = goflux.NewMessage[string]
