package maestro_test

import (
	"context"
	"strings"
	"testing"
	"time"

	testingx "github.com/foomo/go/testing"
	"github.com/foomo/maestro/internal/testutil"
	"github.com/foomo/maestro/pkg/blobstore/localfs"
	"github.com/foomo/maestro/pkg/player"
	"github.com/foomo/maestro/pkg/soloist"
	"github.com/foomo/maestro/pkg/transport"
	"go.uber.org/zap/zaptest"
)

// startPartialFleetSoloist mirrors startSoloist but enables PartialFleet.
func startPartialFleetSoloist(t *testing.T, url string, bs *localfs.Store) *soloist.Soloist {
	t.Helper()

	s, err := soloist.New(soloist.Options{
		Transport:        transport.NewTransport(dialNATS(t, url)),
		BlobStore:        bs,
		InstanceID:       "soloist-1",
		HeartbeatWindow:  5 * time.Second,
		RosterScanTick:   100 * time.Millisecond,
		ResyncDebounce:   50 * time.Millisecond,
		CanCommitTimeout: 500 * time.Millisecond,
		DoCommitTimeout:  500 * time.Millisecond,
		PartialFleet:     true,
		Logger:           zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	go s.Start(ctx) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return s.Ready() })

	return s
}

// TestPartialFleet_SilentPlayerDoesNotBlockPublish is the regression test for
// the availability failure found while scaling the catalogue testbed: a player
// that is in the roster (still heartbeating) but no longer answering rounds —
// a pod mid-termination, or one that has just joined and is not wired yet —
// made every Publish fail for the whole fleet until its roster entry aged out.
//
// The healthy replica must still receive the new version.
func TestPartialFleet_SilentPlayerDoesNotBlockPublish(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	s := startPartialFleetSoloist(t, url, bs)

	// Healthy player.
	hOK := newCapturingHandler()
	pOK := startPlayer(t, url, "p-healthy", bs, hOK)

	// Silent player: heartbeats (so it is in the roster) but its round
	// subscriptions are torn down, so it never votes. Cancelling the
	// player's context after Wired stops the subscribers; we then keep its
	// roster entry alive by publishing heartbeats from a second player
	// sharing the same InstanceID is unnecessary — instead we simply start
	// it and cancel, then publish within the heartbeat window.
	hSilent := newCapturingHandler()
	ncSilent := dialNATS(t, url)
	pSilent := newPlayer(t, ncSilent, "p-silent", bs, hSilent)

	silentCtx, cancelSilent := context.WithCancel(t.Context())
	go pSilent.Start(silentCtx) //nolint:errcheck
	testingx.WaitFor(t, 2*time.Second, func() bool { return pSilent.Wired() })

	// Let both heartbeats land, then silence one of them. Its roster entry
	// survives for HeartbeatWindow (5s), which is the window in which
	// strict mode would fail every publish.
	time.Sleep(250 * time.Millisecond)
	cancelSilent()
	time.Sleep(100 * time.Millisecond)

	v, err := s.Publish(t.Context(), []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("partial-ok")}})
	if err != nil {
		t.Fatalf("Publish failed despite a healthy player being available: %v", err)
	}

	testingx.WaitFor(t, 5*time.Second, func() bool { return pOK.CurrentVersion() == v })

	if cur := hOK.Current(); cur == nil || cur["doc.bin"] != "partial-ok" {
		t.Fatalf("healthy player has unexpected body: %+v", cur)
	}
	if s.Current() != v {
		t.Fatalf("soloist current = %q, want %q", s.Current(), v)
	}
}

// TestPartialFleet_StrictModeStillFails is the control: with PartialFleet off
// the same scenario must abort the publish, preserving the lockstep guarantee
// other maestro consumers rely on.
func TestPartialFleet_StrictModeStillFails(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	s := startSoloist(t, url, bs) // strict: PartialFleet defaults to false

	hOK := newCapturingHandler()
	_ = startPlayer(t, url, "p-healthy", bs, hOK)

	hSilent := newCapturingHandler()
	ncSilent := dialNATS(t, url)
	pSilent := newPlayer(t, ncSilent, "p-silent", bs, hSilent)

	silentCtx, cancelSilent := context.WithCancel(t.Context())
	go pSilent.Start(silentCtx) //nolint:errcheck
	testingx.WaitFor(t, 2*time.Second, func() bool { return pSilent.Wired() })

	time.Sleep(250 * time.Millisecond)
	cancelSilent()
	time.Sleep(100 * time.Millisecond)

	if _, err := s.Publish(t.Context(), []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("strict")}}); err == nil {
		t.Fatal("expected strict-mode Publish to fail when a roster member does not vote")
	}
}

// TestPartialFleet_NoPlayersAtAllStillFails asserts PartialFleet does not
// degrade into "publish always succeeds": if the roster is non-empty but not a
// single member follows the round, that is still a failure the producer must
// hear about.
func TestPartialFleet_NoPlayersAtAllStillFails(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	s := startPartialFleetSoloist(t, url, bs)

	h := newCapturingHandler()
	nc := dialNATS(t, url)
	pl := newPlayer(t, nc, "p-only", bs, h)

	ctx, cancel := context.WithCancel(t.Context())
	go pl.Start(ctx) //nolint:errcheck
	testingx.WaitFor(t, 2*time.Second, func() bool { return pl.Wired() })

	time.Sleep(250 * time.Millisecond)
	cancel() // the only roster member goes silent
	time.Sleep(100 * time.Millisecond)

	if _, err := s.Publish(t.Context(), []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("nobody")}}); err == nil {
		t.Fatal("expected Publish to fail when no roster member follows the round")
	}
}

// TestPartialFleet_LaggardResyncsAfterwards asserts the dropped player is not
// abandoned: once it is healthy again the roster monitor resyncs it to the
// version it missed. This is what makes tolerating a partial round safe rather
// than a silent data-divergence bug.
func TestPartialFleet_LaggardResyncsAfterwards(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	s := startPartialFleetSoloist(t, url, bs)

	hOK := newCapturingHandler()
	pOK := startPlayer(t, url, "p-healthy", bs, hOK)

	// Laggard is wired, then silenced before the publish.
	hLag := newCapturingHandler()
	ncLag := dialNATS(t, url)
	plLag := newPlayer(t, ncLag, "p-laggard", bs, hLag)

	lagCtx, cancelLag := context.WithCancel(t.Context())
	go plLag.Start(lagCtx) //nolint:errcheck
	testingx.WaitFor(t, 2*time.Second, func() bool { return plLag.Wired() })

	time.Sleep(250 * time.Millisecond)
	cancelLag()
	time.Sleep(100 * time.Millisecond)

	v, err := s.Publish(t.Context(), []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("resync-me")}})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	testingx.WaitFor(t, 5*time.Second, func() bool { return pOK.CurrentVersion() == v })

	// Bring the laggard back with the same InstanceID and a fresh handler.
	hLag2 := newCapturingHandler()
	plLag2 := startPlayer(t, url, "p-laggard", bs, hLag2)

	testingx.WaitFor(t, 5*time.Second, func() bool { return plLag2.CurrentVersion() == v })

	if cur := hLag2.Current(); cur == nil || cur["doc.bin"] != "resync-me" {
		t.Fatalf("laggard did not resync to the missed version: %+v", cur)
	}
}

// TestPartialFleet_RejectingPlayerDoesNotBlockOthers covers an explicit "no"
// vote (as opposed to silence): a player whose StageHandler fails must not stop
// the healthy replicas from advancing.
func TestPartialFleet_RejectingPlayerDoesNotBlockOthers(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	s := startPartialFleetSoloist(t, url, bs)

	hOK := newCapturingHandler()
	pOK := startPlayer(t, url, "p-healthy", bs, hOK)

	// This player fails Stage from its very first round.
	hBad := newCapturingHandler()
	bad := &rejectAfterN{n: 1, wrapped: hBad}
	pBad := startPlayer(t, url, "p-bad", bs, bad)

	time.Sleep(300 * time.Millisecond)

	v, err := s.Publish(t.Context(), []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("skip-the-bad-one")}})
	if err != nil {
		t.Fatalf("Publish failed despite a healthy player being available: %v", err)
	}

	testingx.WaitFor(t, 5*time.Second, func() bool { return pOK.CurrentVersion() == v })

	if cur := hOK.Current(); cur == nil || cur["doc.bin"] != "skip-the-bad-one" {
		t.Fatalf("healthy player has unexpected body: %+v", cur)
	}
	if pBad.CurrentVersion() == v {
		t.Fatal("rejecting player should not have advanced to the new version")
	}
}

// TestPartialFleet_FullFleetStillConverges guards the happy path: enabling
// PartialFleet must not change behaviour when every player is healthy.
func TestPartialFleet_FullFleetStillConverges(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	s := startPartialFleetSoloist(t, url, bs)
	h1, h2, h3 := newCapturingHandler(), newCapturingHandler(), newCapturingHandler()
	p1 := startPlayer(t, url, "p1", bs, h1)
	p2 := startPlayer(t, url, "p2", bs, h2)
	p3 := startPlayer(t, url, "p3", bs, h3)

	time.Sleep(300 * time.Millisecond)

	v, err := s.Publish(t.Context(), []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("all-three")}})
	if err != nil {
		t.Fatal(err)
	}

	testingx.WaitFor(t, 5*time.Second, func() bool {
		return p1.CurrentVersion() == v && p2.CurrentVersion() == v && p3.CurrentVersion() == v
	})

	for i, h := range []*capturingHandler{h1, h2, h3} {
		if cur := h.Current(); cur == nil || cur["doc.bin"] != "all-three" {
			t.Errorf("player %d has unexpected body: %+v", i, cur)
		}
	}

	for i, p := range []*player.Player{p1, p2, p3} {
		if !p.Ready() {
			t.Errorf("player %d not Ready after converge", i)
		}
	}
}
