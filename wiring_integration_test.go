package maestro_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	testingx "github.com/foomo/go/testing"
	"github.com/foomo/maestro/internal/testutil"
	"github.com/foomo/maestro/pkg/blobstore/localfs"
	"github.com/foomo/maestro/pkg/player"
	"github.com/foomo/maestro/pkg/soloist"
	"github.com/foomo/maestro/pkg/transport"
)

// These tests cover the availability failure found while scaling the catalogue
// testbed: a player that was in the roster but could not answer a round made
// every Publish fail for the whole fleet.
//
// The fix is roster accuracy rather than relaxed commit semantics. A player
// advertises transport.Heartbeat.NotWired until the broker has acknowledged all
// four of its round subscriptions, and the soloist leaves un-wired players out
// of a round's expected set while still tracking them for resync. Commit itself
// stays strictly unanimous among the players that can actually vote.

// TestWiring_StartingPlayerDoesNotBlockPublish is the regression test. A player
// that is heartbeating but has not finished establishing its round
// subscriptions must not be counted as a voter, because it provably cannot vote
// and would abort the round for the healthy replicas.
func TestWiring_StartingPlayerDoesNotBlockPublish(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	s := startSoloist(t, url, bs)

	hOK := newCapturingHandler()
	pOK := startPlayer(t, url, "p-healthy", bs, hOK)
	testingx.WaitFor(t, 2*time.Second, func() bool { return pOK.Wired() })

	// A player that heartbeats as not-yet-wired. This is what a real player
	// looks like between its first heartbeat and its subscriptions going live.
	stopStarting := heartbeatAs(t, url, transport.Heartbeat{
		InstanceID: "p-starting",
		NotWired:   true,
	})
	defer stopStarting()

	// Let both the healthy player and the starting one land in the roster.
	time.Sleep(300 * time.Millisecond)

	v, err := s.Publish(t.Context(), []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("wired-only")}})
	if err != nil {
		t.Fatalf("Publish failed despite every wired player being available: %v", err)
	}

	testingx.WaitFor(t, 5*time.Second, func() bool { return pOK.CurrentVersion() == v })

	if cur := hOK.Current(); cur == nil || cur["doc.bin"] != "wired-only" {
		t.Fatalf("healthy player has unexpected body: %+v", cur)
	}
	if s.Current() != v {
		t.Fatalf("soloist current = %q, want %q", s.Current(), v)
	}
}

// TestWiring_WiredButSilentPlayerStillAborts is the control, and marks the
// deliberate limit of this approach. A player that claims to be wired and then
// does not answer is a genuine protocol failure: the soloist cannot tell it
// apart from one that is about to answer, so the round must abort rather than
// commit a version only part of the fleet has.
func TestWiring_WiredButSilentPlayerStillAborts(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	s := startSoloist(t, url, bs)

	hOK := newCapturingHandler()
	_ = startPlayer(t, url, "p-healthy", bs, hOK)

	// Heartbeats as fully wired but has no subscriptions behind it at all.
	stopLiar := heartbeatAs(t, url, transport.Heartbeat{InstanceID: "p-silent"})
	defer stopLiar()

	time.Sleep(300 * time.Millisecond)

	if _, err := s.Publish(t.Context(), []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("strict")}}); err == nil {
		t.Fatal("expected Publish to fail when a wired roster member does not vote")
	}
}

// TestWiring_NoWiredPlayersFails asserts the exclusion does not degrade into
// "publish always succeeds". A roster of nothing but starting-up players is not
// an empty roster: there are players out there that will want this version, so
// committing silently behind their backs would leave them permanently behind.
func TestWiring_NoWiredPlayersFails(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	s := startSoloist(t, url, bs)

	stopStarting := heartbeatAs(t, url, transport.Heartbeat{
		InstanceID: "p-starting",
		NotWired:   true,
	})
	defer stopStarting()

	time.Sleep(300 * time.Millisecond)

	if _, err := s.Publish(t.Context(), []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("nobody")}}); err == nil {
		t.Fatal("expected Publish to fail when the roster holds only un-wired players")
	}
}

// TestWiring_StartingPlayerResyncsAfterwards asserts the excluded player is not
// abandoned: once its subscriptions are live it heartbeats as wired and stale,
// and the roster monitor resyncs it to the version it missed. This is what makes
// excluding it safe rather than a silent data-divergence bug.
func TestWiring_StartingPlayerResyncsAfterwards(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	s := startSoloist(t, url, bs)

	hOK := newCapturingHandler()
	pOK := startPlayer(t, url, "p-healthy", bs, hOK)
	testingx.WaitFor(t, 2*time.Second, func() bool { return pOK.Wired() })

	// p-laggard is only heartbeating as un-wired while the publish happens.
	stopStarting := heartbeatAs(t, url, transport.Heartbeat{
		InstanceID: "p-laggard",
		NotWired:   true,
	})

	time.Sleep(300 * time.Millisecond)

	v, err := s.Publish(t.Context(), []soloist.File{{Name: "doc.bin", Reader: strings.NewReader("resync-me")}})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	testingx.WaitFor(t, 5*time.Second, func() bool { return pOK.CurrentVersion() == v })

	// Now bring up the real player under that same InstanceID. Once it is
	// wired the monitor sees it as stale and resyncs it.
	stopStarting()

	hLag := newCapturingHandler()
	pLag := startPlayer(t, url, "p-laggard", bs, hLag)

	testingx.WaitFor(t, 5*time.Second, func() bool { return pLag.CurrentVersion() == v })

	if cur := hLag.Current(); cur == nil || cur["doc.bin"] != "resync-me" {
		t.Fatalf("laggard did not resync to the missed version: %+v", cur)
	}
}

// TestWiring_FullFleetStillConverges guards the happy path: the wiredness gate
// must not change behaviour when every player is healthy.
func TestWiring_FullFleetStillConverges(t *testing.T) {
	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	s := startSoloist(t, url, bs)
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

// heartbeatAs publishes hb on the player heartbeat subject until the returned
// stop func is called, standing in for a player in a state that is awkward to
// reach with a real one (starting up, or claiming to be wired while deaf).
func heartbeatAs(t *testing.T, url string, hb transport.Heartbeat) func() {
	t.Helper()

	tr := transport.NewTransport(dialNATS(t, url))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)

		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()

		for {
			_ = tr.Heartbeat.Publish(ctx, tr.Subjects.PlayerHeartbeat(), hb)

			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()

	var once sync.Once

	stop := func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}

	t.Cleanup(stop)

	return stop
}
