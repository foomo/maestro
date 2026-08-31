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

// startPrefixedSoloist mirrors startSoloist but scopes every subject
// under prefix.
func startPrefixedSoloist(t *testing.T, url, prefix string, bs *localfs.Store, id string) *soloist.Soloist {
	t.Helper()

	nc := dialNATS(t, url)

	tr, err := transport.NewTransportWithPrefix(nc, prefix)
	if err != nil {
		t.Fatalf("NewTransportWithPrefix(%q): %v", prefix, err)
	}

	s, err := soloist.New(soloist.Options{
		Transport:        tr,
		BlobStore:        bs,
		InstanceID:       id,
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

	t.Cleanup(cancel)

	go s.Start(ctx) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return s.Ready() })

	return s
}

// startPrefixedPlayer mirrors startPlayer but scopes every subject under
// prefix.
func startPrefixedPlayer(
	t *testing.T,
	url, prefix, id string,
	bs *localfs.Store,
	h player.StageHandler,
) *player.Player {
	t.Helper()

	nc := dialNATS(t, url)

	tr, err := transport.NewTransportWithPrefix(nc, prefix)
	if err != nil {
		t.Fatalf("NewTransportWithPrefix(%q): %v", prefix, err)
	}

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

	ctx, cancel := context.WithCancel(t.Context())

	t.Cleanup(cancel)

	go pl.Start(ctx) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return pl.Wired() })

	return pl
}

// settleHeartbeats waits long enough for a player heartbeat (50ms in
// these tests) to have landed in the soloist's roster.
func settleHeartbeats(t *testing.T) {
	t.Helper()
	time.Sleep(250 * time.Millisecond)
}

// A full 3PC round must complete with every subject scoped under a
// prefix. This is the test that would have caught RIDFromSubject's
// hardcoded "round." prefix: with it, players receive CanCommit but
// reply on round "", the soloist's aggregators never match, and the
// round times out despite perfectly healthy players.
func TestPrefixedDeploymentCompletesRound(t *testing.T) {
	const prefix = "catalogue.maestro"

	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	s := startPrefixedSoloist(t, url, prefix, bs, "soloist-1")
	h1, h2 := newCapturingHandler(), newCapturingHandler()
	p1 := startPrefixedPlayer(t, url, prefix, "p1", bs, h1)
	p2 := startPrefixedPlayer(t, url, prefix, "p2", bs, h2)

	// Let both heartbeats land so the roster is populated. With an empty
	// roster Publish takes the silent-commit fast path, which would
	// prove nothing about subject routing. With a populated roster and
	// the default strict mode, every member must vote and stage — so a
	// Publish that returns without error is itself the assertion that
	// prefixed subjects route in both directions.
	settleHeartbeats(t)

	v, err := s.Publish(t.Context(), []soloist.File{
		{Name: "payload.txt", Reader: strings.NewReader("prefixed-body")},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	testingx.WaitFor(t, 5*time.Second, func() bool {
		return p1.CurrentVersion() == v && p2.CurrentVersion() == v
	})

	for i, h := range []*capturingHandler{h1, h2} {
		if got := h.Current()["payload.txt"]; got != "prefixed-body" {
			t.Errorf("player %d body = %q, want %q", i+1, got, "prefixed-body")
		}
	}
}

// Two deployments sharing one NATS cluster must not observe each other. This
// is the property that makes a shared bus safe: without prefixing, both
// soloists would enrol all four players into their rosters and each
// publish would be gated on players that never staged its blobs.
func TestTwoPrefixedDeploymentsAreIsolated(t *testing.T) {
	url := testutil.StartNATS(t)

	bsA, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})
	bsB, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	solA := startPrefixedSoloist(t, url, "deploy.a", bsA, "soloist-a")
	_ = startPrefixedSoloist(t, url, "deploy.b", bsB, "soloist-b")

	hA := newCapturingHandler()
	hB := newCapturingHandler()
	pA := startPrefixedPlayer(t, url, "deploy.a", "player-a", bsA, hA)
	pB := startPrefixedPlayer(t, url, "deploy.b", "player-b", bsB, hB)

	settleHeartbeats(t)

	// Strict mode is the assertion here. If prefixes leaked, each
	// soloist would have enrolled both players, and deployment A's round
	// would be gated on deployment B's player — which cannot stage a blob it
	// has no store for. A clean Publish means the rosters stayed
	// disjoint.
	vA, err := solA.Publish(t.Context(), []soloist.File{
		{Name: "payload.txt", Reader: strings.NewReader("body-a")},
	})
	if err != nil {
		t.Fatalf("deployment A Publish: %v", err)
	}

	testingx.WaitFor(t, 5*time.Second, func() bool { return pA.CurrentVersion() == vA })

	if got := hA.Current()["payload.txt"]; got != "body-a" {
		t.Errorf("deployment A player body = %q, want body-a", got)
	}

	// Deployment B's player must be entirely untouched by deployment A's round.
	if v := pB.CurrentVersion(); v != "" {
		t.Errorf("deployment B player adopted version %q from deployment A's round", v)
	}

	if got := hB.Current(); got != nil {
		t.Errorf("deployment B player staged %v from deployment A's round", got)
	}
}

// A real round must record a real outcome. The unit tests prove the
// instruments work; this proves they are actually reached from the
// publish path, which is where instrumentation usually rots.
