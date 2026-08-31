package soloist_test

import (
	"testing"
	"time"

	"github.com/foomo/maestro"
	"github.com/foomo/maestro/pkg/soloist"
	"github.com/foomo/maestro/pkg/transport"
)

func TestRosterAliveWithinWindow(t *testing.T) {
	r := soloist.NewRoster(15*time.Second, nil)
	r.Observe(transport.Heartbeat{InstanceID: "p1", CurrentVersion: "v1"})

	alive := r.Snapshot()
	if len(alive) != 1 || alive["p1"].CurrentVersion != maestro.Version("v1") {
		t.Errorf("got %+v", alive)
	}
}

func TestRosterExpiresStale(t *testing.T) {
	r := soloist.NewRoster(50*time.Millisecond, nil)
	r.Observe(transport.Heartbeat{InstanceID: "p1"})
	time.Sleep(80 * time.Millisecond)

	if len(r.Snapshot()) != 0 {
		t.Error("expected expiry")
	}
}

func TestRosterDuplicateConflict(t *testing.T) {
	r := soloist.NewRoster(15*time.Second, nil)
	r.Observe(transport.Heartbeat{InstanceID: "p1", CurrentVersion: "v1"})
	r.Observe(transport.Heartbeat{InstanceID: "p1", CurrentVersion: "v2"})

	if r.DuplicateConflicts() < 1 {
		t.Error("expected duplicate-conflict counter to increment")
	}
}

func TestRosterRemove(t *testing.T) {
	r := soloist.NewRoster(15*time.Second, nil)
	r.Observe(transport.Heartbeat{InstanceID: "p1", CurrentVersion: "v1"})
	r.MarkDirty("p1")

	r.Remove("p1")

	if len(r.Snapshot()) != 0 {
		t.Error("expected p1 removed from roster")
	}

	if r.DirtyCount() != 0 {
		t.Error("expected p1 cleared from dirty set")
	}
}

func TestRosterParticipantsExcludesUnwired(t *testing.T) {
	r := soloist.NewRoster(15*time.Second, nil)
	r.Observe(transport.Heartbeat{InstanceID: "p1", CurrentVersion: "v1"})
	r.Observe(transport.Heartbeat{InstanceID: "p2", CurrentVersion: "v1", NotWired: true})

	// Still a roster member: it is a real player, tracked for resync.
	if len(r.Snapshot()) != 2 {
		t.Errorf("snapshot should keep un-wired players: %+v", r.Snapshot())
	}

	part := r.Participants()
	if len(part) != 1 {
		t.Fatalf("participants: %+v", part)
	}

	if _, ok := part["p1"]; !ok {
		t.Errorf("expected the wired player, got %+v", part)
	}
}

// A heartbeat from a player built before NotWired existed decodes with the
// field absent. It must be treated as a full participant, not silently
// excluded from every round forever.
func TestRosterParticipantsDefaultsToWired(t *testing.T) {
	r := soloist.NewRoster(15*time.Second, nil)
	r.Observe(transport.Heartbeat{InstanceID: "p-old", CurrentVersion: "v1"})

	if len(r.Participants()) != 1 {
		t.Errorf("zero-value NotWired must mean eligible: %+v", r.Participants())
	}
}

func TestRosterStaleAgainstSkipsUnwired(t *testing.T) {
	r := soloist.NewRoster(15*time.Second, nil)
	r.Observe(transport.Heartbeat{InstanceID: "p1", CurrentVersion: "v-old"})
	r.Observe(transport.Heartbeat{InstanceID: "p2", CurrentVersion: "v-old", NotWired: true})

	// p2 is stale too, but a resync round would fail against a player that
	// cannot vote; it is picked up once it reports wired.
	stale := r.StaleAgainst("v1")
	if len(stale) != 1 || stale[0] != "p1" {
		t.Errorf("stale: %v", stale)
	}

	r.Observe(transport.Heartbeat{InstanceID: "p2", CurrentVersion: "v-old"})

	if len(r.StaleAgainst("v1")) != 2 {
		t.Errorf("p2 should be resynced once wired: %v", r.StaleAgainst("v1"))
	}
}

func TestRosterStaleAgainst(t *testing.T) {
	r := soloist.NewRoster(15*time.Second, nil)
	r.Observe(transport.Heartbeat{InstanceID: "p1", CurrentVersion: "v1"})
	r.Observe(transport.Heartbeat{InstanceID: "p2", CurrentVersion: "v2"})
	r.Observe(transport.Heartbeat{InstanceID: "p3", CurrentVersion: "v1"})

	stale := r.StaleAgainst("v1")
	if len(stale) != 1 || stale[0] != "p2" {
		t.Errorf("stale: %v", stale)
	}
}
