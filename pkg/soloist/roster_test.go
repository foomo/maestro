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
