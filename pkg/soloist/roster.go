package soloist

import (
	"sync"
	"time"

	"github.com/foomo/maestro"
	"github.com/foomo/maestro/pkg/transport"
	"go.uber.org/zap"
)

// RosterEntry captures the soloist's view of one player as of LastSeen.
type RosterEntry struct {
	InstanceID     string
	CurrentVersion maestro.Version
	GenAcked       int64
	LastSeen       time.Time
}

// Roster is a thread-safe set of recently-heartbeating players.
type Roster struct {
	mu        sync.RWMutex
	window    time.Duration
	entries   map[string]RosterEntry
	dirty     map[string]struct{}
	conflicts int64
	now       func() time.Time
	l         *zap.Logger
}

// NewRoster constructs a Roster with the given heartbeat window. If l is nil,
// a no-op logger is used.
func NewRoster(window time.Duration, l *zap.Logger) *Roster {
	if l == nil {
		l = zap.NewNop()
	}

	return &Roster{
		window:  window,
		entries: make(map[string]RosterEntry),
		dirty:   make(map[string]struct{}),
		now:     time.Now,
		l:       l,
	}
}

// Observe records a heartbeat. If the same InstanceID reports a different
// CurrentVersion than its prior heartbeat (without any reset in between), the
// duplicate-conflict counter increments.
func (r *Roster) Observe(hb transport.Heartbeat) {
	r.mu.Lock()
	defer r.mu.Unlock()

	prev, existed := r.entries[hb.InstanceID]
	switch {
	case !existed:
		r.l.Info("player joined",
			zap.String("iid", hb.InstanceID),
			zap.String("version", string(hb.CurrentVersion)),
		)
	case prev.CurrentVersion != hb.CurrentVersion:
		r.conflicts++

		r.l.Info("player version change",
			zap.String("iid", hb.InstanceID),
			zap.String("prev", string(prev.CurrentVersion)),
			zap.String("next", string(hb.CurrentVersion)),
		)
	}

	r.entries[hb.InstanceID] = RosterEntry{
		InstanceID:     hb.InstanceID,
		CurrentVersion: hb.CurrentVersion,
		GenAcked:       hb.GenAcked,
		LastSeen:       r.now(),
	}
}

// Snapshot returns the currently-alive players (heartbeat within window).
func (r *Roster) Snapshot() map[string]RosterEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cutoff := r.now().Add(-r.window)

	out := make(map[string]RosterEntry, len(r.entries))
	for k, v := range r.entries {
		if v.LastSeen.After(cutoff) {
			out[k] = v
		}
	}

	return out
}

// MarkDirty flags id as needing a resync.
func (r *Roster) MarkDirty(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.dirty[id] = struct{}{}
}

// ClearDirty removes id from the dirty set.
func (r *Roster) ClearDirty(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.dirty, id)
}

// DirtyCount returns the number of players currently flagged dirty.
func (r *Roster) DirtyCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.dirty)
}

// DuplicateConflicts returns the number of observed Heartbeat conflicts.
func (r *Roster) DuplicateConflicts() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.conflicts
}

// StaleAgainst returns instance IDs that are alive but whose CurrentVersion
// differs from want.
func (r *Roster) StaleAgainst(want maestro.Version) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cutoff := r.now().Add(-r.window)

	var stale []string

	for id, e := range r.entries {
		if !e.LastSeen.After(cutoff) {
			continue
		}

		if e.CurrentVersion != want {
			stale = append(stale, id)
		}
	}

	return stale
}
