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
	// Wired reports whether the player's round subscriptions were established
	// as of its last heartbeat. A player that is alive but not wired is a real
	// roster member — tracked, and resynced once it is ready — but it cannot
	// answer a round, so Snapshot leaves it out of the expected set.
	Wired bool
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
		Wired:          !hb.NotWired,
	}
}

// Remove drops id from the roster immediately, without waiting for its
// heartbeat to age out of the window. Used for graceful player departure: a
// player that announces it is leaving should not be counted in the next
// round's expected set.
func (r *Roster) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, existed := r.entries[id]; existed {
		r.l.Info("player left", zap.String("iid", id))
		delete(r.entries, id)
	}

	delete(r.dirty, id)
}

// Snapshot returns the currently-alive players (heartbeat within window),
// including any that are not wired yet. Use [Roster.Participants] to build a
// round's expected set.
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

// Participants returns the alive players that can actually answer a round,
// i.e. those whose last heartbeat reported their round subscriptions
// established.
//
// A player that is alive but not wired is deliberately excluded rather than
// tolerated as a non-responder: it is starting up and provably cannot vote, so
// counting it would abort every round for the whole fleet until it finishes.
// It stays in the roster and [Roster.StaleAgainst] still reports it, so the
// monitor resyncs it to whatever it missed once it is ready.
func (r *Roster) Participants() map[string]RosterEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cutoff := r.now().Add(-r.window)

	out := make(map[string]RosterEntry, len(r.entries))
	for k, v := range r.entries {
		if v.LastSeen.After(cutoff) && v.Wired {
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

// StaleAgainst returns instance IDs that are alive, wired, and whose
// CurrentVersion differs from want.
//
// Un-wired players are skipped for the same reason they are left out of a
// round's expected set: a resync round is an ordinary round, so targeting a
// player that cannot vote would just fail. They keep heartbeating as stale, so
// a later scan picks them up once their subscriptions are established.
func (r *Roster) StaleAgainst(want maestro.Version) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cutoff := r.now().Add(-r.window)

	var stale []string

	for id, e := range r.entries {
		if !e.LastSeen.After(cutoff) || !e.Wired {
			continue
		}

		if e.CurrentVersion != want {
			stale = append(stale, id)
		}
	}

	return stale
}
