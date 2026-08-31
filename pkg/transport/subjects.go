package transport

import (
	"fmt"
	"strings"
)

// Subject name fragments. Exported so callers can reason about the
// shape of the namespace maestro occupies on a shared bus.
const (
	segmentRound     = "round"
	segmentPlayer    = "player"
	segmentHeartbeat = "heartbeat"
)

// Subjects builds every maestro NATS subject under an optional prefix.
//
// The zero value is usable and produces the unprefixed subjects maestro
// has always used ("round.<rid>.can_commit", "player.heartbeat"), so a
// deployment that owns its NATS instance outright needs to do nothing.
//
// A prefix exists for the case maestro was not originally designed for:
// sharing a NATS cluster with unrelated services. Maestro's subjects are
// single-token-rooted ("round", "player") and its wildcard subscriptions
// are broad, so on a shared bus they both risk colliding with another
// service's namespace and, worse, allow two maestro deployments on the
// same cluster to enrol each other's players into their rosters. A prefix
// scopes a deployment so neither can happen.
type Subjects struct {
	// prefix is stored already dot-terminated (or empty) so every
	// builder is a plain concatenation and no builder has to re-derive
	// the separator.
	prefix string
}

// NewSubjects returns a Subjects that scopes every maestro subject under
// prefix. An empty prefix yields the unprefixed layout.
//
// prefix may contain multiple dot-separated tokens ("catalogue.maestro").
// It is validated because an invalid prefix does not fail loudly at
// publish time — NATS would simply route nothing, which presents as a
// deployment where every round times out despite every player being healthy.
func NewSubjects(prefix string) (Subjects, error) {
	if prefix == "" {
		return Subjects{}, nil
	}

	if err := validatePrefix(prefix); err != nil {
		return Subjects{}, err
	}

	return Subjects{prefix: strings.TrimSuffix(prefix, ".") + "."}, nil
}

// MustSubjects is NewSubjects for callers with a compile-time-constant
// prefix, where an error can only mean a programming mistake.
func MustSubjects(prefix string) Subjects {
	s, err := NewSubjects(prefix)
	if err != nil {
		panic(err)
	}

	return s
}

// validatePrefix rejects prefixes NATS cannot route. Wildcards are
// rejected because a prefix is a literal scope, not a pattern; empty
// tokens because "a..b" is not a valid subject; whitespace because it
// is almost always an unintended config artefact.
func validatePrefix(prefix string) error {
	trimmed := strings.TrimSuffix(prefix, ".")
	if trimmed == "" {
		return fmt.Errorf("maestro: subject prefix %q has no tokens", prefix)
	}

	if strings.ContainsAny(trimmed, "*>") {
		return fmt.Errorf("maestro: subject prefix %q must not contain wildcards", prefix)
	}

	if strings.ContainsAny(trimmed, " \t\r\n") {
		return fmt.Errorf("maestro: subject prefix %q must not contain whitespace", prefix)
	}

	for _, token := range strings.Split(trimmed, ".") {
		if token == "" {
			return fmt.Errorf("maestro: subject prefix %q has an empty token", prefix)
		}
	}

	return nil
}

// Prefix returns the configured prefix without its trailing dot, or ""
// when unprefixed. Intended for logs and metrics labels.
func (s Subjects) Prefix() string { return strings.TrimSuffix(s.prefix, ".") }

// PlayerHeartbeat is the subject every player heartbeats on.
func (s Subjects) PlayerHeartbeat() string {
	return s.prefix + segmentPlayer + "." + segmentHeartbeat
}

// RoundWildcard matches every subject of every round.
func (s Subjects) RoundWildcard() string { return s.prefix + segmentRound + ".>" }

// RoundCanCommitWildcard and its siblings are the per-phase wildcards
// players use to subscribe across all rounds.
func (s Subjects) RoundCanCommitWildcard() string { return s.roundWildcard("can_commit") }
func (s Subjects) RoundPreCommitWildcard() string { return s.roundWildcard("pre_commit") }
func (s Subjects) RoundDoCommitWildcard() string  { return s.roundWildcard("do_commit") }
func (s Subjects) RoundAbortWildcard() string     { return s.roundWildcard("abort") }

// Per-round subject builders.
func (s Subjects) RoundCanCommit(rid string) string { return s.round(rid, "can_commit") }
func (s Subjects) RoundVote(rid string) string      { return s.round(rid, "vote") }
func (s Subjects) RoundPreCommit(rid string) string { return s.round(rid, "pre_commit") }
func (s Subjects) RoundStaged(rid string) string    { return s.round(rid, "staged") }
func (s Subjects) RoundDoCommit(rid string) string  { return s.round(rid, "do_commit") }
func (s Subjects) RoundCommitted(rid string) string { return s.round(rid, "committed") }
func (s Subjects) RoundAbort(rid string) string     { return s.round(rid, "abort") }

// RIDFromSubject extracts the round id from a subject this Subjects
// would have produced. Returns "" when subject does not belong to this
// prefix, which is also how a message that leaked in from another
// maestro deployment on a shared bus is rejected.
func (s Subjects) RIDFromSubject(subject string) string {
	want := s.prefix + segmentRound + "."
	if !strings.HasPrefix(subject, want) {
		return ""
	}

	before, _, ok := strings.Cut(subject[len(want):], ".")
	if !ok {
		return ""
	}

	return before
}

func (s Subjects) round(rid, phase string) string {
	return s.prefix + segmentRound + "." + rid + "." + phase
}

func (s Subjects) roundWildcard(phase string) string {
	return s.prefix + segmentRound + ".*." + phase
}
