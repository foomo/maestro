package transport_test

import (
	"strings"
	"testing"

	"github.com/foomo/maestro/pkg/transport"
)

// The zero value must reproduce the historical unprefixed layout
// exactly, so an existing deployment keeps working untouched.
func TestSubjects_ZeroValueIsUnprefixed(t *testing.T) {
	var s transport.Subjects

	assertSubjects(t, s, "")
}

func TestSubjects_EmptyPrefixIsUnprefixed(t *testing.T) {
	s, err := transport.NewSubjects("")
	if err != nil {
		t.Fatalf("NewSubjects(\"\"): %v", err)
	}

	assertSubjects(t, s, "")

	if s.Prefix() != "" {
		t.Errorf("Prefix() = %q, want empty", s.Prefix())
	}
}

func TestSubjects_Prefixed(t *testing.T) {
	s, err := transport.NewSubjects("catalogue.maestro")
	if err != nil {
		t.Fatalf("NewSubjects: %v", err)
	}

	assertSubjects(t, s, "catalogue.maestro.")

	if s.Prefix() != "catalogue.maestro" {
		t.Errorf("Prefix() = %q, want %q", s.Prefix(), "catalogue.maestro")
	}
}

// A trailing dot is a natural thing to write in config; it must not
// produce a double separator.
func TestSubjects_TrailingDotIsNormalised(t *testing.T) {
	s, err := transport.NewSubjects("catalogue.maestro.")
	if err != nil {
		t.Fatalf("NewSubjects: %v", err)
	}

	if got := s.PlayerHeartbeat(); got != "catalogue.maestro.player.heartbeat" {
		t.Errorf("PlayerHeartbeat() = %q", got)
	}
}

func assertSubjects(t *testing.T, s transport.Subjects, prefix string) {
	t.Helper()

	cases := []struct {
		got  string
		want string
	}{
		{s.PlayerHeartbeat(), prefix + "player.heartbeat"},
		{s.RoundCanCommit("rid1"), prefix + "round.rid1.can_commit"},
		{s.RoundVote("rid1"), prefix + "round.rid1.vote"},
		{s.RoundPreCommit("rid1"), prefix + "round.rid1.pre_commit"},
		{s.RoundStaged("rid1"), prefix + "round.rid1.staged"},
		{s.RoundDoCommit("rid1"), prefix + "round.rid1.do_commit"},
		{s.RoundCommitted("rid1"), prefix + "round.rid1.committed"},
		{s.RoundAbort("rid1"), prefix + "round.rid1.abort"},
		{s.RoundWildcard(), prefix + "round.>"},
		{s.RoundCanCommitWildcard(), prefix + "round.*.can_commit"},
		{s.RoundPreCommitWildcard(), prefix + "round.*.pre_commit"},
		{s.RoundDoCommitWildcard(), prefix + "round.*.do_commit"},
		{s.RoundAbortWildcard(), prefix + "round.*.abort"},
	}

	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("got %q want %q", c.got, c.want)
		}
	}
}

func TestSubjects_InvalidPrefixRejected(t *testing.T) {
	for _, prefix := range []string{
		".",
		"..",
		"catalogue..maestro",
		".maestro",
		"catalogue.*",
		"catalogue.>",
		"cata logue",
		"catalogue\tmaestro",
	} {
		if _, err := transport.NewSubjects(prefix); err == nil {
			t.Errorf("NewSubjects(%q) should have failed", prefix)
		}
	}
}

func TestMustSubjects_PanicsOnInvalidPrefix(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("MustSubjects should panic on an invalid prefix")
		}
	}()

	_ = transport.MustSubjects("catalogue.>")
}

func TestRIDFromSubject_Unprefixed(t *testing.T) {
	var s transport.Subjects

	cases := map[string]string{
		"round.abc.can_commit": "abc",
		"round.xyz123.vote":    "xyz123",
		"round.x.pre_commit":   "x",
		"player.heartbeat":     "",
		"":                     "",
		"round.":               "",
		"round.solo":           "",
	}

	for in, want := range cases {
		if got := s.RIDFromSubject(in); got != want {
			t.Errorf("RIDFromSubject(%q): got %q want %q", in, got, want)
		}
	}
}

func TestRIDFromSubject_Prefixed(t *testing.T) {
	s := transport.MustSubjects("catalogue.maestro")

	cases := map[string]string{
		"catalogue.maestro.round.abc.can_commit": "abc",
		"catalogue.maestro.round.xyz123.vote":    "xyz123",
		"catalogue.maestro.round.":               "",
		"catalogue.maestro.round.solo":           "",
		"catalogue.maestro.player.heartbeat":     "",

		// The regression this whole change exists to prevent: parsing
		// with a hardcoded "round." prefix would return "" here and the
		// player would vote against round "", silently timing out every
		// round on a fleet that looks healthy.
		"round.abc.can_commit": "",

		// A message from a different maestro fleet sharing the bus must
		// not be adopted.
		"other.fleet.round.abc.can_commit": "",
		"":                                 "",
	}

	for in, want := range cases {
		if got := s.RIDFromSubject(in); got != want {
			t.Errorf("RIDFromSubject(%q): got %q want %q", in, got, want)
		}
	}
}

// Every subject a Subjects produces must round-trip back through its own
// parser, for any prefix.
func TestRIDFromSubject_RoundTrips(t *testing.T) {
	for _, prefix := range []string{"", "maestro", "catalogue.maestro", "a.b.c.d"} {
		s := transport.MustSubjects(prefix)

		for _, subject := range []string{
			s.RoundCanCommit("r1"),
			s.RoundVote("r1"),
			s.RoundPreCommit("r1"),
			s.RoundStaged("r1"),
			s.RoundDoCommit("r1"),
			s.RoundCommitted("r1"),
			s.RoundAbort("r1"),
		} {
			if got := s.RIDFromSubject(subject); got != "r1" {
				t.Errorf("prefix %q: RIDFromSubject(%q) = %q, want r1", prefix, subject, got)
			}
		}
	}
}

// Two fleets on one bus must not see each other's rounds: neither the
// per-phase wildcards nor the parser may match across prefixes.
func TestSubjects_FleetsAreIsolated(t *testing.T) {
	a := transport.MustSubjects("fleet.a")
	b := transport.MustSubjects("fleet.b")

	if got := b.RIDFromSubject(a.RoundCanCommit("r1")); got != "" {
		t.Errorf("fleet b parsed fleet a's subject as %q", got)
	}

	if a.PlayerHeartbeat() == b.PlayerHeartbeat() {
		t.Error("heartbeat subjects must differ between fleets")
	}

	// The wildcard is what a player actually subscribes with, so verify
	// the literal scope rather than trusting the builder.
	if !strings.HasPrefix(a.RoundCanCommitWildcard(), "fleet.a.") {
		t.Errorf("wildcard %q is not scoped to fleet.a", a.RoundCanCommitWildcard())
	}
}
