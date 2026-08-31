package transport_test

import (
	"testing"

	"github.com/foomo/maestro/pkg/transport"
)

func TestSubjects(t *testing.T) {
	cases := map[string]string{
		transport.SubjectPlayerHeartbeat:        "player.heartbeat",
		transport.SubjectRoundCanCommit("rid1"): "round.rid1.can_commit",
		transport.SubjectRoundVote("rid1"):      "round.rid1.vote",
		transport.SubjectRoundPreCommit("rid1"): "round.rid1.pre_commit",
		transport.SubjectRoundStaged("rid1"):    "round.rid1.staged",
		transport.SubjectRoundDoCommit("rid1"):  "round.rid1.do_commit",
		transport.SubjectRoundCommitted("rid1"): "round.rid1.committed",
		transport.SubjectRoundAbort("rid1"):     "round.rid1.abort",
		transport.SubjectRoundWildcard:          "round.>",
		transport.SubjectRoundCanCommitWildcard: "round.*.can_commit",
		transport.SubjectRoundPreCommitWildcard: "round.*.pre_commit",
		transport.SubjectRoundDoCommitWildcard:  "round.*.do_commit",
		transport.SubjectRoundAbortWildcard:     "round.*.abort",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("got %q want %q", got, want)
		}
	}
}

func TestRIDFromSubject(t *testing.T) {
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
		if got := transport.RIDFromSubject(in); got != want {
			t.Errorf("RIDFromSubject(%q): got %q want %q", in, got, want)
		}
	}
}
