package transport

import "strings"

// All maestro NATS subjects. The soloist embeds its own NATS server and
// players connect to that one server — no namespace prefix is needed because
// no two maestro deployments ever share a NATS instance.
const (
	SubjectPlayerHeartbeat = "player.heartbeat"

	SubjectRoundWildcard          = "round.>"
	SubjectRoundCanCommitWildcard = "round.*.can_commit"
	SubjectRoundPreCommitWildcard = "round.*.pre_commit"
	SubjectRoundDoCommitWildcard  = "round.*.do_commit"
	SubjectRoundAbortWildcard     = "round.*.abort"
)

// Per-round subject builders.

func SubjectRoundCanCommit(rid string) string { return "round." + rid + ".can_commit" }
func SubjectRoundVote(rid string) string      { return "round." + rid + ".vote" }
func SubjectRoundPreCommit(rid string) string { return "round." + rid + ".pre_commit" }
func SubjectRoundStaged(rid string) string    { return "round." + rid + ".staged" }
func SubjectRoundDoCommit(rid string) string  { return "round." + rid + ".do_commit" }
func SubjectRoundCommitted(rid string) string { return "round." + rid + ".committed" }
func SubjectRoundAbort(rid string) string     { return "round." + rid + ".abort" }

// RIDFromSubject extracts the round id from a subject of the form
// "round.<rid>.<phase>". Returns the empty string on malformed input.
func RIDFromSubject(subject string) string {
	const prefix = "round."
	if !strings.HasPrefix(subject, prefix) {
		return ""
	}

	rest := subject[len(prefix):]

	before, _, ok := strings.Cut(rest, ".")
	if !ok {
		return ""
	}

	return before
}
