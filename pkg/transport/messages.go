package transport

import (
	msgpackcodec "github.com/foomo/goencode/msgpack/vmihailenco"

	"github.com/foomo/maestro"
)

// CanCommit is broadcast to all players to open a new commit round.
type CanCommit struct {
	// RoundID identifies the round.
	RoundID string `msgpack:"rid"`
	// Gen is the soloist's boot epoch, in Unix milliseconds.
	Gen int64 `msgpack:"gen"`
	// Target is the version being committed.
	Target maestro.Version `msgpack:"target"`
	// Manifest describes the files of Target.
	Manifest maestro.Manifest `msgpack:"manifest"`
	// DeadlineUnixMs is when the soloist stops waiting for replies, in Unix
	// milliseconds.
	DeadlineUnixMs int64 `msgpack:"deadline"`
}

// Vote is sent by each player in response to CanCommit.
type Vote struct {
	// RoundID identifies the round.
	RoundID string `msgpack:"rid"`
	// InstanceID identifies the replying player.
	InstanceID string `msgpack:"iid"`
	// OK reports whether the player votes to commit.
	OK bool `msgpack:"ok"`
	// Err describes the failure when OK is false.
	Err string `msgpack:"err,omitempty"`
}

// PreCommit tells players to stage (download) the target version.
type PreCommit struct {
	// RoundID identifies the round.
	RoundID string `msgpack:"rid"`
	// Gen is the soloist's boot epoch, in Unix milliseconds.
	Gen int64 `msgpack:"gen"`
	// Target is the version being committed.
	Target maestro.Version `msgpack:"target"`
	// DeadlineUnixMs is when the soloist stops waiting for replies, in Unix
	// milliseconds.
	DeadlineUnixMs int64 `msgpack:"deadline"`
}

// Staged is sent by each player after staging completes.
type Staged struct {
	// RoundID identifies the round.
	RoundID string `msgpack:"rid"`
	// InstanceID identifies the replying player.
	InstanceID string `msgpack:"iid"`
	// OK reports whether the player staged the round's target version.
	OK bool `msgpack:"ok"`
	// Err describes the failure when OK is false.
	Err string `msgpack:"err,omitempty"`
}

// DoCommit instructs each player to activate the staged version. Activation
// is atomic within a player — it serves the old version or the new one, never
// a mixture — but is not simultaneous across players: each activates when it
// receives this, and confirms with a Committed afterwards.
type DoCommit struct {
	// RoundID identifies the round.
	RoundID string `msgpack:"rid"`
	// Gen is the soloist's boot epoch, in Unix milliseconds.
	Gen int64 `msgpack:"gen"`
	// Target is the version being committed.
	Target maestro.Version `msgpack:"target"`
}

// Committed is sent by each player after the commit (or failure).
type Committed struct {
	// RoundID identifies the round.
	RoundID string `msgpack:"rid"`
	// InstanceID identifies the replying player.
	InstanceID string `msgpack:"iid"`
	// OK reports whether the player activated the round's target version.
	OK bool `msgpack:"ok"`
	// Err describes the failure when OK is false.
	Err string `msgpack:"err,omitempty"`
}

// Abort is broadcast to cancel an in-progress round.
type Abort struct {
	// RoundID identifies the round.
	RoundID string `msgpack:"rid"`
	// Gen is the soloist's boot epoch, in Unix milliseconds.
	Gen int64 `msgpack:"gen"`
	// Reason describes why the round was aborted.
	Reason string `msgpack:"reason"`
}

// Heartbeat is periodically published by each player.
type Heartbeat struct {
	// InstanceID identifies the player.
	InstanceID string `msgpack:"iid"`
	// GenAcked is reserved for generation acknowledgement.
	// [github.com/foomo/maestro/pkg/player.Player] always sends 0.
	GenAcked int64 `msgpack:"gen_acked"`
	// CurrentVersion is the version the player has active, or "" if none.
	CurrentVersion maestro.Version `msgpack:"current_version"`
	// Leaving marks a player's final heartbeat before a graceful shutdown.
	// The soloist removes it from the roster immediately instead of waiting
	// out HeartbeatWindow, so it is excluded from the next round's expected
	// set rather than needing to be tolerated as a non-responder in one.
	Leaving bool `msgpack:"leaving,omitempty"`
	// NotWired marks a player that is alive but whose round subscriptions are
	// not established yet, so it cannot answer a round. The soloist keeps it
	// in the roster (it is a real player, and it is tracked for resync) but
	// leaves it out of the expected set, because counting a player that
	// provably cannot vote would abort every round until it finishes starting.
	//
	// The sense is inverted — "not wired" rather than "wired" — so that the
	// zero value means eligible. A heartbeat from a player built against an
	// older version of this struct decodes with the field absent, and must be
	// treated as a full participant rather than silently excluded forever.
	NotWired bool `msgpack:"not_wired,omitempty"`
}

// Per-type codec singletons. Backed by vmihailenco/msgpack/v5 via foomo/goencode.
var (
	CanCommitCodec = msgpackcodec.NewCodec[CanCommit]()
	VoteCodec      = msgpackcodec.NewCodec[Vote]()
	PreCommitCodec = msgpackcodec.NewCodec[PreCommit]()
	StagedCodec    = msgpackcodec.NewCodec[Staged]()
	DoCommitCodec  = msgpackcodec.NewCodec[DoCommit]()
	CommittedCodec = msgpackcodec.NewCodec[Committed]()
	AbortCodec     = msgpackcodec.NewCodec[Abort]()
	HeartbeatCodec = msgpackcodec.NewCodec[Heartbeat]()
)

// var _ goencode.Codec[CanCommit, []byte] = CanCommitCodec
