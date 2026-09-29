package player

import (
	"context"

	"github.com/foomo/maestro"
)

// StageHandler is the user-provided phase callback. It owns the lifecycle of
// the in-memory artifact built from each round's manifest. The [Player]
// drives it through the Stage, Activate and Abort phases.
type StageHandler interface {
	// Stage builds the artifact for version v from m and src and retains it
	// as pending. Returning an error votes against the round.
	Stage(ctx context.Context, v maestro.Version, m maestro.Manifest, src FileSource) error
	// Activate promotes the pending artifact for v to active. The Player
	// calls it on DoCommit. Returning an error fails the commit reply.
	Activate(ctx context.Context, v maestro.Version) error
	// Abort discards any pending artifact for v. The Player calls it on
	// Abort, or when a later Stage supersedes an earlier pending artifact.
	Abort(ctx context.Context, v maestro.Version) error
}
