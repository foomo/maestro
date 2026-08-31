package player

import (
	"context"

	"github.com/foomo/maestro"
)

// StageHandler is the user-provided phase callback. It owns the lifecycle of
// the in-memory artifact built from each round's manifest. The Player drives
// it through three phases:
//
//   - Stage:    build the artifact from manifest+src and retain it as pending
//     for the given version. Returning error votes against the round.
//   - Activate: promote the pending artifact for v to active. Player calls this
//     on DoCommit. Returning error fails the commit reply.
//   - Abort:    discard any pending artifact for v. Player calls this on Abort
//     or when a later Stage supersedes an earlier pending.
type StageHandler interface {
	Stage(ctx context.Context, v maestro.Version, m maestro.Manifest, src FileSource) error
	Activate(ctx context.Context, v maestro.Version) error
	Abort(ctx context.Context, v maestro.Version) error
}
