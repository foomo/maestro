// Package player implements the read-side of the maestro 3PC protocol.
// A [Player] subscribes to round broadcasts from the
// [github.com/foomo/maestro/pkg/soloist.Soloist], drives the
// CanCommit → PreCommit → DoCommit state machine, and tracks the active
// version. Artifact lifecycle lives entirely inside the user-provided
// [StageHandler] implementation.
package player
