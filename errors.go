package maestro

import "errors"

// Sentinel errors shared by the soloist and player roles. Call sites wrap
// them with %w; match with [errors.Is].
var (
	// ErrNoPlayers reports that the roster was empty when an operation
	// needed at least one player.
	ErrNoPlayers = errors.New("maestro: no players in roster")
	// ErrAbort reports that a round was aborted, for example because a
	// player voted no or failed to stage.
	ErrAbort = errors.New("maestro: round aborted")
	// ErrGenStale reports a message whose generation token is older than
	// the soloist's current boot epoch.
	ErrGenStale = errors.New("maestro: stale generation token")
	// ErrManifestMismatch reports that [Manifest.Validate] rejected a
	// manifest.
	ErrManifestMismatch = errors.New("maestro: manifest validation failed")
	// ErrBlobstoreMismatch reports that the soloist and a player disagree on
	// the blobstore kind.
	ErrBlobstoreMismatch = errors.New("maestro: blobstore kind mismatch between soloist and player")
	// ErrDuplicateInstance reports that two players heartbeat with the same
	// instance ID.
	ErrDuplicateInstance = errors.New("maestro: duplicate instance id in roster")
	// ErrRoundInFlight reports that another round is already running.
	ErrRoundInFlight = errors.New("maestro: another round is in flight")
	// ErrUnsafeName reports a file name that is absolute, unclean or escapes
	// its root directory.
	ErrUnsafeName = errors.New("maestro: manifest file name failed path safety check")
)
