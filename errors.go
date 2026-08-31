package maestro

import "errors"

var (
	ErrNoPlayers         = errors.New("maestro: no players in roster")
	ErrAbort             = errors.New("maestro: round aborted")
	ErrGenStale          = errors.New("maestro: stale generation token")
	ErrManifestMismatch  = errors.New("maestro: manifest validation failed")
	ErrBlobstoreMismatch = errors.New("maestro: blobstore kind mismatch between soloist and player")
	ErrDuplicateInstance = errors.New("maestro: duplicate instance id in roster")
	ErrRoundInFlight     = errors.New("maestro: another round is in flight")
	ErrUnsafeName        = errors.New("maestro: manifest file name failed path safety check")
)
