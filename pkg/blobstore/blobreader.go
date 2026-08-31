package blobstore

import (
	"context"
	"io"

	"github.com/foomo/maestro"
)

// BlobReader is the read-only surface used by the Player. Implementations
// satisfying BlobReader can serve as a Player-side blob source without exposing
// the writer-side mutation methods.
type BlobReader interface {
	Reader(ctx context.Context, v maestro.Version, name string) (io.ReadCloser, int64, error)
}
