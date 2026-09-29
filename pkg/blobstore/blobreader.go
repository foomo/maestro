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
	// Reader opens file name of the finalized version v and returns its
	// contents and size. A negative size means the size is unknown. The
	// caller must close the returned reader.
	Reader(ctx context.Context, v maestro.Version, name string) (io.ReadCloser, int64, error)
}
