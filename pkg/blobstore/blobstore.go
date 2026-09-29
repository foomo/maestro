package blobstore

import (
	"context"
	"io"

	"github.com/foomo/maestro"
)

// BlobStore is the pluggable writer-side backing used by the Soloist.
//
// Lifecycle of a Version on the writer side:
//  1. Caller invokes Writer once per file, writes the file's bytes, Closes the
//     returned WriteCloser.
//  2. Caller invokes Finalize with the staging Version label and the final
//     Manifest. Implementations atomically promote the staged files to the
//     destination keyed by m.Version (which may differ from the staging label).
//  3. Stat returns the file's sha256 + size, computed by the implementation.
//  4. Delete removes all artifacts of a Version (used for GC).
//
// Reader-side access is exposed by [BlobReader] and consumed by the Player.
// Implementations are free to satisfy both interfaces on a single type.
type BlobStore interface {
	// Writer returns a writer that stages the bytes of file name under the
	// staging label v. The file is stored once the writer is closed.
	Writer(ctx context.Context, v maestro.Version, name string) (io.WriteCloser, error)
	// Finalize promotes the files staged under v to the version m.Version and
	// records m. It must be atomic: readers see all files or none.
	Finalize(ctx context.Context, v maestro.Version, m maestro.Manifest) error
	// Stat returns the hex-encoded SHA-256 digest and size of file name in
	// version v.
	Stat(ctx context.Context, v maestro.Version, name string) (sha256 string, size int64, err error)
	// Delete removes every artifact of version v.
	Delete(ctx context.Context, v maestro.Version) error
}
