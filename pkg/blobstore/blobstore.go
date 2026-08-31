// Package blobstore defines the pluggable byte-transfer layer used by
// maestro. [BlobStore] is the writer-side surface (used by
// [github.com/foomo/maestro/pkg/soloist.Soloist]); [BlobReader] is the
// read-only subset used by [github.com/foomo/maestro/pkg/player.Player].
// See [github.com/foomo/maestro/pkg/blobstore/localfs] for the in-box
// filesystem-backed implementation.
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
	Writer(ctx context.Context, v maestro.Version, name string) (io.WriteCloser, error)
	Finalize(ctx context.Context, v maestro.Version, m maestro.Manifest) error
	Stat(ctx context.Context, v maestro.Version, name string) (sha256 string, size int64, err error)
	Delete(ctx context.Context, v maestro.Version) error
}
