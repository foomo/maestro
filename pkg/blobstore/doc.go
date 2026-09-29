// Package blobstore defines the pluggable byte-transfer layer used by
// maestro. [BlobStore] is the writer-side surface (used by
// [github.com/foomo/maestro/pkg/soloist.Soloist]); [BlobReader] is the
// read-only subset used by [github.com/foomo/maestro/pkg/player.Player].
// See [github.com/foomo/maestro/pkg/blobstore/localfs] for the in-box
// filesystem-backed implementation.
package blobstore
