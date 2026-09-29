// Package soloist implements the write-side of the maestro 3PC protocol.
// A Soloist owns the [github.com/foomo/maestro/pkg/blobstore.BlobStore],
// tracks a player roster from heartbeats, and drives three-phase-commit
// rounds via [Soloist.Publish]. There is no leader election: a Soloist is
// a single, designated writer.
package soloist
