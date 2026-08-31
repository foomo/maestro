// Package maestro replicates a single in-memory state from one writer
// ([github.com/foomo/maestro/pkg/soloist.Soloist]) to every replica
// ([github.com/foomo/maestro/pkg/player.Player]) using a three-phase commit.
// One soloist writes the score; every player turns the page together — each
// either flips to the new [Version] or keeps the old one, so partial updates
// across the players are not observable.
//
// Control-plane traffic (round coordination, votes, heartbeats) is small
// typed messages carried by [github.com/foomo/maestro/pkg/transport]. File
// bytes move separately through a pluggable
// [github.com/foomo/maestro/pkg/blobstore.BlobStore]; this package's own
// wire format never carries application data, only a [Manifest] describing
// named files by hash and size.
//
// Application code implements
// [github.com/foomo/maestro/pkg/player.StageHandler] to decode a
// [Manifest]'s files into whatever Go value it wants replicated, and calls
// [github.com/foomo/maestro/pkg/soloist.Soloist.Publish] to publish a new
// version from the writer side.
//
// There is no leader election: the Soloist is a single, designated writer
// (typically a replicas=1 deployment). See the package documentation for
// [github.com/foomo/maestro/pkg/soloist] and
// [github.com/foomo/maestro/pkg/player] for the two roles, and
// https://foomo.github.io/maestro/ for the full guide.
package maestro
