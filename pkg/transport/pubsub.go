// Package transport provides typed publish/subscribe bundles for the maestro
// protocol.  All message types are carried by goflux Publishers/Subscribers
// constructed over the goflux/transport/nats package — no raw nats.Conn handling
// leaks into Soloist or Player.
//
// Callers wire the bundle once at startup:
//
//	nc, _ := nats.Connect(url)
//	tr := transport.NewTransport(nc)
//	sol, _ := soloist.New(soloist.Options{Transport: tr, ...})
package transport

import (
	"github.com/foomo/goencode"
	"github.com/foomo/goflux"
	natsgoflux "github.com/foomo/goflux/transport/nats"
	"github.com/nats-io/nats.go"
)

// Transport bundles every typed Topic the maestro protocol needs. Each Topic
// embeds both Publisher[T] and Subscriber[T]; the Soloist and Player each
// invoke only their role-appropriate direction. Publishers are unbound — the
// caller binds them per-round at runtime.
type Transport struct {
	CanCommit goflux.Topic[CanCommit]
	PreCommit goflux.Topic[PreCommit]
	DoCommit  goflux.Topic[DoCommit]
	Abort     goflux.Topic[Abort]
	Heartbeat goflux.Topic[Heartbeat]
	Vote      goflux.Topic[Vote]
	Staged    goflux.Topic[Staged]
	Committed goflux.Topic[Committed]
}

// NewTransport constructs the maestro pub/sub bundle over the given
// *nats.Conn.  The caller retains ownership of nc and must Close it.
func NewTransport(nc *nats.Conn, opts ...natsgoflux.Option) Transport {
	return Transport{
		CanCommit: newTopic(nc, CanCommitCodec, opts...),
		PreCommit: newTopic(nc, PreCommitCodec, opts...),
		DoCommit:  newTopic(nc, DoCommitCodec, opts...),
		Abort:     newTopic(nc, AbortCodec, opts...),
		Heartbeat: newTopic(nc, HeartbeatCodec, opts...),
		Vote:      newTopic(nc, VoteCodec, opts...),
		Staged:    newTopic(nc, StagedCodec, opts...),
		Committed: newTopic(nc, CommittedCodec, opts...),
	}
}

func newTopic[T any](nc *nats.Conn, c goencode.Codec[T, []byte], opts ...natsgoflux.Option) goflux.Topic[T] {
	return goflux.Topic[T]{
		Publisher:  natsgoflux.NewPublisher[T](nc, c.Encode, opts...),
		Subscriber: natsgoflux.NewSubscriber[T](nc, c.Decode, opts...),
	}
}
