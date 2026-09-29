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
	// Soloist-to-player broadcasts.
	CanCommit goflux.Topic[CanCommit]
	PreCommit goflux.Topic[PreCommit]
	DoCommit  goflux.Topic[DoCommit]
	Abort     goflux.Topic[Abort]

	// Player-to-soloist messages.
	Heartbeat goflux.Topic[Heartbeat]
	Vote      goflux.Topic[Vote]
	Staged    goflux.Topic[Staged]
	Committed goflux.Topic[Committed]

	// Subjects is the subject layout this bundle publishes and
	// subscribes on. Carrying it here rather than on each role's
	// Options means a soloist and its players cannot be configured with
	// mismatched prefixes while still appearing correctly wired.
	//
	// The zero value produces the unprefixed layout.
	Subjects Subjects
}

// NewTransport constructs the maestro pub/sub bundle over the given
// *nats.Conn using the unprefixed subject layout. The caller retains
// ownership of nc and must Close it.
func NewTransport(nc *nats.Conn, opts ...natsgoflux.Option) Transport {
	return newTransport(nc, Subjects{}, opts...)
}

// NewTransportWithPrefix is NewTransport with every subject scoped under
// prefix, for deployments sharing a NATS cluster with other services.
// An empty prefix is equivalent to NewTransport.
func NewTransportWithPrefix(nc *nats.Conn, prefix string, opts ...natsgoflux.Option) (Transport, error) {
	subjects, err := NewSubjects(prefix)
	if err != nil {
		return Transport{}, err
	}

	return newTransport(nc, subjects, opts...), nil
}

func newTransport(nc *nats.Conn, subjects Subjects, opts ...natsgoflux.Option) Transport {
	// Round subjects carry a fresh round id on every publish, so without a
	// destination template every round mints a new metric series that is
	// never reused. Set the default here, before opts, so a caller-supplied
	// natsgoflux.WithTelemetry still wins — goflux applies Options in order
	// and the last one wins.
	tel, err := goflux.NewTelemetry(goflux.WithDestinationTemplate(subjects.Template))
	if err != nil {
		tel = goflux.NewNoopTelemetry()
	}

	opts = append([]natsgoflux.Option{natsgoflux.WithTelemetry(tel)}, opts...)

	return Transport{
		CanCommit: newTopic(nc, CanCommitCodec, opts...),
		PreCommit: newTopic(nc, PreCommitCodec, opts...),
		DoCommit:  newTopic(nc, DoCommitCodec, opts...),
		Abort:     newTopic(nc, AbortCodec, opts...),
		Heartbeat: newTopic(nc, HeartbeatCodec, opts...),
		Vote:      newTopic(nc, VoteCodec, opts...),
		Staged:    newTopic(nc, StagedCodec, opts...),
		Committed: newTopic(nc, CommittedCodec, opts...),
		Subjects:  subjects,
	}
}

func newTopic[T any](nc *nats.Conn, c goencode.Codec[T, []byte], opts ...natsgoflux.Option) goflux.Topic[T] {
	return goflux.Topic[T]{
		Publisher:  natsgoflux.NewPublisher[T](nc, c.Encode, opts...),
		Subscriber: natsgoflux.NewSubscriber[T](nc, c.Decode, opts...),
	}
}
