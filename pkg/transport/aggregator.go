package transport

import (
	"context"
	"maps"
	"sync"

	"github.com/foomo/goflux"
)

// Aggregator collects messages from a goflux.BoundSubscriber, keyed by a
// per-message identity function, until it has received exactly one message
// from each expected key or the supplied context is cancelled.
//
// Once all expected messages are received, Wait returns the per-key map.
// The subscription is torn down after Wait returns.
type Aggregator[T any] struct {
	mu       sync.Mutex
	received map[string]T
	expected map[string]struct{}
	keyOf    func(T) string
	done     chan struct{}
	cancel   context.CancelFunc
}

// NewAggregator subscribes to sub and blocks until the subscription is
// confirmed live at the broker before returning — callers publish a request
// on the same subject right after, and an unconfirmed subscription would
// race that publish (core NATS drops messages with no matching subscriber,
// silently and on neither side). expected is the set of keys that must
// arrive before Wait unblocks. keyOf extracts the key from a received
// message.
func NewAggregator[T any](ctx context.Context, sub goflux.BoundSubscriber[T], expected []string, keyOf func(T) string) (*Aggregator[T], error) {
	subCtx, cancel := context.WithCancel(ctx) //nolint:gosec //G118

	a := &Aggregator[T]{
		received: make(map[string]T, len(expected)),
		expected: make(map[string]struct{}, len(expected)),
		keyOf:    keyOf,
		done:     make(chan struct{}),
		cancel:   cancel,
	}
	for _, k := range expected {
		a.expected[k] = struct{}{}
	}

	ready := make(chan struct{})

	go func() {
		_ = sub.SubscribeWithReady(subCtx, func(_ context.Context, msg goflux.Message[T]) error {
			a.mu.Lock()
			defer a.mu.Unlock()

			k := a.keyOf(msg.Payload)
			if _, want := a.expected[k]; !want {
				return nil
			}

			if _, dup := a.received[k]; dup {
				return nil
			}

			a.received[k] = msg.Payload
			if len(a.received) == len(a.expected) {
				select {
				case <-a.done:
				default:
					close(a.done)
				}
			}

			return nil
		}, func() { close(ready) })
	}()

	select {
	case <-ready:
	case <-subCtx.Done():
		cancel()

		return nil, subCtx.Err()
	}

	return a, nil
}

// Wait blocks until all expected messages have arrived or ctx is cancelled.
// It cancels the underlying subscription before returning.
func (a *Aggregator[T]) Wait(ctx context.Context) (map[string]T, error) {
	defer a.cancel()

	select {
	case <-a.done:
		a.mu.Lock()
		defer a.mu.Unlock()

		out := make(map[string]T, len(a.received))
		maps.Copy(out, a.received)

		return out, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// WaitPartial blocks until all expected messages have arrived or ctx is
// cancelled, and returns whatever was received either way. complete reports
// whether every expected key arrived.
//
// Unlike Wait, a deadline is not an error: the caller decides whether a
// partial result is usable. This exists for fan-outs where the participants
// are interchangeable replicas and one unresponsive member must not veto
// progress for the rest — see soloist Options.PartialFleet.
//
// Like Wait, it cancels the underlying subscription before returning and
// must not be called twice.
func (a *Aggregator[T]) WaitPartial(ctx context.Context) (map[string]T, bool) {
	defer a.cancel()

	select {
	case <-a.done:
	case <-ctx.Done():
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	out := make(map[string]T, len(a.received))
	maps.Copy(out, a.received)

	return out, len(a.received) == len(a.expected)
}

// Per-message-type convenience constructors.

func NewVoteAggregator(ctx context.Context, sub goflux.BoundSubscriber[Vote], expected []string, keyOf func(Vote) string) (*Aggregator[Vote], error) {
	return NewAggregator[Vote](ctx, sub, expected, keyOf)
}

func NewStagedAggregator(ctx context.Context, sub goflux.BoundSubscriber[Staged], expected []string, keyOf func(Staged) string) (*Aggregator[Staged], error) {
	return NewAggregator[Staged](ctx, sub, expected, keyOf)
}

func NewCommittedAggregator(ctx context.Context, sub goflux.BoundSubscriber[Committed], expected []string, keyOf func(Committed) string) (*Aggregator[Committed], error) {
	return NewAggregator[Committed](ctx, sub, expected, keyOf)
}
