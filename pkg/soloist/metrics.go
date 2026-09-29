package soloist

import (
	"context"
	"errors"
	"time"

	"github.com/foomo/maestro/pkg/semconv"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// meterName is the instrumentation scope for every soloist instrument.
const meterName = "github.com/foomo/maestro/pkg/soloist"

// metrics holds the soloist's OTel instruments.
//
// A round that aborts is not an error in the usual sense — it is the
// protocol working — so the interesting signal is not "did Publish
// return an error" but "which outcome, and how often". Publish failures
// are already visible to the caller; the outcome breakdown is not
// recoverable from anywhere else, because an abort is decided across
// several players and reported to none of them.
//
// A nil *metrics is usable and records nothing, so instrumentation
// cannot become a reason for a publish to fail.
type metrics struct {
	rounds        metric.Int64Counter
	phaseDuration metric.Float64Histogram
	servedVersion metric.Int64Counter
}

// newMetrics registers the soloist's instruments. players is called on every
// collection to report the current roster size.
func newMetrics(mp metric.MeterProvider, players func() int) (*metrics, error) {
	m := mp.Meter(meterName)

	rounds, err := m.Int64Counter("maestro.publish.rounds",
		metric.WithDescription("Publish rounds by outcome."),
		metric.WithUnit("{round}"),
	)
	if err != nil {
		return nil, err
	}

	phaseDuration, err := m.Float64Histogram("maestro.publish.phase.duration",
		metric.WithDescription("Duration of each publish phase."),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	// maestro.roster.players reports how many players the roster holds right
	// now, counting only those wired for rounds. It is observed on every
	// collection rather than recorded per round: resync rounds and an idle
	// soloist never publish, and a per-publish value would read whatever the
	// last Publish saw, however long ago. Joins, departures and heartbeat
	// expiry therefore show up within one collection interval.
	//
	// Worth watching alongside publish outcomes because players that are alive but
	// still starting up are excluded from the expected set: players stuck in a
	// crash-loop can leave this near zero while the outcome counter reports
	// nothing but successes.
	if _, err := m.Int64ObservableGauge("maestro.roster.players",
		metric.WithDescription("Players in the roster wired for rounds."),
		metric.WithUnit("{player}"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(players()))
			return nil
		}),
	); err != nil {
		return nil, err
	}

	servedVersion, err := m.Int64Counter("maestro.publish.committed",
		metric.WithDescription("Versions committed by the soloist."),
		metric.WithUnit("{version}"),
	)
	if err != nil {
		return nil, err
	}

	return &metrics{
		rounds:        rounds,
		phaseDuration: phaseDuration,
		servedVersion: servedVersion,
	}, nil
}

// recordOutcome counts one completed round under its outcome.
func (m *metrics) recordOutcome(ctx context.Context, outcome string) {
	if m == nil {
		return
	}

	m.rounds.Add(ctx, 1, metric.WithAttributes(semconv.AttrPublishOutcome.String(outcome)))
}

// recordPhase records how long one phase took.
//
// Phase durations are the signal that distinguishes a player that is slow
// from one that is broken: a pre_commit that grows with snapshot size is
// expected, one that grows with replica count is not.
func (m *metrics) recordPhase(ctx context.Context, phase string, d time.Duration) {
	if m == nil {
		return
	}

	m.phaseDuration.Record(ctx, d.Seconds(),
		metric.WithAttributes(semconv.AttrPublishPhase.String(phase)))
}

// recordCommitted counts a version the soloist adopted as current.
func (m *metrics) recordCommitted(ctx context.Context, attrs ...attribute.KeyValue) {
	if m == nil {
		return
	}

	m.servedVersion.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// canCommitOutcome classifies a phase-1 failure.
//
// The distinction that matters operationally is whether players said no
// or said nothing: a rejection means the players are healthy and refusing
// this payload, silence means they are not answering. They call for
// opposite responses, and both currently surface as the same log line.
func canCommitOutcome(err error) string {
	if isTimeout(err) {
		return semconv.PublishOutcomeAbortVoteTimeout
	}

	return semconv.PublishOutcomeAbortVoteNo
}

// preCommitOutcome classifies a phase-2 failure along the same lines:
// staging that failed versus staging that never answered.
func preCommitOutcome(err error) string {
	if isTimeout(err) {
		return semconv.PublishOutcomeAbortStageTimeout
	}

	return semconv.PublishOutcomeAbortStageFailed
}

// isTimeout reports whether err came from a deadline rather than from a
// decision. Aggregator.Wait returns ctx.Err() verbatim on expiry, so the
// distinction survives the wrapping the phase helpers apply.
func isTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}
