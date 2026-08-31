package soloist

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/foomo/maestro/pkg/semconv"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// collect gathers everything recorded through a fresh in-memory reader.
func collect(t *testing.T) (*metrics, func() metricdata.ResourceMetrics) {
	t.Helper()

	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))

	m, err := newMetrics(provider)
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}

	return m, func() metricdata.ResourceMetrics {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatalf("collect: %v", err)
		}

		return rm
	}
}

// counterValue returns the value recorded for name under the given
// attribute, or -1 when no such data point exists.
func counterValue(rm metricdata.ResourceMetrics, name string, attr attribute.KeyValue) int64 {
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != name {
				continue
			}

			sum, ok := md.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}

			for _, dp := range sum.DataPoints {
				if v, found := dp.Attributes.Value(attr.Key); found && v.AsString() == attr.Value.AsString() {
					return dp.Value
				}
			}
		}
	}

	return -1
}

func histogramCount(rm metricdata.ResourceMetrics, name string, attr attribute.KeyValue) uint64 {
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != name {
				continue
			}

			h, ok := md.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}

			for _, dp := range h.DataPoints {
				if v, found := dp.Attributes.Value(attr.Key); found && v.AsString() == attr.Value.AsString() {
					return dp.Count
				}
			}
		}
	}

	return 0
}

// A nil *metrics must be completely inert. Instrumentation is never a
// good reason for a publish to panic, and New() deliberately degrades to
// nil when the meter provider rejects an instrument.
func TestMetrics_NilIsInert(t *testing.T) {
	var m *metrics

	m.recordOutcome(context.Background(), semconv.PublishOutcomeSuccess)
	m.recordPhase(context.Background(), semconv.PublishPhaseTotal, time.Second)
	m.recordRoster(context.Background(), 3)
	m.recordCommitted(context.Background())
}

func TestMetrics_RecordsOutcomesSeparately(t *testing.T) {
	m, gather := collect(t)
	ctx := context.Background()

	m.recordOutcome(ctx, semconv.PublishOutcomeSuccess)
	m.recordOutcome(ctx, semconv.PublishOutcomeSuccess)
	m.recordOutcome(ctx, semconv.PublishOutcomeAbortVoteNo)

	rm := gather()

	if got := counterValue(rm, "maestro.publish.rounds",
		semconv.AttrPublishOutcome.String(semconv.PublishOutcomeSuccess)); got != 2 {
		t.Errorf("success rounds = %d, want 2", got)
	}

	if got := counterValue(rm, "maestro.publish.rounds",
		semconv.AttrPublishOutcome.String(semconv.PublishOutcomeAbortVoteNo)); got != 1 {
		t.Errorf("abort_vote_no rounds = %d, want 1", got)
	}

	// An outcome that never happened must not appear at all, otherwise a
	// dashboard shows a flat zero line for a condition that was never
	// evaluated.
	if got := counterValue(rm, "maestro.publish.rounds",
		semconv.AttrPublishOutcome.String(semconv.PublishOutcomeAbortStageTimeout)); got != -1 {
		t.Errorf("unrecorded outcome should be absent, got %d", got)
	}
}

func TestMetrics_RecordsPhasesSeparately(t *testing.T) {
	m, gather := collect(t)
	ctx := context.Background()

	for _, phase := range []string{
		semconv.PublishPhaseIngest,
		semconv.PublishPhaseCanCommit,
		semconv.PublishPhasePreCommit,
		semconv.PublishPhaseDoCommit,
		semconv.PublishPhaseTotal,
	} {
		m.recordPhase(ctx, phase, 10*time.Millisecond)
	}

	rm := gather()

	for _, phase := range []string{
		semconv.PublishPhaseIngest,
		semconv.PublishPhaseCanCommit,
		semconv.PublishPhasePreCommit,
		semconv.PublishPhaseDoCommit,
		semconv.PublishPhaseTotal,
	} {
		if got := histogramCount(rm, "maestro.publish.phase.duration",
			semconv.AttrPublishPhase.String(phase)); got != 1 {
			t.Errorf("phase %q count = %d, want 1", phase, got)
		}
	}
}

func TestMetrics_RecordsRoster(t *testing.T) {
	m, gather := collect(t)

	m.recordRoster(context.Background(), 4)

	rm := gather()

	var found bool

	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != "maestro.roster.players" {
				continue
			}

			g, ok := md.Data.(metricdata.Gauge[int64])
			if !ok {
				continue
			}

			for _, dp := range g.DataPoints {
				found = true

				if dp.Value != 4 {
					t.Errorf("roster players = %d, want 4", dp.Value)
				}
			}
		}
	}

	if !found {
		t.Error("maestro.roster.players was not recorded")
	}
}

// The classifiers are the whole point of the outcome breakdown: a fleet
// that rejects a payload and a fleet that has stopped answering call for
// opposite responses, and both otherwise look like "publish failed".
func TestOutcomeClassifiers(t *testing.T) {
	cases := []struct {
		name          string
		err           error
		wantCanCommit string
		wantPreCommit string
	}{
		{
			name:          "rejection",
			err:           errors.New("can_commit: no player accepted the round (0/2 responded)"),
			wantCanCommit: semconv.PublishOutcomeAbortVoteNo,
			wantPreCommit: semconv.PublishOutcomeAbortStageFailed,
		},
		{
			name:          "deadline",
			err:           fmt.Errorf("can_commit wait: %w", context.DeadlineExceeded),
			wantCanCommit: semconv.PublishOutcomeAbortVoteTimeout,
			wantPreCommit: semconv.PublishOutcomeAbortStageTimeout,
		},
		{
			name:          "cancellation",
			err:           fmt.Errorf("pre_commit wait: %w", context.Canceled),
			wantCanCommit: semconv.PublishOutcomeAbortVoteTimeout,
			wantPreCommit: semconv.PublishOutcomeAbortStageTimeout,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := canCommitOutcome(tc.err); got != tc.wantCanCommit {
				t.Errorf("canCommitOutcome = %q, want %q", got, tc.wantCanCommit)
			}

			if got := preCommitOutcome(tc.err); got != tc.wantPreCommit {
				t.Errorf("preCommitOutcome = %q, want %q", got, tc.wantPreCommit)
			}
		})
	}
}
