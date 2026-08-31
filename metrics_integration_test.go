package maestro_test

import (
	"context"
	"strings"
	"testing"
	"time"

	testingx "github.com/foomo/go/testing"
	"github.com/foomo/maestro/internal/testutil"
	"github.com/foomo/maestro/pkg/blobstore/localfs"
	"github.com/foomo/maestro/pkg/semconv"
	"github.com/foomo/maestro/pkg/soloist"
	"github.com/foomo/maestro/pkg/transport"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap/zaptest"
)

func TestPublishRecordsOutcomeMetrics(t *testing.T) {
	const prefix = "metrics.fleet"

	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	nc := dialNATS(t, url)

	tr, err := transport.NewTransportWithPrefix(nc, prefix)
	if err != nil {
		t.Fatal(err)
	}

	s, err := soloist.New(soloist.Options{
		Transport:        tr,
		BlobStore:        bs,
		InstanceID:       "soloist-metrics",
		HeartbeatWindow:  5 * time.Second,
		RosterScanTick:   100 * time.Millisecond,
		ResyncDebounce:   50 * time.Millisecond,
		CanCommitTimeout: 2 * time.Second,
		DoCommitTimeout:  2 * time.Second,
		MeterProvider:    provider,
		Logger:           zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	go s.Start(ctx) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return s.Ready() })

	h := newCapturingHandler()
	startPrefixedPlayer(t, url, prefix, "p1", bs, h)
	settleHeartbeats(t)

	if _, err := s.Publish(t.Context(), []soloist.File{
		{Name: "payload.txt", Reader: strings.NewReader("metric-body")},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}

	var (
		sawSuccess bool
		sawTotal   bool
	)

	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			switch md.Name {
			case "maestro.publish.rounds":
				sum, ok := md.Data.(metricdata.Sum[int64])
				if !ok {
					continue
				}

				for _, dp := range sum.DataPoints {
					v, found := dp.Attributes.Value(semconv.AttrPublishOutcome)
					if found && v.AsString() == semconv.PublishOutcomeSuccess && dp.Value >= 1 {
						sawSuccess = true
					}
				}
			case "maestro.publish.phase.duration":
				h, ok := md.Data.(metricdata.Histogram[float64])
				if !ok {
					continue
				}

				for _, dp := range h.DataPoints {
					v, found := dp.Attributes.Value(semconv.AttrPublishPhase)
					if found && v.AsString() == semconv.PublishPhaseTotal && dp.Count >= 1 {
						sawTotal = true
					}
				}
			}
		}
	}

	if !sawSuccess {
		t.Error("a successful round did not record a success outcome")
	}

	if !sawTotal {
		t.Error("a successful round did not record a total phase duration")
	}
}
