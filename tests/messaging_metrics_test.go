package tests_test

import (
	"context"
	"strings"
	"testing"
	"time"

	testingx "github.com/foomo/go/testing"
	"github.com/foomo/goflux"
	natsgoflux "github.com/foomo/goflux/transport/nats"
	"github.com/foomo/maestro/internal/testutil"
	"github.com/foomo/maestro/pkg/blobstore/localfs"
	"github.com/foomo/maestro/pkg/player"
	"github.com/foomo/maestro/pkg/soloist"
	"github.com/foomo/maestro/pkg/transport"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap/zaptest"
)

// TestRoundMessagingMetricsDoNotLeakRoundID runs two full 3PC rounds and
// proves the goflux messaging metrics they produce (messaging.client.*,
// messaging.process.duration) carry the same attribute set for both rounds
// — no round id ever reaches a metric point, only the stable
// messaging.destination.template. Without transport.newTransport wiring a
// shared, templated *goflux.Telemetry, each round's concrete
// "round.<rid>.<phase>" subject would mint its own metric series and this
// test would see the attribute-set count grow from round 1 to round 2.
func TestRoundMessagingMetricsDoNotLeakRoundID(t *testing.T) {
	const prefix = "catalogue.maestro"

	url := testutil.StartNATS(t)
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	subjects, err := transport.NewSubjects(prefix)
	if err != nil {
		t.Fatalf("NewSubjects: %v", err)
	}

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	tel, err := goflux.NewTelemetry(
		goflux.WithMeterProvider(provider),
		goflux.WithDestinationTemplate(subjects.Template),
	)
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}

	withTel := natsgoflux.WithTelemetry(tel)

	solNC := dialNATS(t, url)

	tr, err := transport.NewTransportWithPrefix(solNC, prefix, withTel)
	if err != nil {
		t.Fatalf("NewTransportWithPrefix: %v", err)
	}

	s, err := soloist.New(soloist.Options{
		Transport:        tr,
		BlobStore:        bs,
		InstanceID:       "soloist-msg-metrics",
		HeartbeatWindow:  5 * time.Second,
		RosterScanTick:   100 * time.Millisecond,
		ResyncDebounce:   50 * time.Millisecond,
		CanCommitTimeout: 2 * time.Second,
		DoCommitTimeout:  2 * time.Second,
		Logger:           zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	go s.Start(ctx) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return s.Ready() })

	playerNC := dialNATS(t, url)

	playerTr, err := transport.NewTransportWithPrefix(playerNC, prefix, withTel)
	if err != nil {
		t.Fatalf("NewTransportWithPrefix: %v", err)
	}

	h := newCapturingHandler()

	pl, err := player.New(player.Options{
		Transport:       playerTr,
		BlobReader:      bs,
		InstanceID:      "player-msg-metrics",
		HeartbeatPeriod: 50 * time.Millisecond,
		StageHandler:    h,
		Logger:          zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}

	pctx, pcancel := context.WithCancel(t.Context())
	t.Cleanup(pcancel)

	go pl.Start(pctx) //nolint:errcheck

	testingx.WaitFor(t, 2*time.Second, func() bool { return pl.Wired() })
	settleHeartbeats(t)

	fingerprintsAfterRound := make([]map[string]map[string]struct{}, 0, 2)

	for i := range 2 {
		v, err := s.Publish(t.Context(), []soloist.File{
			{Name: "payload.txt", Reader: strings.NewReader("round-body")},
		})
		if err != nil {
			t.Fatalf("round %d Publish: %v", i+1, err)
		}

		testingx.WaitFor(t, 5*time.Second, func() bool { return pl.CurrentVersion() == v })

		var rm metricdata.ResourceMetrics
		if err := reader.Collect(t.Context(), &rm); err != nil {
			t.Fatalf("collect: %v", err)
		}

		fingerprintsAfterRound = append(fingerprintsAfterRound, messagingAttrFingerprints(t, rm))
	}

	roundOneNames := fingerprintsAfterRound[0]
	roundTwoNames := fingerprintsAfterRound[1]

	if len(roundOneNames) == 0 {
		t.Fatal("expected at least one messaging_* metric after round 1")
	}

	for name, roundTwoSet := range roundTwoNames {
		roundOneSet := roundOneNames[name]

		for fp := range roundOneSet {
			if strings.Contains(fp, "messaging.destination.name=") && strings.Contains(fp, prefix+".round.") {
				t.Errorf("%s: round 1 recorded the concrete round subject in destination.name: %s", name, fp)
			}
		}

		for fp := range roundTwoSet {
			if strings.Contains(fp, "messaging.destination.name=") && strings.Contains(fp, prefix+".round.") {
				t.Errorf("%s: round 2 recorded the concrete round subject in destination.name: %s", name, fp)
			}
		}

		// The set of distinct attribute combinations for round-scoped
		// subjects must not grow between rounds — that is the whole point
		// of templating.
		if got, want := len(roundTwoSet), len(roundOneSet); got > want {
			t.Errorf("%s: attribute-set count grew from round 1 (%d) to round 2 (%d) — round id is leaking into metrics", name, want, got)
		}
	}
}

// messagingAttrFingerprints returns, per messaging.* metric name, the set of
// distinct attribute-set fingerprints observed across its data points.
func messagingAttrFingerprints(t *testing.T, rm metricdata.ResourceMetrics) map[string]map[string]struct{} {
	t.Helper()

	out := make(map[string]map[string]struct{})

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if !strings.HasPrefix(m.Name, "messaging.") {
				continue
			}

			for _, fp := range dataPointFingerprints(m) {
				if out[m.Name] == nil {
					out[m.Name] = make(map[string]struct{})
				}

				out[m.Name][fp] = struct{}{}
			}
		}
	}

	return out
}

func dataPointFingerprints(m metricdata.Metrics) []string {
	var fps []string

	switch d := m.Data.(type) {
	case metricdata.Sum[int64]:
		for _, dp := range d.DataPoints {
			fps = append(fps, fingerprintAttrSet(dp.Attributes.ToSlice()))
		}
	case metricdata.Histogram[float64]:
		for _, dp := range d.DataPoints {
			fps = append(fps, fingerprintAttrSet(dp.Attributes.ToSlice()))
		}
	}

	return fps
}

func fingerprintAttrSet(kvs []attribute.KeyValue) string {
	var b strings.Builder

	for _, kv := range kvs {
		b.WriteString(string(kv.Key))
		b.WriteString("=")
		b.WriteString(kv.Value.String())
		b.WriteString(";")
	}

	return b.String()
}
