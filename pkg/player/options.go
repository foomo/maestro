package player

import (
	"time"

	"github.com/foomo/maestro/pkg/blobstore"
	"github.com/foomo/maestro/pkg/transport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Options configures a Player.
type Options struct {
	// Transport carries every typed Topic the maestro protocol needs.
	// Build it via transport.NewTransport(nc). Required.
	Transport transport.Transport

	// BlobReader serves manifest file bytes (read-only). The Player never
	// writes; callers should pass a read-only client (e.g. localfs.NewClient).
	BlobReader blobstore.BlobReader

	InstanceID          string
	HeartbeatPeriod     time.Duration
	DownloadConcurrency int
	StageHandler        StageHandler
	MeterProvider       metric.MeterProvider
	TracerProvider      trace.TracerProvider
	Logger              *zap.Logger
}

func (o *Options) applyDefaults() {
	if o.HeartbeatPeriod == 0 {
		o.HeartbeatPeriod = 5 * time.Second
	}

	if o.DownloadConcurrency == 0 {
		o.DownloadConcurrency = 4
	}

	if o.MeterProvider == nil {
		o.MeterProvider = otel.GetMeterProvider()
	}

	if o.TracerProvider == nil {
		o.TracerProvider = otel.GetTracerProvider()
	}

	if o.Logger == nil {
		o.Logger = zap.NewNop()
	}
}
