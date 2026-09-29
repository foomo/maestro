package soloist

import (
	"time"

	"github.com/foomo/maestro/pkg/blobstore"
	"github.com/foomo/maestro/pkg/transport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Options configures a Soloist.
type Options struct {
	// Transport carries every typed Topic the maestro protocol needs.
	// Build it via transport.NewTransport(nc). Required.
	Transport transport.Transport

	// BlobStore stores the files of each published version. Required.
	BlobStore blobstore.BlobStore

	// InstanceID names the soloist in logs. It plays no part in the
	// protocol.
	InstanceID string

	// HeartbeatWindow is how recently a player must have heartbeated to
	// count as alive. Defaults to 15s.
	HeartbeatWindow time.Duration

	// RosterScanTick is how often the soloist checks the roster for players
	// that need a resync. Defaults to 5s.
	RosterScanTick time.Duration

	// ResyncDebounce is the minimum gap between two resync rounds.
	// Defaults to 10s.
	ResyncDebounce time.Duration

	// CanCommitTimeout bounds phase 1. A timeout aborts the round.
	// Defaults to 10s.
	CanCommitTimeout time.Duration

	// StageTimeout returns the phase 2 deadline for a manifest of totalSize
	// bytes. A timeout aborts the round. Defaults to twice the time needed
	// at 10 MiB/s, clamped to [60s, 30m].
	StageTimeout func(totalSize int64) time.Duration

	// DoCommitTimeout bounds phase 3. A timeout does not abort the round;
	// players that did not confirm are resynced later. Defaults to 10s.
	DoCommitTimeout time.Duration

	// MeterProvider provides the OpenTelemetry meter. Defaults to
	// otel.GetMeterProvider().
	MeterProvider metric.MeterProvider

	// TracerProvider provides the OpenTelemetry tracer. Defaults to
	// otel.GetTracerProvider().
	TracerProvider trace.TracerProvider

	// Logger receives the soloist's logs. Defaults to a no-op logger.
	Logger *zap.Logger
}

func (o *Options) applyDefaults() {
	if o.HeartbeatWindow == 0 {
		o.HeartbeatWindow = 15 * time.Second
	}

	if o.RosterScanTick == 0 {
		o.RosterScanTick = 5 * time.Second
	}

	if o.ResyncDebounce == 0 {
		o.ResyncDebounce = 10 * time.Second
	}

	if o.CanCommitTimeout == 0 {
		o.CanCommitTimeout = 10 * time.Second
	}

	if o.DoCommitTimeout == 0 {
		o.DoCommitTimeout = 10 * time.Second
	}

	if o.StageTimeout == nil {
		o.StageTimeout = defaultStageTimeout
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

func defaultStageTimeout(totalSize int64) time.Duration {
	const minBps = 10 * 1024 * 1024 // 10 MiB/s

	d := time.Duration(totalSize/minBps) * time.Second * 2
	if d < 60*time.Second {
		return 60 * time.Second
	}

	if d > 30*time.Minute {
		return 30 * time.Minute
	}

	return d
}
