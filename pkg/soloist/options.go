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

	BlobStore        blobstore.BlobStore
	InstanceID       string
	HeartbeatWindow  time.Duration
	RosterScanTick   time.Duration
	ResyncDebounce   time.Duration
	CanCommitTimeout time.Duration
	StageTimeout     func(totalSize int64) time.Duration
	DoCommitTimeout  time.Duration

	// PartialFleet relaxes the CanCommit and PreCommit phases from
	// "every roster member must participate" to "at least one must".
	// Players that do not vote, vote no, or fail to stage are marked
	// dirty and picked up by the next resync scan instead of aborting
	// the round for everyone.
	//
	// Leave this false when the players must flip in lockstep, e.g. when
	// they share a downstream contract and a version skew between them
	// would be visible to clients.
	//
	// Set it to true when the players are interchangeable read replicas
	// behind a load balancer. There, strict unanimity inverts the
	// availability goal of scaling out: every additional replica is
	// another single point of failure for publishing, and any replica
	// that is starting up, shutting down, or briefly wedged blocks new
	// data from reaching the healthy ones. A terminating pod keeps
	// heartbeating (so it stays in the roster) while no longer voting,
	// which is enough to fail every publish for a full HeartbeatWindow
	// on each rollout or scale event.
	//
	// DoCommit is already best-effort in both modes: a player that fails
	// the final phase is marked dirty rather than aborting the round.
	// PartialFleet extends that same tolerance to the earlier phases.
	PartialFleet bool

	MeterProvider  metric.MeterProvider
	TracerProvider trace.TracerProvider
	Logger         *zap.Logger
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
