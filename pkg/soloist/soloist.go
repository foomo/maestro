// Package soloist implements the write-side of the maestro 3PC protocol.
// A Soloist owns the [github.com/foomo/maestro/pkg/blobstore.BlobStore],
// tracks a player roster from heartbeats, and drives three-phase-commit
// rounds via [Soloist.Publish]. There is no leader election: a Soloist is
// a single, designated writer.
package soloist

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/foomo/goflux"
	"github.com/foomo/gofuncy"
	"github.com/foomo/maestro"
	"github.com/foomo/maestro/pkg/semconv"
	"github.com/foomo/maestro/pkg/transport"
	"go.uber.org/zap"
)

// ErrAlreadyStarted is returned when Start is called more than once.
var ErrAlreadyStarted = errors.New("soloist: Start already called")

// Soloist is the single writer in a maestro deployment.
type Soloist struct {
	opts       Options
	tr         transport.Transport
	l          *zap.Logger
	bootEpoch  int64
	mu         sync.Mutex // round mutex
	currentVer atomic.Pointer[maestro.Version]
	currentMan atomic.Pointer[maestro.Manifest]
	roster     *Roster
	lastResync atomic.Int64
	ready      atomic.Bool
	metrics    *metrics

	cancel    context.CancelFunc
	done      chan struct{}
	started   atomic.Bool
	closeOnce sync.Once
}

// New constructs a Soloist. The transport is supplied via opts.Transport — the
// caller owns the underlying *nats.Conn.
func New(opts Options) (*Soloist, error) {
	opts.applyDefaults()

	if opts.BlobStore == nil {
		return nil, errors.New("soloist: BlobStore required")
	}

	if opts.Transport.CanCommit.Publisher == nil {
		return nil, errors.New("soloist: Transport required (use transport.NewTransport)")
	}

	l := opts.Logger.Named("maestro.soloist")

	// Instrumentation must not be a reason a soloist fails to start: a
	// nil *metrics records nothing and every call site tolerates it.
	m, err := newMetrics(opts.MeterProvider)
	if err != nil {
		l.Warn("metrics unavailable; continuing uninstrumented", zap.Error(err))

		m = nil
	}

	return &Soloist{
		opts:      opts,
		tr:        opts.Transport,
		l:         l,
		bootEpoch: time.Now().UnixMilli(),
		roster:    NewRoster(opts.HeartbeatWindow, l.Named("roster")),
		metrics:   m,
	}, nil
}

// Name implements the foomo/keel service.Service interface.
func (s *Soloist) Name() string { return "maestro.soloist" }

// Start wires the heartbeat subscriber + roster monitor under one errgroup.
// Blocks until ctx cancels, Close is called, or a goroutine returns an error.
// Calling Start more than once returns ErrAlreadyStarted.
func (s *Soloist) Start(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}

	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.done = make(chan struct{})

	defer close(s.done)

	s.l.Info("soloist running",
		zap.Duration("roster_window", s.opts.HeartbeatWindow),
		zap.Duration("scan_tick", s.opts.RosterScanTick),
	)

	g := gofuncy.NewGroup(runCtx,
		gofuncy.WithName("maestro.soloist"),
		gofuncy.WithFailFast(),
		gofuncy.WithoutTracing(),
	)

	g.Add(s.subscribeHeartbeats)
	g.Add(s.runMonitor)

	s.ready.Store(true)

	err := g.Wait()
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}

	return err
}

// Close signals shutdown and blocks until every goroutine spawned by Start
// has exited or ctx expires. Safe to call before Start (no-op) and safe to
// call multiple times.
func (s *Soloist) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
	})

	if s.done == nil {
		return nil
	}

	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Ready reports whether Start has wired heartbeats. Tests block on this.
func (s *Soloist) Ready() bool { return s.ready.Load() }

// Current returns the last committed Version (empty if none).
func (s *Soloist) Current() maestro.Version {
	if v := s.currentVer.Load(); v != nil {
		return *v
	}

	return ""
}

// Publish writes each File into BlobStore, computes a content-addressed
// Version over the batch, then drives a 3PC over NATS. If the roster is empty,
// the new Version becomes current via silent commit. Returns the new Version
// on success. The batch commits atomically: all files land under one Manifest
// in a single 3PC round.
func (s *Soloist) Publish(ctx context.Context, files []File) (maestro.Version, error) {
	if len(files) == 0 {
		return "", errors.New("soloist: Publish requires at least one file")
	}

	s.l.Info("publish requested", zap.Int("files", len(files)))

	s.mu.Lock()
	defer s.mu.Unlock()

	ingestStart := time.Now()

	m, err := IngestFiles(ctx, s.opts.BlobStore, files)
	if err != nil {
		s.l.Error("ingest failed", zap.Error(err))
		s.metrics.recordOutcome(ctx, semconv.PublishOutcomeAbortStageFailed)

		return "", err
	}

	s.metrics.recordPhase(ctx, semconv.PublishPhaseIngest, time.Since(ingestStart))

	s.l.Info("manifest built",
		zap.String("version", string(m.Version)),
		zap.Int("files", len(m.Files)),
		zap.Int64("total_size", m.TotalSize),
	)

	if err := s.commitManifest(ctx, m); err != nil {
		return "", err
	}

	return m.Version, nil
}

func (s *Soloist) commitManifest(ctx context.Context, m maestro.Manifest) error {
	// Participants, not Snapshot: players that are alive but still wiring up
	// their round subscriptions cannot vote, and counting them would abort
	// this round for everyone. They are resynced by the monitor once ready.
	rosterSnap := s.roster.Participants()

	s.metrics.recordRoster(ctx, len(rosterSnap))

	if len(rosterSnap) == 0 {
		// An empty roster and a roster of only starting-up players are not
		// the same thing. With nobody out there, committing silently is
		// correct — there is no one to diverge from. With players present but
		// none able to vote yet, a silent commit would advance the soloist
		// past a version they never saw and never asked to skip, so this has
		// to be a failure the producer hears about and retries.
		if alive := len(s.roster.Snapshot()); alive > 0 {
			s.metrics.recordOutcome(ctx, semconv.PublishOutcomeAbortVoteTimeout)

			return fmt.Errorf(
				"can_commit: %d player(s) in the roster, none wired for rounds yet; retry once they finish starting",
				alive,
			)
		}

		s.l.Info("silent commit", zap.String("version", string(m.Version)))
		s.setCurrent(m)
		s.metrics.recordOutcome(ctx, semconv.PublishOutcomeNoPlayersSilent)
		s.metrics.recordCommitted(ctx)

		return nil
	}

	expected := make([]string, 0, len(rosterSnap))
	for id := range rosterSnap {
		expected = append(expected, id)
	}

	s.l.Info("commit via 3PC",
		zap.String("version", string(m.Version)),
		zap.Int("expected", len(expected)),
	)

	if err := s.runRound(ctx, m, expected); err != nil {
		return err
	}

	s.setCurrent(m)
	s.metrics.recordCommitted(ctx)

	return nil
}

func (s *Soloist) setCurrent(m maestro.Manifest) {
	v := m.Version
	s.currentVer.Store(&v)
	s.currentMan.Store(&m)
}

// runRound drives the 3PC against expected instance IDs.
func (s *Soloist) runRound(ctx context.Context, m maestro.Manifest, expected []string) error {
	rid := randHex(16)
	gen := s.bootEpoch

	rl := s.l.With(
		zap.String("rid", rid),
		zap.String("target", string(m.Version)),
		zap.Int("expected", len(expected)),
	)
	rl.Info("round start", zap.Int64("gen", gen))

	roundStart := time.Now()

	// Phase 1: CanCommit
	t1 := time.Now()

	if err := s.runPhase1(ctx, rid, gen, m, expected); err != nil {
		rl.Warn("can_commit failed", zap.Error(err))
		s.publishAbort(ctx, rid, gen, "can_commit failed")
		s.metrics.recordPhase(ctx, semconv.PublishPhaseCanCommit, time.Since(t1))
		s.metrics.recordOutcome(ctx, canCommitOutcome(err))

		return err
	}

	s.metrics.recordPhase(ctx, semconv.PublishPhaseCanCommit, time.Since(t1))
	rl.Debug("phase ok", zap.String("phase", "can_commit"), zap.Duration("dur", time.Since(t1)))

	// Phase 2: PreCommit (stage)
	stageTimeout := s.opts.StageTimeout(m.TotalSize)
	t2 := time.Now()

	if err := s.runPhase2(ctx, rid, gen, m, expected, stageTimeout); err != nil {
		rl.Warn("pre_commit failed", zap.Error(err))
		s.publishAbort(ctx, rid, gen, "pre_commit failed")
		s.metrics.recordPhase(ctx, semconv.PublishPhasePreCommit, time.Since(t2))
		s.metrics.recordOutcome(ctx, preCommitOutcome(err))

		return err
	}

	s.metrics.recordPhase(ctx, semconv.PublishPhasePreCommit, time.Since(t2))
	rl.Debug("phase ok", zap.String("phase", "pre_commit"), zap.Duration("dur", time.Since(t2)))

	// Phase 3: DoCommit
	t3 := time.Now()

	if err := s.runPhase3(ctx, rid, gen, m, expected); err != nil {
		rl.Warn("do_commit timeout: marking all players dirty", zap.Error(err))

		for _, id := range expected {
			s.roster.MarkDirty(id)
		}

		s.metrics.recordPhase(ctx, semconv.PublishPhaseDoCommit, time.Since(t3))
		s.metrics.recordOutcome(ctx, semconv.PublishOutcomeAbortStageTimeout)

		return fmt.Errorf("do_commit timeout: %w", err)
	}

	s.metrics.recordPhase(ctx, semconv.PublishPhaseDoCommit, time.Since(t3))
	s.metrics.recordPhase(ctx, semconv.PublishPhaseTotal, time.Since(roundStart))
	s.metrics.recordOutcome(ctx, semconv.PublishOutcomeSuccess)

	rl.Debug("phase ok", zap.String("phase", "do_commit"), zap.Duration("dur", time.Since(t3)))
	rl.Info("round committed", zap.Duration("dur", time.Since(roundStart)))

	return nil
}

// runPhase1 drives CanCommit. Every expected player must vote OK; anything
// less aborts the round.
func (s *Soloist) runPhase1(ctx context.Context, rid string, gen int64, m maestro.Manifest, expected []string) error {
	voteSub := goflux.BindSubscriber(s.tr.Vote.Subscriber, s.tr.Subjects.RoundVote(rid))

	cctx, cancel := context.WithTimeout(ctx, s.opts.CanCommitTimeout)
	defer cancel()

	agg, err := transport.NewVoteAggregator(cctx, voteSub, expected, func(v transport.Vote) string { return v.InstanceID })
	if err != nil {
		return fmt.Errorf("can_commit agg: %w", err)
	}

	if err := s.tr.CanCommit.Publish(ctx, s.tr.Subjects.RoundCanCommit(rid), transport.CanCommit{
		RoundID:        rid,
		Gen:            gen,
		Target:         m.Version,
		Manifest:       m,
		DeadlineUnixMs: time.Now().Add(s.opts.CanCommitTimeout).UnixMilli(),
	}); err != nil {
		return fmt.Errorf("can_commit publish: %w", err)
	}

	res, err := agg.Wait(cctx)
	if err != nil {
		return fmt.Errorf("can_commit wait: %w", err)
	}

	s.logVoteRejections(rid, "can_commit", res)

	return checkVotes("can_commit", res, expected)
}

// runPhase2 drives PreCommit (stage). Semantics mirror runPhase1: every
// expected player must stage successfully.
func (s *Soloist) runPhase2(ctx context.Context, rid string, gen int64, m maestro.Manifest, expected []string, stageTimeout time.Duration) error {
	stagedSub := goflux.BindSubscriber(s.tr.Staged.Subscriber, s.tr.Subjects.RoundStaged(rid))

	cctx, cancel := context.WithTimeout(ctx, stageTimeout)
	defer cancel()

	agg, err := transport.NewStagedAggregator(cctx, stagedSub, expected, func(v transport.Staged) string { return v.InstanceID })
	if err != nil {
		return fmt.Errorf("pre_commit agg: %w", err)
	}

	if err := s.tr.PreCommit.Publish(ctx, s.tr.Subjects.RoundPreCommit(rid), transport.PreCommit{
		RoundID:        rid,
		Gen:            gen,
		Target:         m.Version,
		DeadlineUnixMs: time.Now().Add(stageTimeout).UnixMilli(),
	}); err != nil {
		return fmt.Errorf("pre_commit publish: %w", err)
	}

	res, err := agg.Wait(cctx)
	if err != nil {
		return fmt.Errorf("pre_commit wait: %w", err)
	}

	s.logStagedRejections(rid, "pre_commit", res)

	return checkStaged("pre_commit", res, expected)
}

func (s *Soloist) runPhase3(ctx context.Context, rid string, gen int64, m maestro.Manifest, expected []string) error {
	commitSub := goflux.BindSubscriber(s.tr.Committed.Subscriber, s.tr.Subjects.RoundCommitted(rid))

	cctx, cancel := context.WithTimeout(ctx, s.opts.DoCommitTimeout)
	defer cancel()

	agg, err := transport.NewCommittedAggregator(cctx, commitSub, expected, func(v transport.Committed) string { return v.InstanceID })
	if err != nil {
		return fmt.Errorf("do_commit agg: %w", err)
	}

	if err := s.tr.DoCommit.Publish(ctx, s.tr.Subjects.RoundDoCommit(rid), transport.DoCommit{
		RoundID: rid,
		Gen:     gen,
		Target:  m.Version,
	}); err != nil {
		return fmt.Errorf("do_commit publish: %w", err)
	}

	res, err := agg.Wait(cctx)
	if err != nil {
		return fmt.Errorf("do_commit wait: %w", err)
	}

	// Partial-DoCommit: false -> mark dirty, but don't abort.
	for id, ok := range res {
		if !ok.OK {
			s.l.Warn("dirty player",
				zap.String("rid", rid),
				zap.String("player", id),
				zap.String("err", ok.Err),
			)
			s.roster.MarkDirty(id)
		}
	}

	return nil
}

func (s *Soloist) logVoteRejections(rid, phase string, res map[string]transport.Vote) {
	for id, v := range res {
		if !v.OK {
			s.l.Warn("phase rejection",
				zap.String("rid", rid),
				zap.String("phase", phase),
				zap.String("player", id),
				zap.String("err", v.Err),
			)
		}
	}
}

func (s *Soloist) logStagedRejections(rid, phase string, res map[string]transport.Staged) {
	for id, v := range res {
		if !v.OK {
			s.l.Warn("phase rejection",
				zap.String("rid", rid),
				zap.String("phase", phase),
				zap.String("player", id),
				zap.String("err", v.Err),
			)
		}
	}
}

func checkVotes(phase string, res map[string]transport.Vote, expected []string) error {
	if len(res) < len(expected) {
		return fmt.Errorf("%s incomplete: %d/%d", phase, len(res), len(expected))
	}

	for id, v := range res {
		if !v.OK {
			return fmt.Errorf("%s rejected by %s: %w", phase, id, maestro.ErrAbort)
		}
	}

	return nil
}

func checkStaged(phase string, res map[string]transport.Staged, expected []string) error {
	if len(res) < len(expected) {
		return fmt.Errorf("%s incomplete: %d/%d", phase, len(res), len(expected))
	}

	for id, v := range res {
		if !v.OK {
			return fmt.Errorf("%s rejected by %s: %w", phase, id, maestro.ErrAbort)
		}
	}

	return nil
}

func (s *Soloist) publishAbort(ctx context.Context, rid string, gen int64, reason string) {
	s.l.Warn("abort", zap.String("rid", rid), zap.Int64("gen", gen), zap.String("reason", reason))

	pubCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.opts.CanCommitTimeout)
	defer cancel()

	_ = s.tr.Abort.Publish(pubCtx, s.tr.Subjects.RoundAbort(rid), transport.Abort{
		RoundID: rid, Gen: gen, Reason: reason,
	})
}

// tryResync issues a 3PC round for stale players, debounced by ResyncDebounce.
func (s *Soloist) tryResync(ctx context.Context) {
	curVer := s.Current()
	if curVer == "" {
		return
	}

	stale := s.roster.StaleAgainst(curVer)
	if len(stale) == 0 {
		return
	}

	last := time.UnixMilli(s.lastResync.Load())
	if time.Since(last) < s.opts.ResyncDebounce {
		s.l.Debug("resync debounced", zap.Duration("since_last", time.Since(last)))
		return
	}

	if !s.mu.TryLock() {
		s.l.Debug("resync skipped: round busy")
		return
	}
	defer s.mu.Unlock()

	s.lastResync.Store(time.Now().UnixMilli())

	man := s.currentMan.Load()
	if man == nil {
		return
	}

	s.l.Info("resync triggered",
		zap.String("target", string(curVer)),
		zap.Int("stale", len(stale)),
		zap.Strings("players", stale),
	)

	if err := s.runRound(ctx, *man, stale); err != nil {
		s.l.Warn("resync round failed", zap.Error(err))
	}
}

func (s *Soloist) subscribeHeartbeats(ctx context.Context) error {
	return s.tr.Heartbeat.Subscribe(ctx, s.tr.Subjects.PlayerHeartbeat(),
		func(_ context.Context, msg goflux.Message[transport.Heartbeat]) error {
			if msg.Payload.Leaving {
				s.roster.Remove(msg.Payload.InstanceID)
				return nil
			}

			s.roster.Observe(msg.Payload)
			return nil
		})
}

func (s *Soloist) runMonitor(ctx context.Context) error {
	monitor := time.NewTicker(s.opts.RosterScanTick)
	defer monitor.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-monitor.C:
			s.tryResync(ctx)
		}
	}
}
