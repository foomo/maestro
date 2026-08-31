// Package player implements the read-side of the maestro 3PC protocol.
// A Player subscribes to round broadcasts from the Soloist, drives the
// CanCommit → PreCommit → DoCommit state machine, and tracks the active
// version. Artifact lifecycle lives entirely inside the user-provided
// StageHandler implementation.
package player

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/foomo/goflux"
	"github.com/foomo/gofuncy"
	"github.com/foomo/maestro"
	"github.com/foomo/maestro/pkg/transport"
	"go.uber.org/zap"
)

// Player is one of N readers in a maestro deployment.
type Player struct {
	opts    Options
	tr      transport.Transport
	l       *zap.Logger
	active  atomic.Pointer[activeSlot]
	staged  atomic.Pointer[stagedSlot]
	pending sync.Map // rid string -> maestro.Manifest
	ready   atomic.Bool
	wired   atomic.Bool

	// subsLive counts the round subscriptions the broker has acknowledged.
	// Wired only reports true once all of roundSubscriptions are live, which
	// is what the heartbeat advertises to the soloist's roster.
	subsLive atomic.Int32

	cancel        context.CancelFunc
	done          chan struct{}
	leaving       chan struct{}
	heartbeatDone chan struct{}
	started       atomic.Bool
	closeOnce     sync.Once
}

// ErrAlreadyStarted is returned when Start is called more than once.
var ErrAlreadyStarted = errors.New("player: Start already called")

type activeSlot struct {
	version maestro.Version
}

type stagedSlot struct {
	rid     string
	version maestro.Version
}

// New constructs a Player. The transport is supplied via opts.Transport — the
// caller owns the underlying *nats.Conn.
func New(opts Options) (*Player, error) {
	opts.applyDefaults()

	if opts.StageHandler == nil {
		return nil, errors.New("player: StageHandler required")
	}

	if opts.BlobReader == nil {
		return nil, errors.New("player: BlobReader required")
	}

	if opts.Transport.Heartbeat.Publisher == nil {
		return nil, errors.New("player: Transport required (use transport.NewTransport)")
	}

	return &Player{
		opts: opts,
		tr:   opts.Transport,
		l:    opts.Logger.Named("maestro.player").With(zap.String("iid", opts.InstanceID)),
	}, nil
}

// ------------------------------------------------------------------------------------------------
// ~ Public methods
// ------------------------------------------------------------------------------------------------

// Start drives the player. Blocks until ctx cancels or Close is called.
// Spawns one errgroup that owns the heartbeat publisher and the four phase
// subscribers — if any subscriber fails at subscribe time the group cancels
// and Start returns the underlying error. Calling Start more than once
// returns ErrAlreadyStarted.
func (p *Player) Start(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}

	runCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.done = make(chan struct{})
	p.leaving = make(chan struct{})
	p.heartbeatDone = make(chan struct{})

	defer close(p.done)

	p.l.Info("player running", zap.Duration("heartbeat_period", p.opts.HeartbeatPeriod))

	g := gofuncy.NewGroup(runCtx,
		gofuncy.WithName("maestro.player"),
		gofuncy.WithFailFast(),
		gofuncy.WithoutTracing(),
	)

	g.Add(p.runHeartbeat)
	g.Add(p.subscribeCanCommit)
	g.Add(p.subscribePreCommit)
	g.Add(p.subscribeDoCommit)
	g.Add(p.subscribeAbort)

	err := g.Wait()
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}

	return err
}

// Close signals shutdown and blocks until every goroutine spawned by Start
// has exited or ctx expires. Safe to call before Start (no-op) and safe to
// call multiple times.
//
// Before tearing down, it stops the periodic heartbeat and best-effort
// publishes one final heartbeat marked Leaving, so the soloist drops this
// player from its roster immediately instead of waiting out HeartbeatWindow
// — a graceful shutdown then excludes it from the next round's expected set
// rather than needing to be tolerated as a non-responder in one. The
// periodic heartbeat is fully stopped first so it cannot race a scheduled
// tick and re-add the player to the roster right after Leaving lands. A
// crash cannot send this; that residual case is still covered by heartbeat
// expiry.
func (p *Player) Close(ctx context.Context) error {
	p.closeOnce.Do(func() {
		// Gated on started, not on Wired: a player still establishing its
		// round subscriptions has already been heartbeating, so it is in the
		// roster and still needs to announce its departure.
		if p.started.Load() {
			close(p.leaving)
			<-p.heartbeatDone
			p.emitLeaving(ctx)
		}

		if p.cancel != nil {
			p.cancel()
		}
	})

	if p.done == nil {
		return nil
	}

	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CurrentVersion returns the active Version (empty until first commit).
func (p *Player) CurrentVersion() maestro.Version {
	if s := p.active.Load(); s != nil {
		return s.version
	}

	return ""
}

// Ready reports whether at least one DoCommit has succeeded.
func (p *Player) Ready() bool { return p.ready.Load() }

// Wired reports whether every round subscription is established and certain
// to receive broadcasts, i.e. whether this player can actually answer a round.
// It is what the heartbeat advertises to the soloist, which excludes un-wired
// players from a round's expected set instead of aborting on their silence.
func (p *Player) Wired() bool { return p.wired.Load() }

// ------------------------------------------------------------------------------------------------
// ~ Private methods
// ------------------------------------------------------------------------------------------------

func (p *Player) handleCanCommit(ctx context.Context, rid string, cc transport.CanCommit) {
	reply := transport.Vote{RoundID: cc.RoundID, InstanceID: p.opts.InstanceID, OK: true}
	subj := p.tr.Subjects.RoundVote(cc.RoundID)

	p.l.Info("can_commit received",
		zap.String("rid", cc.RoundID),
		zap.String("target", string(cc.Target)),
	)

	if err := cc.Manifest.Validate(); err != nil {
		p.l.Warn("manifest invalid", zap.String("rid", rid), zap.Error(err))

		reply.OK = false
		reply.Err = err.Error()
		_ = p.tr.Vote.Publish(ctx, subj, reply)

		return
	}

	// Stash manifest before replying so PreCommit can retrieve it.
	p.setPending(rid, cc.Manifest)

	// Idempotent short-circuit: target == current → already committed
	if cc.Target == p.CurrentVersion() {
		p.l.Debug("can_commit idempotent: already at target",
			zap.String("rid", rid),
			zap.String("target", string(cc.Target)),
		)
		_ = p.tr.Vote.Publish(ctx, subj, reply)

		return
	}

	_ = p.tr.Vote.Publish(ctx, subj, reply)
}

func (p *Player) handlePreCommit(ctx context.Context, pc transport.PreCommit) {
	reply := transport.Staged{RoundID: pc.RoundID, InstanceID: p.opts.InstanceID, OK: true}
	subj := p.tr.Subjects.RoundStaged(pc.RoundID)

	p.l.Info("pre_commit received",
		zap.String("rid", pc.RoundID),
		zap.String("target", string(pc.Target)),
	)

	// Idempotent: already active at target
	if pc.Target == p.CurrentVersion() {
		p.l.Debug("pre_commit idempotent: already at target", zap.String("rid", pc.RoundID))
		_ = p.tr.Staged.Publish(ctx, subj, reply)

		return
	}

	man, ok := p.takePending(pc.RoundID)
	if !ok {
		p.l.Warn("pre_commit without pending manifest", zap.String("rid", pc.RoundID))

		reply.OK = false
		reply.Err = "no pending manifest for rid (CanCommit not seen)"
		_ = p.tr.Staged.Publish(ctx, subj, reply)

		return
	}

	d := newDownloader(p.opts.BlobReader, pc.Target, man, p.opts.DownloadConcurrency, p.l)
	if err := d.prefetch(ctx); err != nil {
		p.l.Error("prefetch failed", zap.String("rid", pc.RoundID), zap.Error(err))

		reply.OK = false
		reply.Err = err.Error()
		_ = p.tr.Staged.Publish(ctx, subj, reply)

		return
	}

	if err := p.opts.StageHandler.Stage(ctx, pc.Target, man, &fileSource{d: d}); err != nil {
		p.l.Error("stage failed", zap.String("rid", pc.RoundID), zap.Error(err))

		reply.OK = false
		reply.Err = err.Error()
		_ = p.tr.Staged.Publish(ctx, subj, reply)

		return
	}

	p.staged.Store(&stagedSlot{rid: pc.RoundID, version: pc.Target})

	p.l.Info("staged",
		zap.String("rid", pc.RoundID),
		zap.String("target", string(pc.Target)),
		zap.Int("files", len(man.Files)),
	)

	_ = p.tr.Staged.Publish(ctx, subj, reply)
}

func (p *Player) handleDoCommit(ctx context.Context, dc transport.DoCommit) {
	reply := transport.Committed{RoundID: dc.RoundID, InstanceID: p.opts.InstanceID, OK: true}
	subj := p.tr.Subjects.RoundCommitted(dc.RoundID)

	p.l.Info("do_commit received",
		zap.String("rid", dc.RoundID),
		zap.String("target", string(dc.Target)),
	)

	// Idempotent: already active at target
	if dc.Target == p.CurrentVersion() {
		p.l.Debug("do_commit idempotent: already at target", zap.String("rid", dc.RoundID))
		_ = p.tr.Committed.Publish(ctx, subj, reply)

		return
	}

	staged := p.staged.Load()
	if staged == nil || staged.version != dc.Target {
		p.l.Warn("do_commit without staged",
			zap.String("rid", dc.RoundID),
			zap.String("target", string(dc.Target)),
		)

		reply.OK = false
		reply.Err = "no staged result for target"
		_ = p.tr.Committed.Publish(ctx, subj, reply)

		return
	}

	if err := p.opts.StageHandler.Activate(ctx, staged.version); err != nil {
		p.l.Error("activate failed",
			zap.String("rid", dc.RoundID),
			zap.String("target", string(dc.Target)),
			zap.Error(err),
		)

		reply.OK = false
		reply.Err = err.Error()
		_ = p.tr.Committed.Publish(ctx, subj, reply)

		return
	}

	prev := p.active.Swap(&activeSlot{version: staged.version})
	p.staged.Store(nil)

	var prevVer string
	if prev != nil {
		prevVer = string(prev.version)
	}

	p.ready.Store(true)

	p.l.Info("committed",
		zap.String("rid", dc.RoundID),
		zap.String("target", string(dc.Target)),
		zap.String("prev", prevVer),
	)

	_ = p.tr.Committed.Publish(ctx, subj, reply)
}

func (p *Player) handleAbort(ctx context.Context, ab transport.Abort, rid string) {
	p.l.Warn("abort received",
		zap.String("rid", rid),
		zap.String("reason", ab.Reason),
	)

	staged := p.staged.Load()
	if staged != nil && staged.rid == rid {
		if err := p.opts.StageHandler.Abort(ctx, staged.version); err != nil {
			p.l.Warn("stage handler abort returned error",
				zap.String("rid", rid),
				zap.Error(err),
			)
		}

		p.staged.Store(nil)

		p.l.Info("staged discarded", zap.String("rid", rid))
	}

	p.takePending(rid) // clean up any pending manifest
}

// runHeartbeat publishes a Heartbeat every HeartbeatPeriod with the player's
// current state. Returns nil when ctx cancels or Close signals leaving.
// Per-publish errors are logged inside emitHeartbeat — a single missed
// publish is not player-fatal.
//
// Checked ahead of ctx.Done() and t.C: Close closes p.leaving and waits for
// heartbeatDone before publishing its own final Leaving heartbeat, so this
// loop must stop (and be observed as stopped) before that publish — otherwise
// an already-scheduled regular heartbeat could land right after Leaving and
// re-add the player to the roster.
func (p *Player) runHeartbeat(ctx context.Context) error {
	defer close(p.heartbeatDone)

	t := time.NewTicker(p.opts.HeartbeatPeriod)
	defer t.Stop()
	// Send one immediately so soloist roster picks us up quickly in tests.
	p.emitHeartbeat(ctx)

	for {
		select {
		case <-p.leaving:
			return nil
		case <-ctx.Done():
			return nil
		case <-t.C:
			select {
			case <-p.leaving:
				return nil
			default:
			}

			p.emitHeartbeat(ctx)
		}
	}
}

func (p *Player) emitHeartbeat(ctx context.Context) {
	cur := p.CurrentVersion()

	if err := p.tr.Heartbeat.Publish(ctx, p.tr.Subjects.PlayerHeartbeat(), transport.Heartbeat{
		InstanceID:     p.opts.InstanceID,
		GenAcked:       0,
		CurrentVersion: cur,
		NotWired:       !p.wired.Load(),
	}); err != nil {
		p.l.Warn("heartbeat publish failed", zap.Error(err))
		return
	}
}

// emitLeaving publishes a final Heartbeat{Leaving: true}. Best-effort: a
// failed publish just leaves the soloist to expire this player normally.
func (p *Player) emitLeaving(ctx context.Context) {
	if err := p.tr.Heartbeat.Publish(ctx, p.tr.Subjects.PlayerHeartbeat(), transport.Heartbeat{
		InstanceID:     p.opts.InstanceID,
		CurrentVersion: p.CurrentVersion(),
		Leaving:        true,
	}); err != nil {
		p.l.Warn("leaving heartbeat publish failed", zap.Error(err))
	}
}

// roundSubscriptions is the number of subscribers that must be established
// before this player can answer a round. Wired flips once all of them report
// ready; the heartbeat advertises anything less as NotWired.
const roundSubscriptions = 4

// subscribeCanCommit, subscribePreCommit, subscribeDoCommit, subscribeAbort
// each block until ctx cancels or the underlying NATS subscription fails.
// An errgroup in Start runs all four concurrently so a single subscribe-time
// failure tears the player down instead of silently going deaf to one phase.
//
// Each subscribes via goflux.SubscribeWithReady rather than Subscribe: the
// broker must acknowledge the subscription before this player claims it can
// answer a round. Subscribe alone returns no such signal — it blocks for the
// lifetime of the subscription — so a player that merely called it may still
// miss a CanCommit that is already in flight.

func (p *Player) subscribeCanCommit(ctx context.Context) error {
	p.l.Debug("coordinator subscribing", zap.String("phase", "can_commit"))

	return goflux.SubscribeWithReady(ctx, p.tr.CanCommit.Subscriber, p.tr.Subjects.RoundCanCommitWildcard(),
		func(hctx context.Context, m goflux.Message[transport.CanCommit]) error {
			p.handleCanCommit(hctx, p.tr.Subjects.RIDFromSubject(m.Subject), m.Payload)
			return nil
		}, p.subscriptionLive)
}

func (p *Player) subscribePreCommit(ctx context.Context) error {
	p.l.Debug("coordinator subscribing", zap.String("phase", "pre_commit"))

	return goflux.SubscribeWithReady(ctx, p.tr.PreCommit.Subscriber, p.tr.Subjects.RoundPreCommitWildcard(),
		func(hctx context.Context, m goflux.Message[transport.PreCommit]) error {
			p.handlePreCommit(hctx, m.Payload)
			return nil
		}, p.subscriptionLive)
}

func (p *Player) subscribeDoCommit(ctx context.Context) error {
	p.l.Debug("coordinator subscribing", zap.String("phase", "do_commit"))

	return goflux.SubscribeWithReady(ctx, p.tr.DoCommit.Subscriber, p.tr.Subjects.RoundDoCommitWildcard(),
		func(hctx context.Context, m goflux.Message[transport.DoCommit]) error {
			p.handleDoCommit(hctx, m.Payload)
			return nil
		}, p.subscriptionLive)
}

func (p *Player) subscribeAbort(ctx context.Context) error {
	p.l.Debug("coordinator subscribing", zap.String("phase", "abort"))

	return goflux.SubscribeWithReady(ctx, p.tr.Abort.Subscriber, p.tr.Subjects.RoundAbortWildcard(),
		func(hctx context.Context, m goflux.Message[transport.Abort]) error {
			p.handleAbort(hctx, m.Payload, p.tr.Subjects.RIDFromSubject(m.Subject))
			return nil
		}, p.subscriptionLive)
}

// subscriptionLive records one established round subscription, flipping Wired
// once the last of them lands. Passed as the ready callback to each subscriber,
// so it must not block.
func (p *Player) subscriptionLive() {
	if p.subsLive.Add(1) == roundSubscriptions {
		p.wired.Store(true)
		p.l.Info("round subscriptions established")
	}
}

// setPending stores a manifest keyed by round id (between CanCommit and PreCommit).
func (p *Player) setPending(rid string, man maestro.Manifest) {
	p.pending.Store(rid, man)
}

// takePending retrieves and deletes the pending manifest for a round id.
func (p *Player) takePending(rid string) (maestro.Manifest, bool) {
	v, ok := p.pending.LoadAndDelete(rid)
	if !ok {
		return maestro.Manifest{}, false
	}

	man, ok := v.(maestro.Manifest)

	return man, ok
}
