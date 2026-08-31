# The 3PC Protocol

This describes what happens on the wire for a `Publish` call, and what
happens when it doesn't go smoothly. Read this if you're deciding timeouts,
debugging a stuck round, or reasoning about failure modes.

## Roles and identity

- **Soloist** — one process, `InstanceID` arbitrary (not part of the
  protocol's identity scheme). Owns `Publish`, drives every round.
- **Player** — N processes, each with a caller-supplied `InstanceID`
  (`soloist.Options.InstanceID` / `player.Options.InstanceID`). This is the
  key the Soloist's [`Roster`](#the-roster) tracks players by across
  restarts — use something stable, like the pod hostname.

## Sequence

```mermaid
sequenceDiagram
  participant S as Soloist
  participant B as BlobStore
  participant P as Player(s)
  Note over P, S: Heartbeat (every HeartbeatPeriod)
  P ->> S: Heartbeat{iid, currentVersion}
  Note over S: Publish(files) →<br/>compute Version → write blobs
  S ->> B: Writer + Finalize(v, manifest)
  Note over S, P: Phase 1 — CanCommit
  S ->> P: CanCommit{rid, target, manifest}
  P ->> P: Validate manifest, stash pending
  P -->> S: Vote{ok}
  Note over S, P: Phase 2 — PreCommit (download + stage)
  S ->> P: PreCommit{rid, target}
  P ->> B: GET each blob (parallel, DownloadConcurrency)
  P ->> P: StageHandler.Stage(v, manifest, src)
  P -->> S: Staged{ok}
  Note over S, P: Phase 3 — DoCommit (atomic activation)
  S ->> P: DoCommit{rid, target}
  P ->> P: StageHandler.Activate(v)
  P -->> S: Committed{ok}
  Note over S: setCurrent(v)
```

`Publish` builds the `Manifest` via `IngestFiles` (writes every file to the
`BlobStore`, computes the content-addressed `Version`, calls `Finalize`)
*before* the round starts — by the time any player sees `CanCommit`, the
bytes are already durably staged in the store and safe to fetch. Read the
`Manifest`/`Version` model in [BlobStore](/guide/blobstore#manifest-and-version).

## Phase timeouts

Each phase runs under its own deadline (`soloist.Options`):

| Phase | Option | Behavior on timeout/failure |
|---|---|---|
| 1 — CanCommit | `CanCommitTimeout` | Round aborts. `Publish` returns error. |
| 2 — PreCommit | `StageTimeout(totalSize)` | Round aborts. `Publish` returns error. |
| 3 — DoCommit | `DoCommitTimeout` | **Not aborted.** Slow/missing repliers are marked dirty for the next resync. `Publish` still returns an error to the caller (do_commit timeout), but players that *did* commit keep the new version. |

`StageTimeout` defaults to a function of manifest size (`~2×` at an assumed
10 MiB/s floor, clamped to `[60s, 30m]`) rather than a fixed duration —
large manifests get proportionally longer to download and stage. Override
it if your `StageHandler.Stage` does expensive decoding beyond the
download itself.

The asymmetry between phase 1/2 and phase 3 is deliberate: phases 1–2 are
reversible (nothing has been activated yet, `Abort` just discards staged
state), but phase 3 is a one-way door — a player that already ran
`Activate` cannot be told to un-activate. Rather than invent a rollback,
the protocol accepts the player is now ahead and lets the roster's
[resync](#resync) mechanism catch up any stragglers instead.

## Abort path

If any player's `Vote.OK` is `false` in phase 1, or any `Staged.OK` is
`false` in phase 2, the Soloist publishes `Abort{rid, reason}` and stops
the round — it never proceeds to the next phase for anyone. Every player
that had stashed a pending manifest or staged artifact for that round
calls `StageHandler.Abort(v)` to discard it. `Abort` is expected to be a
no-op for a version that never actually staged; the callback should treat
"nothing to discard" as success, not an error.

A player is free to reject in phase 1 (e.g. `Manifest.Validate()` fails,
or its `StageHandler` has business reasons to refuse) or in phase 2 (e.g.
download failure, decode error in `Stage`). Either way the *whole cluster*
stays on the prior version — there's no partial rollout to reason about.

## Silent commit

If the Soloist's roster is empty when `Publish` is called (no players have
heartbeated within `HeartbeatWindow`), it skips the 3PC round entirely and
sets `currentVersion` directly. This is the common case for the very first
publish, before any player has started. Players that join later pick up
the version through [resync](#resync), not through a round they missed.

An empty roster and a roster of only starting-up players are **not** the
same thing. If players have heartbeated but none is [wired](#the-roster)
yet, `Publish` returns an error instead of committing silently:

```
can_commit: 2 player(s) in the roster, none wired for rounds yet; retry once they finish starting
```

With nobody out there, a silent commit is correct — there is no one to
diverge from. With players present but not yet able to vote, committing
silently would advance the Soloist past a version they never saw and never
declined, so the producer needs to hear about it and retry.

## The roster

The Soloist maintains a `Roster`: a map of `InstanceID → (CurrentVersion,
LastSeen, Wired)` built from incoming `Heartbeat` messages, published by
every Player every `HeartbeatPeriod`. A heartbeat-silent player falls out
of the roster once `HeartbeatWindow` elapses, rather than blocking future
rounds.

Two views of the roster serve different purposes:

- **`Snapshot()`** — every player alive within `HeartbeatWindow`, wired or
  not. This is the liveness view.
- **`Participants()`** — the alive players that are also **wired**, i.e.
  whose round subscriptions the broker has acknowledged. This is what
  computes a round's `expected` set.

The distinction matters because a player heartbeats before it can answer a
round. A Player publishes its first `Heartbeat` as soon as `Start` runs,
but its four round subscriptions become live slightly later; until they
do, it sets `Heartbeat.NotWired` and the Soloist keeps it out of the
`expected` set.

Without that gate, every replica rolling out is briefly a single point of
failure for publishing: it is in the roster, it cannot vote, and strict
unanimity turns its silence into an aborted round for the whole fleet.
Excluding it costs nothing — it has no data to lose — and [resync](#resync)
brings it to the current version as soon as it is wired.

::: tip Why "NotWired" and not "Wired"
The wire field is negated so its zero value means *eligible*. A heartbeat
from a player built before the field existed decodes with it absent, and
must be treated as a full participant rather than silently excluded from
every round.
:::

A player that claims to be wired and then does not answer is a different
matter: that is a genuine protocol failure, indistinguishable from one
that is about to reply, so the round aborts. Rounds stay strictly
unanimous among the players that can actually vote.

## Resync

Every `RosterScanTick`, the Soloist calls `StaleAgainst(currentVersion)` —
any alive **and wired** player whose last-reported `CurrentVersion` doesn't
match gets a fresh, targeted 3PC round (`expected` = just the stale
players), no more often than once per `ResyncDebounce`. Un-wired players
are skipped for the same reason they are left out of any other round: a
resync round is an ordinary round, and targeting a player that cannot vote
would just fail. They are picked up on a later tick, once wired.

This is what heals:

- **A newly-joined or restarted player.** It shows up in the roster on
  `HeartbeatPeriod`, reports its (possibly empty) `CurrentVersion`, and
  once it is wired and that doesn't match `Current()`, gets targeted on
  the next scan tick.
- **A player marked dirty after a phase-3 timeout** (see above) — same
  mechanism, it's just already been flagged.

Resync uses the *same* `runRound` as `Publish` — from a player's
perspective there is no difference between an original round and a resync
round targeting it.

## Idempotency

Both `CanCommit` and `PreCommit`/`DoCommit` handlers short-circuit if the
player's `CurrentVersion()` already equals the round's `Target` — it votes
`OK` immediately without re-validating, re-downloading, or re-activating.
This makes resync cheap to over-trigger and makes duplicate delivery (NATS
at-least-once semantics under reconnects) harmless.

## What's not covered

- **No rollback.** Forward-only — a bad publish is fixed by the next
  `Publish`, not by reverting.
- **No leader election.** The Soloist is assumed `replicas=1`; if the pod
  restarts, `currentVersion` and the roster reset to empty in memory (see
  [Operational notes in the README](https://github.com/foomo/maestro#operational-notes)).
- **No cross-round ordering guarantee beyond "current wins."** If you call
  `Publish` again before a round finishes, the new round targets the
  latest roster snapshot; there is a round mutex (`Soloist.mu`) so rounds
  don't interleave, but nothing queues concurrent `Publish` callers beyond
  blocking on that mutex.
