package semconv

import (
	"go.opentelemetry.io/otel/attribute"
)

// ErrorTypeKey re-exports the OTel error.type attribute key for convenience.
// Per OTel semconv: https://opentelemetry.io/docs/specs/semconv/errors/
var ErrorTypeKey = attribute.Key("error.type")

const (
	AttrPublishOutcome = attribute.Key("maestro.publish.outcome")
	AttrPublishPhase   = attribute.Key("maestro.publish.phase")
	AttrStageOutcome   = attribute.Key("maestro.stage.outcome")
	AttrBlobBackend    = attribute.Key("maestro.blobstore.backend")
	AttrBlobOp         = attribute.Key("maestro.blobstore.op")
	AttrVersion        = attribute.Key("maestro.version")
	AttrRoundID        = attribute.Key("maestro.round.id")
	AttrInstanceID     = attribute.Key("maestro.instance.id")
	AttrGen            = attribute.Key("maestro.gen")
)

const (
	PublishOutcomeSuccess           = "success"
	PublishOutcomeAbortVoteNo       = "abort_vote_no"
	PublishOutcomeAbortVoteTimeout  = "abort_vote_timeout"
	PublishOutcomeAbortStageFailed  = "abort_stage_failed"
	PublishOutcomeAbortStageTimeout = "abort_stage_timeout"
	PublishOutcomeNoPlayersSilent   = "no_players_silent"

	PublishPhaseIngest    = "ingest"
	PublishPhaseCanCommit = "can_commit"
	PublishPhasePreCommit = "pre_commit"
	PublishPhaseDoCommit  = "do_commit"
	PublishPhaseTotal     = "total"

	StageOutcomeCommitted     = "committed"
	StageOutcomeVoteFailed    = "vote_failed"
	StageOutcomeStageFailed   = "stage_failed"
	StageOutcomeAbortReceived = "abort_received"
	StageOutcomeGenStale      = "gen_stale"

	BlobBackendLocalfs = "localfs"
	BlobBackendS3      = "s3"

	BlobOpWriter   = "writer"
	BlobOpFinalize = "finalize"
	BlobOpReader   = "reader"
	BlobOpStat     = "stat"
	BlobOpDelete   = "delete"
)
