package semconv_test

import (
	"testing"

	"github.com/foomo/maestro/pkg/semconv"
)

func TestAttrKeysAreNamespaced(t *testing.T) {
	keys := []string{
		string(semconv.AttrPublishOutcome),
		string(semconv.AttrPublishPhase),
		string(semconv.AttrStageOutcome),
		string(semconv.AttrBlobBackend),
		string(semconv.AttrBlobOp),
		string(semconv.AttrVersion),
		string(semconv.AttrRoundID),
		string(semconv.AttrInstanceID),
		string(semconv.AttrGen),
	}
	for _, k := range keys {
		if len(k) < len("maestro.") || k[:len("maestro.")] != "maestro." {
			t.Errorf("attr key %q must be under maestro.* namespace", k)
		}
	}
}

func TestPublishOutcomeEnums(t *testing.T) {
	for _, v := range []string{
		semconv.PublishOutcomeSuccess,
		semconv.PublishOutcomeAbortVoteNo,
		semconv.PublishOutcomeAbortVoteTimeout,
		semconv.PublishOutcomeAbortStageFailed,
		semconv.PublishOutcomeAbortStageTimeout,
		semconv.PublishOutcomeNoPlayersSilent,
	} {
		if v == "" {
			t.Error("empty PublishOutcome enum value")
		}
	}
}

func TestErrorTypeKeyReExport(t *testing.T) {
	if semconv.ErrorTypeKey == "" {
		t.Error("ErrorTypeKey must re-export otel/semconv ErrorTypeKey")
	}
}
