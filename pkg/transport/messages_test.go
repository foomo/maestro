package transport_test

import (
	"reflect"
	"testing"

	"github.com/foomo/maestro"
	"github.com/foomo/maestro/pkg/transport"
)

func TestCanCommitRoundTrip(t *testing.T) {
	in := transport.CanCommit{
		RoundID: "rid1",
		Gen:     12345,
		Target:  maestro.Version("v1"),
		Manifest: maestro.Manifest{
			Version: "v1", TotalSize: 4,
			Files: []maestro.ManifestFile{{Name: "a", Hash: "h", Size: 4}},
		},
		DeadlineUnixMs: 1700000000000,
	}

	b, err := transport.CanCommitCodec.Encode(in)
	if err != nil {
		t.Fatal(err)
	}

	var out transport.CanCommit
	if err := transport.CanCommitCodec.Decode(b, &out); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(in, out) {
		t.Errorf("mismatch:\n in=%#v\nout=%#v", in, out)
	}
}

func TestVoteRoundTrip(t *testing.T) {
	in := transport.Vote{RoundID: "r", InstanceID: "p1", OK: false, Err: "no"}

	b, err := transport.VoteCodec.Encode(in)
	if err != nil {
		t.Fatal(err)
	}

	var out transport.Vote
	if err := transport.VoteCodec.Decode(b, &out); err != nil {
		t.Fatal(err)
	}

	if in != out {
		t.Errorf("mismatch: %#v vs %#v", in, out)
	}
}

func TestHeartbeatRoundTrip(t *testing.T) {
	in := transport.Heartbeat{
		InstanceID:     "p1",
		GenAcked:       42,
		CurrentVersion: "v1",
	}

	b, err := transport.HeartbeatCodec.Encode(in)
	if err != nil {
		t.Fatal(err)
	}

	var out transport.Heartbeat
	if err := transport.HeartbeatCodec.Decode(b, &out); err != nil {
		t.Fatal(err)
	}

	if in != out {
		t.Error("mismatch")
	}
}
