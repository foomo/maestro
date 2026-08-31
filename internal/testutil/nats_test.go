package testutil_test

import (
	"testing"

	"github.com/foomo/maestro/internal/testutil"
	"github.com/nats-io/nats.go"
)

func TestStartNATSConnect(t *testing.T) {
	url := testutil.StartNATS(t)

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	if !nc.IsConnected() {
		t.Fatal("not connected")
	}
}
