package transport_test

import (
	"context"
	"testing"
	"time"

	"github.com/foomo/goflux"
	"github.com/foomo/maestro/internal/testutil"
	"github.com/foomo/maestro/pkg/transport"
	"github.com/nats-io/nats.go"
)

func TestPubSubRoundTrip(t *testing.T) {
	url := testutil.StartNATS(t)

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	tr := transport.NewTransport(nc)
	pl := transport.NewTransport(nc)

	subj := "test.subj"
	pub := goflux.BindPublisher(pl.Vote.Publisher, subj)
	sub := goflux.BindSubscriber(tr.Vote.Subscriber, subj)

	got := make(chan transport.Vote, 1)
	ctx := t.Context()

	go func() {
		_ = sub.Subscribe(ctx, func(_ context.Context, v goflux.Message[transport.Vote]) error {
			got <- v.Payload
			return nil
		})
	}()

	time.Sleep(150 * time.Millisecond)

	want := transport.Vote{RoundID: "r", InstanceID: "p", OK: true}
	if err := pub.Publish(ctx, want); err != nil {
		t.Fatal(err)
	}

	select {
	case v := <-got:
		if v != want {
			t.Errorf("got %#v want %#v", v, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}
