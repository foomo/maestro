package transport_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/foomo/goflux"
	"github.com/foomo/maestro/internal/testutil"
	"github.com/foomo/maestro/pkg/transport"
	"github.com/nats-io/nats.go"
)

func TestAggregatorCollectsAll(t *testing.T) {
	url := testutil.StartNATS(t)

	nc, _ := nats.Connect(url)
	defer nc.Close()

	tr := transport.NewTransport(nc)
	pl := transport.NewTransport(nc)

	subj := "test.agg"
	pub := goflux.BindPublisher(pl.Vote.Publisher, subj)
	sub := goflux.BindSubscriber(tr.Vote.Subscriber, subj)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	agg, err := transport.NewVoteAggregator(ctx, sub, []string{"p1", "p2"}, func(v transport.Vote) string { return v.InstanceID })
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	_ = pub.Publish(ctx, transport.Vote{InstanceID: "p1", OK: true})
	_ = pub.Publish(ctx, transport.Vote{InstanceID: "p2", OK: true})

	res, err := agg.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(res) != 2 || !res["p1"].OK || !res["p2"].OK {
		t.Errorf("unexpected: %+v", res)
	}
}

func TestAggregatorTimesOutOnMissing(t *testing.T) {
	url := testutil.StartNATS(t)

	nc, _ := nats.Connect(url)
	defer nc.Close()

	tr := transport.NewTransport(nc)
	pl := transport.NewTransport(nc)

	subj := "test.agg2"
	pub := goflux.BindPublisher(pl.Vote.Publisher, subj)
	sub := goflux.BindSubscriber(tr.Vote.Subscriber, subj)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	agg, _ := transport.NewVoteAggregator(ctx, sub, []string{"p1", "p2"}, func(v transport.Vote) string { return v.InstanceID })

	time.Sleep(100 * time.Millisecond)

	_ = pub.Publish(context.Background(), transport.Vote{InstanceID: "p1", OK: true})

	_, err := agg.Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}
