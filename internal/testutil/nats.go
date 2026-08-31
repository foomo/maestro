package testutil

import (
	"testing"
	"time"

	gonet "github.com/foomo/go/net"
	natsd "github.com/nats-io/nats-server/v2/server"
)

// StartNATS starts an in-process NATS server bound to a free loopback port.
// Returns the client URL. Server stops on test cleanup.
func StartNATS(tb testing.TB) string {
	tb.Helper()

	port, err := gonet.FreePort(tb.Context())
	if err != nil {
		tb.Fatalf("FreePort: %v", err)
	}

	opts := &natsd.Options{
		Host:   "127.0.0.1",
		Port:   port,
		NoLog:  true,
		NoSigs: true,
	}

	s, err := natsd.NewServer(opts)
	if err != nil {
		tb.Fatalf("NewServer: %v", err)
	}

	go s.Start()

	if !s.ReadyForConnections(2 * time.Second) {
		tb.Fatal("nats server not ready")
	}

	tb.Cleanup(s.Shutdown)

	return s.ClientURL()
}
