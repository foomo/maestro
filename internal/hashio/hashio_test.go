package hashio_test

import (
	"bytes"
	"testing"

	"github.com/foomo/maestro/internal/hashio"
)

func TestTeeHashEmpty(t *testing.T) {
	var sink bytes.Buffer

	w := hashio.NewWriter(&sink)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if w.Sum() != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Errorf("unexpected empty hash: %s", w.Sum())
	}
}

func TestTeeHashWritesThrough(t *testing.T) {
	var sink bytes.Buffer

	w := hashio.NewWriter(&sink)
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}

	w.Close()

	if sink.String() != "hello" {
		t.Errorf("data not written: %q", sink.String())
	}
}
