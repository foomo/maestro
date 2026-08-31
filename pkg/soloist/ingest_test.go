package soloist_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/foomo/maestro/pkg/blobstore/localfs"
	"github.com/foomo/maestro/pkg/soloist"
)

func TestIngestSingleFile(t *testing.T) {
	bs, err := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}

	mn, err := soloist.IngestFile(context.Background(), bs, "data.bin", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}

	if mn.Version == "" || len(mn.Files) != 1 || mn.Files[0].Size != 5 {
		t.Fatalf("manifest: %+v", mn)
	}

	r, _, err := bs.Reader(context.Background(), mn.Version, "data.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	body, _ := io.ReadAll(r)
	if string(body) != "hello" {
		t.Errorf("body %q", body)
	}
}

func TestIngestIsContentAddressed(t *testing.T) {
	bs1, err := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}

	bs2, err := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}

	m1, err := soloist.IngestFile(context.Background(), bs1, "x", strings.NewReader("hello world"))
	if err != nil {
		t.Fatal(err)
	}

	m2, err := soloist.IngestFile(context.Background(), bs2, "x", strings.NewReader("hello world"))
	if err != nil {
		t.Fatal(err)
	}

	if m1.Version != m2.Version {
		t.Errorf("identical content -> different versions: %q vs %q", m1.Version, m2.Version)
	}
}
