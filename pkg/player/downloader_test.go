package player //nolint:testpackage // tests cover unexported downloader internals

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/foomo/maestro"
	"github.com/foomo/maestro/pkg/blobstore"
	"github.com/foomo/maestro/pkg/blobstore/localfs"
	"github.com/foomo/maestro/pkg/soloist"
)

func TestDownloaderPrefetchAndOpen(t *testing.T) {
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	m, err := soloist.IngestFiles(context.Background(), bs, []soloist.File{
		{Name: "a", Reader: strings.NewReader("alpha")},
		{Name: "b", Reader: strings.NewReader("beta")},
	})
	if err != nil {
		t.Fatal(err)
	}

	d := newDownloader(bs, m.Version, m, 2, nil)
	if err := d.prefetch(context.Background()); err != nil {
		t.Fatal(err)
	}

	r, err := d.openFile("a")
	if err != nil {
		t.Fatal(err)
	}

	body, _ := io.ReadAll(r)
	r.Close()

	if string(body) != "alpha" {
		t.Errorf("body %q", body)
	}

	if got := d.list(); len(got) != 2 {
		t.Errorf("list: %v", got)
	}

	_ = maestro.Version("")
}

// flakyReader wraps a real BlobReader and forces the first failN calls to
// fail with a transient error before delegating to the underlying reader.
type flakyReader struct {
	inner blobstore.BlobReader
	failN int32
	calls atomic.Int32
}

func (f *flakyReader) Reader(ctx context.Context, v maestro.Version, name string) (io.ReadCloser, int64, error) {
	n := f.calls.Add(1)
	if n <= f.failN {
		return nil, 0, errors.New("transient: simulated blob fetch failure")
	}

	return f.inner.Reader(ctx, v, name)
}

func TestDownloaderRetriesTransientFailure(t *testing.T) {
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	m, err := soloist.IngestFiles(context.Background(), bs, []soloist.File{
		{Name: "a", Reader: strings.NewReader("alpha")},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Fail the first 2 attempts, succeed on the 3rd. Retry budget is 3, so the
	// download should land on the final attempt.
	flaky := &flakyReader{inner: bs, failN: 2}

	d := newDownloader(flaky, m.Version, m, 1, nil)
	if err := d.prefetch(context.Background()); err != nil {
		t.Fatalf("prefetch returned %v, want nil after retry", err)
	}

	r, err := d.openFile("a")
	if err != nil {
		t.Fatal(err)
	}

	body, _ := io.ReadAll(r)
	r.Close()

	if string(body) != "alpha" {
		t.Errorf("body %q, want alpha", body)
	}

	if got := flaky.calls.Load(); got != 3 {
		t.Errorf("Reader calls = %d, want 3 (2 failures + 1 success)", got)
	}
}

func TestDownloaderDetectsHashMismatch(t *testing.T) {
	bs, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})

	m, err := soloist.IngestFile(context.Background(), bs, "a", strings.NewReader("alpha"))
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt the manifest hash to force a mismatch on read.
	m.Files[0].Hash = "deadbeef"

	d := newDownloader(bs, m.Version, m, 1, nil)
	if err := d.prefetch(context.Background()); err == nil {
		t.Fatal("expected hash mismatch")
	}
}
