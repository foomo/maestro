package localfs_test

import (
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/foomo/maestro"
	"github.com/foomo/maestro/pkg/blobstore/localfs"
	"github.com/stretchr/testify/require"
)

const helloWorldHash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

func TestServerClientRoundTrip(t *testing.T) {
	s, _ := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})
	ctx := context.Background()

	w, _ := s.Writer(ctx, "v1", "a.dat")
	if _, err := io.WriteString(w, "hello world"); err != nil {
		t.Fatal(err)
	}

	w.Close()

	if err := s.Finalize(ctx, "v1", maestro.Manifest{
		Version: "v1", TotalSize: 11,
		Files: []maestro.ManifestFile{{Name: "a.dat", Hash: helloWorldHash, Size: 11}},
	}); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	c := localfs.NewClient(srv.URL)

	r, size, err := c.Reader(ctx, "v1", "a.dat")
	require.NoError(t, err)

	defer r.Close()

	if size != 11 {
		t.Errorf("size %d", size)
	}

	body, _ := io.ReadAll(r)
	if string(body) != "hello world" {
		t.Errorf("body %q", body)
	}
}
