package localfs_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/foomo/maestro"
	"github.com/foomo/maestro/pkg/blobstore/localfs"
)

const helloHash = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

func newStore(t *testing.T) *localfs.Store {
	t.Helper()

	s, err := localfs.NewStore(localfs.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}

	return s
}

func TestWriteFinalizeReadStat(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	v := maestro.Version("v1")

	w, err := s.Writer(ctx, v, "a.dat")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := io.WriteString(w, "hello"); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	m := maestro.Manifest{
		Version: v, TotalSize: 5,
		Files: []maestro.ManifestFile{{Name: "a.dat", Hash: helloHash, Size: 5}},
	}
	if err := s.Finalize(ctx, v, m); err != nil {
		t.Fatal(err)
	}

	r, size, err := s.Reader(ctx, v, "a.dat")
	if err != nil {
		t.Fatal(err)
	}

	if size != 5 {
		t.Errorf("size: got %d want 5", size)
	}

	body, _ := io.ReadAll(r)
	r.Close()

	if string(body) != "hello" {
		t.Errorf("body: %q", body)
	}

	sha, sz, err := s.Stat(ctx, v, "a.dat")
	if err != nil {
		t.Fatal(err)
	}

	if sz != 5 || !strings.HasPrefix(sha, "2cf24dba") {
		t.Errorf("stat: sha=%s sz=%d", sha, sz)
	}
}

func TestStagingDifferentFromFinalVersion(t *testing.T) {
	// Caller writes under staging label "staging-1" and finalizes manifest with
	// Version="final-1". Reader should be able to fetch "a.dat" under "final-1".
	s := newStore(t)
	ctx := context.Background()
	staging := maestro.Version("staging-1")
	final := maestro.Version("final-1")

	w, _ := s.Writer(ctx, staging, "a.dat")
	if _, err := io.WriteString(w, "hello"); err != nil {
		t.Fatal(err)
	}

	w.Close()

	m := maestro.Manifest{
		Version: final, TotalSize: 5,
		Files: []maestro.ManifestFile{{Name: "a.dat", Hash: helloHash, Size: 5}},
	}
	if err := s.Finalize(ctx, staging, m); err != nil {
		t.Fatal(err)
	}

	r, _, err := s.Reader(ctx, final, "a.dat")
	if err != nil {
		t.Fatalf("reader on final: %v", err)
	}

	r.Close()

	if _, _, err := s.Reader(ctx, staging, "a.dat"); err == nil {
		t.Fatal("staging label should not be readable after Finalize")
	}
}

func TestReadBeforeFinalizeFails(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	w, _ := s.Writer(ctx, "v1", "a.dat")
	if _, err := io.WriteString(w, "x"); err != nil {
		t.Fatal(err)
	}

	w.Close()

	_, _, err := s.Reader(ctx, "v1", "a.dat")
	if err == nil {
		t.Fatal("expected error: not finalized")
	}
}

func TestDelete(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	v := maestro.Version("v1")

	w, _ := s.Writer(ctx, v, "a.dat")
	if _, err := io.WriteString(w, "hello"); err != nil {
		t.Fatal(err)
	}

	w.Close()

	m := maestro.Manifest{Version: v, TotalSize: 5, Files: []maestro.ManifestFile{{Name: "a.dat", Hash: helloHash, Size: 5}}}
	if err := s.Finalize(ctx, v, m); err != nil {
		t.Fatal(err)
	}

	if err := s.Delete(ctx, v); err != nil {
		t.Fatal(err)
	}

	if _, _, err := s.Reader(ctx, v, "a.dat"); err == nil {
		t.Fatal("expected reader to fail after delete")
	}
}

func TestFinalizeIdempotent(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	v := maestro.Version("v1")

	w, _ := s.Writer(ctx, v, "a.dat")
	if _, err := io.WriteString(w, "hello"); err != nil {
		t.Fatal(err)
	}

	w.Close()

	m := maestro.Manifest{Version: v, TotalSize: 5, Files: []maestro.ManifestFile{{Name: "a.dat", Hash: helloHash, Size: 5}}}
	if err := s.Finalize(ctx, v, m); err != nil {
		t.Fatal(err)
	}
	// Second Finalize with same v + m must not error.
	if err := s.Finalize(ctx, v, m); err != nil {
		t.Fatalf("second finalize: %v", err)
	}
}
