// Package localfs is a filesystem-backed
// [github.com/foomo/maestro/pkg/blobstore.BlobStore] /
// [github.com/foomo/maestro/pkg/blobstore.BlobReader] implementation. A
// [Store] stages files under a temporary label, then atomically promotes
// them to a version-addressed directory on Finalize; [Store.Handler]
// serves finalized files over HTTP for remote [Client] consumers.
package localfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	gosec "github.com/foomo/go/sec"

	"github.com/foomo/maestro"
	"github.com/foomo/maestro/pkg/blobstore"
)

var (
	_ blobstore.BlobStore  = (*Store)(nil)
	_ blobstore.BlobReader = (*Store)(nil)
)

// Config configures the localfs BlobStore.
//
//   - DataDir: root directory for staging + versioned files.
type Config struct {
	DataDir string
}

// Store is a BlobStore backed by the local filesystem.
type Store struct {
	cfg Config
}

// NewStore constructs a Store, creating DataDir if missing.
func NewStore(cfg Config) (*Store, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("localfs: DataDir required")
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}

	return &Store{cfg: cfg}, nil
}

// Writer opens a file under the staging area for (v, name). name is validated
// against path traversal via foomo/go/sec.
func (s *Store) Writer(ctx context.Context, v maestro.Version, name string) (io.WriteCloser, error) {
	if _, err := gosec.Filename(s.stagingDir(v), name); err != nil {
		return nil, fmt.Errorf("%w: %w", maestro.ErrUnsafeName, err)
	}

	full := filepath.Join(s.stagingDir(v), name)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return nil, err
	}

	return os.Create(full)
}

// Finalize promotes the staging directory at v to the version directory at
// m.Version, then writes the manifest file. m.Version is the visibility marker
// for the destination; v is the staging label. They may differ.
func (s *Store) Finalize(ctx context.Context, v maestro.Version, m maestro.Manifest) error {
	if err := m.Validate(); err != nil {
		return err
	}

	dst := s.versionDir(m.Version)
	manPath := s.manifestPath(m.Version)
	// Idempotent: if destination already exists with a manifest, succeed.
	if _, err := os.Stat(manPath); err == nil {
		// Clean any leftover staging.
		_ = os.RemoveAll(s.stagingDir(v))
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	src := s.stagingDir(v)
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("localfs.Finalize: staging dir %q: %w", src, err)
	}

	if v != m.Version {
		// Different label -> rename to destination.
		if err := os.Rename(src, dst); err != nil {
			return err
		}
	} else {
		// Same label -> create destination if absent, then move contents.
		if _, err := os.Stat(dst); errors.Is(err, os.ErrNotExist) {
			if err := os.Rename(src, dst); err != nil {
				return err
			}
		}
	}
	// Manifest written last as the visibility barrier.
	tmp := manPath + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	if _, err := f.Write(encodeManifest(m)); err != nil {
		f.Close()
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmp, manPath)
}

// Reader streams (v, name) once v has been Finalize'd. Errors if manifest
// missing.
func (s *Store) Reader(ctx context.Context, v maestro.Version, name string) (io.ReadCloser, int64, error) {
	if _, err := os.Stat(s.manifestPath(v)); err != nil {
		return nil, 0, fmt.Errorf("version %q not finalized: %w", v, err)
	}

	if _, err := gosec.Filename(s.versionDir(v), name); err != nil {
		return nil, 0, fmt.Errorf("%w: %w", maestro.ErrUnsafeName, err)
	}

	full := filepath.Join(s.versionDir(v), name)

	fi, err := os.Stat(full)
	if err != nil {
		return nil, 0, err
	}

	f, err := os.Open(full)
	if err != nil {
		return nil, 0, err
	}

	return f, fi.Size(), nil
}

// Stat returns the sha256 + size of (v, name) by streaming the file once.
func (s *Store) Stat(ctx context.Context, v maestro.Version, name string) (string, int64, error) {
	if _, err := gosec.Filename(s.versionDir(v), name); err != nil {
		return "", 0, fmt.Errorf("%w: %w", maestro.ErrUnsafeName, err)
	}

	full := filepath.Join(s.versionDir(v), name)

	f, err := os.Open(full)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	h := sha256.New()

	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}

	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// Delete removes all artifacts of v.
func (s *Store) Delete(ctx context.Context, v maestro.Version) error {
	return os.RemoveAll(s.versionDir(v))
}

func (s *Store) versionDir(v maestro.Version) string {
	return filepath.Join(s.cfg.DataDir, "versions", string(v))
}

func (s *Store) stagingDir(v maestro.Version) string {
	return filepath.Join(s.cfg.DataDir, "staging", string(v))
}

func (s *Store) manifestPath(v maestro.Version) string {
	return filepath.Join(s.versionDir(v), "manifest.msgp")
}

func encodeManifest(m maestro.Manifest) []byte {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(m) //nolint:errcheck

	return buf.Bytes()
}
