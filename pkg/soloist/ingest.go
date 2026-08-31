package soloist

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"

	"github.com/foomo/maestro"
	"github.com/foomo/maestro/pkg/blobstore"
)

// File is a single named blob to ingest.
type File struct {
	Name   string
	Reader io.Reader
}

// IngestFile is a convenience wrapper around IngestFiles for a single file.
func IngestFile(ctx context.Context, bs blobstore.BlobStore, name string, r io.Reader) (maestro.Manifest, error) {
	return IngestFiles(ctx, bs, []File{{Name: name, Reader: r}})
}

// IngestFiles writes each File via BlobStore.Writer, computing per-file sha256
// while streaming. The destination Version is derived as sha256-hex of the
// concatenation of per-file hashes (idempotent for identical inputs).
func IngestFiles(ctx context.Context, bs blobstore.BlobStore, files []File) (maestro.Manifest, error) {
	tmpVersion := maestro.Version("staging-" + randHex(8))

	var (
		manifestFiles []maestro.ManifestFile
		total         int64
	)

	roll := sha256.New()

	for _, f := range files {
		w, err := bs.Writer(ctx, tmpVersion, f.Name)
		if err != nil {
			return maestro.Manifest{}, err
		}

		fh := sha256.New()
		mw := io.MultiWriter(w, fh)

		n, err := io.Copy(mw, f.Reader)
		if err != nil {
			w.Close()
			return maestro.Manifest{}, err
		}

		if err := w.Close(); err != nil {
			return maestro.Manifest{}, err
		}

		sum := hex.EncodeToString(fh.Sum(nil))
		manifestFiles = append(manifestFiles, maestro.ManifestFile{Name: f.Name, Hash: sum, Size: n})
		roll.Write([]byte(sum))

		total += n
	}

	finalVersion := maestro.Version(hex.EncodeToString(roll.Sum(nil)))

	m := maestro.Manifest{
		Version:   finalVersion,
		Files:     manifestFiles,
		TotalSize: total,
	}
	if err := bs.Finalize(ctx, tmpVersion, m); err != nil {
		return maestro.Manifest{}, err
	}

	return m, nil
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}
