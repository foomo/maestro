package maestro

import (
	"fmt"
	"path/filepath"
	"strings"

	gosec "github.com/foomo/go/sec"
)

// ManifestFile describes one named file of a [Manifest].
type ManifestFile struct {
	// Name is the slash-separated relative path of the file. It must be
	// clean and must not escape its root.
	Name string `msgpack:"name"`
	// Hash is the hex-encoded SHA-256 digest of the file's bytes.
	Hash string `msgpack:"hash"`
	// Size is the file's length in bytes.
	Size int64 `msgpack:"size"`
}

// Manifest describes the set of files that make up one [Version]. It is the
// only payload the maestro protocol carries; file bytes move through a
// [github.com/foomo/maestro/pkg/blobstore.BlobStore].
type Manifest struct {
	// Version identifies the content described by Files.
	Version Version `msgpack:"version"`
	// Files lists the files of the version, each with a unique Name.
	Files []ManifestFile `msgpack:"files"`
	// TotalSize is the sum of every Files[i].Size.
	TotalSize int64 `msgpack:"total_size"`
}

const manifestSafeRoot = "/__maestro_root__"

// Validate checks that m is well-formed: it has a non-empty Version and at
// least one file, every file has a non-empty hash, a non-negative size and a
// unique, safe name, and TotalSize equals the sum of the file sizes.
// It returns an error wrapping [ErrManifestMismatch], and additionally
// [ErrUnsafeName] when a file name fails the path-safety check.
func (m Manifest) Validate() error {
	if len(m.Files) == 0 {
		return fmt.Errorf("%w: no files", ErrManifestMismatch)
	}

	if m.Version == "" {
		return fmt.Errorf("%w: empty version", ErrManifestMismatch)
	}

	seen := make(map[string]struct{}, len(m.Files))

	var total int64

	for _, f := range m.Files {
		if f.Name == "" {
			return fmt.Errorf("%w: empty file name", ErrManifestMismatch)
		}

		if f.Size < 0 {
			return fmt.Errorf("%w: negative size for %q", ErrManifestMismatch, f.Name)
		}

		if f.Hash == "" {
			return fmt.Errorf("%w: empty hash for %q", ErrManifestMismatch, f.Name)
		}

		if _, dup := seen[f.Name]; dup {
			return fmt.Errorf("%w: duplicate file name %q", ErrManifestMismatch, f.Name)
		}

		seen[f.Name] = struct{}{}
		if filepath.IsAbs(f.Name) {
			return fmt.Errorf("%w: %w (%q)", ErrManifestMismatch, ErrUnsafeName, f.Name)
		}

		if _, err := gosec.Filename(manifestSafeRoot, f.Name); err != nil {
			return fmt.Errorf("%w: %w (%q): %w", ErrManifestMismatch, ErrUnsafeName, f.Name, err)
		}

		clean := filepath.ToSlash(filepath.Clean(f.Name))
		if clean != filepath.ToSlash(f.Name) || strings.HasPrefix(clean, "../") || clean == ".." {
			return fmt.Errorf("%w: %w (%q)", ErrManifestMismatch, ErrUnsafeName, f.Name)
		}

		total += f.Size
	}

	if total != m.TotalSize {
		return fmt.Errorf("%w: TotalSize %d != sum %d", ErrManifestMismatch, m.TotalSize, total)
	}

	return nil
}
