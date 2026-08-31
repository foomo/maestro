package maestro

import (
	"fmt"
	"path/filepath"
	"strings"

	gosec "github.com/foomo/go/sec"
)

type ManifestFile struct {
	Name string `msgpack:"name"`
	Hash string `msgpack:"hash"`
	Size int64  `msgpack:"size"`
}

type Manifest struct {
	Version   Version        `msgpack:"version"`
	Files     []ManifestFile `msgpack:"files"`
	TotalSize int64          `msgpack:"total_size"`
}

const manifestSafeRoot = "/__maestro_root__"

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
