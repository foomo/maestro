package localfs

import (
	"net/http"
	"os"
	"path/filepath"

	gosec "github.com/foomo/go/sec"

	"github.com/foomo/maestro"
)

// Handler returns an http.Handler that serves
//
//	GET /versions/{version}/files/{name...}
//
// via http.ServeContent: zero-copy sendfile when supported, Range requests,
// If-Modified-Since / If-None-Match conditional GET, and Content-Length set
// from os.Stat. Returns 404 if the version is not finalized or the file does
// not exist. Returns 400 on path-traversal attempts.
func (s *Store) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /versions/{version}/files/{name...}", func(w http.ResponseWriter, r *http.Request) {
		v := maestro.Version(r.PathValue("version"))
		name := r.PathValue("name")

		if _, err := os.Stat(s.manifestPath(v)); err != nil {
			http.Error(w, "version not finalized", http.StatusNotFound)
			return
		}

		if _, err := gosec.Filename(s.versionDir(v), name); err != nil {
			http.Error(w, "unsafe name", http.StatusBadRequest)
			return
		}

		full := filepath.Join(s.versionDir(v), name)

		f, err := os.Open(full)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		defer f.Close()

		fi, err := f.Stat()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(w, r, name, fi.ModTime(), f)
	})

	return mux
}
