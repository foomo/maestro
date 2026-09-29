package player

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/foomo/gofuncy"
	"github.com/foomo/maestro"
	"github.com/foomo/maestro/internal/hashio"
	"github.com/foomo/maestro/pkg/blobstore"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// downloader fetches files for a Manifest from a BlobReader, serializing reads
// behind a per-file cache so that FileSource.Open can be invoked in any order
// without re-downloading.
type downloader struct {
	bs   blobstore.BlobReader
	v    maestro.Version
	man  maestro.Manifest
	conc int
	l    *zap.Logger

	mu         sync.Mutex
	prefetched map[string][]byte // file name -> verified bytes
}

func newDownloader(bs blobstore.BlobReader, v maestro.Version, m maestro.Manifest, conc int, l *zap.Logger) *downloader {
	if l == nil {
		l = zap.NewNop()
	}

	return &downloader{
		bs:         bs,
		v:          v,
		man:        m,
		conc:       conc,
		l:          l,
		prefetched: make(map[string][]byte, len(m.Files)),
	}
}

// prefetch runs in parallel up to `conc`, downloading + verifying each file
// with retry on errors (HTTP 5xx, conn refused/reset, hash mismatch from a
// corrupted read). If ctx carries a deadline, each file is retried until it
// expires, so a blob source that is briefly unreachable (e.g. a restarting
// soloist host) is waited out; without one, it's up to 3 attempts. Returns the
// joined error on failure.
func (d *downloader) prefetch(ctx context.Context) error {
	results := make([][]byte, len(d.man.Files))

	attempts := 3
	if _, ok := ctx.Deadline(); ok {
		attempts = math.MaxInt32
	}

	g := gofuncy.NewGroup(ctx,
		gofuncy.WithName("maestro.player.prefetch"),
		gofuncy.WithLimit(d.conc),
		gofuncy.WithFailFast(),
		gofuncy.WithRetry(attempts, gofuncy.RetryBackoff(
			gofuncy.BackoffExponential(250*time.Millisecond, 2, 2*time.Second),
		)),
	)

	for i := range d.man.Files {
		mf := d.man.Files[i]
		// Only the first failure per file logs at Warn; retries log at Debug so
		// an outage spanning the whole deadline doesn't flood the log.
		lvl := zap.WarnLevel

		g.Add(func(ctx context.Context) error {
			body, ferr := d.fetchOne(ctx, mf, lvl)
			if ferr != nil {
				lvl = zap.DebugLevel
				return ferr
			}

			results[i] = body

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	d.mu.Lock()
	for i, body := range results {
		d.prefetched[d.man.Files[i].Name] = body
	}
	d.mu.Unlock()

	return nil
}

func (d *downloader) fetchOne(ctx context.Context, mf maestro.ManifestFile, lvl zapcore.Level) ([]byte, error) {
	r, _, err := d.bs.Reader(ctx, d.v, mf.Name)
	if err != nil {
		d.l.Log(lvl, "blob fetch failed",
			zap.String("version", string(d.v)),
			zap.String("name", mf.Name),
			zap.Error(err),
		)

		return nil, fmt.Errorf("reader %s: %w", mf.Name, err)
	}
	defer r.Close()

	vr := hashio.NewVerifyReader(r, mf.Hash, mf.Size)

	body, err := io.ReadAll(vr)
	if err != nil {
		d.l.Log(lvl, "blob verify failed",
			zap.String("version", string(d.v)),
			zap.String("name", mf.Name),
			zap.Error(err),
		)

		return nil, fmt.Errorf("verify %s: %w", mf.Name, err)
	}

	if int64(len(body)) != mf.Size {
		return nil, fmt.Errorf("size mismatch %s: got %d want %d", mf.Name, len(body), mf.Size)
	}

	return body, nil
}

// openFile returns a ReadCloser over the prefetched body. Errors if the file
// wasn't successfully prefetched.
func (d *downloader) openFile(name string) (io.ReadCloser, error) {
	d.mu.Lock()
	body, ok := d.prefetched[name]
	d.mu.Unlock()

	if !ok {
		return nil, fmt.Errorf("file %q not prefetched", name)
	}

	return io.NopCloser(&byteReader{b: body}), nil
}

// list returns manifest names in stable order.
func (d *downloader) list() []string {
	out := make([]string, len(d.man.Files))
	for i, f := range d.man.Files {
		out[i] = f.Name
	}

	return out
}
