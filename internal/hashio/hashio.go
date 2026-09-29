package hashio

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
)

// Writer is an [io.WriteCloser] that forwards writes to a sink while hashing
// the bytes the sink accepted. It is not safe for concurrent use.
type Writer struct {
	sink io.Writer
	h    hash.Hash
	sum  string
}

// NewWriter returns a Writer that forwards to sink.
func NewWriter(sink io.Writer) *Writer {
	return &Writer{sink: sink, h: sha256.New()}
}

// Write writes p to the sink and hashes the n bytes it accepted.
func (w *Writer) Write(p []byte) (int, error) {
	n, err := w.sink.Write(p)
	if n > 0 {
		w.h.Write(p[:n])
	}

	return n, err
}

// Close finalizes the digest reported by [Writer.Sum] and closes the sink if
// it implements [io.Closer].
func (w *Writer) Close() error {
	w.sum = hex.EncodeToString(w.h.Sum(nil))
	if c, ok := w.sink.(io.Closer); ok {
		return c.Close()
	}

	return nil
}

// Sum returns the hex-encoded SHA-256 digest of the bytes written. It returns
// "" until [Writer.Close] has been called.
func (w *Writer) Sum() string {
	return w.sum
}

// NewVerifyReader returns an [io.Reader] that reads at most size bytes from r
// and returns [io.ErrUnexpectedEOF] at the end of the stream if those bytes do
// not hash to want, a hex-encoded SHA-256 digest.
func NewVerifyReader(r io.Reader, want string, size int64) io.Reader {
	return &verifyReader{r: r, want: want, h: sha256.New(), remaining: size}
}

type verifyReader struct {
	r         io.Reader
	want      string
	h         hash.Hash
	remaining int64
}

func (v *verifyReader) Read(p []byte) (int, error) {
	if v.remaining == 0 {
		got := hex.EncodeToString(v.h.Sum(nil))
		if got != v.want {
			return 0, io.ErrUnexpectedEOF
		}

		return 0, io.EOF
	}

	if int64(len(p)) > v.remaining {
		p = p[:v.remaining]
	}

	n, err := v.r.Read(p)
	if n > 0 {
		v.h.Write(p[:n])
		v.remaining -= int64(n)
	}

	if err == io.EOF {
		got := hex.EncodeToString(v.h.Sum(nil))
		if got != v.want {
			return n, io.ErrUnexpectedEOF
		}
	}

	return n, err
}
