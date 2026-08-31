package hashio

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
)

type Writer struct {
	sink io.Writer
	h    hash.Hash
	sum  string
}

func NewWriter(sink io.Writer) *Writer {
	return &Writer{sink: sink, h: sha256.New()}
}

func (w *Writer) Write(p []byte) (int, error) {
	n, err := w.sink.Write(p)
	if n > 0 {
		w.h.Write(p[:n])
	}

	return n, err
}

func (w *Writer) Close() error {
	w.sum = hex.EncodeToString(w.h.Sum(nil))
	if c, ok := w.sink.(io.Closer); ok {
		return c.Close()
	}

	return nil
}

func (w *Writer) Sum() string {
	return w.sum
}

// NewVerifyReader wraps r and returns an io.Reader that errors on EOF if the
// streamed bytes do not hash to want. `size` is the expected total length.
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
