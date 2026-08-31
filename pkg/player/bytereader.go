package player

import (
	"io"
)

type byteReader struct {
	b []byte
	o int
}

func (b *byteReader) Read(p []byte) (int, error) {
	if b.o >= len(b.b) {
		return 0, io.EOF
	}

	n := copy(p, b.b[b.o:])
	b.o += n

	return n, nil
}
