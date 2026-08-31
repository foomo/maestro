package player

import "io"

// FileSource gives the StageHandler streaming, hash-verified access to the
// files of the manifest being staged.
type FileSource interface {
	Open(name string) (io.ReadCloser, error) // hash-verified stream
	List() []string                          // names in manifest order
}

// fileSource implements FileSource over a downloader.
type fileSource struct {
	d *downloader
}

func (f *fileSource) Open(name string) (io.ReadCloser, error) { return f.d.openFile(name) }

func (f *fileSource) List() []string { return f.d.list() }
