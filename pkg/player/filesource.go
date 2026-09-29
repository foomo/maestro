package player

import "io"

// FileSource gives the [StageHandler] hash-verified access to the files of
// the manifest being staged.
type FileSource interface {
	// Open returns the contents of file name, already verified against the
	// manifest hash. It returns an error if name is not in the manifest.
	Open(name string) (io.ReadCloser, error)
	// List returns the file names in manifest order.
	List() []string
}

// fileSource implements FileSource over a downloader.
type fileSource struct {
	d *downloader
}

func (f *fileSource) Open(name string) (io.ReadCloser, error) { return f.d.openFile(name) }

func (f *fileSource) List() []string { return f.d.list() }
