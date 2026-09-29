package maestro

// Version is an opaque, content-addressable identifier for a [Manifest].
type Version string

// String returns v as a plain string.
func (v Version) String() string {
	return string(v)
}
