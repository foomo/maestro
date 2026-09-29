// Package localfs is a filesystem-backed
// [github.com/foomo/maestro/pkg/blobstore.BlobStore] /
// [github.com/foomo/maestro/pkg/blobstore.BlobReader] implementation. A
// [Store] stages files under a temporary label, then atomically promotes
// them to a version-addressed directory on Finalize; [Store.Handler]
// serves finalized files over HTTP for remote [Client] consumers.
package localfs
