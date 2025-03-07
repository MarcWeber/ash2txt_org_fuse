package filesystems

import (
    "context"
    "bazil.org/fuse"
    "bazil.org/fuse/fs"
)

type File interface {
    SizeBytesApproximate() (int64, error)
    SizeBytesExact() (int64, error)
    EnsureFetched() error
    CachePath() (string, error)
    Attr(ctx context.Context, a *fuse.Attr) error
    Open(req *fuse.OpenRequest, resp *fuse.OpenResponse, fs *FS) (fs.Handle, error)
}

type Folder interface {
    Attr(ctx context.Context, a *fuse.Attr) error
    Lookup(name string, ctx context.Context) (fs.Node, error)
    ReadDirAll(ctx context.Context) ([]fuse.Dirent, error)
    FoldersAndFiles() (map[string]*LazyFolder, map[string]*LazyFile, error)
}
