package filesystems

import (
    "context"
    "fmt"
    "io"
    "os"
    "path/filepath"
    "sync"

    "bazil.org/fuse"
    "bazil.org/fuse/fs"
    "golang.org/x/exp/mmap"
)

type FS struct {
    folder     *LazyFolder
    fileLocks  sync.Map // map[string]*sync.Mutex
    mmapFiles  sync.Map // map[string]*mmap.ReaderAt
    fileHandle sync.Map // map[string]*os.File
}

func NewFS(folder *LazyFolder) *FS {
    return &FS{folder: folder}
}

func (f *FS) Root() (fs.Node, error) {
    fmt.Println("Root called")
    return f.folder, nil
}

// LazyFolder implements fs.Node, fs.NodeRequestLookuper, and fs.NodeReaddirer
func (f *LazyFolder) Attr(ctx context.Context, a *fuse.Attr) error {
    fmt.Printf("Attr called for %s\n", f.path)
    a.Mode = os.ModeDir | 0555
    a.Size = 4096
    return nil
}

func (f *LazyFolder) Lookup(ctx context.Context, name string) (fs.Node, error) {
    fmt.Printf("Lookup called for %s/%s\n", f.path, name)
    folders, files, err := f.FoldersAndFiles()
    if err != nil {
        fmt.Printf("Lookup error for %s/%s: %v\n", f.path, name, err)
        return nil, err
    }
    if folder, ok := folders[name]; ok {
        fmt.Printf("Found folder %s/%s\n", f.path, name)
        return folder, nil
    }
    if file, ok := files[name]; ok {
        fmt.Printf("Found file %s/%s\n", f.path, name)
        return file, nil
    }
    fmt.Printf("No entry found for %s/%s\n", f.path, name)
    return nil, fuse.ENOENT
}

func (f *LazyFolder) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
    fmt.Printf("ReadDirAll called for %s\n", f.path)
    folders, files, err := f.FoldersAndFiles()
    if err != nil {
        fmt.Printf("ReadDirAll error for %s: %v\n", f.path, err)
        return nil, err
    }
    var dirs []fuse.Dirent
    dirs = append(dirs, fuse.Dirent{Name: ".", Type: fuse.DT_Dir})
    dirs = append(dirs, fuse.Dirent{Name: "..", Type: fuse.DT_Dir})
    for name := range folders {
      // fmt.Printf("Adding folder dirent %s/%s\n", f.path, name)
        dirs = append(dirs, fuse.Dirent{Name: name, Type: fuse.DT_Dir})
    }
    for name := range files {
      // fmt.Printf("Adding file dirent %s/%s\n", f.path, name)
        dirs = append(dirs, fuse.Dirent{Name: name, Type: fuse.DT_File})
    }
    return dirs, nil
}

// LazyFile implements fs.Node and fs.NodeOpener
func (f *LazyFile) Attr(ctx context.Context, a *fuse.Attr) error {
    fmt.Printf("Attr called for file %s/%s\n", f.parent.path, f.name)
    size, err := f.SizeBytesExact()
    if err != nil {
        size, _ = f.SizeBytesApproximate()
    }
    a.Mode = 0444
    a.Size = uint64(size)
    return nil
}

func (f *FS) getFileLock(path string) *sync.Mutex {
    l, _ := f.fileLocks.LoadOrStore(path, &sync.Mutex{})
    return l.(*sync.Mutex)
}

func (f *FS) openMmap(path string, file *LazyFile) (*mmap.ReaderAt, error) {
    if m, ok := f.mmapFiles.Load(path); ok {
        return m.(*mmap.ReaderAt), nil
    }
    cachePath, err := file.CachePath()
    if err != nil {
        return nil, err
    }
    m, err := mmap.Open(cachePath)
    if err != nil {
        return nil, err
    }
    f.mmapFiles.Store(path, m)
    return m, nil
}

func (f *FS) openHandle(path string, file *LazyFile) (*os.File, error) {
    if fh, ok := f.fileHandle.Load(path); ok {
        return fh.(*os.File), nil
    }
    cachePath, err := file.CachePath()
    if err != nil {
        return nil, err
    }
    fh, err := os.Open(cachePath)
    if err != nil {
        return nil, err
    }
    f.fileHandle.Store(path, fh)
    return fh, nil
}

func (f *LazyFile) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
    fmt.Printf("Open called for %s/%s\n", f.parent.path, f.name)
    err := f.EnsureFetched()
    if err != nil {
        fmt.Printf("Open error for %s/%s: %v\n", f.parent.path, f.name, err)
        return nil, err
    }
    path := filepath.Join(f.parent.path, f.name)
    lock := f.parent.fs.getFileLock(path)
    lock.Lock()
    defer lock.Unlock()

    resp.Flags |= fuse.OpenDirectIO
    return &FileHandle{file: f, fs: f.parent.fs, path: path}, nil
}

type FileHandle struct {
    file *LazyFile
    fs   *FS
    path string
}

func (h *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
    fmt.Printf("Read called for %s\n", h.path)
    lock := h.fs.getFileLock(h.path)
    lock.Lock()
    defer lock.Unlock()

    mmapFile, err := h.fs.openMmap(h.path, h.file)
    if err != nil {
        fmt.Printf("Read error for %s: %v\n", h.path, err)
        return err
    }
    resp.Data = make([]byte, req.Size)
    n, err := mmapFile.ReadAt(resp.Data, req.Offset)
    if err != nil && err != io.EOF {
        fmt.Printf("Read error for %s: %v\n", h.path, err)
        return err
    }
    resp.Data = resp.Data[:n]
    return nil
}

func (h *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
    fmt.Printf("Release called for %s\n", h.path)
    return nil
}

func Mount(folder *LazyFolder, mountpoint string) {
    fmt.Printf("Mounting %s at %s\n", folder.path, mountpoint)
    c, err := fuse.Mount(mountpoint, fuse.FSName("go_fs"), fuse.Subtype("go_fs"), fuse.ReadOnly())
    if err != nil {
        fmt.Printf("Mount error: %v\n", err)
        os.Exit(1)
    }
    defer func() {
        if err := fuse.Unmount(mountpoint); err != nil {
            fmt.Printf("Unmount error: %v\n", err)
        }
        c.Close()
    }()

    folder.fs = NewFS(folder)
    if err := fs.Serve(c, folder.fs); err != nil {
        fmt.Printf("Serve error: %v\n", err)
        os.Exit(1)
    }
}
