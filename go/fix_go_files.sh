# Updated fuse.go with global currentFS to avoid f.parent.fs
cat > filesystems/fuse.go << 'EOF'
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

var currentFS *FS

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
		dirs = append(dirs, fuse.Dirent{Name: name, Type: fuse.DT_Dir})
	}
	for name := range files {
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
	lock := currentFS.getFileLock(path)
	lock.Lock()
	defer lock.Unlock()

	resp.Flags |= fuse.OpenDirectIO
	return &FileHandle{file: f, fs: currentFS, path: path}, nil
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

func MountFUSE(mountpoint, cacheDir, rootURL string, fetchLimit int) error {
	fmt.Printf("Mounting at %s with cacheDir=%s, rootURL=%s, fetchLimit=%d\n", mountpoint, cacheDir, rootURL, fetchLimit)
	folder := NewLazyFolder("", cacheDir, rootURL)
	c, err := fuse.Mount(mountpoint, fuse.FSName("go_fs"), fuse.Subtype("go_fs"), fuse.ReadOnly())
	if err != nil {
		return fmt.Errorf("mount error: %v", err)
	}
	defer func() {
		if err := fuse.Unmount(mountpoint); err != nil {
			fmt.Printf("Unmount error: %v\n", err)
		}
		c.Close()
	}()

	currentFS = NewFS(folder)
	if err := fs.Serve(c, currentFS); err != nil {
		return fmt.Errorf("serve error: %v", err)
	}
	return nil
}
EOF

# Updated fuse3.go for older go-fuse/v2 compatibility
cat > filesystems/fuse3.go << 'EOF'
package filesystems

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// FuseNode represents a file or directory in the FUSE filesystem (FUSE 3)
type FuseNode struct {
	fs.Inode
	folder *LazyFolder
	file   *LazyFile
}

var _ fs.InodeEmbedder = (*FuseNode)(nil)
var _ fs.NodeReaddirer = (*FuseNode)(nil)
var _ fs.NodeLookuper = (*FuseNode)(nil)
var _ fs.NodeGetattrer = (*FuseNode)(nil)
var _ fs.NodeOpener = (*FuseNode)(nil)
var _ fs.NodeReader = (*FuseNode)(nil)

// EmbeddedInode returns the embedded inode
func (n *FuseNode) EmbeddedInode() *fs.Inode {
	return &n.Inode
}

// Lookup finds a child node by name
func (n *FuseNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n.file != nil {
		return nil, syscall.EISDIR
	}

	folders, files, err := n.folder.FoldersAndFiles()
	if err != nil {
		log.Printf("Lookup error for %s: %v", name, err)
		return nil, syscall.EIO
	}

	now := uint64(time.Now().Unix())
	if subFolder, ok := folders[name]; ok {
		out.Attr.Mode = fuse.S_IFDIR | 0755
		out.Attr.Nlink = 2
		out.Attr.Mtime = now
		out.Attr.Atime = now
		out.Attr.Ctime = now
		// For older go-fuse/v2
		out.EntryValid = 300 // seconds
		out.EntryValidNsec = 0
		out.AttrValid = 300 // seconds
		out.AttrValidNsec = 0
		node := &FuseNode{folder: subFolder}
		return n.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
	}

	if file, ok := files[name]; ok {
		size, err := file.SizeBytesExact()
		if err != nil {
			log.Printf("Size error for %s: %v", name, err)
			return nil, syscall.EIO
		}
		out.Attr.Mode = fuse.S_IFREG | 0644
		out.Attr.Size = uint64(size)
		out.Attr.Nlink = 1
		out.Attr.Mtime = now
		out.Attr.Atime = now
		out.Attr.Ctime = now
		// For older go-fuse/v2
		out.EntryValid = 300 // seconds
		out.EntryValidNsec = 0
		out.AttrValid = 300 // seconds
		out.AttrValidNsec = 0
		node := &FuseNode{folder: n.folder, file: file}
		return n.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFREG}), 0
	}

	return nil, syscall.ENOENT
}

// Readdir lists directory contents
func (n *FuseNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	if n.file != nil {
		return nil, syscall.ENOTDIR
	}

	folders, files, err := n.folder.FoldersAndFiles()
	if err != nil {
		log.Printf("Readdir error for %s: %v", n.folder.Path(), err)
		return nil, syscall.EIO
	}

	entries := []fuse.DirEntry{
		{Name: ".", Mode: fuse.S_IFDIR},
		{Name: "..", Mode: fuse.S_IFDIR},
	}
	for name := range folders {
		entries = append(entries, fuse.DirEntry{Name: name, Mode: fuse.S_IFDIR})
	}
	for name := range files {
		entries = append(entries, fuse.DirEntry{Name: name, Mode: fuse.S_IFREG})
	}

	return fs.NewListDirStream(entries), 0
}

// Getattr retrieves file or directory attributes
func (n *FuseNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	now := uint64(time.Now().Unix())
	out.Mtime = now
	out.Atime = now
	out.Ctime = now
	// For older go-fuse/v2
	out.AttrValid = 300 // seconds
	out.AttrValidNsec = 0

	if n.file != nil {
		size, err := n.file.SizeBytesExact()
		if err != nil {
			log.Printf("Getattr error for file %s: %v", n.file.Name(), err)
			return syscall.EIO
		}
		out.Mode = fuse.S_IFREG | 0644
		out.Size = uint64(size)
		out.Nlink = 1
	} else {
		out.Mode = fuse.S_IFDIR | 0755
		out.Nlink = 2
	}
	return 0
}

// Open opens a file for reading
func (n *FuseNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if n.file == nil {
		return nil, 0, syscall.EISDIR
	}
	if err := n.file.EnsureFetched(); err != nil {
		log.Printf("Open error for %s: %v", n.file.Name(), err)
		return nil, 0, syscall.EIO
	}
	return nil, fuse.FOPEN_DIRECT_IO, 0
}

// Read reads file contents
func (n *FuseNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if n.file == nil {
		return nil, syscall.EISDIR
	}
	path, err := n.file.CachePath()
	if err != nil {
		log.Printf("Read error for %s: %v", n.file.Name(), err)
		return nil, syscall.EIO
	}
	f, err := os.Open(path)
	if err != nil {
		log.Printf("Open error for %s: %v", path, err)
		return nil, syscall.EIO
	}
	defer f.Close()
	nBytes, err := f.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		log.Printf("ReadAt error for %s: %v", path, err)
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:nBytes]), 0
}

// MountFUSE3 mounts the filesystem at the specified mountpoint
func MountFUSE3(mountpoint, cacheDir, rootURL string, fetchLimit int) error {
	fmt.Printf("Mounting FUSE3 at %s with cacheDir=%s, rootURL=%s, fetchLimit=%d\n", mountpoint, cacheDir, rootURL, fetchLimit)
	rootFolder := NewLazyFolder("", cacheDir, rootURL)
	root := &FuseNode{folder: rootFolder}

	server, err := fs.Mount(mountpoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: "ash2txt",
			Name:   "ash2txtfs",
		},
	})
	if err != nil {
		return fmt.Errorf("mount failed: %v", err)
	}

	log.Printf("Mounted FUSE3 at %s", mountpoint)
	server.Wait()
	return nil
}
EOF
