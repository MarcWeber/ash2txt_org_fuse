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
