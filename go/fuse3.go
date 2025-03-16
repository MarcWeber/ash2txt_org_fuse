package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"go_fs_project/filesystems"
)

// Fuse3Node represents a file or directory in the FUSE 3 filesystem
type Fuse3Node struct {
	fs.Inode
	folder *filesystems.LazyFolder
	file   *filesystems.LazyFile
}

var _ fs.InodeEmbedder = (*Fuse3Node)(nil)
var _ fs.NodeReaddirer = (*Fuse3Node)(nil)
var _ fs.NodeLookuper = (*Fuse3Node)(nil)
var _ fs.NodeGetattrer = (*Fuse3Node)(nil)
var _ fs.NodeOpener = (*Fuse3Node)(nil)
var _ fs.NodeReader = (*Fuse3Node)(nil)


// Lookup finds a child node by name
func (n *Fuse3Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
  fmt.Printf("Lookup \n")
	if n.file != nil {
		return nil, syscall.EISDIR // Files don't have children
	}

	folders, files, err := n.folder.FoldersAndFiles()
	if err != nil {
		log.Printf("Lookup error for %s: %v", name, err)
		return nil, syscall.EIO
	}

	if subFolder, ok := folders[name]; ok {
		node := &Fuse3Node{folder: subFolder}
		out.Attr.Mode = fuse.S_IFDIR | 0755
		out.Attr.Nlink = 2
		return n.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
	}

	if file, ok := files[name]; ok {
		node := &Fuse3Node{folder: n.folder, file: file}
		size, err := file.SizeBytesExact()
		if err != nil {
			log.Printf("Size error for %s: %v", name, err)
			return nil, syscall.EIO
		}
		out.Attr.Mode = fuse.S_IFREG | 0644
		out.Attr.Size = uint64(size)
		out.Attr.Nlink = 1
		return n.NewInode(ctx, node, fs.StableAttr{Mode: syscall.S_IFREG}), 0
	}

	return nil, syscall.ENOENT
}

// Readdir lists directory contents
func (n *Fuse3Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
  fmt.Printf("readdir \n")
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
func (n *Fuse3Node) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
  fmt.Printf("GetAttr \n")
	if n.file != nil {
		size, err := n.file.SizeBytesExact()
		if err != nil {
			log.Printf("Getattr error for file %s: %v", n.file.Name(), err)
			return syscall.EIO
		}
		out.Attr.Mode = fuse.S_IFREG | 0644
		out.Attr.Size = uint64(size)
		out.Attr.Nlink = 1
	} else {
		out.Attr.Mode = fuse.S_IFDIR | 0755
		out.Attr.Nlink = 2
	}
	out.Attr.Mtime = uint64(time.Now().Unix())
	out.Attr.Atime = out.Attr.Mtime
	out.Attr.Ctime = out.Attr.Mtime
	return 0
}

// Open opens a file for reading
func (n *Fuse3Node) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
        fmt.Printf("open \n")
	if n.file == nil {
		return nil, 0, syscall.EISDIR
	}
	if err := n.file.EnsureFetched(); err != nil {
		log.Printf("Open error for %s: %v", n.file.Name(), err)
		return nil, 0, syscall.EIO
	}
	return nil, fuse.FOPEN_DIRECT_IO, 0 // Direct I/O to bypass kernel cache
}

// Read reads file contents
func (n *Fuse3Node) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
        fmt.Printf("read \n")
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
	nRead, err := f.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		log.Printf("ReadAt error for %s: %v", path, err)
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:nRead]), 0
}

// MountFUSE3 mounts the filesystem using FUSE 3 protocol
func MountFUSE3(mountpoint, cacheDir, rootURL string, fetchLimit int) error {
	rootFolder := filesystems.NewLazyFolder("", cacheDir, rootURL)
	root := &Fuse3Node{folder: rootFolder}

	sec := time.Second
	server, err := fs.Mount(mountpoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: "ash2txt-fuse3",
			Name:   "ash2txtfs3",
			Debug:  false, // Set to true for debugging
		},
		EntryTimeout:    &sec, // Cache directory entries for 1 second
		AttrTimeout:     &sec, // Cache attributes for 1 second
		NegativeTimeout: &sec, // Cache negative lookups for 1 second
	})
	if err != nil {
		return fmt.Errorf("mount failed: %v", err)
	}

	log.Printf("Mounted FUSE3 at %s", mountpoint)
	server.Wait()
	return nil
}
