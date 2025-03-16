#!/usr/bin/env python3
'''
Read-only passthrough filesystem adapted for pyfuse3 with asyncio.
Mirrors a virtual folder structure with caching.

Copyright © [Your Name] 2025
Adapted for pyfuse3 by Grok 3, xAI
'''
import os
from os import fsdecode, fsencode
import traceback
import sys
import errno
import stat as stat_m
import stat
import mmap
import logging
from os.path import join
from datetime import datetime
import asyncio
from typing import Optional
import pyfuse3
import pyfuse3.asyncio
pyfuse3.asyncio.enable()

from pyfuse3 import FileNameT, InodeT, ModeT, FileHandleT, ReaddirToken, RequestContext, EntryAttributes
from pyfuse3 import FUSEError
from collections import defaultdict

# Assuming these are your custom modules
from . import types as t
from . import walking

# Set up logging
logging.basicConfig(
    filename=f'fuse_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

class AutoNumericKey:

    def __init__(self):
        self.next_ = 2
        self.items = {}

    def next(self, x):
        n = self.next_
        self.next_ +=1
        self.items[n] = x
        return n

    def __getitem__(self, i):
        return self.items[i]

    def __delitem__(self, i):
        del self.items[i]


class FS(pyfuse3.Operations):

    def __init__(self, folder: t.Folder, wait_async):
        super(FS, self).__init__()
        self.folder = folder
        self.wait_async = wait_async  # Assumes this works with asyncio
        self.handles = {}  # Maps file descriptors to paths
        self.open_count = 0
        self.read_count = 0
        self.mmap_count = 0
        self._lock = asyncio.Lock()  # For thread-safe handle management

        self._next_inode = 1
        self._path_to_inode = {}
        self._inode_to_path = {}
        self.open_directories = AutoNumericKey()

    async def path_to_inode(self, path: str, thing: Optional[t.FolderOrFile] = None) -> tuple[InodeT, t.FolderOrFile]:
        # should be ok - todo test
        r = self._path_to_inode.get(path)
        if r == None:
            if not thing:
                # print(f"not thing whalking")
                folder, name = await walking.walk_path(self.folder, t.MyPath(path))
                # print(f"not thing whalking done")
                if not folder:
                    raise FUSEError(errno.ENOSYS)
                thing = folder, name
            # if thing == None:
            #     raise FUSEError(errno.ENOSYS)
            inode = InodeT(self._next_inode)
            self._next_inode += 1
            self._inode_to_path[inode] = path
            r = (inode, thing)
            self._path_to_inode[path] = r
        # print(f"ret from path_to_inode {path}")
        return r

    def inode_to_path(self, inode: InodeT) -> str:
        # should be ok - todo test
        return self._inode_to_path[inode]

    async def _types_from_thing(self, attr, thing: t.FolderOrFile, inode: InodeT):
            folder, name = thing
            if name == None:
                attr.st_mode = ModeT(stat.S_IFDIR | 0o555)
                attr.st_size = 4096
            else:
                attr.st_mode = ModeT(stat.S_IFREG | 0o444)
                attr.st_size = await folder.file_size_bytes_exact(name)
            attr.st_ino = inode
            attr.st_nlink = 1
            attr.st_atime_ns = 0
            attr.st_mtime_ns = 0
            attr.st_ctime_ns = 0
            attr.generation = 0
            attr.attr_timeout = 0
            attr.attr_timeout = 0
            attr.st_blksize = 512
            attr.st_blocks = ((attr.st_size + attr.st_blksize - 1) // attr.st_blksize)

        # how long to cache - lets assume user hase memory :-(
            attr.entry_timeout = 5*60.0
            attr.attr_timeout  = 5*60.0

    async def getattr(self, inode: InodeT, ctx=None):
        print(f"getattr inode={inode}")
        # should be ok - todo test
        # path = fsdecode(inode) 
        if inode == pyfuse3.ROOT_INODE :
            await self.path_to_inode(str(self.folder.path), (self.folder, None))
            path = self.folder.path
        else:
            path = t.MyPath(self.inode_to_path(inode))

        folder, name = await walking.walk_path(self.folder, path)
        if folder is None:
            raise FUSEError(errno.ENOENT)
        entry = pyfuse3.EntryAttributes()
        # print("_types_from_thing")
        await self._types_from_thing(entry, (folder, name), inode)
        # print("returning entry")
        return entry

    async def lookup(
        self,
        parent_inode: InodeT,
        name: FileNameT,
        ctx: "RequestContext"
    ) -> "EntryAttributes":
        try:
            print(f"lookup {fsdecode(name)}")
            # should be ok - todo test
            parent_path = self.inode_to_path(parent_inode)
            path = join(parent_path, fsdecode(name))
            (inode, thing) = await self.path_to_inode(path)
            if thing == None:
                raise FUSEError(errno.ENOSYS)
            attr = pyfuse3.EntryAttributes()
            await self._types_from_thing(attr, thing, inode)
            return attr
        except:
            traceback.print_exc()
            raise

    async def opendir(
        self,
        inode: InodeT,
        ctx: "RequestContext"
    ) -> FileHandleT:
        try:
            print(f"opendir inode={inode}")
            folder_path = self.inode_to_path(inode)
            folder = await walking.walk_path_find_folder(self.folder, t.MyPath(folder_path))
            if folder == None:
                    raise FUSEError(errno.ENOENT)
            folders, files = await folder.folders_and_files()

            async def ttpi(file_or_directory, name, thing):
                path = join(folder_path, name)
                # print("for path_to_inode")
                i = await self.path_to_inode(path, thing)
                # print("aft path_to_inode")
                return (file_or_directory, thing, name, path, i)

            all_entries = [
                # await ttpi("directory", ".", "."),
                # await ttpi("directory", "..", ".."),
                *( await asyncio.gather(*[
                    *[ ttpi("directory", f, (v, None)) for f, v in folders.items()],
                    *[ ttpi("file", k, (folder, k)) for k in files]
                    ]))
            ]
            return FileHandleT(self.open_directories.next((folder_path, folder, all_entries, (folders, files))))
        except:
            traceback.print_exc()
            raise


    async def readdir( self, fh: FileHandleT, start_id: int, token: "ReaddirToken") -> None:
        try:
            # should be ok - todo test
            folder_path, folder, all_entries, (folders, files) = self.open_directories[fh]

            if start_id == 0:
                idx = 0
            else:
                idx = None
                for i, item in enumerate(all_entries):
                    if item[4][0] == start_id:
                        idx = i + 1
                        break
            assert idx != None
            # print(f"idx {idx} len all_entries{len(all_entries)} start_id {start_id}")

            for i, (type, thing, name, path, (inode, thing)) in enumerate(all_entries[idx:]):
                attr = pyfuse3.EntryAttributes()
                attr.st_ino = inode
                # remove duplication
                await self._types_from_thing(attr, thing, inode)
                if not pyfuse3.readdir_reply(token, FileNameT(fsencode(name)), attr, inode):
                    break
        except:
            traceback.print_exc()
            raise

    async def releasedir(
        self,
        fh: FileHandleT
    ) -> None:
        try:
            print("releasedir")
            # should be ok - todo test
            del self.open_directories[fh]
        except:
            traceback.print_exc()
            raise


    # async def access(self, inode, mode, ctx):
    #     path = fsdecode(inode) if inode != pyfuse3.ROOT_INODE else ""
        
    #     async def check_access(folder, path, mode):
    #         thing = await walking.walk_path(folder, path)
    #         if thing is None:
    #             raise FUSEError(errno.ENOENT)
    #         if mode & (os.W_OK | os.X_OK):
    #             raise FUSEError(errno.EACCES)
    #         return 0

    #     return self.wait_async(check_access)(self.folder, path, mode)

#     async def setxattr(self, inode, name, value, ctx):
#         if inode != pyfuse3.ROOT_INODE or name != b'command':
#             raise pyfuse3.FUSEError(errno.ENOTSUP)
#         if value == b'terminate':
#             pyfuse3.terminate()
#         else:
#             raise pyfuse3.FUSEError(errno.EINVAL)



    async def open(self, inode, flags, ctx):
        try:
            print(f"open inode={inode}")
            if flags & (os.O_WRONLY | os.O_RDWR):
                raise FUSEError(errno.EACCES)

            path = self.inode_to_path(inode);
            _, (folder, name) = await self.path_to_inode(path)

            if name == None:
                raise FUSEError(errno.ENOSYS)

            path = await folder.file_cache_path(name)

            fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
            return pyfuse3.FileInfo(fh=FileHandleT(fd))
        except:
            traceback.print_exc()
            raise

    async def read(self, fh, off, size):
        try:
            print(f"read {fh} {off} {size}")
            os.lseek(fh, off, os.SEEK_SET)
            data = os.read(fh, size)
            self.read_count += 1
            path = self.handles.get(fh, "unknown")
            logging.info(f"read called - path: /{path}, size: {size}, offset: {off}, fd: {fh}, total reads: {self.read_count}")
            return data
        except:
            traceback.print_exc()
            raise

    async def release(self, fh):
        try:
            os.close(fh)
        except:
            traceback.print_exc()
            raise

    async def destroy(self):
        try:
            async with self._lock:
                for fd in list(self.handles.keys()):
                    try:
                        os.close(fd)
                    except OSError as exc:
                        logging.error(f"Error closing fd {fd}: {exc}")
                    del self.handles[fd]
            logging.info(f"Filesystem unmounted - final stats: opens={self.open_count}, reads={self.read_count}, mmaps={self.mmap_count}")
        except:
            traceback.print_exc()
            raise

    async def getxattr(self, inode, name, ctx):
        raise FUSEError(errno.ENOTSUP)

    async def listxattr(self, inode, ctx):
        return []

def init_logging(debug=False):
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: [%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    if debug:
        handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

async def mount_async(folder: t.Folder, mountpoint: str, wait_async, debug=False):
    print("mount_async")
    init_logging(debug)
    operations = FS(folder, wait_async)

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=passthroughfs')
    fuse_options.add('ro')
    if debug:
        fuse_options.add('debug')

    pyfuse3.init(operations, mountpoint, fuse_options)
    print("mount_async init done")
    print("running mount_async pyfuse3.main")
    await pyfuse3.main(5, 20)
    print("running mount_async pyfuse3.main done")
    pyfuse3.close()
    # pyfuse3.close(unmount=False)

def mount(folder: t.Folder, mountpoint: str, wait_async, debug=False):
    wait_async(mount_async)(folder, mountpoint, wait_async, debug)
    # asyncio.run(mount_async(folder, mountpoint, wait_async, debug))
