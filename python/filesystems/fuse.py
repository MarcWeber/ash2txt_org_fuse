from collections import defaultdict
import errno
import stat
from fuse import FUSE, FuseOSError, Operations
import threading
import asyncio
from . import types as t
from . import walking

# this works
# see ./fuse-passthrough.py
# see ./fuse3.py
# TODO mmap

# FUSE Implementation
class FS(Operations):

    def __init__(self, folder: t.Folder, wait_async):
        self.folder = folder
        self.global_lock = threading.Lock()
        self.file_locks = defaultdict(threading.Lock)
        self.wait_async = wait_async

    def _run_async(self, coro):
        return asyncio.run(coro)

    def getattr(self, path, fh=None):
        folder, fname = self.wait_async(walking.walk_path)(self.folder, t.MyPath(path))

        if folder == None:
            # print(f"path {path} not found")
            raise FuseOSError(errno.ENOENT)

        st = dict()
        if fname != None:
            st['st_mode'] = stat.S_IFREG | 0o444
            st['st_size'] = self.wait_async(folder.file_size_bytes_exact)(fname)
            print(f"got size {st['st_size']}")
            # print(f"file size: {st['st_size']}")
        else:
            # print(f"path is  dir {path} ")
            st['st_mode'] = stat.S_IFDIR | 0o555
            st['st_size'] = 4096

        st['st_nlink'] = 1
        st['st_atime'] = st['st_mtime'] = st['st_ctime'] = 0
        return st

    def readdir(self, path, fh):
        # print(f"readdir {path}")

        async def fof(path: t.MyPath):
            folder = await walking.walk_path_find_folder(self.folder, path)
            if folder == None:
                raise FuseOSError(errno.ENOENT) # should never happen
            return await folder.folders_and_files()

        folders, files = self.wait_async(fof)(t.MyPath(path))
        # if not isinstance(thing, t.Folder):
        #     # print("thing = None 1")
        #     raise FuseOSError(errno.ENOENT) # should never happen
        n = [".", "..", *folders.keys(), *files]
        return n

    def read(self, path, size, offset, fh):
        print(f"read {path}")
        folder, fname = self.wait_async(walking.walk_path)(self.folder, t.MyPath(path))
        assert fname != None

        # return self.wait_async(thing.bytes)(offset, size)
        cache_path = self.wait_async(folder.file_cache_path)(fname)
        with open(cache_path, "rb") as f:
            f.seek(offset)
            return f.read(size)

        raise FuseOSError(errno.ENOENT)

    def lock(self, path, fh, cmd, lock):
        raise FuseOSError(errno.ENOSYS)

    def destroy(self, path):
        pass
        # self.client.close(#)

def mount(folder: t.Folder, mountpoint: str, wait_async):
    fuse = FUSE( FS( folder, wait_async),
                mountpoint = mountpoint,
                foreground=True,
                nothreads=False
                )
