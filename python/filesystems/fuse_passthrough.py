from collections import defaultdict
import os
import errno
import stat
from fuse import FUSE, FuseOSError, Operations
import threading
import asyncio
from . import types as t
from . import walking

# like fuse but returns file handles
# TODO mmap

raw_fi = True

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
        print(f"getattr {path}")
        folder, fname = self.wait_async(walking.walk_path)(self.folder, path.lstrip('/'))

        if folder == None:
            # print(f"path {path} not found")
            raise FuseOSError(errno.ENOENT)

        st = dict()
        if fname != None:
            st['st_mode'] = stat.S_IFREG | 0o444
            st['st_size'] = self.wait_async(folder.fel_size_bytes_exact)(fname)
            # print(f"file size: {st['st_size']}")
        else:
            # print(f"path is  dir {path} ")
            st['st_mode'] = stat.S_IFDIR | 0o555
            st['st_size'] = 4096

        st['st_nlink'] = 1
        st['st_atime'] = st['st_mtime'] = st['st_ctime'] = 0
        return st

    def readdir(self, path, fh):
        print(f"readdir {path}")

        async def fof(path):
            folder = await walking.walk_path_find_folder(self.folder, path.lstrip('/'))
            if folder == None:
                raise FuseOSError(errno.ENOENT)
            return await folder.folders_and_files()

        folders, files = self.wait_async(fof)(path)
        # if not isinstance(thing, t.Folder):
        #     # print("thing = None 1")
        #     raise FuseOSError(errno.ENOENT) # should never happen
        n = [".", "..", *folders.keys(), *files]
        return n

    def open(self, path, flags):
        print(f"open {path}")

        async def cached_file_path() -> str:
            folder, fname = await walking.walk_path(self.folder, path)
            if fname != None:
                # return self.wait_async(thing.bytes)(offset, size)
                return await folder.file_cache_path(fname)
                return cache_path
            raise FuseOSError(errno.ENOENT)

        cp = self.wait_async(cached_file_path)()
        fh = os.open(cp, os.O_RDONLY)
        if raw_fi:
            flags.fh = fh
        else:
            return fh

    def read(self, path, size, offset, fh):
        print(f"read {[path, size, offset]}")
        if raw_fi:
            print(f"read raw_fi=True   {fh.fh}")
            f = fh.fh
        else:
            print(f"read raw_fi+False  {fh}")
            f = fh
        # todo if we have handle we should be able to use os.read
        os.lseek(f, offset, os.SEEK_SET)
        return os.read(f, size)

        thing = self.wait_async(walking.walk_path)(self.folder, path)
        if (isinstance(thing, t.File)):
            # return self.wait_async(thing.bytes)(offset, size)
            cache_path = self.wait_async(thing.cache_path)()
            with open(cache_path, "rb") as f:
                f.seek(offset)
                return f.read(size)

        raise FuseOSError(errno.ENOENT)


    def flush(self, path, fip):
        print(f"flush {path}")
        if raw_fi:
            fh = fip.fh
        else:
            fh = fip
        os.close(fh)

    def release(self, path, fip):
        print(f"release {path}")
        if raw_fi:
          fh = fip.fh
        else:
          fh = fip
        os.close(fh)

    def lock(self, path, fh, cmd, lock):
        raise FuseOSError(errno.ENOSYS)

    def destroy(self, path):
        pass
        # self.client.close(#)

def mount(folder: t.Folder, mountpoint: str, wait_async):
    fuse = FUSE( FS( folder, wait_async),
                mountpoint = mountpoint,
                # other = True,
                foreground=True,
                nothreads=False,
                raw_fi = raw_fi
                )
