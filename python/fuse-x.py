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
        thing = self.wait_async(walking.walk_path)(self.folder, path.lstrip('/'))

        if thing == None:
            # print(f"path {path} not found")
            raise FuseOSError(errno.ENOENT)

        st = dict()
        if isinstance(thing, t.File):
            st['st_mode'] = stat.S_IFREG | 0o444
            st['st_size'] = self.wait_async(thing.size_bytes_exact)()
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

        async def fof(path):
            folder = await walking.walk_path_find_folder(self.folder, path.lstrip('/'))
            return await folder.folders_and_files()

        folders, files = self.wait_async(fof)(path)
        # if not isinstance(thing, t.Folder):
        #     # print("thing = None 1")
        #     raise FuseOSError(errno.ENOENT) # should never happen
        n = [".", "..", *folders.keys(), *files.keys()]
        return n

    async def _ensure_fetched(self, path: str) -> str:
        print(f"getting thing {path}")
        thing = self.wait_async(walking.walk_path)(self.folder, path)
        if not isinstance(thing, t.File):
            raise FuseOSError(errno.ENOENT)
        print(f"got file {path}")
        await thing.ensure_fetched()
        cache_path = await thing.cache_path()
        print(f"ensure fetch complete cache path is {cache_path}")
        return cache_path

    def access(self, path, mode):
        print(f"access {path}")
        cache_path = self.wait_async(self._ensure_fetched)(path)
        if not os.access(cache_path, mode):
            raise FuseOSError(errno.EACCES)

    def read(self, path, size, offset, fh):
        print(f"read {[path, size, offset]}")
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, size)
        # cache_path = self.wait_async(self.get_cache_path)(path)
        # with open(cache_path, "rb") as f:
        #     f.seek(offset)
        #     return f.read(size)
        # raise FuseOSError(errno.ENOENT)

    def release(self, path, fh):
        os.close(fh)
        return 0

    def open(self, path, flags):
        print(f"open {path} flags={flags}")
        cache_path = self.wait_async(self._ensure_fetched)(path)
        print(f"cache_path {cache_path}")
        fd = os.open(cache_path, os.O_WRONLY | os.O_CREAT)
        print(f"return fd {fd}")
        return fd


    def lock(self, path, fh, cmd, lock):
        raise FuseOSError(errno.ENOSYS)

    def mmap(self, path, length, prot, flags, offset, fh):
        cache_path = self.wait_async(self._ensure_fetched)(path)
        """Memory maps the file (supported in newer FUSE versions)."""
        print(f"mmap {cache_path} length={length} offset={offset}")
        import mmap
        return mmap.mmap(fh, length, prot=prot, flags=flags, offset=offset)


    def destroy(self, path):
        pass
        # self.client.close(#)
