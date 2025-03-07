from typing import TypeVar, Generic, Union, Callable, Any, IO, cast, Protocol, overload, Awaitable, Callable, Optional
from .later import later_instance
from bs4 import BeautifulSoup
from dataclasses import dataclass
from fastclasses_json import dataclass_json
from urllib.parse import unquote
import asyncio
from . import types as t
from . import async_refreshable_weakref

# FETCHING FOLDER AND FILE DETAILS FROM ASH2TXT.ORG
@dataclass
class FetchResultFile:
    size: str
    date: str
@dataclass
class FetchResultFolder:
    folders: list[str]
    files: dict[str, FetchResultFile]

def exact_size_bytes_from_str(size: str) -> None | int:
    # if we have exact value use it!
    if size[-2:-1] == ' B':
        return int(size[0:-2])
    return None

def approximate_size_bytes_from_str(size: str) -> int:
    size, unit = size.split(' ')
    size_ = float(size)
    if unit == 'B':
        return int(size)
    if unit == 'KiB':
        return round(size_ * 1024)
    if unit == 'MiB':
        return round(size_ * 1024 * 1024)
    if unit == 'GiB':
        return round(size_ * 1024 * 1024 * 1024)
    raise NotImplementedError(unit)

def parse_directory_html(html: str) -> FetchResultFolder:
    soup = BeautifulSoup(html, 'html.parser')
    rows = soup.select("#list tbody tr")
    # print(f"processing/fetching path {path}")
    folders = []
    files   = {}

    for row in rows:
        cols = row.find_all("td")
        if len(cols) == 3:
            name_title = cols[0].get_text(strip=True)
            name_a = next(cols[0].children)
            name_href = unquote(name_a["href"])
            splitted = name_href.split("/")
            is_dir, name = (True, splitted[-2]) if splitted[-1] == "" else (False, splitted[-1])  # Extract and decode the filename
            if name_title in ["Parent directory/"]:
                continue

            size = cols[1].get_text(strip=True)
            date = cols[2].get_text(strip=True)

            if is_dir: # recurse. It's a folder
                folders.append(name)
            else:
                files[name] = FetchResultFile(size = size, date = date)

    return FetchResultFolder(folders = folders, files = files)


# CACHING DATA TYPES
@dataclass_json
@dataclass
class CachedFileData:
    size: Optional[int]
    size_approximate: int

@dataclass_json
@dataclass
class CachedFolderData:
    files:   dict[str, CachedFileData]
    folders: list[str]


# CACHED FS IMPLEMENTATION


T = TypeVar("T")  # Generic type for data

class AutoStore(Generic[T]):

    def __init__(self, loop, data: T, store_data: Callable[[T], Awaitable]):
        self.loop = loop
        self.data = data
        self.delay = 4
        self._save_task = None
        self.store_data = store_data


    def do_later(self):
        self.store_data(self.data)

    def changed(self):
        later_instance.once(self, ticks = 5)

        if self._save_task and not self._save_task.done():
            self._save_task.cancel()  # Cancel previous task
        async def _save_after_delay():
            await asyncio.sleep(self.delay)
        self._save_task = self.loop.create_task(_save_after_delay())

@dataclass
class FolderOpts:
    loop: asyncio.AbstractEventLoop
    folder_fetch:    Callable[[t.MyPath], Awaitable[AutoStore[CachedFolderData]]]
    file_fetch_size: Callable[[t.MyPath, str], Awaitable[int]]
    file_ensure_fetched: Callable[[t.MyPath, str], Awaitable]
    file_bytes: Callable[[t.MyPath, str, int, int], Awaitable[bytes]]
    file_cache_path: Callable[[t.MyPath, str], Awaitable[str]]


class LazyFolder(t.Folder):

    def __init__(self, path: t.MyPath, opts: FolderOpts):
        self.path = path
        self.opts = opts
        self.cache = None
        self.wait_size = {}
        self.ensure_fetched = {}
        async def recreate():
            c = await self.cached()
            folders = {k: LazyFolder(self.path / k .lstrip('/'), self.opts)  for k in c.data.folders}
            files   = c.data.files
            return t.FoldersAndFilesDC(folders = folders, files = files)

        ## is it wrorth it ? I if you keep mount running then yes
        ## if you walk then no ?
        self.faf = async_refreshable_weakref.AsyncRefreshableWeakRef(opts.loop, recreate = recreate)
        self.prefetch_count = 0

    def cached(self):
        if not self.cache:
            async def start():
                return await self.opts.folder_fetch(self.path)
            self.cache = self.opts.loop.create_task(start())
        return self.cache

    async def folders_and_files(self) -> t.FoldersAndFiles:
        x = await self.faf.get()
        return x.folders, list(x.files.keys())

    async def file_size_bytes_approximate(self, name) -> int:
        c = await self.cached()
        file = c.data.files[name]
        if  file.size != None:
            return file.size
        return file.size_approximate

    async def file_size_bytes_exact(self, name: str) -> int:
        c = await self.cached()

        def fetch_size(name):
            task = self.opts.loop.create_task(self.opts.file_fetch_size(self.path, name))
            self.wait_size[name] = task
            # prefetch all sizes of this directory
            async def clean():
                size = await task
                c.data.files[name].size = size
                c.changed()
                del self.wait_size[name]
            self.opts.loop.create_task(clean())

        if self.prefetch_count >= 0:
            self.prefetch_count += 1

        if self.prefetch_count > 4:
            self.prefetch_count = -1
            folders, files = await self.folders_and_files()
            for f in files:
                if c.data.files[f] != None and not name in self.wait_size:
                    fetch_size(name)

        file = c.data.files[name]

        if  file.size != None:
            return file.size
        if not name in self.wait_size:
            fetch_size(name)

        return await self.wait_size[name]

    def file_bytes(self, name, offset: int, size: int) -> Awaitable[bytes]:
        return self.opts.file_bytes(self.path, name, offset, size)

    def file_ensure_fetched(self, name):
        return self.opts.file_ensure_fetched(self.path, name)

    def file_cache_path(self, name):
        return self.opts.file_cache_path(self.path, name)

    async def file_exists(self, name: str) -> bool:
        raise NotImplementedError()
