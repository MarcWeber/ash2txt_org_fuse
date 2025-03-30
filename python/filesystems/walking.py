from . import types as t
from typing import TypeVar, Generic, Union, Callable, Any, IO, cast, Protocol, overload, Awaitable, Callable, Iterable
import re
from collections import defaultdict
import os
from . import ash2txtorg_cached as ac
from pathlib import Path
from functools import reduce
import asyncio
from typing import Iterable

"""
some implementations to list size or prefetch files

"""

class Errors(list):

    def print_all(self):
        for e in self:
            print(e)

# TODO correct typing

ind = "    "

def format_size_bytes(b):
    return f"{b} bytes"

def format_size_MiB(b):
    return f"{b / 1024.0 / 1024:.2f} MiB"

re_working_mesh_window = re.compile('^working_mesh_.*window')

def special_folder(folder: t.Folder, folders: Iterable[str], files: Iterable[str]):
    ps = folder.path.split()

    if len(ps) >= 2 and ps[-2].endswith(".volpkg") and ps[-1] == "paths": 
        return "volpkg/paths"

    if len(ps) > 0:
        # whatever working is its not a scroll
        for k in ['working', 'volumetric-instance-labels']:
            if k == ps[-1]:
                return k

    def c_files(l):
        return len([x for x in files if l(x)])
    def c_folders(l):
        return len([x for x in folders if l(x)])

    count_tifs = c_files(lambda x: x.endswith('.tif'))
    count_yxz = c_folders(lambda x: x.startswith('cell_yxz'))
    count_sample = c_folders(lambda x: x.startswith("sample_"))
    count_point_cloud = c_folders(lambda x: x.startswith("point_cloud_"))

    if count_yxz > 2:
        return "yxz?"
    elif c_folders(lambda x: re_working_mesh_window.match(x)) > 2:
        return "working_mesh_*_window_"
    elif count_point_cloud > 2:
        return "pointcloud"
    elif count_sample > 2:
        return "sample_"
    elif ".zarray" in files:
        return "zarr archive"
    elif count_tifs > 20:
        return "tiff archive"
    return None

async def walk_path(folder: t.Folder, path: t.MyPath) -> t.MaybeFolderOrFile:
    """ finds a subfolder or subfile by walking the path"""
    ps = path.split()
    for p in ps:
        folders, files = await folder.folders_and_files()
        if p in folders:
            folder = folders[p]
        elif p in files:
            return (folder, p)
        else:
            return (None, None)
            # raise Exception(f"{p} / {path} not found in {fof.path} folders {folders.keys()}")
    return (folder, None)

async def walk_path_find_folder(folder: t.Folder, path: t.MyPath) -> t.Folder | None:
    """ finds a subfolder or subfile by walking the path"""
    f, file = await walk_path(folder, path)
    if file != None:
        raise Exception(f"not a folder maybe file {path}")
    return f

async def list_special_and_approximate_size_fast(folder: t.Folder, sums_by_ext = True, print_each = True, print_within_special = False, indent = "") -> tuple[int, list[str]]:
    """ first list subs so that there is some output ..
        fast because approximate bytes are given in directory listings found in HTML
    """
    folders, files = await folder.folders_and_files()
    childs = []
    path = folder.path
    special = special_folder(folder, folders.keys(), files)
    pe = print_each and ( special == None or print_within_special)
    folder_size = 0
    items = await asyncio.gather(
        *[ list_special_and_approximate_size_fast(v, sums_by_ext, pe, print_within_special, f"{indent}    ") for name, v in folders.items()],
    )

    for size, lines in items:
        folder_size += size
        childs += lines

    sum_by_ext = defaultdict(lambda: 0)
    counts_by_ext = defaultdict(lambda: 0)
    for name in files:
        r, ext = os.path.splitext(name)
        file_size = await folder.file_size_bytes_approximate(name)
        folder_size += file_size

        if pe:
            if sums_by_ext:
                sum_by_ext[ext] += file_size
                counts_by_ext[ext] += 1
            else:
                childs.append((f"{indent}    {name} {file_size}"))

    if pe and sums_by_ext:
        for name, v in sum_by_ext.items():
            childs.append(f"{indent}{ind}extension={name}: count:{counts_by_ext[name]} {format_size_MiB(v)}")

    flines = []
    if print_each:
        flines.append(f"{indent}{path.name()}/ {format_size_MiB(folder_size)} {str(path)}")
        flines += childs

    return folder_size, flines


async def list_and_size_approximate_fast_parallel(folder: t.Folder, limiter: asyncio.Semaphore, indent = "") -> int:
    """ first list subs so that there is some output ..
        fast because approximate bytes are given in directory listings found in HTML
    """
    # async with limiter: # must be bigger than rec depth!
    # should we have some additional limiting ? ..
    folders, files = await folder.folders_and_files()
    file_sizes   = [folder.file_size_bytes_approximate(name) for name in files]
    folder_sizes = [list_and_size_approximate_fast_parallel(x, limiter, f"{indent}{ind}") for x in folders.values()]
    all = await asyncio.gather(*[*file_sizes, *folder_sizes])
    total = reduce(lambda a, b: a + b, all, 0)
    return total


async def list_and_size_exact_slow(folder: t.Folder, indent = "") -> int:
    """ first list subs so that there is some output ..
        slow because exact bytes must be fetched by using HEAD request unless "20 B" size is given in HTML
        think twice maybe list_and_size_approximate is good enough
    """
    folders, files = await folder.folders_and_files()
    size = 0
    for k, v in folders.items():
        size += await list_and_size_exact_slow(v, f"{indent}{ind}")
    for name in files:
        file_size = await folder.file_size_bytes_approximate(name)
        print(f"file {indent}{ind}{name} {file_size}")
        size += size
    print(f"folder {indent}{folder.path} {size}")
    return size

async def prefetch(folder: ac.LazyFolder, limiter: asyncio.Semaphore, errors: Errors, fix = False):
    # question is what's correct way to fix ?
    # maybe remove all the .directory_contents_cached_v2.json files and refetch ?
    # because you don't know what's wrong .. :-(

    async with limiter: # must be bigger than rec depth !
        # should we have some additional limiting ? ..
        folders, files = await folder.folders_and_files()

    async def ensure(folder: ac.LazyFolder, name: str):
        async with limiter: # must be bigger than rec depth !
            if fix:
                cf = Path(await folder.file_cache_path(name))
                if cf.exists():
                    expected_size = await folder.file_size_bytes_exact(name)
                    size = cf.stat().st_size
                    if size != expected_size:
                        msg = f"UNLINKING {cf} SHOULD{expected_size} WAS {size}"
                        print(msg)
                        errors.append(msg)
                        cf.unlink()

            await folder.file_ensure_fetched(name)

    fetch_files   = [ensure(folder, name) for name in files]
    fetch_folders = [prefetch(x, limiter, errors) for x in folders.values()]
    await asyncio.gather(*[*fetch_folders, *fetch_files])


async def list_special(folder: t.Folder, indent = ""):
    folders, files = await folder.folders_and_files()
    print(f"{indent}{folder.path}")

    special = special_folder(folder, folders, files)
    if special == None:
        # recurse folders
        for k, v in folders.items():
            await list_special(v, f"{indent}{ind}")
        # recurse files
        for name in files:
            sa = await folder.file_size_bytes_approximate(name)
            print(f"{indent}{ind}{name} {format_size_bytes(sa)} {format_size_MiB(sa)}")
    else:
        print(f"{indent}{ind} probably {special}")

async def info(thing: t.FileOrFolder):
    folder, name = thing
    if name is not None:
        return "a file"
    if folder is not None:
        folders, files = await folder.folders_and_files()
        return f"""
        INFO {str(folder.path)}:
        folders: {folders.keys()}
        files: {files}
        """
        return f"is a file approximate size { format_size_bytes(await thing.size_bytes_approximate())}"
    else:
        return "neither file nor directory - not found"

async def walk_cache_dir_check_sizes(folder: t.Folder, cache_dir: Path, errors: Errors):
    folders, files = await folder.folders_and_files()
    size = 0
    for k, v in folders.items():
        await walk_cache_dir_check_sizes(v, cache_dir / k, errors)
    for name in files:
        cf = cache_dir / name
        if cf.exists():
            expected_size = await folder.file_size_bytes_exact(name)
            size = cf.stat().st_size
            if (expected_size != size):
                errors.append(f"{cf} expected={expected_size} size={size}")

async def walk_cache_check_download_completness(folder: t.Folder, cache_dir: Path, errors: Errors):
    total = 0
    downloaded = 0

    async def rec(folder: t.Folder, cache_dir: Path):
        folders, files = await folder.folders_and_files()
        nonlocal total, downloaded
        total = 0
        await asyncio.gather(*[rec(v, cache_dir / k) for [k,v] in folders.items()])
        for name in files:
            cf = cache_dir / name
            expected_size = await folder.file_size_bytes_exact(name)
            if cf.exists():
                size = cf.stat().st_size
                if size != expected_size:
                    errors.append(f"{cf} expected={expected_size} size={size}")
                downloaded += size
                total += expected_size
            else:
                total += expected_size

    await rec(folder, cache_dir)
    print(f" {format_size_MiB(downloaded)} / {format_size_MiB(total)} {downloaded/total:.2f}")
