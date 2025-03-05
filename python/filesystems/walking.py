from . import types as t
from . import ash2txtorg_cached as ac
from functools import reduce
import asyncio

"""
some implementations to list size or prefetch files

"""

# TODO correct typing

ind = "    "

def format_size_bytes(b):
    return f"{b} bytes"

def format_size_MiB(b):
    return f"{b / 1024.0 / 1024:.2f} MiB"

def special_folder(files):
    count_tifs = len([x for x in files.keys() if x[-4:-1] == ".tif"])

    if ".zarray" in files:
        return "zarr archive"
    if count_tifs > 20:
        return "tiff archive"
    return None

async def walk_path(folder: t.Folder, path: str) -> t.Folder | t.File | None:
    """ finds a subfolder or subfile by walking the path"""
    ps = path.strip('/').split('/')
    if ps == ['']:
        ps = []
    fof = folder
    for p in ps:
        if isinstance(fof, t.File):
            raise Exception(f"{p} is a file, folder expected")
        folders, files = await fof.folders_and_files()
        if p in folders:
            fof = folders[p]
        elif p in files:
            fof = files[p]
        else:
            return None
            # raise Exception(f"{p} / {path} not found in {fof.path} folders {folders.keys()}")
    return fof

async def walk_path_find_folder(folder: t.Folder, path: str) -> t.Folder:
    """ finds a subfolder or subfile by walking the path"""
    fof = await walk_path(folder, path)
    if not isinstance(fof, t.Folder):
        raise Exception(f"not a folder maybe file {path}")
    return fof


async def list_and_size_approximate_fast(folder: t.Folder, print_each = True, print_within_special = False, indent = "") -> int:
    """ first list subs so that there is some output ..
        fast because approximate bytes are given in directory listings found in HTML
    """
    folders, files = await folder.folders_and_files()
    special = special_folder(files)
    size = 0
    for k, v in folders.items():
        size += await list_and_size_approximate_fast(v, print_each and ( special == None or print_within_special), print_within_special, f"{indent}    ")
    for k, v in files.items():
        file_size = await v.size_bytes_approximate()
        if print_each:
            print(f"file {indent}    {k} {file_size}")
        size += size
    if print_each:
        print(f"folder {indent}{folder.path} {size}")
    return size


async def list_and_size_approximate_fast_parallel(folder: t.Folder, limiter: asyncio.Semaphore, indent = "") -> int:
    """ first list subs so that there is some output ..
        fast because approximate bytes are given in directory listings found in HTML
    """
    # async with limiter: # must be bigger than rec depth!
    # should we have some additional limiting ? ..
    folders, files = await folder.folders_and_files()
    file_sizes   = [x.size_bytes_approximate() for x in files.values()]
    folder_sizes = [list_and_size_approximate_fast_parallel(x, limiter, f"{indent}{ind}") for x in folders.values()]
    all = await asyncio.gather(*[*file_sizes, *folder_sizes])
    total = reduce(lambda a, b: a + b, all, 0)
    print(f"folder {indent}{folder.path} {format_size_MiB(total)}")
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
    for k, v in files.items():
        file_size = await v.size_bytes_exact()
        print(f"file {indent}{ind}{k} {file_size}")
        size += size
    print(f"folder {indent}{folder.path} {size}")
    return size

async def prefetch(folder: ac.LazyFolder, limiter: asyncio.Semaphore):
    async with limiter: # must be bigger than rec depth !
        # should we have some additional limiting ? ..
        folders, files = await folder.folders_and_files()
        fetch_files   = [x.ensure_fetched() for x in files.values()]
        fetch_folders = [prefetch(x, limiter) for x in folders.values()]
        await asyncio.gather(*[*fetch_folders, *fetch_files])


async def list_important(folder: t.Folder, indent = ""):
    folders, files = await folder.folders_and_files()
    print(f"{indent}{folder.path}")

    special = special_folder(files)
    if special == None:
        # recurse folders
        for k, v in folders.items():
            await list_important(v, f"{indent}{ind}")
        # recurse files
        for k, v in files.items():
            sa = await v.size_bytes_approximate()
            print(f"{indent}{ind}{k} {format_size_bytes(sa)} {format_size_MiB(sa)}")
    else:
        print(f"{indent}{ind} probably {special}")

async def folder_info(folder: t.Folder):
    folders, files = await folder.folders_and_files()
    return f"""
    INFO {folder.path}:
    folders: {folders.keys()}
    files: {files.keys()}
    """
