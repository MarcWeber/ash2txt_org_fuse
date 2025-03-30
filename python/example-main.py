from argparse import ONE_OR_MORE
from dataclasses import dataclass
from os.path import exists
import traceback
from typing import Dict, Optional, Tuple, cast, TypeAlias, Protocol, Callable, TypeVar, Awaitable, Callable, ParamSpec
import sys
from pathlib import Path
import pickle
from time import time
import aiohttp
import asyncio
from filesystems import walking, ash2txtorg_cached
from filesystems.types import MyPath
from threading import Thread, Event
from filesystems.later import later_instance

import nest_asyncio
nest_asyncio.apply()

exiting = Event()
cancel_tasks = []

# import nest_asyncio
# nest_asyncio.apply()

# using extra thread for async operations so that
# fuse can use multiple threads without blocking on fetching files
thread_loop = asyncio.new_event_loop()

class LimitByKey:
    # TODO -> bad place
    def __init__(self, loop):
        self.loop = loop
        self.tasks = {}

    def by_key(self, key,  a):
        if not key in self.tasks:
            async def task():
                r = await a()
                del self.tasks[key]
                return r
            self.tasks[key] = self.loop.create_task(task())
        return self.tasks[key]

def start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Run the event loop in a background thread and ensure all tasks complete before stopping."""
    asyncio.set_event_loop(loop)
    loop.run_forever()

    print("later instance do_regularly")
    print("later instance do_regularly done")

    tasks = asyncio.all_tasks(loop)
    pending_tasks = []
    other = []
    for task in tasks:
        if hasattr(task, "cancel_on_quit"):
            task.cancel()
            other.append(task)
            continue
        if task.done():
            continue
        pending_tasks.append(task)

    try:
        loop.run_until_complete(asyncio.gather(*other, return_exceptions=True))
    except:
        pass
        # traceback.print_exc()

    if pending_tasks:
        try:
            loop.run_until_complete(asyncio.gather(*pending_tasks, return_exceptions=True))
        except:
            traceback.print_exc()

    loop.run_until_complete(loop.create_task( later_instance.do_regularly(force = True)))
    print("later_instance.do_regularly final run force=True done")

    print("Waiting for remaining tasks in loop done.")
    loop.close()  # Ensure resources are released
    print(f"other.len {len(other)}")
    print(f"pending_tasks.len {len(pending_tasks)}")

loop_thread = Thread(target=start_background_loop, args=(thread_loop,), daemon=False)
loop_thread.start()
P = ParamSpec('P')
R = TypeVar('R')
def wait_async(f: Callable[P, Awaitable[R]]) -> Callable[P, R]:
    def x(*args, **kwargs) -> R:
        nonlocal f
        try:
            task = asyncio.run_coroutine_threadsafe(f(*args, **kwargs), thread_loop)
            r = task.result()
            return r
        except:
            traceback.print_exc()
            raise
    return x

mount_point     = ""

def main():
    app = sys.argv[0]
    def usage():
        print(f"""
        usage:
        export FUSE_LIBRARY_PATH=/nix/store/czxy0x8wrklqswmkg75cncphj9cq893p-fuse-2.9.9/lib/libfuse.so.2
        {app} <CACHE_DIR> <URL> fuse-mount <PATH> <MOUNT_POINT>
        {app} <CACHE_DIR> <URL> fuse_passthrough-mount <PATH> <MOUNT_POINT>
        {app} <CACHE_DIR> <URL> fuse3-mount <PATH> <MOUNT_POINT>
        {app} <CACHE_DIR> <URL> list <PATH>
        {app} <CACHE_DIR> <URL> prefetch <PATH>
        {app} <CACHE_DIR> <URL> du_approximate <PATH>
        {app} <CACHE_DIR> <URL> cache_dir_check_sizes <PATH>
        {app} <CACHE_DIR> <URL> walk_cache_check_download_completness<PATH>
        {app} <CACHE_DIR> <URL> list_special_and_approximate_size_fast <PATH>
        """)
    cache_directory = Path(sys.argv[1])
    root_url        = sys.argv[2]
    argv = sys.argv[3:]

    def get_folder(cache_directory: Path, root_url: str):
        loop = thread_loop
        fetch_limiter = asyncio.Semaphore(20) # 80 yields too many requests
        limit = 100
        connector = aiohttp.TCPConnector(limit = limit, limit_per_host= limit, loop = thread_loop)
        session = aiohttp.ClientSession(connector=connector)

        def build_url (*parts: str):
            return '/'.join([x for x in parts])

        fetching = {}
        async def forever_show_fetching():
            while not exiting.is_set():
                await later_instance.do_regularly()
                await asyncio.sleep(10)
                t = time()
                if len(fetching) > 0:
                    print(f"FETCHING STATE SUMMARY {len(fetching)})\n{"\n".join([f"{k} {t-v:.1f}sec" for k, v in fetching.items()])} ")

                pending_tasks = [t for t in asyncio.all_tasks(loop) if not t.done()]
                print(f"running tasks in loop... {len(pending_tasks)}")
        cancel_tasks.append(loop.create_task(forever_show_fetching()))

        async def fetch_text(url:str):
            async with fetch_limiter:
                m = f"fetching text {url}"
                print(m)
                fetching[m] = time()
                try:
                    async with session.get(url) as response:
                        response.raise_for_status()
                        return  await response.text()  # Get text content
                finally:
                    del fetching[m]

        async def fetch_bytes(url:str, f):
            async with fetch_limiter:
                m = f"fetching bytes {url}"
                print(m)
                fetching[m] = time()
                try:
                    async with session.get(url) as response:
                        response.raise_for_status()
                        try:
                            async for chunk in response.content.iter_chunked(10 * 1024 * 1024):  # 10 MB chunks
                                f.write(chunk)
                        finally:
                            response.close()
                finally:
                    del fetching[m]

        async def fetch_headers(url:str):
             async with fetch_limiter:
                 m = f"fetching header {url}"
                 print(m)
                 fetching[m] = time()
                 try:
                     async with session.head(url) as response:
                         response.raise_for_status()
                         return response.headers
                 finally:
                    del fetching[m]

        async def folder_fetch(folder: MyPath):
            nonlocal cache_directory
            f = cache_directory / str(folder)
            f.mkdir(exist_ok=True, parents=True)

            cache_file_json = f / ".directory_contents_cached_v2.json"

            async def store_data(data):
                tmp = cache_file_json.with_suffix('.tmp')
                with tmp.open('w') as f:
                    # dataclasses_json
                    f.write(data.to_json())
                tmp.rename(cache_file_json)
                print(f"stored {cache_file_json}")

            async def frech_fetch():
                print(f"frech_fetch ")
                url = build_url(root_url, str(folder))
                async def fetch():
                    html = await fetch_text(url)
                    parsed = ash2txtorg_cached.parse_directory_html(html)
                    cached = ash2txtorg_cached.CachedFolderData(
                        files = {k: ash2txtorg_cached.CachedFileData(size = ash2txtorg_cached.exact_size_bytes_from_str(v.size), size_approximate = ash2txtorg_cached.approximate_size_bytes_from_str(v.size))  for k, v in parsed.files.items()},
                        folders = parsed.folders
                    )
                    store = ash2txtorg_cached.AutoStore(loop, cached, store_data)
                    store.changed()
                    return store
                return await fetch_once.by_key(url, fetch)

            # cache_file_v1 = f / ".directory_contents_cached"
            if cache_file_json.exists():
                with cache_file_json.open('r') as f:
                    data = ash2txtorg_cached.CachedFolderData.from_json(f.read())
                store = ash2txtorg_cached.AutoStore(loop, data, store_data)
            else:
                store = await frech_fetch()
            return store

        async def file_fetch_size(folder: MyPath, name: str):
            headers = await fetch_headers(build_url(root_url, str(folder), name))
            return int(headers['Content-Length'])

        fetch_once = LimitByKey(loop)

        async def file_ensure_fetched(folder: MyPath, name: str):
            print(f"ensuring fetched {folder} {name}")
            # TODO .. only start this once for large files !
            file = cache_directory / str(folder) / name
            if not file.exists():
                async def fetch():
                    tmp = file.with_suffix(".tmp")
                    # TODO some files are large like 800 MB ! use streaming ?
                    with tmp.open("wb") as f:
                        await fetch_bytes(build_url(root_url, str(folder), name), f)
                    tmp.rename(file)
                return await fetch_once.by_key(file, fetch)

        async def file_cache_path(folder: MyPath, name: str):
            await file_ensure_fetched(folder, name)
            return cache_directory / str(folder) / name

        async def file_bytes(folder: MyPath, name: str, offset: int, size: int):
            await file_ensure_fetched(folder, name)
            file = cache_directory / str(folder) / name
            with file.open("rb") as f:
                    f.seek(offset)
                    return f.read(size)

        lfo = ash2txtorg_cached.FolderOpts(
                loop = loop,
                folder_fetch =    folder_fetch,
                file_fetch_size = file_fetch_size,
                file_ensure_fetched = file_ensure_fetched,
                file_bytes = file_bytes,
                file_cache_path = file_cache_path
            )
        folder = ash2txtorg_cached.LazyFolder(MyPath(""), lfo)
        return folder

    if argv[0] == "fuse-mount":
        from filesystems import fuse
        path = argv[1]
        mountpoint = argv[2]
        print(f" {path} mountpoint={mountpoint}")
        # TODO multi threading ..
        folder = get_folder(cache_directory, root_url)
        folder = wait_async(walking.walk_path_find_folder)(folder, MyPath(path))
        assert folder
        fuse.mount(folder, mountpoint, wait_async)

    if argv[0] == "fuse_passthrough-mount":
        from filesystems import fuse_passthrough
        path = argv[1]
        mountpoint = argv[2]
        print(f" {path} mountpoint={mountpoint}")
        # TODO multi threading ..
        folder = get_folder(cache_directory, root_url)
        folder = wait_async(walking.walk_path_find_folder)(folder, MyPath(path))
        assert folder
        fuse_passthrough.mount(folder, mountpoint, wait_async)


    if argv[0] == "fuse3-mount":
        from filesystems import fuse3
        print("WARNING UNFINISHED!")
        path = argv[1]
        mountpoint = argv[2]
        print(f" {path} mountpoint={mountpoint}")
        # TODO multi threading ..
        folder = get_folder(cache_directory, root_url)
        folder = wait_async(walking.walk_path_find_folder)(folder, MyPath(path))
        assert folder
        fuse3.mount(folder, mountpoint, wait_async)


    elif argv[0] == "list":
        path = argv[1]
        async def list_():
            folder = get_folder(cache_directory, root_url)
            fof = await walking.walk_path(folder, MyPath(path))
            assert folder
            print(await walking.info(fof))
        wait_async(list_)()

    elif argv[0] == "prefetch":
        path = argv[1]
        async def prefetch():
            folder = get_folder(cache_directory, root_url)
            folder = await walking.walk_path_find_folder(folder, MyPath(path))
            assert folder
            limiter = asyncio.Semaphore(120)
            errors = walking.Errors()
            await walking.prefetch(folder, limiter, errors, True)
            errors.print_all()
        wait_async(prefetch)()

    elif argv[0] == "du_approximate":
        limiter = asyncio.Semaphore(50)
        path = argv[1]
        async def du_approximate():
            folder = get_folder(cache_directory, root_url)
            folder = await walking.walk_path_find_folder(folder, MyPath(path))
            assert folder
            size = await walking.list_and_size_approximate_fast_parallel(folder, limiter)
            print(f"size {walking.format_size_MiB(size)}")
        wait_async(du_approximate)()

    elif argv[0] == "cache_dir_check_sizes":
        limiter = asyncio.Semaphore(50)
        path = argv[1]
        async def du_approximate():
            folder = get_folder(cache_directory, root_url)
            folder = await walking.walk_path_find_folder(folder, MyPath(path))
            assert folder
            errors = walking.Errors()
            await walking.walk_cache_dir_check_sizes(folder, cache_directory / path, errors)
        wait_async(du_approximate)()

    elif argv[0] == "walk_cache_check_download_completness":
        limiter = asyncio.Semaphore(50)
        path = argv[1]
        async def du_approximate():
            folder = get_folder(cache_directory, root_url)
            folder = await walking.walk_path_find_folder(folder, MyPath(path))
            assert folder
            errors = walking.Errors()
            await walking.walk_cache_check_download_completness(folder, cache_directory / path, errors)
        wait_async(du_approximate)()


    elif argv[0] == "list_special_and_approximate_size_fast":
        limiter = asyncio.Semaphore(50)
        path = argv[1]
        async def du_approximate():
            folder = get_folder(cache_directory, root_url)
            folder = await walking.walk_path_find_folder(folder, MyPath(path))
            assert folder
            total, lines = await walking.list_special_and_approximate_size_fast(folder)
            [ print(l) for l in lines ]
        wait_async(du_approximate)()
    else:
        usage()
        raise Exception(f"bad command {argv[0]}")


try:
    main()
except:
    traceback.print_exc()
finally:
    [x.cancel() for x in cancel_tasks]
    exiting.set()
    thread_loop.stop()
    print(f"waiting for thread to join")
    loop_thread.join()
    print(f"done")
