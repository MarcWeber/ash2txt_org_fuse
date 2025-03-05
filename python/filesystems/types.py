from typing import Tuple, TypeAlias, Mapping

"""
Simple Folder and File class which can be traversed in a lazy asynchronous way

See filesystemt walking
"""

FoldersLazy: TypeAlias = "Mapping[str, Folder]"
FilesLazy: TypeAlias = "Mapping[str, File]"
FoldersAndFiles: TypeAlias = "Tuple[FoldersLazy, FilesLazy]"

class File:
    async def size_bytes_approximate(self) -> int:
        raise NotImplementedError()
    async def size_bytes_exact(self) -> int:
        raise NotImplementedError()
    async def bytes(self, offset, size) -> bytes:
        raise NotImplementedError()

class Folder:
    path: str
    async def folders_and_files(self) -> FoldersAndFiles:
        raise NotImplementedError()
