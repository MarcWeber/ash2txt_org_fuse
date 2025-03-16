from typing import Optional, Tuple, TypeAlias, Mapping, TypeVar, Generic, Sequence, Iterable
from dataclasses import dataclass

"""
Simple Folder and File class which can be traversed in a lazy asynchronous way

See filesystem walking
"""
class MyPath:
    def __init__(self, path):
        self.path = path.lstrip('/')
    def __str__(self):
        return self.path
    def __truediv__(self, x):
        return MyPath(f"{self.path}/{x}")
    def name(self):
        return self.split()[-1]
    def split(self) -> Sequence[str]:
        ps = self.path.split('/')
        if ps == ['']:
           return []
        return ps

FoldersLazy: TypeAlias = "Mapping[str, Folder]"
FoldersAndFiles: TypeAlias = "Tuple[FoldersLazy, Iterable[str]]"
    
FileOrFolder = Tuple

A = TypeVar("A")  # Generic type for data
B = TypeVar("B")  # Generic type for data

# only exists because tuple cannot be a weak reference
@dataclass
class FoldersAndFilesDC(Generic[A,B]):
    folders: A
    files: B
    def as_tuple(self):
        return (self.folders, self.files)

# # maybe we don't need fils and should have the folder manage all details
# # cause it knows about the caching anyway
# class File:
#     async def size_bytes_approximate(self) -> int:
#         raise NotImplementedError()
#     async def size_bytes_exact(self) -> int:
#         raise NotImplementedError()
#     async def bytes(self, offset, size) -> bytes:
#         raise NotImplementedError()

class Folder:
    path: MyPath
    async def folders_and_files(self) -> FoldersAndFiles:
        raise NotImplementedError()
    # duplication of the file API
    async def file_size_bytes_approximate(self, name) -> int:
        raise NotImplementedError()
    async def file_size_bytes_exact(self, name) -> int:
        raise NotImplementedError()
    async def filefile__bytes(self, name, offset, size) -> bytes:
        raise NotImplementedError()
    async def file_exists(self, name: str) -> bool:
        raise NotImplementedError()
    async def file_ensure_fetched(self, name: str):
        raise NotImplementedError()
        

FolderOrFile: TypeAlias = 'Tuple[Folder, None | str]'
MaybeFolderOrFile: TypeAlias = 'Tuple[Folder | None, None | str]'
