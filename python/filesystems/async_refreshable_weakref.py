import weakref
import asyncio
from .later import later_instance
from typing import Callable, Optional, Awaitable, Generic, TypeVar

# have a reference to an object which can be recreated but also be garbage
# collected after delay time
# Not sure whether its better/worse than swapping ..

T = TypeVar("T")  # Generic type for data

class AsyncRefreshableWeakRef(Generic[T]):

    def __init__(self, loop: asyncio.AbstractEventLoop, recreate: Callable[[], Awaitable[T]], delay: float = 3 * 60):
        self.loop = loop
        self._strong_ref = None  # Hold strong reference initially
        self._weak_ref = None
        self._delay = delay
        self._recreate = recreate

    def do_later(self):
        self.to_weak()

    def to_weak(self):
        if self._strong_ref is not None:
            self._weak_ref = weakref.ref(self._strong_ref)
            self._strong_ref = None  # Drop strong reference

    async def get(self) -> T:
        """Retrieve the object, refreshing the timer if it exists."""
        obj = self._strong_ref if self._strong_ref is not None else (self._weak_ref() if self._weak_ref else None)

        if obj is None:
            obj = await self._recreate()  # Recreate object if possible
            self._strong_ref = obj
            self._weak_ref = None
            later_instance.once(self, ticks = 60)
        self.refresh()
        return obj

    def refresh(self):
        """Manually refresh the timer."""
        if self._strong_ref is not None or (self._weak_ref and self._weak_ref() is not None):
            later_instance.once(self, ticks = 60)
