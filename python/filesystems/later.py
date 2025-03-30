import traceback
import asyncio
from typing import TypeAlias, Optional, Protocol, TypedDict

# have some maintainance tasks of some which have to run before quit

class LOpts(TypedDict):
    once:  Optional[bool]
    ticks: Optional[int]

class Later:

    def __init__(self):
        self.later = {}

    def once(self, a, **kwargs): # LOpts
        kwargs["once"] = True
        self.add(a, **kwargs)

    def add(self, a, **kwargs): # LOpts
        """
        kwargs:
        [ticks = ..]
        [once = True]
        """
        self.later[a] = kwargs

    def remove(self, a):
        del self.later[a]

    async  def do_regularly(self, force = False):
        tasks = []
        for thing, o in list(self.later.items()):
            try:
                ticks = o.get('ticks')
                if ticks:
                   ticks -= 1
                   o["ticks"] = ticks
                if (ticks != None and ticks < 0) or force:
                    if hasattr(thing, "do_later"):
                        thing.do_later()
                    if hasattr(thing, "do_later_async"):
                        tasks.append(thing.do_later_async())
                    if o["once"]:
                        self.remove(thing)
            except:
                traceback.print_exc()
        return await asyncio.gather(*tasks)

later_instance = Later()
