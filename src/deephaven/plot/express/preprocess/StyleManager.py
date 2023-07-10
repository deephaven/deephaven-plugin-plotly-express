# these need to be applied to all in wide mode
from itertools import cycle

class StyleManager:
    def __init__(
            self,
            ls=None,
            map=None,
    ):
        self.ls = ls if isinstance(ls, list) else [ls]
        self.map = map

        self.cycled = cycle(self.ls)
        self.found = {}

    def assign_style(
            self,
            val
    ):
        if val not in self.found:
            new_val = next(self.cycled)
            if self.map and val in self.map:
                new_val = self.map[val]
            elif self.map and len(val) == 1 and val[0] in self.map:
                new_val = self.map[val[0]]
            self.found[val] = new_val
        return self.found[val]
