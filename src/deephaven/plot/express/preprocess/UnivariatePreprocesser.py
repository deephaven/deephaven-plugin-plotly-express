from __future__ import annotations

from deephaven import agg, empty_table, new_table
from ..shared import get_unique_names
from deephaven.column import long_col
from deephaven.updateby import cum_sum
import deephaven.pandas as dhpd

# Used to aggregate within histogram bins
class UnivariatePreprocesser:
    def __init__(
            self,
            args,
    ):
        self.args = args
        self.table = args["table"]
        self.var = "x" if args["x"] else "y"
        self.other_var = "y" if self.var == "x" else "x"
        self.args["orientation"] = "h" if self.var == "y" else "v"
        self.col_val = args[self.var]
        self.cols = self.col_val if isinstance(self.col_val, list) else [self.col_val]