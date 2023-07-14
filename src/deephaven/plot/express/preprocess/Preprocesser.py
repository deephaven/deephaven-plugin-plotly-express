from __future__ import annotations

from .AttachedPreprocesser import AttachedPreprocesser
from .FreqPreprocesser import FreqPreprocesser
from .HistPreprocesser import HistPreprocesser
from .TimePreprocesser import TimePreprocesser


class Preprocesser:
    def __init__(
            self,
            args,
            groups,
            always_attached,
            pivot_vars
    ):
        self.args = args
        self.groups = groups
        self.preprocesser = None
        self.always_attached = always_attached
        self.pivot_vars = pivot_vars
        self.prepare_preprocess()
        pass

    def prepare_preprocess(self):
        if "preprocess_hist" in self.groups:
            self.preprocesser = HistPreprocesser(self.args, self.pivot_vars)
        elif "preprocess_freq" in self.groups:
            self.preprocesser = FreqPreprocesser(self.args)
        elif "always_attached" in self.groups and self.always_attached:
            AttachedPreprocesser(self.args, self.always_attached)
        elif "preprocess_time" in self.groups:
            self.preprocesser = TimePreprocesser(self.args)



    def preprocess_partitioned_tables(self, tables, column=None):
        if self.preprocesser:
            yield from self.preprocesser.preprocess_partitioned_tables(tables, column)
        else:
            yield from tables

