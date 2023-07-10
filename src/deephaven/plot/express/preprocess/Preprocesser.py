from __future__ import annotations

from .AttachedPreprocesser import AttachedPreprocesser
from .FreqPreprocesser import FreqPreprocesser
from .HistPreprocesser import HistPreprocesser

class Preprocesser:
    def __init__(
            self,
            args,
            groups,
            always_attached
    ):
        self.args = args
        self.groups = groups
        self.preprocesser = None
        self.always_attached = always_attached

        self.prepare_preprocess()
        pass

    def prepare_preprocess(self):
        if "preprocess_hist" in self.groups:
            self.preprocesser = HistPreprocesser(self.args)
        if "preprocess_freq" in self.groups:
            self.preprocesser = FreqPreprocesser(self.args)
        if "always_attached" in self.groups and self.always_attached:
            self.preprocesser = AttachedPreprocesser(self.args, self.always_attached)



    def preprocess_partitioned_tables(self, tables, column=None):
        if not self.preprocesser:
            yield from tables
        else:
            yield from self.preprocesser.preprocess_partitioned_tables(tables, column)

