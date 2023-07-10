from __future__ import annotations

from .FreqPreprocesser import FreqPreprocesser
from .HistPreprocesser import HistPreprocesser
from .ViolinPreprocesser import ViolinPreprocesser

class Preprocesser:
    def __init__(
            self,
            args,
            groups
    ):
        self.args = args
        self.groups = groups
        self.preprocesser = None

        self.prepare_preprocess()
        pass

    def prepare_preprocess(self):
        if "preprocess_hist" in self.groups:
            self.preprocesser = HistPreprocesser(self.args)
        if "preprocess_violin" in self.groups:
            self.preprocesser = ViolinPreprocesser(self.args)
        if "preprocess_freq" in self.groups:
            self.preprocesser = FreqPreprocesser(self.args)



    def preprocess_partitioned_tables(self, tables, column=None):
        if not self.preprocesser:
            yield from tables
        else:
            yield from self.preprocesser.preprocess_partitioned_tables(tables, column)

