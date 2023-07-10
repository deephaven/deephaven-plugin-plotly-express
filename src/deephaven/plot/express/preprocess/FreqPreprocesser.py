from __future__ import annotations

from .UnivariatePreprocesser import UnivariatePreprocesser
from ..shared import get_unique_names

class FreqPreprocesser(UnivariatePreprocesser):
    def __init__(self, args):
        super().__init__(args)

    def preprocess_partitioned_tables(
            self,
            column: str
    ):
        """Preprocess frequency bar params into an appropriate table
        This just sums each value by count

        Args:
          table: Table:
            The table to pull data from
          column: str:
            The column that has counts applied

        Returns:
          tuple[Table, str, str]: A tuple containing
            (the new table, the original column name, the name of the count column)

        """
        names = get_unique_names(self.table, ["count"])

        return self.table.view([column]).count_by(names["count"], by=column), {
                self.var: column, self.other_var: names["count"]
            }

