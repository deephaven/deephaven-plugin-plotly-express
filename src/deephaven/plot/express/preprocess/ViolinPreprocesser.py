from __future__ import annotations

from .UnivariatePreprocesser import UnivariatePreprocesser

class ViolinPreprocesser(UnivariatePreprocesser):
    def __init__(self, args)
        super().__init__(args)

    def preprocess_partitioned_tables(
            self,
            table: Table,
            column: str
    ) -> tuple[Table, str, None]:
        """Preprocess the violin (or box or strip) params into an appropriate table
        For each column, the data needs to be reshaped so that there is a column
        that contains the column value.

        Args:
          table: Table:
            The table to pull data from
          column: str:
            The column to use for violin data

        Returns:
          tuple[Table, str, None]:
            A tuple of new_table, column values, and None

        """
        # also used for box and strip
        new_table = table.view([
            f"{column} = {column}"
        ])

        return new_table, column, None

