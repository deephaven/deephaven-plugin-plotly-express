from __future__ import annotations

from ..shared import get_unique_names

from deephaven.time import nanos_to_millis, diff_nanos


def time_length(
        start: str,
        end: str
) -> int:
    """Calculate the difference between the start and end times in milliseconds

    Args:
      start: str:
        The start time
      end: str:
        The end time

    Returns:
      int: The time in milliseconds

    """
    return nanos_to_millis(diff_nanos(start, end))


class TimePreprocesser:
    def __init__(self, args):
        self.args = args

    def preprocess_partitioned_tables(
            self,
            tables,
            column=None
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
        x_start, x_end, y, table = self.args["x_start"], self.args["x_end"], self.args["y"], self.args["table"]

        x_diff = get_unique_names(table, ["x_diff"])["x_diff"]

        for table in tables:
            yield table.update_view([f"{x_start}",
                                           f"{x_end}",
                                           f"{x_diff} = time_length({x_start}, {x_end})",
                                           f"{y}"]), {"x_diff": x_diff}

