from __future__ import annotations

from deephaven.table import Table
from deephaven import agg
#from deephaven.time import nanos_to_millis, diff_nanos
from deephaven.updateby import cum_sum

from ..shared import get_unique_names



def preprocess_aggregate(
        table: Table,
        names: str,
        values: str
) -> Table:
    """Preprocess a table passed to pie or funnel_area to ensure it only has
     1 row per name

    Args:
      table: Table:
        The table to preprocess
      names:  str:
        The column to use for names
      values:  str:
        The column to use for names

    Returns:
      Table: A new table that contains a single row per name and columns of
      specified names and values

    """
    return table.view([names, values]).sum_by(names)


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


def preprocess_frequency_bar(
        table: Table,
        column: str
) -> tuple[Table, str, str]:
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
    names = get_unique_names(table, ["count"])

    return table.view([column]).count_by(names["count"], by=column), column, names["count"]


def preprocess_timeline(
        table: Table,
        x_start: str,
        x_end: str,
        y: str
) -> tuple[Table, str]:
    """Preprocess timeline params into an appropriate table
    The table should contain the Time_Diff, which is milliseconds between the
    provided x_start and x_end

    Args:
      table: Table:
        The table to pull data from
      x_start: str:
        The column that contains start dates
      x_end: str:
        The column that contains end dates
      y: str:
        The label for the row

    Returns:
      tuple[Table, str]: A tuple containing
        (the new table, the name of the new time_diff column)

    """

    names = get_unique_names(table, ["Time_Diff"])

    new_table = table.view([f"{x_start}",
                            f"{x_end}",
                            f"{names['Time_Diff']} = time_length({x_start}, {x_end})",
                            f"{y}"])
    return new_table, names['Time_Diff']


def preprocess_violin(
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
    # The names are None as a third tuple value is required for
    # preprocess_and_layer but putting the names in the figure
    # breaks violinmode=overlay
    return new_table, column, None


def preprocess_ecdf(
        table,
        column
):
    """

    Args:
      table: 
      column: 

    Returns:

    """
    #TODO
    col_dup = f"{column}_2"
    tot_count_col = f"TOTAL_COUNT"
    tot_count_dup = f"{tot_count_col}_2"
    prob_col = "probability"

    # count up how many of each value occurs in the column,
    # ordered and cumulative
    cumulative_counts = table.view([column, f"{col_dup}={column}"]) \
        .count_by(col_dup, by=column) \
        .sort(column) \
        .update_by(
        cum_sum(f"{tot_count_col}={col_dup}")
    )

    # convert the counts to arrays to calculate the percentages then
    # convert back to columns
    probabilities = cumulative_counts \
        .update_view(f"{tot_count_dup}={tot_count_col}") \
        .agg_by([agg.last(cols=tot_count_col),
                 agg.group(cols=[tot_count_dup, column])]) \
        .update_view(f"{prob_col} = {tot_count_dup} / {tot_count_col}") \
        .view([column, prob_col]) \
        .ungroup([column, prob_col])

    return probabilities, column, prob_col
