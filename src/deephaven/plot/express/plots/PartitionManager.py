from __future__ import annotations

from deephaven.table import Table
from deephaven import pandas as dhpd
from deephaven import merge

from ._layer import layer
from ..shared import get_unique_names


PARTITION_ARGS = {
    "plot_by": None,
    "line_group": None,  # this will still use the discrete
    "color": ("color_discrete_sequence", "color_discrete_map"),
    "pattern_shape": ("pattern_shape_sequence", "pattern_shape_map"),
    "symbol": ("symbol_sequence", "symbol_map")
}

NUMERIC_TYPES = {
    "short",
    "int",
    "long",
    "float",
    "double",
}

def get_partition_key_column_tuples(
        key_column_table, columns
):
    list_columns = []
    for column in columns:
        list_columns.append(key_column_table[column].tolist())

    return list(zip(*list_columns))


def numeric_column_set(
        table: Table,
) -> set[str]:
    """Check if the provided column is numeric, check if it is in the provided cols,
    then yield a tuple with the column name and associated null value.

    Args:
      table: Table: The table to pull columns from
      cols: set[str]: The column set to check against

    Yields:
      tuple[str, str]: tuple of the form (column name, associated null value)
    """
    numeric_cols = set()
    for col in table.columns:
        type_ = col.data_type.j_name
        if type_ in NUMERIC_TYPES:
            numeric_cols.add(col.name)
    return numeric_cols


class PartitionManager:
    def __init__(
            self,
            args,
            draw_figure,
            groups
    ):

        self.variable = None
        self.variable_column = None
        self.variable_plot_by = False

        self.args = args
        self.groups = groups
        self.partitioned_table = self.process_partitions()
        self.draw_figure = draw_figure


        pass

    def stack_table(
            self,
    ):
        args = self.args
        table, x, y = args["table"], args["x"], args["y"]
        if isinstance(x, list):
            var, cols = "x", x
        elif isinstance(y, list):
            var, cols = "y", y
        else:
            # if there is no list, there is no need to stack
            return

        new_tables = []
        pivot_vars = get_unique_names(table, ["variable", "value"])

        self.variable = var
        self.variable_column = pivot_vars["variable"]

        # if there is no plot by arg, the variable column becomes it
        if not self.args.get("plot_by", None):
            self.variable_plot_by = True
            self.args["plot_by"] = self.variable_column

        for col in cols:
            new_tables.append(table.update_view(formulas=f"{pivot_vars['variable']} = `{col}`"))

        args["table"] = merge(new_tables)
        # if there is no plot_by, variable column acts like a plot_by
        args["plot_by"] = pivot_vars["variable"] if args["plot_by"] is None else args["plot_by"]

    def handle_plot_by_arg(
            self,
            arg,
            val
    ):
        args = self.args
        numeric_cols = numeric_column_set(args["table"])

        plot_by_cols = args.get("plot_by", None)

        if arg == "color":
            map_ = "color_discrete_map"
            if map_ == "by":
                args["color_by"] = args.pop("color")
            elif map_ == "identity":

                args["attached_color"] = args.pop["attached_color"]
                # attached_color
            elif val and (isinstance(val, str) or len(val) == 1) and val in numeric_cols:
                # just keep the argument in place so it can be passed to plotly
                # express directly
                pass
            elif val:
                args["color_by"] = args.pop("color")
            elif plot_by_cols:
                # this needs to be last as setting "color" in any sense will override
                args["color_by"] = plot_by_cols

        elif arg == "size":
            map_ = "size_map"

            if val and (isinstance(str, val) or len(val) == 1) and val in numeric_cols and map_ != "by":
                # just keep the argument in place so it can be passed to plotly
                # express directly
                pass
            elif plot_by_cols:
                args["size_by"] = plot_by_cols

        elif arg in {"pattern_shape", "symbol"}:
            map_ = PARTITION_ARGS[arg][1]
            if map_ == "identity":
                args[f"{arg}_attached"] = plot_by_cols
            else:
                args[f"{arg}_by"] = args.pop(arg)

        return f"{arg}_by", args.get(f"{arg}_by", None)

    def process_partitions(
            self
    ):
        if "stackable" in self.groups:
            self.stack_table()

        args = self.args

        partition_cols = set()
        partition_map = {}
        for arg, val in list(self.args.items()):
            if (val or args["plot_by"]) and arg in PARTITION_ARGS:
                arg_by, cols = self.handle_plot_by_arg(arg, val)
                if cols:
                    partition_map[arg_by] = cols
                    if isinstance(cols, list):
                        partition_cols.update([col for col in cols])
                    else:
                        partition_cols.add(cols)

        if partition_cols:
            partitioned_table = args["table"].partition_by(list(partition_cols))
            key_column_table = dhpd.to_pandas(partitioned_table.table.select_distinct(partitioned_table.key_columns))
            for arg, val in partition_map.items():
                # remove "by" from arg
                if isinstance(PARTITION_ARGS[arg[:-3]], tuple):
                    # replace the sequence with the sequence, map and distinct keys
                    # so they can be easily used together
                    keys = get_partition_key_column_tuples(key_column_table, val if isinstance(val, list) else [val])
                    sequence, map_ = PARTITION_ARGS[arg[:-3]]
                    args[sequence] = {
                        "ls": args[sequence],
                        "map": args[map_],
                        "keys": keys
                    }
                    args.pop(arg)
            args.pop("plot_by")
            return partitioned_table

        args.pop("plot_by")
        return args

    def generator(self):
        args, partitioned_table = self.args, self.partitioned_table
        if partitioned_table:
            for table in partitioned_table.constituent_tables:
                key_column_table = dhpd.to_pandas(table.select_distinct(partitioned_table.key_columns))
                args["current_partition"] = dict(zip(
                    partitioned_table.key_columns,
                    get_partition_key_column_tuples(key_column_table,
                                                    partitioned_table.key_columns)[0]
                ))


                if self.variable_column:
                    # there is a list of variables, so replace them with the current one
                    args[self.variable] = args["current_partition"][self.variable_column]

                args["table"] = table
                yield args
        else:
            yield args

    def create_figure(self):
        trace_generator = None
        figs = []
        for args in self.generator():
            fig = self.draw_figure(call_args=args, trace_generator=trace_generator)
            if not trace_generator:
                trace_generator = fig.trace_generator
            figs.append(fig)
        return layer(*figs)
