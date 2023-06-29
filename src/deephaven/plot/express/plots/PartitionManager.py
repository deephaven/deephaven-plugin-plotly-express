from __future__ import annotations
import plotly.express as px

from deephaven.table import Table
from deephaven import pandas as dhpd
from deephaven import merge

from ._layer import layer
from ..shared import get_unique_names
from ..preprocess import preprocess_ecdf, create_hist_tables, preprocess_violin


PARTITION_ARGS = {
    "plot_by": None,
    "line_group": None,  # this will still use the discrete
    "color": ("color_discrete_sequence", "color_discrete_map"),
    "pattern_shape": ("pattern_shape_sequence", "pattern_shape_map"),
    "symbol": ("symbol_sequence", "symbol_map"),
    "size": ("size_sequence", "size_map"),
    "line_dash": ("line_dash_sequence", "line_dash_map"),
}

NUMERIC_TYPES = {
    "short",
    "int",
    "long",
    "float",
    "double",
}

STYLE_DEFAULTS = {
    "color": px.colors.qualitative.Plotly,
    "symbol": ["circle", "diamond", "square", "x", "cross"],
    "line_dash": ["solid", "dot", "dash", "longdash", "dashdot", "longdashdot"],
    "pattern_shape": ["", "/", "\\", "x", "+", "."],
    "size": [4, 5, 6, 7, 8, 9]
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

        self.var = None
        self.cols = None
        self.pivot_vars = None
        self.variable_plot_by = False
        self.has_color = None

        if not args.get("color_discrete_sequence"):
            # the colors need to match the plotly qualitative colors so they can be
            # overriden, but has_color should be false as the color was not
            # specified by the user
            self.has_color = False
            args["color_discrete_sequence"] = px.colors.qualitative.Plotly

        self.args = args
        self.groups = groups
        self.preprocess()
        self.set_pivot_variables()
        if "supports_lists" in self.groups:
            # or here
            self.convert_table_to_long_mode()
        self.partitioned_table = self.process_partitions()
        self.draw_figure = draw_figure

    def preprocess(
            self
    ):
        # two preprocessing_modes
        # whole table or
        if "preprocess_hist" in self.groups:
            var = "x" if self.args["x"] else "y"
            columns = self.args[var]
            self.args["orientation"] = "h" if var == "y" else None
            self.args["table"], self.args["x"], self.args["y"] = create_hist_tables(
                columns=columns if isinstance(columns, list) else [columns],
                table=self.args["table"],
                nbins=self.args["nbins"],
                range_bins=self.args["range_bins"],
                histfunc=self.args["histfunc"],
                barnorm=self.args["barnorm"],
                histnorm=self.args["histnorm"],
                cumulative=self.args["cumulative"]
            )


    def set_pivot_variables(self):
        args = self.args
        table, x, y = args["table"], args["x"], args["y"]
        if isinstance(x, list):
            var, cols = "x", x
        elif isinstance(y, list):
            var, cols = "y", y
        else:
            # if there is no list, there is no need to convert to long mode
            self.groups.discard("supports_lists")
            return

        self.var = var
        self.cols = cols

        args["current_var"] = self.var

        self.pivot_vars = get_unique_names(table, ["variable", "value"])
        self.args["pivot_vars"] = self.pivot_vars


    def convert_table_to_long_mode(
            self,
    ):
        args = self.args
        table = args["table"]

        # if there is no plot by arg, the variable column becomes it
        if not self.args.get("plot_by", None):
            self.variable_plot_by = True
            args["plot_by"] = self.pivot_vars["variable"]

        args["table"] = self.to_long_mode(table, self.cols)

    def is_single_numeric_col(
            self,
            val,
            numeric_cols
    ):
        return (isinstance(val, str) or len(val) == 1) and val in numeric_cols

    def is_by(
            self,
            arg
    ):
        sequence_arg = PARTITION_ARGS[arg][0]
        if not self.args[sequence_arg]:
            self.args[sequence_arg] = STYLE_DEFAULTS[arg]

        map_arg = PARTITION_ARGS[arg][1]
        map_val = self.args[map_arg]
        if map_val == "by":
            self.args[map_arg] = None
        if isinstance(map_val, tuple):
            # the first element should be "by" and the map should be in the second, although a tuple with only "by"
            # in it should also work
            self.args[map_arg] = self.args[map_val][2] if len(map_val) == 2 else None
        self.args[f"{arg}_by"] = self.args.pop(arg)

    def handle_plot_by_arg(
            self,
            arg,
            val
    ):
        args = self.args
        numeric_cols = numeric_column_set(args["table"])

        plot_by_cols = args.get("plot_by", None)

        if arg == "color":
            map_ = args["color_discrete_map"]
            if map_ == "by" or (isinstance(map_, tuple) and map_[0] == "by"):
                self.is_by(arg)
            elif map_ == "identity":
                args["attached_color"] = args.pop["color"]
                # attached_color
            elif val and self.is_single_numeric_col(val, numeric_cols):
                # just keep the argument in place so it can be passed to plotly
                # express directly
                pass
            elif val:
                self.is_by(arg)
            elif plot_by_cols:
                # this needs to be last as setting "color" in any sense will override
                if not self.args["color_discrete_sequence"]:
                    self.args["color_discrete_sequence"] = STYLE_DEFAULTS[arg]
                args["color_by"] = plot_by_cols

        elif arg == "size":
            map_ = args["size_map"]
            if map_ == "by" or (isinstance(map_, tuple) and map_[0] == "by"):
                self.is_by(arg)
            elif val and self.is_single_numeric_col(val, numeric_cols):
                # just keep the argument in place so it can be passed to plotly
                # express directly
                pass
            elif val:
                self.is_by(arg)
            elif plot_by_cols and args.get("size_sequence"):
                # for arguments other than color, plot_by does not kick in unless a sequence is specified
                args["size_by"] = plot_by_cols

        elif arg in {"pattern_shape", "symbol", "line_dash"}:
            map_ = args[PARTITION_ARGS[arg][1]]
            if map_ == "by" or (isinstance(map_, tuple) and map_[0] == "by"):
                self.is_by(arg)
            elif map_ == "identity":
                args[f"{arg}_attached"] = args.pop(arg)
            elif val:
                self.is_by(arg)
            elif plot_by_cols and args.get(f"{arg}_sequence"):
                # for arguments other than color, plot_by does not kick in unless a sequence is specified
                args[f"{arg}_by"] = plot_by_cols

        return f"{arg}_by", args.get(f"{arg}_by", None)

    def process_partitions(
            self
    ):
        args = self.args

        partition_cols = set()
        partition_map = {}
        for arg, val in list(self.args.items()):
            if (val or args.get("plot_by", None)) and arg in PARTITION_ARGS:
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
            for arg_by, val in partition_map.items():
                # remove "by" from arg
                arg = arg_by[:-3]
                if isinstance(PARTITION_ARGS[arg], tuple):
                    # replace the sequence with the sequence, map and distinct keys
                    # so they can be easily used together
                    keys = get_partition_key_column_tuples(key_column_table, val if isinstance(val, list) else [val])
                    sequence, map_ = PARTITION_ARGS[arg]
                    args[sequence] = {
                        "ls": args[sequence],
                        "map": args[map_],
                        "keys": keys
                    }
                    args.pop(arg_by)
                    args.pop(PARTITION_ARGS[arg][1])
            args.pop("plot_by")
            return partitioned_table

        args.pop("plot_by", None)
        return args

    def build_ternary_chain(self, cols):
        # todo: fix, this is bad
        ternary_string = f"{self.pivot_vars['value']} = "
        for i, col in enumerate(cols):
            if i == len(cols) - 1:
                ternary_string += f"{col}"
            else:
                ternary_string += f"{self.pivot_vars['variable']} == `{col}` ? {col} : "
        return ternary_string

    def to_long_mode(self, table, cols):
        new_tables = []
        for col in cols:
            new_tables.append(table.update_view(f"{self.pivot_vars['variable']} = `{col}`"))

        merged = merge(new_tables)

        transposed = merged.update_view(self.build_ternary_chain(cols))

        return transposed.drop_columns(cols)

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


                if self.pivot_vars["value"]:
                    # there is a list of variables, so replace them with the combined column
                    args[self.var] = self.pivot_vars["value"]

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
        new_fig = layer(*figs)
        if self.has_color is False:
            new_fig.has_color = False
        return layer(*figs)
