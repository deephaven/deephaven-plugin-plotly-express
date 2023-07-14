from __future__ import annotations
import plotly.express as px

from deephaven.table import Table, PartitionedTable
from deephaven import pandas as dhpd
from deephaven import merge

from ._layer import layer
from ..preprocess.Preprocesser import Preprocesser
from ..shared import get_unique_names

PARTITION_ARGS = {
    "by": None,
    "color": ("color_discrete_sequence", "color_discrete_map"),
    "pattern_shape": ("pattern_shape_sequence", "pattern_shape_map"),
    "symbol": ("symbol_sequence", "symbol_map"),
    "size": ("size_sequence", "size_map"),
    "line_dash": ("line_dash_sequence", "line_dash_map"),
    "width": ("width_sequence", "width_map")
}

FACET_ARGS = {
    "facet_row", "facet_col"
}

NUMERIC_TYPES = {
    "short",
    "int",
    "long",
    "float",
    "double",
}

# color, symbol, line_dash and pattern_shape are plotly defaults
STYLE_DEFAULTS = {
    "color": px.colors.qualitative.Plotly,
    "symbol": ["circle", "diamond", "square", "x", "cross"],
    "line_dash": ["solid", "dot", "dash", "longdash", "dashdot", "longdashdot"],
    "pattern_shape": ["", "/", "\\", "x", "+", "."],
    "size": [4, 5, 6, 7, 8, 9],
    "width": [3, 4, 5, 6, 7, 8]
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
            groups,
            marg_args,
            marg_func
    ):

        self.by_vars = None
        self.list_var = None
        self.cols = None
        self.pivot_vars = None
        self.has_color = None
        self.facet_row = None
        self.facet_col = None
        self.always_attached = {}

        self.marginal_x = args.pop("marginal_x", None)
        self.marginal_y = args.pop("marginal_y", None)
        self.marg_args = marg_args
        self.attach_marginals = marg_func
        self.marg_color = None

        self.args = args
        self.groups = groups
        self.preprocessor = None
        self.set_pivot_variables()
        self.convert_table_to_long_mode()
        self.partitioned_table = self.process_partitions()
        self.draw_figure = draw_figure

    def set_pivot_variables(self):
        if "supports_lists" not in self.groups:
            return

        args = self.args
        table, x, y = args["table"], args["x"], args["y"]

        if isinstance(table, PartitionedTable):
            # if given a partitioned table, pivoting is not supported
            return

        if isinstance(x, list):
            var, cols = "x", x
        elif isinstance(y, list):
            var, cols = "y", y
        else:
            # if there is no list, there is no need to convert to long mode
            self.groups.discard("supports_lists")
            return

        self.list_var = var
        self.cols = cols

        args["current_var"] = self.list_var

        self.pivot_vars = get_unique_names(table, ["variable", "value"])
        self.args["pivot_vars"] = self.pivot_vars

    def convert_table_to_long_mode(
            self,
    ):
        if "supports_lists" not in self.groups:
            return

        args = self.args
        table = args["table"]

        if isinstance(table, PartitionedTable):
            # partitioned tables are assumed to already be properly formatted
            return


        # if there is no plot by arg, the variable column becomes it
        if not self.args.get("by", None):
            args["by"] = self.pivot_vars["variable"]

        args["table"] = self.to_long_mode(table, self.cols)

    def is_single_numeric_col(
            self,
            val,
            numeric_cols
    ):
        return (isinstance(val, str) or len(val) == 1) and val in numeric_cols

    def is_by(
            self,
            arg,
            map_val=None
    ):
        seq_arg = PARTITION_ARGS[arg][0]
        if not self.args[seq_arg]:
            self.args[seq_arg] = STYLE_DEFAULTS[arg]

        if "always_attached" in self.groups:
            new_col = get_unique_names(self.args["table"], [arg])[arg]
            self.always_attached[(arg, self.args[arg])] = (map_val, self.args[seq_arg], new_col)
            # a new column will be constructed so this color is always updated
            self.args[f"attached_{arg}"] = new_col
            self.args.pop(arg)
        else:
            map_arg = PARTITION_ARGS[arg][1]
            map_val = self.args[map_arg]
            if map_val == "by":
                self.args[map_arg] = None
            if isinstance(map_val, tuple):
                # the first element should be "by" and the map should be in the second, although a tuple with only "by"
                # in it should also work
                self.args[map_arg] = map_val[1] if len(map_val) == 2 else None
            self.args[f"{arg}_by"] = self.args.pop(arg)

    def handle_plot_by_arg(
            self,
            arg,
            val
    ):
        args = self.args
        table = args["table"]
        table = table if isinstance(table, Table) else table.constituent_tables[0]
        numeric_cols = numeric_column_set(table)

        plot_by_cols = args.get("by", None)

        if arg == "color":
            map_name = "color_discrete_map"
            map_ = args[map_name]
            if map_ == "by" or isinstance(map_, dict):
                self.is_by(arg, args[map_name])
            elif map_ == "identity":
                args.pop(map_name)
                args["attached_color"] = args.pop("color")
            elif val and self.is_single_numeric_col(val, numeric_cols) and "color_continuous_scale" in self.args:
                if "always_attached" in self.groups:
                    args["colors"] = args.pop("color")
                # just keep the argument in place so it can be passed to plotly
                # express directly
                pass
            elif val:
                self.is_by(arg, args[map_name])
            elif plot_by_cols and (args.get("color_discrete_sequence") or "color" in self.by_vars):
                # this needs to be last as setting "color" in any sense will override
                if not self.args["color_discrete_sequence"]:
                    self.args["color_discrete_sequence"] = STYLE_DEFAULTS[arg]
                args["color_by"] = plot_by_cols

            # save whatever column is being used for colors for marginals
            self.marg_color = args.get("color_by", None)

        elif arg == "size":
            map_ = args["size_map"]
            if map_ == "by" or isinstance(map_, dict):
                self.is_by(arg)
            elif val and self.is_single_numeric_col(val, numeric_cols):
                # just keep the argument in place so it can be passed to plotly
                # express directly
                pass
            elif val:
                self.is_by(arg)
            elif plot_by_cols and (args.get("size_sequence") or "size" in self.by_vars):
                if not self.args["size_sequence"]:
                    self.args["size_sequence"] = STYLE_DEFAULTS[arg]
                args["size_by"] = plot_by_cols

        elif arg in {"pattern_shape", "symbol", "line_dash", "width"}:
            seq_name, map_name = PARTITION_ARGS[arg][0], PARTITION_ARGS[arg][1]
            seq, map_ = args[seq_name], args[map_name]
            if map_ == "by" or isinstance(map_, dict):
                self.is_by(arg, args[map_name])
            elif map_ == "identity":
                args.pop(map_name)
                args[f"attached_{arg}"] = args.pop(arg)
            elif val:
                self.is_by(arg, args[map_name])
            elif plot_by_cols and (args.get(seq_name) or arg in self.by_vars):
                if not seq:
                    self.args[seq_name] = STYLE_DEFAULTS[arg]
                args[f"{arg}_by"] = plot_by_cols

        return f"{arg}_by", args.get(f"{arg}_by", None)

    def process_partitions(
            self
    ):

        args = self.args

        partitioned_table = None
        partition_cols = set()
        partition_map = {}

        by_vars = args.get("by_vars", None)
        if by_vars:
            self.by_vars = set([by_vars] if isinstance(by_vars, str) else by_vars)
        args.pop("by_vars", None)

        if isinstance(args["table"], PartitionedTable):
            partitioned_table = args["table"]

        for arg, val in list(args.items()):
            if (val or args.get("by", None)) and arg in PARTITION_ARGS:
                arg_by, cols = self.handle_plot_by_arg(arg, val)
                if cols:
                    partition_map[arg_by] = cols
                    if isinstance(cols, list):
                        partition_cols.update([col for col in cols])
                    else:
                        partition_cols.add(cols)
            elif val and arg in FACET_ARGS:
                partition_cols.add(val)
                if arg == "facet_row":
                    self.facet_row = val
                else:
                    self.facet_col = val

        # preprocessor needs to be initialized after the always attached arguments are found
        self.preprocessor = Preprocesser(args, self.groups, self.always_attached, self.pivot_vars)

        if partition_cols:
            if not partitioned_table:
                partitioned_table = args["table"].partition_by(list(partition_cols))

            key_column_table = dhpd.to_pandas(partitioned_table.table.select_distinct(partitioned_table.key_columns))
            for arg_by, val in partition_map.items():
                # remove "by" from arg
                arg = arg_by[:-3]
                if arg in PARTITION_ARGS and isinstance(PARTITION_ARGS[arg], tuple):
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
            args.pop("by")
            args.pop("by_vars", None)
            return partitioned_table

        args.pop("by", None)
        args.pop("by_vars", None)
        return args["table"]

    def build_ternary_chain(self, cols):
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

    def current_partition_generator(self):
        for table in self.partitioned_table.constituent_tables:
            key_column_table = dhpd.to_pandas(table.select_distinct(self.partitioned_table.key_columns))
            current_partition = dict(zip(
                self.partitioned_table.key_columns,
                get_partition_key_column_tuples(key_column_table,
                                                self.partitioned_table.key_columns)[0]
            ))
            yield current_partition

    def table_partition_generator(self):
        constituents = self.partitioned_table.constituent_tables
        column = self.pivot_vars["value"] if self.pivot_vars else None
        tables = self.preprocessor.preprocess_partitioned_tables(constituents, column)
        for table, current_partition in zip(tables, self.current_partition_generator()):
            yield table, current_partition

    def partition_generator(self):
        args, partitioned_table = self.args, self.partitioned_table
        if hasattr(partitioned_table, "constituent_tables"):
            for table, current_partition in self.table_partition_generator():
                if isinstance(table, tuple):
                    # if a tuple is returned here, it was preprocessed already so pivots aren't needed
                    table, arg_update = table
                    args.update(arg_update)
                elif self.pivot_vars and self.pivot_vars["value"]:
                    # there is a list of variables, so replace them with the combined column
                    args[self.list_var] = self.pivot_vars["value"]

                args["current_partition"] = current_partition

                args["table"] = table
                yield args
        elif "preprocess_hist" in self.groups or "preprocess_freq" in self.groups or "preprocess_time" in self.groups:
            # still need to preprocess the base table
            table, arg_update = list(self.preprocessor.preprocess_partitioned_tables([args["table"]]))[0]
            args["table"] = table
            args.update(arg_update)
            yield args
        else:
            yield args

    def create_figure(self):
        trace_generator = None
        figs = []
        for i, args in enumerate(self.partition_generator()):
            fig = self.draw_figure(call_args=args, trace_generator=trace_generator)
            if not trace_generator:
                trace_generator = fig.trace_generator

            facet_key = []
            if "current_partition" in args:
                partition = args["current_partition"]
                if "preprocess_hist" in self.groups or "preprocess_violin" in self.groups:
                    # offsetgroup is needed mostly to prevent spacing issues in
                    # marginals
                    # not setting the offsetgroup and having both marginals set to box,
                    # violin, etc. leads to extra spacing in each marginal
                    # offsetgroup needs to be unique within the subchart as columns
                    # could have the same name
                    fig.fig.update_traces(offsetgroup=f"{'-'.join(args['current_partition'])}{i}")
                facet_key.extend([partition.get(self.facet_col, None), partition.get(self.facet_row, None)])
            facet_key = tuple(facet_key)

            if "preprocess_hist" in self.groups or "preprocess_violin" in self.groups:
                if "current_partition" in args:
                    fig.fig.update_layout(legend_tracegroupgap=0)
                else:
                    fig.fig.update_layout(showlegend=False)

            figs.append(fig)

        layered_fig = layer(*figs, which_layout=0)

        if self.has_color is False:
            layered_fig.has_color = False

        if self.marg_args:
            # the marginals need to use the already partitioned table as they
            # will have the same partitions although they will only be styled
            # by color (colors might be used multiple times)
            self.marg_args["table"] = self.partitioned_table

            if self.pivot_vars and self.pivot_vars["value"]:
                self.marg_args[self.list_var] = self.pivot_vars["value"]

            self.marg_args["color"] = self.marg_color

            return self.attach_marginals(
                layered_fig, self.marg_args, self.marginal_x, self.marginal_y
            )

        return layered_fig
