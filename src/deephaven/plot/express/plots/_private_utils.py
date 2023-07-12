from __future__ import annotations

from copy import deepcopy
from functools import partial
from collections.abc import Generator, Callable

import plotly.express as px

from deephaven.table import Table, PartitionedTable

from ._layer import layer
from .PartitionManager import PartitionManager
from ._update_wrapper import unsafe_figure_update_wrapper
from ..deephaven_figure import generate_figure, DeephavenFigure
from ..shared import get_unique_names
from ._update_wrapper import default_callback


PARTITION_ARGS = {
    "plot_by": None,
    "line_group": None,  # this will still use the discrete
    "color": ("color_discrete_sequence", "color_discrete_map"),
    "pattern_shape": ("pattern_shape_sequence", "pattern_shape_map"),
    "symbol": ("symbol_sequence", "symbol_map")
}


def validate_common_args(
        args: dict
) -> None:
    """Validate common args amongst plots

    Args:
      args: dict: The args to validate

    """
    if not isinstance(args["table"], (Table, PartitionedTable)):
        raise ValueError("Argument table is not of type Table")


def remap_scene_args(
        args: dict
) -> None:
    """Remap layout scenes args so that they are not converted to a list

    Args:
      args: dict: The args to remap

    """
    for arg in ["range_x", "range_y", "range_z", "log_x", "log_y", "log_z"]:
        args[arg + '_scene'] = args.pop(arg)


def preprocessed_fig(
        draw: callable,
        keys: list[str],
        args: dict[str, any],
        is_list: bool,
        trace_generator: Generator[dict[str, any]],
        col: str | list[str],
        preprocesser: callable = None,
        table: Table = None,
        preprocessed_args: tuple[Table, str, list[str]] = None
) -> DeephavenFigure:
    """Preprocess and return a figure
    Either both preprocesser and table or just preprocessed_table should be
    specified

    Args:
      draw: callable: A draw function, generally from plotly express
      keys: list[str]: A list of the variables to assign the preprocessed
        results to
      args: The args to pass to figure creation
      is_list: bool: True if the current column is one of a list
      trace_generator: Generator[dict[str, any]]: The trace generator to use
        to pass to generate_figure
      col: str | list[str]: The columns that are being plotted
      preprocesser: callable: (Default value = None)
        A function that returns a tuple that contains
        (new table, first data columnn, second data column)
      table: Table: The table to use
      preprocessed_args: tuple[Table, str, list[str]]:  (Default value = None)
        If the data was already preprocessed, use that tuple of data instead

    Returns:
      DeephavenFigure: The resulting DeephavenFigure

    """
    # if preprocessed args are specified, the table is created,
    # but the list of columns (the last of the preprocessed args)
    # needs to be overriden with the current column
    if preprocessed_args:
        output = preprocessed_args
        output = [output[0], output[1], col]
    else:
        output = preprocesser(table, col)

    for k, v in zip(keys, output):
        args[k] = v

    if is_list:
        # the col is needed for proper hover text, but only if
        # there's a list
        args["current_col"] = col

    return generate_figure(
        draw=draw,
        call_args=args,
        trace_generator=trace_generator,
    )


def preprocess_and_layer(
        preprocesser: callable,
        draw: callable,
        args: dict[str, any],
        var: str,
        orientation: str = None,
        is_hist: bool = False,
) -> DeephavenFigure:
    """Given a preprocessing function, a draw function, and several
    columns, layer up the resulting figures

    Args:
      preprocesser: callable: A function that takes a table, list of cols
        and returns a tuple that contains
        (new table, first data columnn, second data column)
      draw: callable: A draw function, generally from plotly express
      args: dict[str, any]: The args to pass to figure creation
      var: str: Which var to map to the first column. If "x", then the
        preprocessor output is mapped to table, x, y. If "y" then preprocessor
        output is mapped to table, y, x.
      orientation: str:  (Default value = None)
        optional orientation if it is needed
      is_hist: bool:  (Default value = False)
        If true, the figure is a histogram and requires custom

    Returns:
      DeephavenFigure: The resulting DeephavenFigure

    """

    cols = args[var]
    # to mirror px, list_var_axis_name and legend should only be used when cols
    # are a list (regardless of length)
    is_list = isinstance(cols, list)
    cols = cols if is_list else [cols]
    keys = ["table", "x", "y"] if var == "x" else ["table", "y", "x"]
    table = args["table"]
    figs = []
    trace_generator = None
    has_color = None

    # the var is needed for proper hover text
    args["current_var"] = var

    if not args.get("color_discrete_sequence_marker"):
        # the colors need to match the plotly qualitative colors so they can be
        # overriden, but has_color should be false as the color was not
        # specified by the user
        has_color = False
        args["color_discrete_sequence_marker"] = px.colors.qualitative.Plotly

    if orientation:
        args["orientation"] = orientation

    if is_hist:
        # histograms generate one table with all info
        create_fig = partial(
            preprocessed_fig,
            preprocessed_args=preprocesser(table, cols)
        )
    else:
        # pivot vars need to be calculated here to be shared between layers
        args["pivot_vars"] = get_unique_names(table, ["variable", "value"])

        create_fig = partial(
            preprocessed_fig,
            preprocesser=preprocesser,
            table=table
        )

    for i, col in enumerate(cols):

        new_fig = create_fig(draw, keys, args, is_list, trace_generator, col)
        # offsetgroup is needed mostly to prevent spacing issues in
        # marginals
        # not setting the offsetgroup and having both marginals set to box,
        # violin, etc. leads to extra spacing in each marginal
        # offsetgroup needs to be unique within the subchart as columns
        # could have the same name
        new_fig.fig.update_traces(offsetgroup=f"{col}{i}")
        figs.append(new_fig)

        if not trace_generator:
            trace_generator = figs[0].trace_generator

    layered = layer(*figs, which_layout=0)

    if has_color is False:
        layered.has_color = False

    if is_list:
        layered.fig.update_layout(legend_tracegroupgap=0)
    else:
        layered.fig.update_layout(showlegend=False)

    return layered


def calculate_mode(
        base_mode: str,
        args: dict[str, any]
) -> str:
    """Calculate the mode of the traces based on the arguments

    Args:
      base_mode: The mode that this trace definitely has, either lines or markers
      args: The args to use to figure out the mode
      base_mode: str: 
      args: dict[str: 
      any]: 

    Returns:
      The mode. Some combination of markers, lines, text, joined by '+'.

    """
    modes = [base_mode]
    if base_mode == "lines" and any([
        args.get("markers", None),
        args.get("symbol", None),
        args.get("symbol_sequence", None),
        args.get("text", None)]
    ):
        modes.append("markers")
    if args.get("text", None):
        modes.append("text")
    return "+".join(modes)


def append_suffixes(
        args,
        prefixes,
        sync_dict
):
    for arg in args:
        for prefix in prefixes:
            if arg in sync_dict.d:
                sync_dict.d[f"{arg}_{prefix}"] = sync_dict.will_pop(arg)


def apply_args_groups(
        args: dict[str, any],
        groups: set[str]
) -> None:
    """Transform args depending on groups

    Args:
      args: dict[str, any]: A dictionary of args to transform
      groups: set[str]: A set of groups used to transform the args

    """
    groups = groups if isinstance(groups, set) else {groups}

    sync_dict = SyncDict(args)

    if "scatter" in groups:
        args["mode"] = calculate_mode("markers", args)
        append_suffixes(
            ["color_discrete_sequence", "attached_color"],
            ["marker"],
            sync_dict
        )

    if "line" in groups:
        args["mode"] = calculate_mode("lines", args)
        append_suffixes(
            ["color_discrete_sequence", "attached_color"],
            ["marker", "line"],
            sync_dict
        )

    if "ecdf" in groups:
        # ecdf should be forced to lines even if both "lines" and "markers" are False
        base_mode = "lines" if args["lines"] or not args["markers"] else "markers"
        args["mode"] = calculate_mode(base_mode, args)
        append_suffixes(
            ["color_discrete_sequence", "attached_color"],
            ["marker", "line"],
            sync_dict
        )

    if 'scene' in groups:
        for arg in ["range_x", "range_y", "range_z", "log_x", "log_y", "log_z"]:
            args[arg + '_scene'] = args.pop(arg)

    if 'bar' in groups:
        append_suffixes(
            ["color_discrete_sequence", "attached_color"],
            ["marker"],
            sync_dict
        )
        append_suffixes(
            ["pattern_shape_sequence", "attached_pattern_shape"],
            ["bar"],
            sync_dict
        )

    if 'marker' in groups:
        append_suffixes(
            ["color_discrete_sequence", "attached_color"],
            ["marker"],
            sync_dict
        )

    if 'always_attached' in groups:
        append_suffixes(
            ["color_discrete_sequence", "attached_color",
             "pattern_shape_sequence", "attached_pattern_shape"],
            ["markers"],
            sync_dict
        )

    if 'area' in groups:
        append_suffixes(
            ["pattern_shape_sequence", "attached_pattern_shape"],
            ["area"],
            sync_dict
        )

    if "webgl" in groups:
        args["render_mode"] = "webgl"

    sync_dict.sync_pop()


def process_args(
        args: dict[str, any],
        groups: set[str] = None,
        add: dict[str, any] = None,
        pop: list[str] = None,
        remap: dict[str, str] = None,
        px_func=Callable
) -> DeephavenFigure:

    """Process the provided args

    Args:
      args: dict[str, any]: A dictionary of args to process
      groups: set[str]:  (Default value = None)
        A set of groups that apply transformations to the args
      add: dict[str, any] (Default value = None)
        A dictionary to add to the args
      pop: list[str]:  (Default value = None)
        A list of keys to remove from the args
      remap: dict[str, str]:  (Default value = None)
        A dictionary mapping of keys to keys

    Returns:
      partial: The update wrapper, based on the update function in the args

    """
    validate_common_args(args)

    marg_args = None
    if any(arg in args for arg in ["marginal" "marginal_x", "marginal_y"]):
        marg_args = get_marg_args(args)
        if "marginal" in args:
            var = "x" if args["x"] else "y"
            args[f"marginal_{var}"] = args.pop("marginal")

    draw_figure = partial(generate_figure, draw=px_func)
    partitioned = PartitionManager(args, draw_figure, groups, marg_args, attach_marginals)

    apply_args_groups(args, groups)

    if add:
        args.update(add)

    if pop:
        for arg in pop:
            args.pop(arg)

    if remap:
        for old_arg, new_arg in remap.items():
            args[new_arg] = args.pop(old_arg)

    update_wrapper = partial(
        unsafe_figure_update_wrapper,
        args.pop("unsafe_update_figure")
    )

    return update_wrapper(partitioned.create_figure())


class SyncDict:
    """A dictionary wrapper that will queue up keys to remove and remove them
    all at once


    Args:
      d: dict: the dictionary to wrap


    """

    def __init__(
            self,
            d: dict
    ):
        self.d = d
        self.pop_set = set()

    def will_pop(
            self,
            key: any
    ) -> any:
        """Add a key to the set of keys that will eventually be popped

        Args:
          key: The key to add to the set

        Returns:
          The value associated with the key that will be popped

        """
        self.pop_set.add(key)
        return self.d[key]

    def sync_pop(
            self
    ):
        """Pop all elements from the dictionary that have been added to the pop
        set

        """
        for k in self.pop_set:
            self.d.pop(k)

def set_shared_defaults(args):
    args["by_vars"] = args.get("by_vars", ("color",))
    args["unsafe_update_figure"] = args.get("unsafe_update_figure", default_callback)
    args["x"] = args.get("x", None)
    args["y"] = args.get("y", None)

def shared_violin(
        **args
) -> DeephavenFigure:
    set_shared_defaults(args)
    args["violinmode"] = args.get("violinmode", "group")
    args["points"] = args.get("points", "outliers")
    return process_args(args, {"marker", "preprocess_violin", "supports_lists"}, px_func=px.violin)


def shared_box(
        **args
) -> DeephavenFigure:
    set_shared_defaults(args)
    args["boxmode"] = args.get("boxmode", "group")
    args["points"] = args.get("points", "outliers")
    return process_args(args, {"marker", "preprocess_violin", "supports_lists"}, px_func=px.box)


def shared_strip(
        **args
) -> DeephavenFigure:
    set_shared_defaults(args)
    args["stripmode"] = args.get("stripmode", "group")
    args["points"] = args.get("points", "outliers")
    return process_args(args, {"marker", "preprocess_violin", "supports_lists"}, px_func=px.strip)


def shared_histogram(
        **args
) -> DeephavenFigure:
    set_shared_defaults(args)
    args["barmode"] = args.get("stripmode", "relative")
    args["nbins"] = args.get("nbins", 10)
    args["histfunc"] = args.get("histfunc", "count")
    args["histnorm"] = args.get("histnorm", None)
    args["cumulative"] = args.get("cumulative", False)
    args["range_bins"] = args.get("range_bins", None)
    args["barnorm"] = args.get("barnorm", None)

    args["bargap"] = 0
    args["hist_val_name"] = args["histfunc"]

    return process_args(
        args, {"bar", "preprocess_hist", "supports_lists"}, px_func=px.bar
    )

def marginal_axis_update(
        matches: str = None
) -> dict[str, any]:
    """Create an update to a marginal axis so it hides much of the axis info

    Args:
      matches: str:  (Default value = None)
        An optional axis, such as x, y, x2 to match this axis to

    Returns:
      dict[str, any]: The update

    """
    return {
        "matches": matches,
        "title": {},
        'showgrid': False,
        'showline': False,
        'showticklabels': False,
        'ticks': ''
    }


def create_marginal(
        marginal: str,
        args: dict[str, any],
        which: str
) -> DeephavenFigure:
    """Create a marginal figure

    Args:
      marginal: str: The type of marginal; histogram, violin, rug, box
      args: dict[str, any] The args to pass to the marginal function
      style: dict[str, any] The style args to pass to the marginal function
      which: str: x or y depending on which marginal is being drawn

    Returns:
      DeephavenFigure: The marginal figure

    """
    if marginal == "histogram":
        args["barmode"] = "overlay"
    marginal_map = {
        "histogram": shared_histogram,
        "violin": shared_violin,
        "rug": shared_strip,
        "box": shared_box
    }

    fig_marg = marginal_map[marginal](**args)
    fig_marg.fig.update_traces(showlegend=False)

    if marginal == "rug":
        symbol = "line-ns-open" if which == "x" else "line-ew-open"
        fig_marg.fig.update_traces(marker_symbol=symbol, jitter=0)

    return fig_marg


def attach_marginals(
        fig: DeephavenFigure,
        args: dict[str, any],
        marginal_x: str = None,
        marginal_y: str = None
) -> DeephavenFigure:
    """Create and attach marginals to the provided figure.

    Args:
      fig: DeephavenFigure: The figure to attach marginals to
      args: dict[str, any]: The data args to use
      marginal_x: str:  (Default value = None)
        The type of marginal; histogram, violin, rug, box
      marginal_y: str:  (Default value = None)
        The type of marginal; histogram, violin, rug, box

    Returns:
      DeephavenFigure: The figure, with marginals attached if marginal_x/y was
        specified

    """
    figs = [fig]

    data = {
        "x": args.pop("x"),
        "y": args.pop("y")
    }

    specs = []

    if marginal_x:
        args = {
            **args,
            "x": data["x"]
        }
        print(args)
        figs.append(create_marginal(marginal_x, args, "x"))
        specs = [
            {'y': [0, 0.74]},
            {
                'y': [0.75, 1],
                "xaxis_update": marginal_axis_update("x"),
                "yaxis_update": marginal_axis_update(),
            },
        ]

    if marginal_y:
        args = {
            **args,
            "y": data["y"]
        }
        figs.append(create_marginal(marginal_y, args, "y"))
        if specs:
            specs[0]["x"] = [0, 0.745]
            specs[1]["x"] = [0, 0.745]
            specs.append(
                {
                    'x': [0.75, 1], 'y': [0, 0.74],
                    "yaxis_update": marginal_axis_update("y"),
                    "xaxis_update": marginal_axis_update(),
                })

        else:
            specs = [
                {'x': [0, 0.745]},
                {'x': [0.75, 1],
                 "yaxis_update": marginal_axis_update("y"),
                 "xaxis_update": marginal_axis_update(),
                 },
            ]

    return layer(*figs, specs=specs) if specs else fig


def get_marg_args(
        args: dict[str, any]
) -> dict[str, any]:
    """Copy the required args into data and style for marginal creation

    Args:
      args: dict[str, any]: The args to split

    Returns:
      tuple[dict[str, any], dict[str, any]]: A tuple of
        (data args dict, style args dict)

    """
    marg_args = {
        "x", "y", "by", "by_vars", "color", "hover_name", "labels",
        "color_discrete_sequence", "color_discrete_map", "nbins"
    }

    new_args = {}

    for arg in marg_args:
        if arg in args:
            new_args[arg] = args[arg]

    return new_args
