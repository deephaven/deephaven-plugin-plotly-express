from __future__ import annotations

from functools import partial
from typing import Callable, Any
from collections.abc import Generator

import plotly.express as px
from plotly.graph_objects import Figure

from deephaven.table import Table
from deephaven import pandas as dhpd

from ..deephaven_figure import generate_figure, DeephavenFigure
from ..shared import get_unique_names

PARTITION_ARGS = {
    "plot_by": None,
    "line_group": None,  # this will still use the discrete
    "color": ("color_discrete_sequence", "color_discrete_map"),
    "pattern_shape": ("pattern_shape_sequence", "pattern_shape_map"),
    "symbol": ("symbol_sequence", "symbol_map")
}


def default_callback(
        fig: Figure
) -> Figure:
    """A default callback that returns the passed fig

    Args:
      fig: Figure: The input figure

    Returns:
      Figure: The same figure

    """
    return fig


def unsafe_figure_update_wrapper(
        unsafe_figure_update: callable,
        dh_fig: DeephavenFigure
) -> DeephavenFigure:
    """Wrap the callback to be applied last before a figure is returned

    Args:
      unsafe_figure_update: The function to call on the plotly figure
      dh_fig: DeephavenFigure: The DeephavenFigure to update

    Returns:
      DeephavenFigure: The resulting DeephavenFigure

    """
    # allow either returning a new fig or not from callback
    new_fig = unsafe_figure_update(dh_fig.fig)
    dh_fig.fig = new_fig if new_fig else dh_fig.fig
    return dh_fig


def normalize_position(
        position: float,
        chart_start: float,
        chart_range: float
) -> float:
    """Normalize a position so that it falls between 0 and 1 (inclusive)

    Args:
      position: float: The current position
      chart_start: float: The start of the domain the existing chart has
      chart_range: float: The range the existing chart has

    Returns:
      float: The normalized position
    """
    return (position - chart_start) / chart_range


def get_new_positions(
        new_domain: list[float],
        positions: list[float],
        chart_domain: list[float]
) -> list[float]:
    """Get positions within the new domain of an arbitrary list of positions
    The positions will first be normalized to fall between 0 and 1 inclusive
    using the current chart_domain. Then, the positions are mapped onto
    new_domain.
    For example, if a position is at 0.5, chart_domain is [0, 1] and new_domain
    is [0, 0.6], the new position is 0.3.

    Args:
      new_domain: list[float]: The new domain to map the points to
      positions: list[float]: The current positions of the points
      chart_domain: list[float]: The current domain of the whole chart

    Returns:
      list[float]: The new positions
    """
    if not isinstance(positions, list):
        positions = [positions]
    new_positions = []
    new_range = new_domain[1] - new_domain[0]
    for position in positions:
        chart_range = chart_domain[1] - chart_domain[0]
        normalized = normalize_position(position, chart_domain[0], chart_range)
        new_position = new_domain[0] + normalized * new_range
        new_positions.append(new_position)
    return new_positions


def resize_domain(
        obj: dict,
        new_domain: dict[str, list[float]]
) -> None:
    """Resize the domain of the given object

    Args:
      obj: dict: The object to resize. It should have a "domain" key that
        references a dict that has "x" and "y" keys.
      new_domain: dict[str, list[float]]: The new domain the map the figure to.
        Contains keys of x and y and values of domains, such as [0,0.5]

    """
    new_domain_x = new_domain.get("x", None)
    new_domain_y = new_domain.get("y", None)
    obj_domain_x = obj["domain"]["x"]
    obj_domain_y = obj["domain"]["y"]
    domain_update = {}
    try:
        # assuming that the whole chart spans [0,1] in both directions as
        # passing a subplot is currently not supported
        if new_domain_x:
            domain_update["x"] = get_new_positions(new_domain_x, obj_domain_x, [0, 1])
        if new_domain_y:
            domain_update["y"] = get_new_positions(new_domain_y, obj_domain_y, [0, 1])
        if domain_update:
            obj.update({"domain": domain_update})
    except ValueError:
        # the obj might not have a domain to resize
        pass


def resize_xy_axis(
        axis: dict,
        new_domain: dict[str, list[float]],
        which: str
) -> None:
    """Resize either an x or y axis.

    Args:
      axis: dict: The axis object to resize.
      new_domain: dict[str, list[float]]: The new domain the map the figure to.
        Contains keys of x and y and values of domains, such as [0,0.5]
      which: str: Either "x" or "y"

    """
    new_domain_x = new_domain.get("x", None)
    new_domain_y = new_domain.get("y", None)
    # the existing domain is assumed to be 0, 1 if not set
    axis_domain = axis.get("domain", [0, 1])
    axis_position = axis.get("position", None)
    axis_update = {}
    try:
        if which == "x":
            if new_domain_x:
                axis_update["domain"] = get_new_positions(new_domain_x, axis_domain, [0, 1])
            if new_domain_y and axis_position is not None:
                axis_update["position"] = get_new_positions(new_domain_y, axis_position, [0, 1])[0]
        else:
            if new_domain_y:
                axis_update["domain"] = get_new_positions(new_domain_y, axis_domain, [0, 1])
            if new_domain_x and axis_position is not None:
                axis_update["position"] = get_new_positions(new_domain_x, axis_position, [0, 1])[0]

        axis.update(axis_update)
    except ValueError:
        # the obj might not have an axis to resize
        pass


def reassign_axes(
        trace: dict,
        axes_remapping: dict[str, str]
) -> None:
    """Update the trace with its new axes using with the remapping

    Args:
      trace: dict: The trace to remap axes within
      axes_remapping: dict[str, str]: The mapping of old to new axes

    """
    if 'xaxis' in trace:
        trace.update(xaxis=axes_remapping[trace['xaxis']])

    if 'yaxis' in trace:
        trace.update(yaxis=axes_remapping[trace['yaxis']])

    if 'scene' in trace:
        trace.update(scene=axes_remapping[trace['scene']])

    if 'subplot' in trace:
        trace.update(subplot=axes_remapping[trace['subplot']])

    if 'ternary' in trace:
        trace.update(ternary=axes_remapping[trace['ternary']])


def reassign_attributes(
        axis: dict,
        axes_remapping: dict[str, str]
) -> None:
    """Reassign attributes of a layout object using with the remapping

    Args:
      axis: dict: The axis object to remap attributes from
      axes_remapping: dict[str, str]: The mapping of old to new axes

    """
    # anchor can also be free, which does not need to be modified
    if 'anchor' in axis and axis['anchor'] in axes_remapping:
        axis.update(anchor=axes_remapping[axis['anchor']])

    if 'overlaying' in axis and axis['overlaying'] in axes_remapping:
        axis.update(overlaying=axes_remapping[axis['overlaying']])


def resize_axis(
        type_: str,
        old_axis: str,
        axis: dict,
        num: str,
        new_domain: dict[str, list[float]]
) -> tuple[str, str, str]:
    """Maps the specified axis to new_domain and returns info to help remap axes

    Args:
      type_: str: The type of axis to resize
      old_axis: str: The old axis name
      axis: dict: The axis object to resize
      num: str: The number (possibly empty) of this axis within the new chart
      new_domain: dict[str, list[float]]: The new domain the map the figure to.
        Contains keys of x and y and values of domains, such as [0,0.5]

    Returns:
      tuple[str, str, str]: A tuple of new axis name, old axis name (for trace
        remapping), new axis name (for trace remapping). The new axis name
        isn't always the same within the trace as it is in the layout (such as
        in the case of xaxis or yaxis), hence the need for both of the names.

    """
    new_axis = f"{type_}{num}"
    if type_ == 'xaxis' or type_ == 'yaxis':
        which = type_[0]
        resize_xy_axis(axis, new_domain, which)
        old_trace_axis = old_axis.replace(type_, which)
        return new_axis, old_trace_axis, f"{which}{num}"
    else:
        resize_domain(axis, new_domain)
        return new_axis, old_axis, new_axis


def get_axis_update(
        spec: dict[str, any],
        type_: str
) -> dict[str, any] | None:
    """Retrieve an axis update from the spec

    Args:
      spec: dict[str, any]: The full spec object
      type_: str: The type of axis to retrieve the update of

    Returns:
      dict[str, any] | None: A dictionary of updates to make to the x or y-axis

    """
    if 'xaxis_update' in spec and type_ == "xaxis":
        return spec["xaxis_update"]
    if 'yaxis_update' in spec and type_ == "yaxis":
        return spec["yaxis_update"]
    return {}


def match_axes(
        type_: str,
        spec: dict[str, str | bool | list[float]],
        matches_axes: dict[Any, dict[int, str]],
        axis_indices: dict[str, int],
        new_trace_axis: str
) -> dict[str, str]:
    """
    Create an update to the axis if this axis matches another axis

    Args:
        type_: str: The type of the axis
        spec: dict[str, str | bool | list[float]]:
          The spec to retrieve matching axes from
        matches_axes: dict[Any, dict[int, str]]:
          A dictionary with keys that are unique per matching dictionary group.
          The value is a dictionary that maps an axis index to a specific
        axis_indices: dict[str, int]:
          The index of the axes within the figure
        new_trace_axis: str
          The new trace axes to add to matches_axes if there is
          not currently an axis at the index defined by axis_indices

    Returns:
        A dictionary with a key of "matches" and a value of the axis matched to
          if there is a dictionary to match to

    """
    match_axis_key = spec.get(f"matched_{type_}", None)
    axis_index = axis_indices.get(type_)

    if match_axis_key is not None:
        # add type to key to ensure uniqueness per axis
        match_axis_key = (match_axis_key, type_)
        if match_axis_key not in matches_axes:
            matches_axes[match_axis_key] = {}
        if not matches_axes[match_axis_key].get(axis_index, None):
            # this is the base axis to match to, so matches is not added
            matches_axes[match_axis_key][axis_index] = new_trace_axis
            return {}
        return {"matches": matches_axes[match_axis_key][axis_index]}

    return {}


def resize_fig(
        fig_data: dict,
        fig_layout: dict,
        spec: dict[str, str | bool | list[float]],
        new_axes_start: dict[str, int],
        matches_axes: dict[Any, dict[int, str]]
) -> tuple[dict, dict]:
    """Resize a figure into new_domain, reindexing with the indices specified in
    new_axes_start

    Args:
      fig_data: dict: The current figure data
      fig_layout: dict: The current figure layout
      spec: dict[str, str | bool | list[float]]:
        A dictionary that contains keys of "x" and "y"
        that have values that are lists of two floats from 0 to 1. The chart
        that corresponds with a domain will be resized to that domain. Either
        x or y can be excluded if only resizing on one axis. Can also specify
        xaxis_update or yaxis_update with a dictionary value to update all axes
        with that dict.
      new_axes_start: dict[str, int]: A dictionary containing the start of
        new indices to ensure there is no reindexing collisions
      matches_axes: dict[Any, dict[int, str]]:
          A dictionary with keys that are unique per matching dictionary group.
          The value is a dictionary that maps an axis index to a specific

    Returns:
      tuple[dict, dict]: A tuple of the new figure data, the new figure layout

    """
    if not spec:
        # if there is no spec, nothing needs to be done
        return fig_data, fig_layout

    axes_remapping = {}
    new_axes = {}
    old_axes = []
    type_ = None

    # keep track of the axis number within the chart so these axes can be
    # appropriately linked across charts
    axis_indices = {
        "xaxis": 0,
        "yaxis": 0
    }

    for name, obj in fig_layout.items():
        # todo: coloraxis; thickness, len, x, y
        if name.startswith("xaxis"):
            axis_indices["xaxis"] += 1
            type_ = "xaxis"

        elif name.startswith("yaxis"):
            axis_indices["yaxis"] += 1
            type_ = "yaxis"

        elif name.startswith("scene"):
            type_ = "scene"

        elif name.startswith("polar"):
            type_ = "polar"

        elif name.startswith("ternary"):
            type_ = "ternary"

        if type_:
            # axes start at 1, and the 1 is dropped
            num = "" if new_axes_start[type_] == 1 else new_axes_start[type_]
            new_axes_start[type_] += 1
            old_axes.append(name)

            update = get_axis_update(spec, type_)

            new_axis, old_trace_axis, new_trace_axis = resize_axis(
                type_, name, obj, num, spec)

            matches_update = match_axes(
                type_,
                spec,
                matches_axes,
                axis_indices,
                new_trace_axis
            )

            obj.update(**update, **matches_update)

            new_axes[new_axis] = obj
            axes_remapping[old_trace_axis] = new_trace_axis

        type_ = None

    if spec.get("wipe_layout", False):
        # completely wipe out the layout (and axes will be added back)
        fig_layout = {}
    else:
        # need to remove old axes in case there is one with a very high number
        for axis in old_axes:
            fig_layout.pop(axis)

    fig_layout.update(new_axes)

    for trace in fig_data:
        reassign_axes(trace, axes_remapping)
        if "domain" in trace:
            resize_domain(trace, spec)

    for axis in fig_layout.values():
        if isinstance(axis, dict):
            reassign_attributes(axis, axes_remapping)

    return fig_data, fig_layout


def fig_data_and_layout(
        fig: Figure,
        i: int,
        specs: list[dict[str, str | bool | list[float]]],
        which_layout: int,
        new_axes_start: dict[str, int],
        matches_axes: dict[Any, dict[int, str]]
) -> tuple[tuple | dict, dict]:
    """Get new data and layout for the specified figure

    Args:
      fig: Figure: The current figure
      i: int: The index of the figure, used for which_layout
      specs: list[dict[str, str | bool | list[float]]]:
        A list of dictionaries that contains keys of "x" and "y"
        that have values that are lists of two floats from 0 to 1. The chart
        that corresponds with a domain will be resized to that domain. Either
        x or y can be excluded if only resizing on one axis. Can also specify
        xaxis_update or yaxis_update with a dictionary value to update all axes
        with that dict.
      which_layout: int: None to layer layouts, or an index of which arg to
        take the layout from
      new_axes_start: dict[str, int]: A dict that keeps track of starting
       points when recreating axes
      matches_axes: dict[Any, dict[int, str]]:
          A dictionary with keys that are unique per matching dictionary group.
          The value is a dictionary that maps an axis index to a specific

    Returns:
      tuple[tuple | dict, dict]: A tuple of figure data, figure layout

    """
    if specs:
        return resize_fig(fig.to_dict()['data'], fig.to_dict()['layout'],
                          specs[i], new_axes_start, matches_axes)

    fig_layout = {}
    if which_layout is None or which_layout == i:
        fig_layout.update(fig.to_dict()['layout'])

    return fig.data, fig_layout


def layer(
        *figs: DeephavenFigure | Figure,
        which_layout: int = None,
        specs: list[dict[str, any]] = None,
        unsafe_update_figure: callable = default_callback
) -> DeephavenFigure:
    """Layers the provided figures. Be default, the layouts are sequentially
    applied, so the layouts of later figures will override the layouts of early
    figures.

    Args:
      *figs: DeephavenFigure | Figure: The charts to layer
      which_layout: int:  (Default value = None) None to layer layouts, or an
        index of which arg to take the layout from. Currently only valid if
        domains are not specified.
      specs: list[dict[str, str | bool | list[float]]]:
        A list of dictionaries that contains keys of "x" and "y"
        that have values that are lists of two floats from 0 to 1. The chart
        that corresponds with a domain will be resized to that domain. Either
        x or y can be excluded if only resizing on one axis.
        Can also specify "xaxis_update" or "yaxis_update" with a dictionary
        value to update all axes with that dict.
        Can also specify "matched_xaxis" or "matched_yaxis" to add this figure
        to a match group. All figures with the same value of this group will
        have matching axes.
      unsafe_update_figure: An update function that takes a plotly figure
        as an argument and optionally returns a plotly figure. If a figure is not
        returned, the plotly figure passed will be assumed to be the return value.
        Used to add any custom changes to the underlying plotly figure. Note that
        the existing data traces should not be removed. This may lead to unexpected
        behavior if traces are modified in a way that break data mappings.

    Returns:
      DeephavenFigure: The layered chart

    """
    if len(figs) == 0:
        raise ValueError("No figures provided to compose")

    new_data = []
    new_layout = {}
    new_data_mappings = []
    new_has_template = False
    new_has_color = False

    # when recreating axes, need to keep track of start of new axes
    new_axes_start = {
        "xaxis": 1,
        "yaxis": 1,
        "scene": 1,
        "polar": 1,
        "ternary": 1
    }

    matches_axes = {}

    for i, arg in enumerate(figs):
        if not arg:
            continue

        elif isinstance(arg, Figure):
            fig_data, fig_layout = fig_data_and_layout(
                arg, i, specs, which_layout, new_axes_start, matches_axes
            )

        elif isinstance(arg, DeephavenFigure):
            offset = len(new_data)
            if arg.has_subplots:
                raise NotImplementedError("Cannot currently add figure with subplots as a subplot")
            fig_data, fig_layout = fig_data_and_layout(
                arg.fig, i, specs, which_layout, new_axes_start, matches_axes
            )
            new_data_mappings += arg.copy_mappings(offset=offset)
            new_has_template = arg.has_template or new_has_template
            new_has_color = arg.has_color or new_has_color

        else:
            raise TypeError("All arguments must be of type Figure or DeephavenFigure")

        new_data += fig_data
        new_layout.update(fig_layout)

    new_fig = Figure(data=new_data, layout=new_layout)

    update_wrapper = partial(
        unsafe_figure_update_wrapper,
        unsafe_update_figure
    )

    # todo: this doesn't maintain call args, but that isn't currently needed
    return update_wrapper(
        DeephavenFigure(
            fig=new_fig,
            data_mappings=new_data_mappings,
            has_template=new_has_template,
            has_color=new_has_color,
            has_subplots=True if specs else False
        )
    )


def validate_common_args(
        args: dict
) -> None:
    """Validate common args amongst plots

    Args:
      args: dict: The args to validate

    """
    if not isinstance(args["table"], Table):
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


def append_prefixes(
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
        append_prefixes(
            ["color_discrete_sequence", "attached_color"],
            ["marker"],
            sync_dict
        )

    if "line" in groups:
        args["mode"] = calculate_mode("lines", args)
        append_prefixes(
            ["color_discrete_sequence", "attached_color"],
            ["marker", "line"],
            sync_dict
        )

    if "ecdf" in groups:
        # ecdf should be forced to lines even if both "lines" and "markers" are False
        base_mode = "lines" if args["lines"] or not args["markers"] else "markers"
        args["mode"] = calculate_mode(base_mode, args)
        append_prefixes(
            ["color_discrete_sequence", "attached_color"],
            ["marker", "line"],
            sync_dict
        )

    if 'scene' in groups:
        for arg in ["range_x", "range_y", "range_z", "log_x", "log_y", "log_z"]:
            args[arg + '_scene'] = args.pop(arg)

    if 'bar' in groups:
        append_prefixes(
            ["color_discrete_sequence", "attached_color"],
            ["marker"],
            sync_dict
        )
        append_prefixes(
            ["pattern_shape_sequence", "attached_pattern_shape"],
            ["bar"],
            sync_dict
        )

    if 'marker' in groups:
        append_prefixes(
            ["color_discrete_sequence", "attached_color"],
            ["marker"],
            sync_dict
        )

    if 'area' in groups:
        append_prefixes(
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
        remap: dict[str, str] = None
) -> partial:
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

    return update_wrapper


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


def get_partition_key_column_tuples(
        key_column_table, columns
):
    list_columns = []
    for column in columns:
        list_columns.append(key_column_table[column].tolist())

    return list(zip(*list_columns))


NUMERIC_TYPES = {
    "short",
    "int",
    "long",
    "float",
    "double",
}


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


def handle_plot_by_arg(
        args,
        arg,
        val
):
    numeric_cols = numeric_column_set(args["table"])
    plot_by_cols = args.get("plot_by", "cols")

    if arg == "color":
        map_ = "color_discrete_map"
        if map_ == "by":
            args["color_by"] = args.pop("color")
        elif map_ == "identity":
            args["attached_color"] = args.pop["attached_color"]
            # attached_color
        elif (isinstance(val, str) or len(val) == 1) and val in numeric_cols:
            # just keep the argument in place so it can be passed to plotly
            # express directly
            pass
        elif val:
            args["color_by"] = args.pop("color")
        elif plot_by_cols:
            # this needs to be last as setting "color "in any sense will override
            args["color_by"] = plot_by_cols

    elif arg == "size":
        # size is numer
        map_ = "size_map"

        if (isinstance(str, val) or len(val) == 1) and val in numeric_cols and map_ != "by":
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
        args
):
    partition_cols = set()
    partition_map = {}
    for arg, val in list(args.items()):
        if val and arg in PARTITION_ARGS:
            # partition_map[arg] = val
            arg_by, cols = handle_plot_by_arg(args, arg, val)
            if cols:
                partition_map[arg_by] = cols
                if isinstance(cols, list):
                    partition_cols.update([col for col in cols])
                else:
                    partition_cols.add(cols)

    # TODO:
    # cases: numeric, factor - color_by (controlled by "by" in map or plot_by)
    # numeric, non factor - color (passed to plotly express)
    # identity - attached_color
    # other, factor - color_by

    # TODO: size
    # numeric: attached_size
    # other: size_by (unless "by" is specified in map or plot_by)

    # TODO general: symbol, pattern
    # any type: plot_by
    # identity - attached_whatever

    if partition_cols:
        partitioned = args["table"].partition_by(list(partition_cols))
        key_column_table = dhpd.to_pandas(partitioned.table.select_distinct(partitioned.key_columns))
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
        return partitioned


def partition_generator(
        args, partitioned
):
    if partitioned:
        for table in partitioned.constituent_tables:
            args["table"] = table
            yield args
    else:
        yield args
