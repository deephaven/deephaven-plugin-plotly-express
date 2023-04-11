from typing import Callable

from plotly import express as px

from deephaven.table import Table

from ._private_utils import default_callback, validate_common_args, remap_scene_args
from ..deephaven_figure import generate_figure, DeephavenFigure


# todo: size sequence
def scatter(
        table: Table = None,
        x: str | list[str] = None,
        y: str | list[str] = None,
        error_x: str | list[str] = None,
        error_x_minus: str | list[str] = None,
        error_y: str | list[str] = None,
        error_y_minus: str | list[str] = None,
        # labels: dict[str, str] = None
        color_discrete_sequence: list[str] = None,
        symbol_sequence: list[str] = None,
        xaxis_sequence: list[int] = None,
        yaxis_sequence: list[int] = None,
        yaxis_title_sequence: list[str] = None,
        xaxis_title_sequence: list[str] = None,
        opacity: float = None,
        # marginal_x: str = None, #not supported at the moment, will probably be slow
        # marginal_y: str = None, #with lots of data
        log_x: bool | list[bool] = False,
        log_y: bool | list[bool] = False,
        range_x: list[int] | list[list[int]] = None,
        range_y: list[int] | list[list[int]] = None,
        title: str = None,
        template: str = None,
        callback: Callable = default_callback
) -> DeephavenFigure:
    """
    Returns a scatter chart

    :param table: A table to pull data from.
    :param x: A column or list of columns that contain x-axis values.
    :param y: A column or list of columns that contain y-axis values.
    :param error_x: A column or list of columns with x error bar
    values. These form the error bars in both the positive and negative
    direction if error_x_minus is not specified, and the error bars in only the
    positive direction if error_x_minus is specified. None can be used to
    specify no error bars on the corresponding series.
    :param error_x_minus: A column or list of columns with x error
    bar values. These form the error bars in the negative direction, and are
    ignored if error_x is not specified.
    :param error_y: A column or list of columns with x error bar
    values. These form the error bars in both the positive and negative
    direction if error_y_minus is not specified, and the error bars in only the
    positive direction if error_y_minus is specified. None can be used to
    specify no error bars on the corresponding series.
    :param error_y_minus: A column or list of columns with x error
    bar values. These form the error bars in the negative direction, and are
    ignored if error_y is not specified.
    :param color_discrete_sequence: A list of colors to sequentially apply to
    the series. The colors loop, so if there are more series than colors,
    colors will be reused.
    :param symbol_sequence: A list of symbols to sequentially apply to the
    series. The symbols loop, so if there are more series than symbols, symbols
    will be reused.
    :param xaxis_sequence: A list of x axes to assign series to. Odd numbers
    starting with 1 are created on the bottom x axis and even numbers starting
    with 2 are created on the top x axis. Axes are created up
    to the maximum number specified. The axes loop, so if there are more series
    than axes, axes will be reused.
    :param yaxis_sequence: A list of y axes to assign series to. Odd numbers
    starting with 1 are created on the left y axis and even numbers starting
    with 2 are created on the top y axis. Axes are created up
    to the maximum number specified. The axes loop, so if there are more series
    than axes, axes will be reused.
    :param yaxis_title_sequence: A list of titles to sequentially apply to the
    y axes. The titles do not loop.
    :param xaxis_title_sequence: A list of titles to sequentially apply to the
    x axes. The titles do not loop.
    :param opacity: Opacity to apply to all points. 0 is completely transparent
    and 1 is completely opaque.
    :param log_x: Default False. A boolean or list of booleans that specify if
    the corresponding axis is a log axis or not. The booleans loop, so if there
    are more series than booleans, booleans will be reused.
    :param log_y: Default False. A boolean or list of booleans that specify if
    the corresponding axis is a log axis or not. The booleans loop, so if there
    are more series than booleans, booleans will be reused.
    :param range_x: A list of two numbers or a list of lists of two numbers
    that specify the range of the x axes. None can be specified for no range
    The ranges loop, so if there are more axes than ranges, ranges will
    be reused.
    :param range_y: A list of two numbers or a list of lists of two numbers
     that specify the range of the x axes. None can be specified for no range
    The ranges loop, so if there are more axes than ranges, ranges will
    be reused.
    :param title: The title of the chart
    :param template: The template for the chart.
    :param callback: A callback function that takes a figure as an argument and
    returns a figure. Used to add any custom changes to the underlying plotly
    figure. Note that the existing data traces should not be removed.
    :return: A DeephavenFigure that contains the scatter chart
    """
    render_mode = "webgl"
    args = locals()
    args["color_discrete_sequence_marker"] = args.pop("color_discrete_sequence")

    validate_common_args(args)

    fig = generate_figure(draw=px.scatter, call_args=args)

    return fig


def scatter_3d(
        table: Table = None,
        x: str = None,
        y: str = None,
        z: str = None,
        error_x: str | list[str] = None,
        error_x_minus: str | list[str] = None,
        error_y: str | list[str] = None,
        error_y_minus: str | list[str] = None,
        error_z: str | list[str] = None,
        error_z_minus: str | list[str] = None,
        color_discrete_sequence: list[str] = None,
        symbol_sequence: list[str] = None,
        opacity: float = None,
        log_x: bool = False,
        log_y: bool = False,
        log_z: bool = False,
        range_x: list[int] = None,
        range_y: list[int] = None,
        range_z: list[int] = None,
        title: str = None,
        template: str = None,
        callback: Callable = default_callback
) -> DeephavenFigure:
    """
    Returns a 3D scatter chart

    :param table: A table to pull data from.
    :param x: A column that contains x-axis values.
    :param y: A column that contains y-axis values.
    :param z: A column that contains z-axis values.
    :param error_x: A column with x error bar values. These form the error
    bars in both the positive and negative direction if error_x_minus is not
    specified, and the error bars in only the positive direction if
    error_x_minus is specified.
    :param error_x_minus: A column with x error bar values. These form
    the error bars in the negative direction, and are ignored if error_x is not
    specified.
    :param error_y: A column with x error bar values. These form the error
    bars in both the positive and negative direction if error_z_minus is not
    specified, and the error bars in only the positive direction if
    error_x_minus is specified.
    :param error_y_minus: A column with y error bar values. These form
    the error bars in the negative direction, and are ignored if error_x is not
    specified.
    :param error_z: A column with x error bar values. These form the error
    bars in both the positive and negative direction if error_z_minus is not
    specified, and the error bars in only the positive direction if
    error_x_minus is specified.
    :param error_z_minus: A column with z error bar values. These form
    the error bars in the negative direction, and are ignored if error_x is not
    specified.
    :param color_discrete_sequence: A list of colors to sequentially apply to
    the series. The colors loop, so if there are more series than colors,
    colors will be reused.
    :param symbol_sequence: A list of symbols to sequentially apply to the
    series. The symbols loop, so if there are more series than symbols, symbols
    will be reused.
    :param opacity: Opacity to apply to all points. 0 is completely transparent
    and 1 is completely opaque.
    :param log_x: A boolean that specifies if the corresponding axis is a log
    axis or not.
    :param log_y: A boolean that specifies if the corresponding axis is a log
    axis or not.
    :param log_z: A boolean that specifies if the corresponding axis is a log
    axis or not.
    :param range_x: A list of two numbers that specify the range of the x axis.
    :param range_y: A list of two numbers that specify the range of the y axis.
    :param range_z: A list of two numbers that specify the range of the z axis.
    :param title: The title of the chart.
    :param template: The template for the chart.
    :param callback: A callback function that takes a figure as an argument and
    returns a figure. Used to add any custom changes to the underlying plotly
    figure. Note that the existing data traces should not be removed.
    :return: A DeephavenFigure that contains the 3D scatter chart
    """
    args = locals()
    args["color_discrete_sequence_marker"] = args.pop("color_discrete_sequence")

    remap_scene_args(args)

    validate_common_args(args)

    return generate_figure(draw=px.scatter_3d, call_args=args)


def scatter_polar(
        table: Table = None,
        r: str = None,
        theta: str = None,
        color_discrete_sequence: list[str] = None,
        symbol_sequence: list[str] = None,
        opacity: float = None,
        direction: str = 'clockwise',
        start_angle: int = 90,
        range_r: list[int] = None,
        range_theta: list[int] = None,
        log_r: bool = False,
        title: str = None,
        template: str = None,
        callback: Callable = default_callback
) -> DeephavenFigure:
    """
    Returns a polar scatter chart

    :param table: A table to pull data from.
    :param r: A column that contains r values.
    :param theta: A column that contains theta values.
    :param color_discrete_sequence: A list of colors to sequentially apply to
    the series. The colors loop, so if there are more series than colors,
    colors will be reused.
    :param symbol_sequence: A list of symbols to sequentially apply to the
    series. The symbols loop, so if there are more series than symbols, symbols
    will be reused.
    :param opacity: Opacity to apply to all points. 0 is completely transparent
    and 1 is completely opaque.
    :param direction: Which direction points are drawn. Default clockwise.
    :param start_angle: Sets start angle. Default 90.
    :param range_r: A list of two numbers that specify the range of r.
    :param range_theta: A list of two numbers that specify the range of theta.
    :param log_r: A boolean that specifies if the corresponding axis is a log
    axis or not.
    :param title: The title of the chart.
    :param template: The template for the chart.
    :param callback: A callback function that takes a figure as an argument and
    returns a figure. Used to add any custom changes to the underlying plotly
    figure. Note that the existing data traces should not be removed.
    :return: A DeephavenFigure that contains the polar scatter chart
    """
    render_mode = "webgl"
    args = locals()
    args["color_discrete_sequence_marker"] = args.pop("color_discrete_sequence")

    validate_common_args(args)

    return generate_figure(draw=px.scatter_polar, call_args=args)


def scatter_ternary(
        table: Table = None,
        a: str = None,
        b: str = None,
        c: str = None,
        color_discrete_sequence: list[str] = None,
        symbol_sequence: list[str] = None,
        opacity: float = None,
        title: str = None,
        template: str = None,
        callback: Callable = default_callback
) -> DeephavenFigure:
    """
    Returns a ternary scatter chart

    :param table: A table to pull data from.
    :param a: A column that contains a-axis values.
    :param b: A column that contains b-axis values.
    :param c: A column that contains c-axis values.
    :param color_discrete_sequence: A list of colors to sequentially apply to
    the series. The colors loop, so if there are more series than colors,
    colors will be reused.
    :param symbol_sequence: A list of symbols to sequentially apply to the
    series. The symbols loop, so if there are more series than symbols, symbols
    will be reused.
    :param opacity: Opacity to apply to all points. 0 is completely transparent
    and 1 is completely opaque.
    :param title: The title of the chart.
    :param template: The template for the chart.
    :param callback: A callback function that takes a figure as an argument and
    returns a figure. Used to add any custom changes to the underlying plotly
    figure. Note that the existing data traces should not be removed.
    :return: A DeephavenFigure that contains the ternary scatter chart
    """
    args = locals()
    args["color_discrete_sequence_marker"] = args.pop("color_discrete_sequence")

    validate_common_args(args)

    return generate_figure(draw=px.scatter_ternary, call_args=args)


def _scatter_matrix():
    # todo: not yet implemented
    pass