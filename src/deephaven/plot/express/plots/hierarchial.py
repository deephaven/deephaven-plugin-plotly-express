from __future__ import annotations

from plotly import express as px

from deephaven.table import Table

from ._private_utils import process_args
from ._update_wrapper import default_callback
from ..deephaven_figure import DeephavenFigure


def treemap(
        table: Table = None,
        names: str = None,
        values: str = None,
        parents: str = None,
        ids: str = None,
        color: str | list[str] = None,
        hover_name: str | list[str] = None,
        color_discrete_sequence: list[str] = None,
        color_discrete_map: dict[str, str] = None,
        color_continuous_scale=None,
        range_color=None,
        color_continuous_midpoint=None,
        labels: dict[str, str] = None,
        title: str = None,
        template: str = None,
        branchvalues: str = None,
        maxdepth: int = None,
        unsafe_update_figure: callable = default_callback
):
    """Returns a treemap chart

    Args:
      table: Table:  (Default value = None)
        A table to pull data from.
      names: str:  (Default value = None)
        The column containing names of the sections
      values: str:  (Default value = None)
        The column containing values of the sections
      parents: str:  (Default value = None)
        The column containing parents of the sections
      ids: str:  (Default value = None)
        The column containing ids of the sections. Unlike values, these
        must be unique. Values are used for ids if ids are not specified.
      hover_name: str | list[str]:  (Default value = None)
        A column or list of columns that contain names to bold in the hover
          tooltip.
      labels: dict[str, str]:  (Default value = None)
        A dictionary of labels mapping columns to new labels.
      title: str: (Default value = None)
        The title of the chart
      template: str:  (Default value = None)
        The template for the chart.
      branchvalues: str:  (Default value = None)
        Set to 'total' to take the value at a level to include
        all descendants and 'remainder' to the value as the remainder after
        subtracting leaf values.
      maxdepth: int:  (Default value = None)
        Sets the total number of visible levels. Set to -1 to
        render all levels.
      unsafe_update_figure: callable:  (Default value = default_callback)
        An update function that takes a plotly figure
        as an argument and optionally returns a plotly figure. If a figure is
        not returned, the plotly figure passed will be assumed to be the return
        value. Used to add any custom changes to the underlying plotly figure.
        Note that the existing data traces should not be removed. This may lead
        to unexpected behavior if traces are modified in a way that break data
        mappings.

    Returns:
      DeephavenFigure: A DeephavenFigure that contains the treemap chart

    """
    args = locals()

    return process_args(args, {"always_attached"}, px_func=px.treemap)


def sunburst(
        table: Table = None,
        names: str = None,
        values: str = None,
        parents: str = None,
        ids: str = None,
        color: str | list[str] = None,
        hover_name: str | list[str] = None,
        color_discrete_sequence: list[str] = None,
        color_discrete_map: dict[str, str] = None,
        color_continuous_scale=None,
        range_color=None,
        color_continuous_midpoint=None,
        labels: dict[str, str] = None,
        title: str = None,
        template: str = None,
        branchvalues: str = None,
        maxdepth: int = None,
        unsafe_update_figure: callable = default_callback
):
    """Returns a sunburst chart

    Args:
      table: Table:  (Default value = None)
        A table to pull data from.
      names: str:  (Default value = None)
        The column containing names of the sections
      values: str:  (Default value = None)
        The column containing values of the sections
      parents: str:  (Default value = None)
        The column containing parents of the sections
      ids: str:  (Default value = None)
        The column containing ids of the sections. Unlike values, these
        must be unique. Values are used for ids if ids are not specified.
      hover_name: str | list[str]:  (Default value = None)
        A column or list of columns that contain names to bold in the hover
          tooltip.
      labels: dict[str, str]:  (Default value = None)
        A dictionary of labels mapping columns to new labels.
      title: str: (Default value = None)
        The title of the chart
      template: str:  (Default value = None)
        The template for the chart.
      branchvalues: str:  (Default value = None)
        Set to 'total' to take the value at a level to include
        all descendants and 'remainder' to the value as the remainder after
        subtracting leaf values.
      maxdepth: int:  (Default value = None)
        Sets the total number of visible levels. Set to -1 to
        render all levels.
      unsafe_update_figure: callable:  (Default value = default_callback)
        An update function that takes a plotly figure
        as an argument and optionally returns a plotly figure. If a figure is
        not returned, the plotly figure passed will be assumed to be the return
        value. Used to add any custom changes to the underlying plotly figure.
        Note that the existing data traces should not be removed. This may lead
        to unexpected behavior if traces are modified in a way that break data
        mappings.

    Returns:
      A DeephavenFigure that contains the sunburst chart

    """
    args = locals()

    return process_args(args, {"always_attached"}, px_func=px.sunburst)


def icicle(
        table: Table = None,
        names: str = None,
        values: str = None,
        parents: str = None,
        ids: str = None,
        color: str | list[str] = None,
        hover_name: str | list[str] = None,
        color_discrete_sequence: list[str] = None,
        color_discrete_map: dict[str, str] = None,
        color_continuous_scale=None,
        range_color=None,
        color_continuous_midpoint=None,
        labels: dict[str, str] = None,
        title: str = None,
        template: str = None,
        branchvalues: str = None,
        maxdepth: int = None,
        unsafe_update_figure: callable = default_callback
):
    """Returns a icicle chart

    Args:
      table: Table:  (Default value = None)
        A table to pull data from.
      names: str:  (Default value = None)
        The column containing names of the sections
      values: str:  (Default value = None)
        The column containing values of the sections
      parents: str:  (Default value = None)
        The column containing parents of the sections
      ids: str:  (Default value = None)
        The column containing ids of the sections. Unlike values, these
        must be unique. Values are used for ids if ids are not specified.
      hover_name: str | list[str]:  (Default value = None)
        A column or list of columns that contain names to bold in the hover
          tooltip.
      labels: dict[str, str]:  (Default value = None)
        A dictionary of labels mapping columns to new labels.
      title: str: (Default value = None)
        The title of the chart
      template: str:  (Default value = None)
        The template for the chart.
      branchvalues: str:  (Default value = None)
        Set to 'total' to take the value at a level to include
        all descendants and 'remainder' to the value as the remainder after
        subtracting leaf values.
      maxdepth: int:  (Default value = None)
        Sets the total number of visible levels. Set to -1 to
        render all levels.
      unsafe_update_figure: callable:  (Default value = default_callback)
        An update function that takes a plotly figure
        as an argument and optionally returns a plotly figure. If a figure is
        not returned, the plotly figure passed will be assumed to be the return
        value. Used to add any custom changes to the underlying plotly figure.
        Note that the existing data traces should not be removed. This may lead
        to unexpected behavior if traces are modified in a way that break data
        mappings.

    Returns:
      A DeephavenFigure that contains the icicle chart

    """
    args = locals()

    return process_args(args, {"always_attached"}, px_func=px.icicle)


def funnel(
        table: Table = None,
        x: str | list[str] = None,
        y: str | list[str] = None,
        text: str | list[str] = None,
        hover_name: str | list[str] = None,
        labels: dict[str, str] = None,
        color: str | list[str] = None,
        color_discrete_sequence: list[str] = None,
        color_discrete_map: dict[str, str] = None,
        opacity: float = None,
        orientation: str = None,
        log_x: bool = False,
        log_y: bool = False,
        range_x: list[int] = None,
        range_y: list[int] = None,
        title: str = None,
        template: str = None,
        unsafe_update_figure: callable = default_callback
) -> DeephavenFigure:
    """Returns a funnel chart

    Args:
      table: Table:  (Default value = None)
        A table to pull data from.
      x: str | list[str]:  (Default value = None)
        A column or list of columns that contain x-axis values.
      y: str | list[str]:  (Default value = None)
        A column or list of columns that contain y-axis values.
      text: str | list[str]:  (Default value = None)
        A column or list of columns that contain text annotations.
      hover_name: str | list[str]:  (Default value = None)
        A column or list of columns that contain names to bold in the hover
          tooltip.
      labels: dict[str, str]:  (Default value = None)
        A dictionary of labels mapping columns to new labels.
      color_discrete_sequence: list[str]:  (Default value = None)
        A list of colors to sequentially apply to
        the series. The colors loop, so if there are more series than colors,
        colors will be reused.
      opacity: float:  (Default value = None)
        Opacity to apply to all markers. 0 is completely transparent
        and 1 is completely opaque.
      orientation: str:  (Default value = None)
        "h" for horizontal or "v" for vertical
      log_x: bool
        A boolean that specifies if the corresponding axis is a log
        axis or not.
      log_y: bool
        A boolean that specifies if the corresponding axis is a log
        axis or not.
      range_x: list[int]:  (Default value = None)
        A list of two numbers that specify the range of the x-axis.
      range_y: list[int]:  (Default value = None)
        A list of two numbers that specify the range of the y-axis.
      title: str: (Default value = None)
        The title of the chart
      template: str:  (Default value = None)
        The template for the chart.
      unsafe_update_figure: callable:  (Default value = default_callback)
        An update function that takes a plotly figure
        as an argument and optionally returns a plotly figure. If a figure is
        not returned, the plotly figure passed will be assumed to be the return
        value. Used to add any custom changes to the underlying plotly figure.
        Note that the existing data traces should not be removed. This may lead
        to unexpected behavior if traces are modified in a way that break data
        mappings.

    Returns:
      DeephavenFigure: A DeephavenFigure that contains the funnel chart

    """
    args = locals()

    return process_args(args, {"marker", "supports_lists"}, px_func=px.funnel)


def funnel_area(
        table: Table = None,
        names: str = None,
        values: str = None,
        hover_name: str = None,
        labels: dict[str, str] = None,
        color: str | list[str] = None,
        color_discrete_sequence: list[str] = None,
        color_discrete_map: dict[str, str] = None,
        title: str = None,
        template: str = None,
        opacity: float = None,
        aggregate: bool = True,
        unsafe_update_figure: callable = default_callback
):
    """Returns a funnel area chart

    Args:
      table: Table:  (Default value = None)
        A table to pull data from.
      names: str:  (Default value = None)
        The column containing names of the sections
      values: str:  (Default value = None)
        The column containing values of the sections
      hover_name: str | list[str]:  (Default value = None)
        A column that contain names to bold in the hover tooltip.
      labels: dict[str, str]:  (Default value = None)
        A dictionary of labels mapping columns to new labels.
      color_discrete_sequence: list[str]:  (Default value = None)
        A list of colors to sequentially apply to
        the series. The colors loop, so if there are more series than colors,
        colors will be reused.
      title: str: (Default value = None)
        The title of the chart
      template: str:  (Default value = None)
        The template for the chart.
      opacity: float:  (Default value = None)
        Opacity to apply to all markers. 0 is completely transparent
        and 1 is completely opaque.
      aggregate: bool:  (Default value = True)
        Default True, aggregate the table names by total values. Can
        be set to False if the table is already aggregated by name.
      opacity: Opacity to apply to all points. 0 is completely transparent
        and 1 is completely opaque.
      aggregate: Default True, aggregate the table names by total values. Can
        be set to False if the table is already aggregated by name.
      unsafe_update_figure: callable:  (Default value = default_callback)
        An update function that takes a plotly figure
        as an argument and optionally returns a plotly figure. If a figure is
        not returned, the plotly figure passed will be assumed to be the return
        value. Used to add any custom changes to the underlying plotly figure.
        Note that the existing data traces should not be removed. This may lead
        to unexpected behavior if traces are modified in a way that break data
        mappings.

    Returns:
      A DeephavenFigure that contains the funnel area chart

    """

    args = locals()

    return process_args(args, {"always_attached"}, px_func=px.funnel_area)
