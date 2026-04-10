"""
Labeling tools for smaller ML use cases.
"""

import os
from collections.abc import Callable, Mapping
from typing import TypedDict

import geopandas as gpd
import ipywidgets as widgets
import matplotlib.pyplot as plt
import pandas as pd
from IPython.display import display
from matplotlib.backend_bases import MouseButton, MouseEvent
from matplotlib.backend_tools import Cursors
from matplotlib.patches import Circle
from shapely.geometry import Point


class ClassValue(TypedDict):
    value: int
    color: str


class PointLabelingTool(widgets.VBox):
    """
    Create points labels by overlaying them over a background.

    Select a class and click to add one.
    """

    points: gpd.GeoDataFrame
    crs: str
    filename: str
    class_column: str
    selected_class_value: int
    classes: Mapping[str, ClassValue]
    color_map: Mapping[int, str]

    fig: plt.Figure
    ax: plt.Axes
    plt_fg: pd.plotting.PlotAccessor | None
    legend_handles: list[Circle]

    def __init__(
        self,
        *,
        filename: str,
        class_column: str,
        classes: Mapping[str, ClassValue],
        crs: str,
        background: Callable[[plt.Axes], None],
        figsize: tuple[int, int] | None = None,
    ) -> None:
        super().__init__()

        self.filename = filename
        self.crs = crs

        self.class_column = class_column
        self.classes = classes

        self.color_map = {c["value"]: c["color"] for c in self.classes.values()}
        self.selected_class_value = next(iter(self.classes.values()))["value"]

        self.points = self.__make_gdf(None)

        if os.path.exists(filename):
            self.points = pd.concat(
                [
                    self.points,
                    gpd.read_file(filename).set_crs(self.crs, allow_override=True),
                ],
                ignore_index=True,
            )

        self.children = [
            self.__create_selection(background),
            self.__create_plot(background, figsize),
        ]

        self.__plot_and_save(background)

    def __make_gdf(self, entry: tuple[Point, int] | None) -> gpd.GeoDataFrame:
        """
        Create a GeoDataFrame from a given entry.
        If `entry` is None, an empty GeoDataFrame is created.
        """

        data: dict[str, list] = {"geometry": [], self.class_column: []}

        if entry:
            geom, class_value = entry
            data["geometry"].append(geom)
            data[self.class_column].append(class_value)

        return gpd.GeoDataFrame(
            data=data,
            crs=self.crs,
            geometry="geometry",
        )

    def __create_plot(self, background: Callable[[plt.Axes], None], figsize: tuple[int, int] | None) -> widgets.Output:
        """
        Creates a plot with a specified background and figure size,
        and sets up an interactive widget for labeling points.
        """

        output = widgets.Output()

        with output, plt.ioff():
            self.fig, self.ax = plt.subplots(figsize=figsize, constrained_layout=True)

            self.fig.canvas.toolbar_visible = False
            self.fig.canvas.header_visible = False
            self.fig.canvas.footer_visible = False

            display(self.fig.canvas)

        background(self.ax)

        def on_click(event: MouseEvent):
            if self.fig.canvas.widgetlock.locked():
                return  # Don't do anything if the zoom/pan tools have been enabled.
            if event.button is not MouseButton.LEFT:
                return
            if not event.inaxes:
                return

            self.points.loc[len(self.points)] = [
                Point(event.xdata, event.ydata),
                self.selected_class_value,
            ]
            self.points.set_geometry(col="geometry", inplace=True)

            self.__plot_and_save(background)

        self.fig.canvas.mpl_connect("button_press_event", on_click)

        self.legend_handles = [
            Circle((0.5, 0.5), 1, label=name, facecolor=v["color"]) for (name, v) in self.classes.items()
        ]

        return output

    def __create_selection(self, background: Callable[[plt.Axes], None]) -> widgets.HBox:
        """
        Creates a selection interface for labeling water bodies.
        This method generates a horizontal box layout containing toggle buttons for class selection
        and an undo button. The class buttons allow the user to select a class from the available
        options, while the undo button removes the last point added to the selection.
        """

        class_buttons = widgets.ToggleButtons(
            options=[(c, v["value"]) for (c, v) in self.classes.items()],
            description="Class:",
            button_style="info",
        )

        undo_button = widgets.Button(
            icon="undo",
            button_style="warning",
        )

        def on_undo(_event):
            self.points.drop(self.points.tail(1).index, inplace=True)

            self.__plot_and_save(background)

        undo_button.on_click(on_undo)

        def set_selected_class_value(change) -> None:
            self.selected_class_value = change["new"]

        class_buttons.observe(set_selected_class_value, names=["value"])

        return widgets.HBox([class_buttons, undo_button])

    def __plot_and_save(self, background: Callable[[plt.Axes], None]) -> None:
        """
        Plots the points on the given background and saves the plot to a file.
        """

        self.ax.clear()

        background(self.ax)

        if len(self.points) == 0:
            return

        colors = self.points[self.class_column].map(self.color_map)
        self.plt_fg = self.points.plot(ax=self.ax, c=colors)

        self.points.to_file(self.filename)

        self.ax.cursor_to_use = Cursors.POINTER

        self.ax.legend(
            handles=self.legend_handles,
            loc="center left",
            bbox_to_anchor=(1, 0.5),
        )
