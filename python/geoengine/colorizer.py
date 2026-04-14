"""This module is used to generate geoengine compatible color map definitions as a json string."""

from __future__ import annotations

import json
import warnings
from abc import abstractmethod
from dataclasses import dataclass
from typing import cast

import geoengine_openapi_client
import numpy as np
from matplotlib.cm import ScalarMappable
from matplotlib.colors import Colormap

Rgba = tuple[int, int, int, int]


@dataclass
class ColorBreakpoint:
    """This class is used to generate geoengine compatible color breakpoint definitions."""

    value: float
    color: Rgba

    def to_api_dict(self) -> geoengine_openapi_client.Breakpoint:
        """Return the color breakpoint as a dictionary."""
        return geoengine_openapi_client.Breakpoint(value=self.value, color=self.color)

    @staticmethod
    def from_response(response: geoengine_openapi_client.Breakpoint) -> ColorBreakpoint:
        """Parse a http response to a `ColorBreakpoint`."""
        return ColorBreakpoint(cast(float, response.value), cast(Rgba, tuple(cast(list[int], response.color))))


@dataclass
class Colorizer:
    """This class is used to generate geoengine compatible color map definitions as a json string."""

    no_data_color: Rgba

    @staticmethod
    def linear_with_mpl_cmap(
        color_map: str | Colormap,
        min_max: tuple[float, float],
        n_steps: int = 10,
        over_color: Rgba = (0, 0, 0, 0),
        under_color: Rgba = (0, 0, 0, 0),
        no_data_color: Rgba = (0, 0, 0, 0),
    ) -> LinearGradientColorizer:
        """Initialize the colorizer."""
        # pylint: disable=too-many-arguments,too-many-positional-arguments

        if n_steps < 2:
            raise ValueError(f"n_steps must be greater than or equal to 2, got {n_steps} instead.")
        if min_max[1] <= min_max[0]:
            raise ValueError(f"min_max[1] must be greater than min_max[0], got {min_max[1]} and {min_max[0]}.")
        if len(over_color) != 4:
            raise ValueError(f"overColor must be a tuple of length 4, got {len(over_color)} instead.")
        if len(under_color) != 4:
            raise ValueError(f"underColor must be a tuple of length 4, got {len(under_color)} instead.")
        if len(no_data_color) != 4:
            raise ValueError(f"noDataColor must be a tuple of length 4, got {len(no_data_color)} instead.")
        if not all(0 <= elem < 256 for elem in no_data_color):
            raise ValueError(f"noDataColor must be a RGBA color specification, got {no_data_color} instead.")
        if not all(0 <= elem < 256 for elem in over_color):
            raise ValueError(f"overColor must be a RGBA color specification, got {over_color} instead.")
        if not all(0 <= elem < 256 for elem in under_color):
            raise ValueError(f"underColor must be a RGBA color specification, got {under_color} instead.")

        # get the map, and transform it to a list of (uint8) rgba values
        list_of_rgba_colors = ScalarMappable(cmap=color_map).to_rgba(
            np.linspace(min_max[0], min_max[1], n_steps), bytes=True
        )

        # if you want to remap the colors, you can do it here (e.g. cutting of the most extreme colors)
        values_of_breakpoints: list[float] = np.linspace(min_max[0], min_max[1], n_steps).tolist()

        # generate color map steps for geoengine
        breakpoints: list[ColorBreakpoint] = [
            ColorBreakpoint(color=(int(color[0]), int(color[1]), int(color[2]), int(color[3])), value=value)
            for (value, color) in zip(values_of_breakpoints, list_of_rgba_colors, strict=False)
        ]

        return LinearGradientColorizer(
            breakpoints=breakpoints, no_data_color=no_data_color, over_color=over_color, under_color=under_color
        )

    @staticmethod
    def logarithmic_with_mpl_cmap(
        color_map: str | Colormap,
        min_max: tuple[float, float],
        n_steps: int = 10,
        over_color: Rgba = (0, 0, 0, 0),
        under_color: Rgba = (0, 0, 0, 0),
        no_data_color: Rgba = (0, 0, 0, 0),
    ) -> LogarithmicGradientColorizer:
        """Initialize the colorizer."""
        # pylint: disable=too-many-arguments, too-many-positional-arguments

        if n_steps < 2:
            raise ValueError(f"n_steps must be greater than or equal to 2, got {n_steps} instead.")
        if min_max[0] <= 0:
            raise ValueError(f"min_max[0] must be greater than 0 for a logarithmic gradient, got {min_max[0]}.")
        if min_max[1] <= min_max[0]:
            raise ValueError(f"min_max[1] must be greater than min_max[0], got {min_max[1]} and {min_max[0]}.")
        if len(over_color) != 4:
            raise ValueError(f"overColor must be a tuple of length 4, got {len(over_color)} instead.")
        if len(under_color) != 4:
            raise ValueError(f"underColor must be a tuple of length 4, got {len(under_color)} instead.")
        if len(no_data_color) != 4:
            raise ValueError(f"noDataColor must be a tuple of length 4, got {len(no_data_color)} instead.")
        if not all(0 <= elem < 256 for elem in no_data_color):
            raise ValueError(f"noDataColor must be a RGBA color specification, got {no_data_color} instead.")
        if not all(0 <= elem < 256 for elem in over_color):
            raise ValueError(f"overColor must be a RGBA color specification, got {over_color} instead.")
        if not all(0 <= elem < 256 for elem in under_color):
            raise ValueError(f"underColor must be a RGBA color specification, got {under_color} instead.")

        # get the map, and transform it to a list of (uint8) rgba values
        list_of_rgba_colors = ScalarMappable(cmap=color_map).to_rgba(
            np.linspace(min_max[0], min_max[1], n_steps), bytes=True
        )

        # if you want to remap the colors, you can do it here (e.g. cutting of the most extreme colors)
        values_of_breakpoints: list[float] = np.logspace(np.log10(min_max[0]), np.log10(min_max[1]), n_steps).tolist()

        # generate color map steps for geoengine
        breakpoints: list[ColorBreakpoint] = [
            ColorBreakpoint(color=(int(color[0]), int(color[1]), int(color[2]), int(color[3])), value=value)
            for (value, color) in zip(values_of_breakpoints, list_of_rgba_colors, strict=False)
        ]

        return LogarithmicGradientColorizer(
            breakpoints=breakpoints, no_data_color=no_data_color, over_color=over_color, under_color=under_color
        )

    @staticmethod
    def palette(
        color_mapping: dict[float, Rgba],
        default_color: Rgba = (0, 0, 0, 0),
        no_data_color: Rgba = (0, 0, 0, 0),
    ) -> PaletteColorizer:
        """Initialize the colorizer."""

        if len(no_data_color) != 4:
            raise ValueError(f"noDataColor must be a tuple of length 4, got {len(no_data_color)} instead.")
        if len(default_color) != 4:
            raise ValueError(f"defaultColor must be a tuple of length 4, got {len(default_color)} instead.")
        if not all(0 <= elem < 256 for elem in no_data_color):
            raise ValueError(f"noDataColor must be a RGBA color specification, got {no_data_color} instead.")
        if not all(0 <= elem < 256 for elem in default_color):
            raise ValueError(f"defaultColor must be a RGBA color specification, got {default_color} instead.")

        return PaletteColorizer(
            colors=color_mapping,
            no_data_color=no_data_color,
            default_color=default_color,
        )

    @staticmethod
    def palette_with_colormap(
        values: list[float],
        color_map: str | Colormap,
        default_color: Rgba = (0, 0, 0, 0),
        no_data_color: Rgba = (0, 0, 0, 0),
    ) -> PaletteColorizer:
        """This method generates a palette colorizer from a given list of values.
        A colormap can be given as an object or by name only."""

        if len(no_data_color) != 4:
            raise ValueError(f"noDataColor must be a tuple of length 4, got {len(no_data_color)} instead.")
        if len(default_color) != 4:
            raise ValueError(f"defaultColor must be a tuple of length 4, got {len(default_color)} instead.")
        if not all(0 <= elem < 256 for elem in no_data_color):
            raise ValueError(f"noDataColor must be a RGBA color specification, got {no_data_color} instead.")
        if not all(0 <= elem < 256 for elem in default_color):
            raise ValueError(f"defaultColor must be a RGBA color specification, got {default_color} instead.")

        n_colors_of_cmap: int = ScalarMappable(cmap=color_map).get_cmap().N

        if n_colors_of_cmap < len(values):
            warnings.warn(
                UserWarning(
                    f"Warning!\nYour colormap does not have enough colors "
                    "to display all unique values of the palette!"
                    f"\nNumber of values given: {len(values)} vs. "
                    f"Number of available colors: {n_colors_of_cmap}",
                ),
                stacklevel=2,
            )

        # we only need to generate enough different colors for all values specified in the colors parameter
        list_of_rgba_colors = ScalarMappable(cmap=color_map).to_rgba(
            np.linspace(0, len(values), len(values)), bytes=True
        )

        # generate the dict with value: color mapping
        color_mapping: dict[float, Rgba] = dict(
            zip(
                values,
                [(int(color[0]), int(color[1]), int(color[2]), int(color[3])) for color in list_of_rgba_colors],
                strict=False,
            )
        )

        return PaletteColorizer(
            colors=color_mapping,
            no_data_color=no_data_color,
            default_color=default_color,
        )

    @abstractmethod
    def to_api_dict(self) -> geoengine_openapi_client.Colorizer:
        pass

    def to_json(self) -> str:
        """Return the colorizer as a JSON string."""
        return json.dumps(self.to_api_dict())

    @staticmethod
    def from_response(response: geoengine_openapi_client.Colorizer) -> Colorizer:
        """Create a colorizer from a response."""
        inner = response.actual_instance

        if isinstance(inner, geoengine_openapi_client.LinearGradient):
            return LinearGradientColorizer.from_response_linear(inner)
        if isinstance(inner, geoengine_openapi_client.PaletteColorizer):
            return PaletteColorizer.from_response_palette(inner)
        if isinstance(inner, geoengine_openapi_client.LogarithmicGradient):
            return LogarithmicGradientColorizer.from_response_logarithmic(inner)

        raise TypeError("Unknown colorizer type")


def rgba_from_list(values: list[int]) -> Rgba:
    """Convert a list of integers to an RGBA tuple."""
    if len(values) != 4:
        raise ValueError(f"Expected a list of 4 integers, got {len(values)} instead.")
    return (values[0], values[1], values[2], values[3])


@dataclass
class LinearGradientColorizer(Colorizer):
    """A linear gradient colorizer."""

    breakpoints: list[ColorBreakpoint]
    over_color: Rgba
    under_color: Rgba

    @staticmethod
    def from_response_linear(response: geoengine_openapi_client.LinearGradient) -> LinearGradientColorizer:
        """Create a colorizer from a response."""
        breakpoints = [ColorBreakpoint.from_response(breakpoint) for breakpoint in response.breakpoints]
        return LinearGradientColorizer(
            no_data_color=rgba_from_list(response.no_data_color),
            breakpoints=breakpoints,
            over_color=rgba_from_list(response.over_color),
            under_color=rgba_from_list(response.under_color),
        )

    def to_api_dict(self) -> geoengine_openapi_client.Colorizer:
        """Return the colorizer as a dictionary."""
        return geoengine_openapi_client.Colorizer(
            geoengine_openapi_client.LinearGradient(
                type="linearGradient",
                breakpoints=[breakpoint.to_api_dict() for breakpoint in self.breakpoints],
                no_data_color=self.no_data_color,
                over_color=self.over_color,
                under_color=self.under_color,
            )
        )


@dataclass
class LogarithmicGradientColorizer(Colorizer):
    """A logarithmic gradient colorizer."""

    breakpoints: list[ColorBreakpoint]
    over_color: Rgba
    under_color: Rgba

    @staticmethod
    def from_response_logarithmic(
        response: geoengine_openapi_client.LogarithmicGradient,
    ) -> LogarithmicGradientColorizer:
        """Create a colorizer from a response."""
        breakpoints = [ColorBreakpoint.from_response(breakpoint) for breakpoint in response.breakpoints]
        return LogarithmicGradientColorizer(
            breakpoints=breakpoints,
            no_data_color=rgba_from_list(response.no_data_color),
            over_color=rgba_from_list(response.over_color),
            under_color=rgba_from_list(response.under_color),
        )

    def to_api_dict(self) -> geoengine_openapi_client.Colorizer:
        """Return the colorizer as a dictionary."""
        return geoengine_openapi_client.Colorizer(
            geoengine_openapi_client.LogarithmicGradient(
                type="logarithmicGradient",
                breakpoints=[breakpoint.to_api_dict() for breakpoint in self.breakpoints],
                no_data_color=self.no_data_color,
                over_color=self.over_color,
                under_color=self.under_color,
            )
        )


@dataclass
class PaletteColorizer(Colorizer):
    """A palette colorizer."""

    colors: dict[float, Rgba]
    default_color: Rgba

    @staticmethod
    def from_response_palette(response: geoengine_openapi_client.PaletteColorizer) -> PaletteColorizer:
        """Create a colorizer from a response."""

        return PaletteColorizer(
            colors={float(k): rgba_from_list(v) for k, v in response.colors.items()},
            no_data_color=rgba_from_list(response.no_data_color),
            default_color=rgba_from_list(response.default_color),
        )

    def to_api_dict(self) -> geoengine_openapi_client.Colorizer:
        """Return the colorizer as a dictionary."""
        return geoengine_openapi_client.Colorizer(
            geoengine_openapi_client.PaletteColorizer(
                type="palette",
                colors={str(k): v for k, v in self.colors.items()},
                default_color=self.default_color,
                no_data_color=self.no_data_color,
            )
        )
