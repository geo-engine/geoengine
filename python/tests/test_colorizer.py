"""Tests for the colorizer module."""

import json
import unittest

import matplotlib.pyplot as plt
import pytest
from matplotlib.colors import ListedColormap

import geoengine as ge
from geoengine import colorizer


class ColorizerTests(unittest.TestCase):
    """Colorizer test runner."""

    def setUp(self) -> None:
        """Set up the geo engine session."""
        ge.reset(logout=False)

    def test_gray_linear(self):
        """Test the basic black to white cmap colorizer."""
        expected = {
            "type": "linearGradient",
            "breakpoints": [
                {"value": 0.0, "color": [0, 0, 0, 255]},
                {"value": 63.75, "color": [64, 64, 64, 255]},
                {"value": 127.5, "color": [128, 128, 128, 255]},
                {"value": 191.25, "color": [192, 192, 192, 255]},
                {"value": 255.0, "color": [255, 255, 255, 255]},
            ],
            "noDataColor": [0, 0, 0, 0],
            "overColor": [0, 0, 0, 0],
            "underColor": [0, 0, 0, 0],
        }

        geo_colorizer = colorizer.Colorizer.linear_with_mpl_cmap(color_map="gray", min_max=(0.0, 255.0), n_steps=5)
        gray = geo_colorizer.to_api_dict().to_dict()

        assert gray == expected

    def test_gray_logarithmic(self):
        """Test the basic black to white cmap colorizer."""
        expected = {
            "type": "logarithmicGradient",
            "breakpoints": [
                {"value": 1.0, "color": [0, 0, 0, 255]},
                {"value": 10.0, "color": [64, 64, 64, 255]},
                {"value": 100.0, "color": [128, 128, 128, 255]},
                {"value": 1000.0, "color": [192, 192, 192, 255]},
                {"value": 10000.0, "color": [255, 255, 255, 255]},
            ],
            "noDataColor": [0, 0, 0, 0],
            "overColor": [0, 0, 0, 0],
            "underColor": [0, 0, 0, 0],
        }

        geo_colorizer = colorizer.Colorizer.logarithmic_with_mpl_cmap(
            color_map="gray", min_max=(1.0, 10000.0), n_steps=5
        )
        gray = geo_colorizer.to_api_dict().to_dict()

        assert gray == expected

    def test_gray_palette_with_explicit_mapping(self):
        """Test the basic black to white cmap colorizer."""
        expected = {
            "type": "palette",
            "colors": {"1.0": [0, 0, 0, 255], "2.0": [128, 128, 128, 255], "3.0": [255, 255, 255, 255]},
            "noDataColor": [0, 0, 0, 0],
            "defaultColor": [0, 0, 0, 0],
        }

        geo_colorizer = colorizer.Colorizer.palette(
            color_mapping={
                1.0: [0, 0, 0, 255],
                2.0: [128, 128, 128, 255],
                3.0: [255, 255, 255, 255],
            }
        )

        gray = geo_colorizer.to_api_dict().to_dict()

        assert gray == expected

    def test_gray_palette_with_mpl_cmap(self):
        """Test the basic black to white cmap colorizer.
        Checks, if cmap or name of cmap can be given as a parameter."""
        expected = {
            "type": "palette",
            "colors": {"1.0": [0, 0, 0, 255], "2.0": [128, 128, 128, 255], "3.0": [255, 255, 255, 255]},
            "noDataColor": [0, 0, 0, 0],
            "defaultColor": [0, 0, 0, 0],
        }

        # verify color map object variant
        cmap_obj = plt.colormaps["gray"]

        geo_colorizer = colorizer.Colorizer.palette_with_colormap(values=[1.0, 2.0, 3.0], color_map=cmap_obj)

        gray_obj = geo_colorizer.to_api_dict().to_dict()

        assert gray_obj == expected

        # verify color map name variant
        cmap_name = "gray"

        geo_colorizer = colorizer.Colorizer.palette_with_colormap(values=[1.0, 2.0, 3.0], color_map=cmap_name)

        gray_name = geo_colorizer.to_api_dict().to_dict()

        assert gray_name == expected

    def test_colormap_not_available(self):
        """Test that an error is raised when a colormap is not available."""
        with self.assertRaises(ValueError) as ctx:
            colorizer.Colorizer.linear_with_mpl_cmap(color_map="some_map", min_max=(0.0, 255.0))

        result = str(ctx.exception)

        expected_start = "'some_map' is not a valid value for"
        expected_contains = "; supported values are 'Accent',"

        assert result.startswith(expected_start), (
            f"The error should start with `{expected_start}`, but starts with `{result[: len(expected_start)]}`"
        )
        assert expected_contains in result, (
            f"The error should contain `{expected_contains}`, but does not. Full error: {result}"
        )

    def test_defaults(self):
        """Tests the manipulation of the default values."""
        expected = {
            "type": "linearGradient",
            "breakpoints": [{"value": 0.0, "color": [68, 1, 84, 255]}, {"value": 255.0, "color": [253, 231, 36, 255]}],
            "noDataColor": [100, 100, 100, 100],
            "overColor": [100, 100, 100, 100],
            "underColor": [100, 100, 100, 100],
        }

        geo_colorizer = colorizer.Colorizer.linear_with_mpl_cmap(
            color_map="viridis",
            min_max=(0.0, 255.0),
            n_steps=2,
            no_data_color=(100, 100, 100, 100),
            over_color=(100, 100, 100, 100),
            under_color=(100, 100, 100, 100),
        )
        viridis = geo_colorizer.to_api_dict().to_dict()

        assert viridis == expected

    def test_set_steps(self):
        """Tests the setting of the number of steps."""
        geo_colorizer = colorizer.Colorizer.linear_with_mpl_cmap(color_map="viridis", min_max=(0.0, 255.0), n_steps=2)
        viridis = geo_colorizer.to_api_dict().to_dict()
        expected = {
            "type": "linearGradient",
            "breakpoints": [{"value": 0.0, "color": [68, 1, 84, 255]}, {"value": 255.0, "color": [253, 231, 36, 255]}],
            "noDataColor": [0, 0, 0, 0],
            "overColor": [0, 0, 0, 0],
            "underColor": [0, 0, 0, 0],
        }

        assert viridis == expected

        geo_colorizer = colorizer.Colorizer.linear_with_mpl_cmap(color_map="viridis", min_max=(0.0, 255.0), n_steps=3)
        viridis = geo_colorizer.to_api_dict().to_dict()
        expected = {
            "type": "linearGradient",
            "breakpoints": [
                {"value": 0.0, "color": [68, 1, 84, 255]},
                {"value": 127.5, "color": [32, 144, 140, 255]},
                {"value": 255.0, "color": [253, 231, 36, 255]},
            ],
            "noDataColor": [0, 0, 0, 0],
            "overColor": [0, 0, 0, 0],
            "underColor": [0, 0, 0, 0],
        }

        assert viridis == expected

    def test_set_minmax(self):
        """Tests the setting of the min and max values."""
        geo_colorizer = colorizer.Colorizer.linear_with_mpl_cmap(color_map="viridis", min_max=(-10.0, 10.0), n_steps=3)
        expected = {
            "type": "linearGradient",
            "breakpoints": [
                {"value": -10.0, "color": [68, 1, 84, 255]},
                {"value": 0.0, "color": [32, 144, 140, 255]},
                {"value": 10.0, "color": [253, 231, 36, 255]},
            ],
            "noDataColor": [0, 0, 0, 0],
            "overColor": [0, 0, 0, 0],
            "underColor": [0, 0, 0, 0],
        }
        viridis = geo_colorizer.to_api_dict().to_dict()

        assert viridis == expected

    def test_set_minmax_log_wrong_values(self):
        """Tests the setting of wrong values for the min and max of a logarithmic gradient."""

        with self.assertRaises(ValueError) as ctx:
            colorizer.Colorizer.logarithmic_with_mpl_cmap(color_map="viridis", min_max=(-10.0, 10.0), n_steps=3)

        self.assertEqual(str(ctx.exception), "min_max[0] must be greater than 0 for a logarithmic gradient, got -10.0.")

    def test_minmax_wrong_order(self):
        """Tests if an error is raised, when the setting of the min and max values is wrong."""
        wrong_min = 10.0
        wrong_max = -10.0

        with self.assertRaises(ValueError) as ctx:
            colorizer.Colorizer.linear_with_mpl_cmap(color_map="viridis", min_max=(wrong_min, wrong_max), n_steps=3)

        self.assertEqual(
            str(ctx.exception), f"min_max[1] must be greater than min_max[0], got {wrong_max} and {wrong_min}."
        )

    def test_wrong_color_specification(self):
        """Tests if an error is raised, when the color specification is wrong."""
        # no data color
        wrong_colors = [(-1, 0, 0, 0), (0, 0, 0, -1), (256, 0, 0, 0), (0, 256, 0, 0), (-1, 258, 0, 0)]
        for wrong_color_code in wrong_colors:
            with self.assertRaises(ValueError) as ctx:
                colorizer.Colorizer.linear_with_mpl_cmap(
                    color_map="viridis", min_max=(0.0, 255.0), n_steps=3, no_data_color=wrong_color_code
                )

            self.assertEqual(
                str(ctx.exception), f"noDataColor must be a RGBA color specification, got {wrong_color_code} instead."
            )

            # over color
            with self.assertRaises(ValueError) as ctx:
                colorizer.Colorizer.linear_with_mpl_cmap(
                    color_map="viridis", min_max=(0.0, 255.0), n_steps=3, over_color=wrong_color_code
                )

            self.assertEqual(
                str(ctx.exception), f"overColor must be a RGBA color specification, got {wrong_color_code} instead."
            )

            # under color
            with self.assertRaises(ValueError) as ctx:
                colorizer.Colorizer.linear_with_mpl_cmap(
                    color_map="viridis", min_max=(0.0, 255.0), n_steps=3, under_color=wrong_color_code
                )

            self.assertEqual(
                str(ctx.exception), f"underColor must be a RGBA color specification, got {wrong_color_code} instead."
            )

    def test_custom_map(self):
        """Tests the setting of a custom map."""
        expected = {
            "type": "linearGradient",
            "breakpoints": [
                {"value": 0.0, "color": [255, 140, 0, 255]},
                {"value": 127.5, "color": [124, 252, 0, 255]},
                {"value": 255.0, "color": [32, 178, 170, 255]},
            ],
            "noDataColor": [0, 0, 0, 0],
            "overColor": [0, 0, 0, 0],
            "underColor": [0, 0, 0, 0],
        }
        custom_map = ListedColormap(["darkorange", "gold", "lawngreen", "lightseagreen"])
        geo_colorizer = colorizer.Colorizer.linear_with_mpl_cmap(color_map=custom_map, min_max=(0.0, 255.0), n_steps=3)
        custom = geo_colorizer.to_api_dict().to_dict()

        assert custom == expected

    def test_custom_map_with_options(self):
        """Tests the setting of a custom map with options."""
        expected = {
            "type": "linearGradient",
            "breakpoints": [
                {"value": 40.0, "color": [255, 140, 0, 255]},
                {"value": 220.0, "color": [124, 252, 0, 255]},
                {"value": 400.0, "color": [32, 178, 170, 255]},
            ],
            "noDataColor": [100, 100, 100, 100],
            "overColor": [100, 100, 100, 100],
            "underColor": [100, 100, 100, 100],
        }
        custom_map = ListedColormap(["darkorange", "gold", "lawngreen", "lightseagreen"])
        geo_colorizer = colorizer.Colorizer.linear_with_mpl_cmap(
            color_map=custom_map,
            min_max=(40.0, 400.0),
            n_steps=3,
            no_data_color=(100, 100, 100, 100),
            over_color=(100, 100, 100, 100),
            under_color=(100, 100, 100, 100),
        )
        custom = geo_colorizer.to_api_dict().to_dict()

        assert custom == expected

    def test_to_json(self):
        """Tests the to_json method."""
        expected = (
            '{"overColor": [0, 0, 0, 0], "underColor": [0, 0, 0, 0],'
            ' "breakpoints": [{"color": [68, 1, 84, 255], "value": 0.0'
            '}, {"color": [253, 231, 36, 255], "value": 255.0}], "noDataColor": [0, 0, 0, 0],'
            ' "type": "linearGradient"}'
        )

        geo_colorizer = colorizer.Colorizer.linear_with_mpl_cmap(color_map="viridis", min_max=(0.0, 255.0), n_steps=2)
        jsonstr = geo_colorizer.to_api_dict().to_json()

        self.assertDictEqual(json.loads(jsonstr), json.loads(expected))

    def test_palette_with_too_small_colormap(self):
        """Tests, if the warning is emitted if an unappropriate color map is chosen."""

        with pytest.warns(
            UserWarning,
            match="Warning!\nYour colormap does not have enough colors "
            "to display all unique values of the palette!"
            "\nNumber of values given: 6 vs. Number of available colors: 4",
        ):
            custom_map = ListedColormap(["darkorange", "gold", "lawngreen", "lightseagreen"])
            colorizer.Colorizer.palette_with_colormap(
                values=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                color_map=custom_map,
                no_data_color=(0, 0, 0, 0),
                default_color=(0, 0, 0, 0),
            )

    def test_palette_with_too_small_colormap_from_mpl(self):
        """Tests, if the warning is emittd if an unappropriate color map is chosen."""

        with pytest.warns(
            UserWarning,
            match="Warning!\nYour colormap does not have enough colors "
            "to display all unique values of the palette!"
            "\nNumber of values given: 10 vs. Number of available colors: 8",
        ):
            colorizer.Colorizer.palette_with_colormap(
                values=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
                color_map="Accent",  # holds 8 colors
                no_data_color=(0, 0, 0, 0),
                default_color=(0, 0, 0, 0),
            )


if __name__ == "__main__":
    unittest.main()
