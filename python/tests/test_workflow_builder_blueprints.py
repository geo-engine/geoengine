"""Tests for the workflow builder blueprints."""

import unittest

from geoengine import workflow_builder as wb


class BlueprintsTests(unittest.TestCase):
    """Tests for the workflow builder blueprints."""

    def test_sentinel2_band(self):
        source_operator = wb.blueprints.sentinel2_band("B02")
        self.assertIsInstance(source_operator, wb.operators.GdalSource)
        self.assertEqual(
            source_operator.to_dict(),
            {"type": "GdalSource", "params": {"data": "_:5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5:`UTM32N:B02`"}},
        )

    def test_sentinel2_cloud_free_band(self):
        source_operator = wb.blueprints.sentinel2_cloud_free_band("B02")
        self.assertIsInstance(source_operator, wb.operators.Expression)
        self.assertEqual(
            source_operator.to_dict(),
            {
                "type": "Expression",
                "params": {
                    "expression": "if (B == 3 || (B >= 7 && B <= 11)) { NODATA } else { A }",
                    "outputType": "U16",
                    "mapNoData": False,
                },
                "sources": {
                    "raster": {
                        "type": "RasterStacker",
                        "params": {"renameBands": {"type": "default"}},
                        "sources": {
                            "rasters": [
                                {
                                    "type": "GdalSource",
                                    "params": {"data": "_:5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5:`UTM32N:B02`"},
                                },
                                {
                                    "type": "RasterTypeConversion",
                                    "params": {"outputDataType": "U16"},
                                    "sources": {
                                        "raster": {
                                            "type": "GdalSource",
                                            "params": {"data": "_:5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5:`UTM32N:SCL`"},
                                        }
                                    },
                                },
                            ]
                        },
                    }
                },
            },
        )

    def test_sentinel2_ndvi(self):
        source_operator = wb.blueprints.sentinel2_cloud_free_ndvi()
        self.assertIsInstance(source_operator, wb.operators.Expression)
        self.assertEqual(
            source_operator.to_dict(),
            {
                "type": "Expression",
                "params": {
                    "expression": "if (C == 3 || (C >= 7 && C <= 11)) { NODATA } else { (A - B) / (A + B) }",
                    "outputType": "F32",
                    "mapNoData": False,
                },
                "sources": {
                    "raster": {
                        "type": "RasterStacker",
                        "params": {"renameBands": {"type": "default"}},
                        "sources": {
                            "rasters": [
                                {
                                    "type": "GdalSource",
                                    "params": {"data": "_:5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5:`UTM32N:B08`"},
                                },
                                {
                                    "type": "GdalSource",
                                    "params": {"data": "_:5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5:`UTM32N:B04`"},
                                },
                                {
                                    "type": "RasterTypeConversion",
                                    "params": {"outputDataType": "U16"},
                                    "sources": {
                                        "raster": {
                                            "type": "GdalSource",
                                            "params": {"data": "_:5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5:`UTM32N:SCL`"},
                                        }
                                    },
                                },
                            ]
                        },
                    }
                },
            },
        )

    def test_sentinel2_cloud_free_band_custom_input(self):
        source_operator = wb.blueprints.sentinel2_cloud_free_band_custom_input("data_band", "scl_band")
        self.assertIsInstance(source_operator, wb.operators.Expression)
        self.assertEqual(
            source_operator.to_dict(),
            {
                "type": "Expression",
                "params": {
                    "expression": "if (B == 3 || (B >= 7 && B <= 11)) { NODATA } else { A }",
                    "outputType": "U16",
                    "mapNoData": False,
                },
                "sources": {
                    "raster": {
                        "type": "RasterStacker",
                        "params": {"renameBands": {"type": "default"}},
                        "sources": {
                            "rasters": [
                                {"type": "GdalSource", "params": {"data": "data_band"}},
                                {
                                    "type": "RasterTypeConversion",
                                    "params": {"outputDataType": "U16"},
                                    "sources": {"raster": {"type": "GdalSource", "params": {"data": "scl_band"}}},
                                },
                            ]
                        },
                    }
                },
            },
        )

    def test_sentinel2_cloud_free_ndvi_custom_input(self):
        source_operator = wb.blueprints.sentinel2_cloud_free_ndvi_custom_input("band8", "band4", "scl_band")
        self.assertIsInstance(source_operator, wb.operators.Expression)
        self.assertEqual(
            source_operator.to_dict(),
            {
                "type": "Expression",
                "params": {
                    "expression": "if (C == 3 || (C >= 7 && C <= 11)) { NODATA } else { (A - B) / (A + B) }",
                    "outputType": "F32",
                    "mapNoData": False,
                },
                "sources": {
                    "raster": {
                        "type": "RasterStacker",
                        "params": {"renameBands": {"type": "default"}},
                        "sources": {
                            "rasters": [
                                {"type": "GdalSource", "params": {"data": "band8"}},
                                {"type": "GdalSource", "params": {"data": "band4"}},
                                {
                                    "type": "RasterTypeConversion",
                                    "params": {"outputDataType": "U16"},
                                    "sources": {"raster": {"type": "GdalSource", "params": {"data": "scl_band"}}},
                                },
                            ]
                        },
                    }
                },
            },
        )

    def test_s2_cloud_free_aggregated_band(self):
        source_operator = wb.blueprints.s2_cloud_free_aggregated_band("B04")
        self.assertIsInstance(source_operator, wb.operators.TemporalRasterAggregation)
        self.assertEqual(
            source_operator.to_dict(),
            {
                "type": "TemporalRasterAggregation",
                "params": {
                    "aggregation": {"type": "mean", "ignoreNoData": True, "percentile": None},
                    "window": {"granularity": "days", "step": 1},
                    "windowReference": None,
                    "outputType": "F32",
                },
                "sources": {
                    "raster": {
                        "type": "Expression",
                        "params": {
                            "expression": "if (B == 3 || (B >= 7 && B <= 11)) { NODATA } else { A }",
                            "outputType": "U16",
                            "mapNoData": False,
                        },
                        "sources": {
                            "raster": {
                                "type": "RasterStacker",
                                "params": {"renameBands": {"type": "default"}},
                                "sources": {
                                    "rasters": [
                                        {
                                            "type": "GdalSource",
                                            "params": {"data": "_:5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5:`UTM32N:B04`"},
                                        },
                                        {
                                            "type": "RasterTypeConversion",
                                            "params": {"outputDataType": "U16"},
                                            "sources": {
                                                "raster": {
                                                    "type": "GdalSource",
                                                    "params": {
                                                        "data": "_:5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5:`UTM32N:SCL`"
                                                    },
                                                }
                                            },
                                        },
                                    ]
                                },
                            }
                        },
                    }
                },
            },
        )

    def test_s2_cloud_free_aggregated_band_custom_input(self):
        source_operator = wb.blueprints.s2_cloud_free_aggregated_band_custom_input("band8", "scl_band")
        self.assertIsInstance(source_operator, wb.operators.TemporalRasterAggregation)
        self.assertEqual(
            source_operator.to_dict(),
            {
                "type": "TemporalRasterAggregation",
                "params": {
                    "aggregation": {"type": "mean", "ignoreNoData": True, "percentile": None},
                    "window": {"granularity": "days", "step": 1},
                    "windowReference": None,
                    "outputType": "F32",
                },
                "sources": {
                    "raster": {
                        "type": "Expression",
                        "params": {
                            "expression": "if (B == 3 || (B >= 7 && B <= 11)) { NODATA } else { A }",
                            "outputType": "U16",
                            "mapNoData": False,
                        },
                        "sources": {
                            "raster": {
                                "type": "RasterStacker",
                                "params": {"renameBands": {"type": "default"}},
                                "sources": {
                                    "rasters": [
                                        {"type": "GdalSource", "params": {"data": "band8"}},
                                        {
                                            "type": "RasterTypeConversion",
                                            "params": {"outputDataType": "U16"},
                                            "sources": {
                                                "raster": {"type": "GdalSource", "params": {"data": "scl_band"}}
                                            },
                                        },
                                    ]
                                },
                            }
                        },
                    }
                },
            },
        )

    def test_s2_cloud_free_aggregated_ndvi_custom_input(self):
        source_operator = wb.blueprints.s2_cloud_free_aggregated_ndvi_custom_input("band8", "band4", "scl_band")
        self.assertIsInstance(source_operator, wb.operators.TemporalRasterAggregation)
        self.assertEqual(
            source_operator.to_dict(),
            {
                "type": "TemporalRasterAggregation",
                "params": {
                    "aggregation": {"type": "mean", "ignoreNoData": True, "percentile": None},
                    "window": {"granularity": "days", "step": 1},
                    "windowReference": None,
                    "outputType": "F32",
                },
                "sources": {
                    "raster": {
                        "type": "Expression",
                        "params": {
                            "expression": "if (C == 3 || (C >= 7 && C <= 11)) { NODATA } else { (A - B) / (A + B) }",
                            "outputType": "F32",
                            "mapNoData": False,
                        },
                        "sources": {
                            "raster": {
                                "type": "RasterStacker",
                                "params": {"renameBands": {"type": "default"}},
                                "sources": {
                                    "rasters": [
                                        {"type": "GdalSource", "params": {"data": "band8"}},
                                        {"type": "GdalSource", "params": {"data": "band4"}},
                                        {
                                            "type": "RasterTypeConversion",
                                            "params": {"outputDataType": "U16"},
                                            "sources": {
                                                "raster": {"type": "GdalSource", "params": {"data": "scl_band"}}
                                            },
                                        },
                                    ]
                                },
                            }
                        },
                    }
                },
            },
        )


if __name__ == "__main__":
    unittest.main()
