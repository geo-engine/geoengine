"""Tests for the datasets module."""

import os
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import geoengine_api_client
import geoengine_api_client.models
import geoengine_api_client.models.spatial_grid_descriptor_state
import numpy as np
import rasterio as rio

import geoengine as ge
from geoengine.permissions import REGISTERED_USER_ROLE_ID, Permission, PermissionListing, Role
from geoengine.resource_identifier import Resource
from geoengine.types import RasterBandDescriptor
from geoengine.workflow_builder import operators as wb
from tests.ge_test import GeoEngineTestInstance


class DatasetsTests(unittest.TestCase):
    """Dataset test runner."""

    def setUp(self) -> None:
        """Set up the geo engine session."""
        ge.reset(False)

    def test_list_datasets(self):
        """Test `GET /datasets`."""

        # TODO: use `enterContext(cm)` instead of `with cm:` in Python 3.11
        with GeoEngineTestInstance() as ge_instance:
            ge_instance.wait_for_ready()

            ge.initialize(ge_instance.address())

            datasets = ge.list_datasets(
                offset=0, limit=10, order=ge.DatasetListOrder.NAME_ASC, name_filter="Natural Earth II"
            )

            datasets = list(datasets)

            self.assertEqual(len(datasets), 3)

            dataset = datasets[0]

            self.assertEqual(dataset.name, "ne2_raster_blue")
            self.assertEqual(dataset.display_name, "Natural Earth II – Blue")
            self.assertEqual(dataset.result_descriptor.actual_instance.type, "raster")

    def test_add_dataset(self):
        """Test `add_datset`."""

        # TODO: use `enterContext(cm)` instead of `with cm:` in Python 3.11
        with GeoEngineTestInstance() as ge_instance:
            ge_instance.wait_for_ready()

            ge.initialize(ge_instance.address(), credentials=("admin@localhost", "adminadmin"))

            volume = ge.volume_by_name("test_data")

            gdal_params = geoengine_api_client.GdalDatasetParameters.from_dict(
                {
                    "filePath": "raster/landcover/landcover.tif",
                    "rasterbandChannel": 1,
                    "geoTransform": {
                        "originCoordinate": {"x": -180.0, "y": 90.0},
                        "xPixelSize": 0.1,
                        "yPixelSize": -0.1,
                    },
                    "width": 3600,
                    "height": 1800,
                    "fileNotFoundHandling": geoengine_api_client.FileNotFoundHandling.NODATA,
                    "noDataValue": None,
                    "propertiesMapping": None,
                    "gdalOpenOptions": None,
                    "gdalConfigOptions": None,
                    "allowAlphabandAsMask": True,
                }
            )

            result_descriptor_measurement = ge.ClassificationMeasurement(
                measurement="Land Cover",
                classes={
                    0: "Water Bodies",
                    1: "Evergreen Needleleaf Forests",
                    2: "Evergreen Broadleaf Forests",
                    3: "Deciduous Needleleaf Forests",
                    4: "Deciduous Broadleleaf Forests",
                    5: "Mixed Forests",
                    6: "Closed Shrublands",
                    7: "Open Shrublands",
                    8: "Woody Savannas",
                    9: "Savannas",
                    10: "Grasslands",
                    11: "Permanent Wtlands",
                    12: "Croplands",
                    13: "Urban and Built-Up",
                    14: "Cropland-Natural Vegetation Mosaics",
                    15: "Snow and Ice",
                    16: "Barren or Sparsely Vegetated",
                },
            )

            result_descriptor = ge.RasterResultDescriptor(
                "U8",
                bands=[RasterBandDescriptor("band", result_descriptor_measurement)],
                spatial_reference="EPSG:4326",
                spatial_grid=ge.types.SpatialGridDescriptor(
                    spatial_grid=ge.types.SpatialGridDefinition(
                        geo_transform=ge.GeoTransform(x_min=-180.0, y_max=90.0, x_pixel_size=0.1, y_pixel_size=-0.1),
                        grid_bounds=ge.GridBoundingBox2D(
                            top_left_idx=ge.GridIdx2D(0, 0), bottom_right_idx=ge.GridIdx2D(1799, 3599)
                        ),
                    ),
                    descriptor=geoengine_api_client.SpatialGridDescriptorState.SOURCE,
                ),
                time=ge.TimeDescriptor(dimension=ge.IrregularTimeDimension(), bounds=None),
            )

            meta_data = geoengine_api_client.GdalMetaDataStatic.from_dict(
                {
                    "type": "GdalStatic",
                    "time": None,
                    "params": gdal_params,
                    "resultDescriptor": result_descriptor.to_api_dict().to_dict(),
                    "cacheTtl": 0,
                }
            )

            add_dataset_properties = ge.AddDatasetProperties(
                name="MCD12C1_test",
                display_name="Land Cover TEST",
                source_operator="GdalSource",
                description="Land Cover",
                symbology=ge.RasterSymbology(
                    opacity=1.0,
                    raster_colorizer=ge.SingleBandRasterColorizer(
                        band=0,
                        band_colorizer=ge.LinearGradientColorizer(
                            breakpoints=[
                                ge.ColorBreakpoint(value=0, color=(0, 0, 255, 255)),
                                ge.ColorBreakpoint(value=8, color=(0, 255, 0, 255)),
                                ge.ColorBreakpoint(value=16, color=(255, 0, 0, 255)),
                            ],
                            no_data_color=(0, 0, 0, 0),
                            over_color=(0, 0, 0, 0),
                            under_color=(0, 0, 0, 0),
                        ),
                    ),
                ),
                provenance=[
                    ge.Provenance(
                        citation="The data was obtained from <https://lpdaac.usgs.gov/products/mcd12c1v006>.",
                        uri="https://lpdaac.usgs.gov/products/mcd12c1v006/",
                        license="All data distributed by the LP DAAC contain no restrictions on the data reuse.",
                    )
                ],
            )

            metadata_for_api = geoengine_api_client.MetaDataDefinition(
                meta_data,
            )

            dataset_name = ge.add_dataset(
                volume,
                add_dataset_properties,
                metadata_for_api,
            )

            self.assertEqual(dataset_name, ge.DatasetName("MCD12C1_test"))
            self.assertEqual(len(list(ge.list_datasets(name_filter="Land Cover TEST"))), 1)

            metadata_from_api = ge.dataset_metadata_by_name(dataset_name)
            self.assertEqual(
                metadata_from_api.actual_instance.result_descriptor, metadata_for_api.actual_instance.result_descriptor
            )
            self.assertTrue(
                metadata_from_api.actual_instance.params.file_path.endswith(
                    metadata_for_api.actual_instance.params.file_path
                )
            )

    def test_add_dataset_with_permissions(self):
        """Test `add_datset`."""

        # TODO: use `enterContext(cm)` instead of `with cm:` in Python 3.11
        with GeoEngineTestInstance() as ge_instance:
            ge_instance.wait_for_ready()

            ge.initialize(ge_instance.address(), credentials=("admin@localhost", "adminadmin"))

            volume = ge.volume_by_name("test_data")

            gdal_params = geoengine_api_client.GdalDatasetParameters.from_dict(
                {
                    "filePath": "raster/landcover/landcover.tif",
                    "rasterbandChannel": 1,
                    "geoTransform": {
                        "originCoordinate": {"x": -180.0, "y": 90.0},
                        "xPixelSize": 0.1,
                        "yPixelSize": -0.1,
                    },
                    "width": 3600,
                    "height": 1800,
                    "fileNotFoundHandling": geoengine_api_client.FileNotFoundHandling.NODATA,
                    "noDataValue": None,
                    "propertiesMapping": None,
                    "gdalOpenOptions": None,
                    "gdalConfigOptions": None,
                    "allowAlphabandAsMask": True,
                }
            )

            result_descriptor_measurement = ge.ClassificationMeasurement(
                measurement="Land Cover",
                classes={
                    0: "Water Bodies",
                    1: "Evergreen Needleleaf Forests",
                    2: "Evergreen Broadleaf Forests",
                    3: "Deciduous Needleleaf Forests",
                    4: "Deciduous Broadleleaf Forests",
                    5: "Mixed Forests",
                    6: "Closed Shrublands",
                    7: "Open Shrublands",
                    8: "Woody Savannas",
                    9: "Savannas",
                    10: "Grasslands",
                    11: "Permanent Wtlands",
                    12: "Croplands",
                    13: "Urban and Built-Up",
                    14: "Cropland-Natural Vegetation Mosaics",
                    15: "Snow and Ice",
                    16: "Barren or Sparsely Vegetated",
                },
            )

            result_descriptor = ge.RasterResultDescriptor(
                "U8",
                [RasterBandDescriptor("band", result_descriptor_measurement)],
                "EPSG:4326",
                spatial_grid=ge.types.SpatialGridDescriptor(
                    spatial_grid=ge.types.SpatialGridDefinition(
                        geo_transform=ge.GeoTransform(x_min=-180.0, y_max=90.0, x_pixel_size=0.1, y_pixel_size=-0.1),
                        grid_bounds=ge.GridBoundingBox2D(
                            top_left_idx=ge.GridIdx2D(0, 0), bottom_right_idx=ge.GridIdx2D(1799, 3599)
                        ),
                    ),
                    descriptor=geoengine_api_client.SpatialGridDescriptorState.SOURCE,
                ),
                time=ge.TimeDescriptor(dimension=ge.IrregularTimeDimension(), bounds=None),
            )

            meta_data = geoengine_api_client.GdalMetaDataStatic.from_dict(
                {
                    "type": "GdalStatic",
                    "time": None,
                    "params": gdal_params,
                    "resultDescriptor": result_descriptor.to_api_dict().to_dict(),
                }
            )

            add_dataset_properties = ge.AddDatasetProperties(
                name="MCD12C1_test",
                display_name="Land Cover TEST",
                source_operator="GdalSource",
                description="Land Cover",
                symbology=ge.RasterSymbology(
                    opacity=1.0,
                    raster_colorizer=ge.SingleBandRasterColorizer(
                        band=0,
                        band_colorizer=ge.LinearGradientColorizer(
                            breakpoints=[
                                ge.ColorBreakpoint(value=0, color=(0, 0, 255, 255)),
                                ge.ColorBreakpoint(value=16, color=(255, 0, 0, 255)),
                            ],
                            no_data_color=(0, 0, 0, 0),
                            over_color=(0, 0, 0, 0),
                            under_color=(0, 0, 0, 0),
                        ),
                    ),
                ),
                provenance=[],
            )

            permisions = [(REGISTERED_USER_ROLE_ID, Permission.READ)]

            dataset_name = ge.add_or_replace_dataset_with_permissions(
                volume,
                add_dataset_properties,
                geoengine_api_client.MetaDataDefinition(
                    meta_data,
                ),
                permission_tuples=permisions,
            )

            self.assertEqual(dataset_name, ge.DatasetName("MCD12C1_test"))
            self.assertEqual(len(list(ge.list_datasets(name_filter="Land Cover TEST"))), 1)
            dataset_info = ge.dataset_info_by_name(ge.DatasetName("MCD12C1_test"))
            self.assertEqual(dataset_info.name, "MCD12C1_test")
            self.assertEqual(dataset_info.description, "Land Cover")

            expected_permission = PermissionListing(
                role=Role(role_name="user", role_id=REGISTERED_USER_ROLE_ID),
                resource=Resource.from_dataset_name(dataset_name),
                permission=Permission.READ,
            )
            self.assertIn(
                expected_permission, ge.permissions.list_permissions(Resource.from_dataset_name(dataset_name))
            )

            # now get without overwrite
            add_dataset_properties = ge.AddDatasetProperties(
                name="MCD12C1_test",
                display_name="Land Cover TEST",
                source_operator="GdalSource",
                description="Land Cover 2",
                symbology=ge.RasterSymbology(
                    opacity=1.0,
                    raster_colorizer=ge.SingleBandRasterColorizer(
                        band=0,
                        band_colorizer=ge.LinearGradientColorizer(
                            breakpoints=[
                                ge.ColorBreakpoint(value=0, color=(0, 0, 255, 255)),
                                ge.ColorBreakpoint(value=16, color=(255, 0, 0, 255)),
                            ],
                            no_data_color=(0, 0, 0, 0),
                            over_color=(0, 0, 0, 0),
                            under_color=(0, 0, 0, 0),
                        ),
                    ),
                ),
                provenance=[],
            )

            dataset_name = ge.add_or_replace_dataset_with_permissions(
                volume,
                add_dataset_properties,
                geoengine_api_client.MetaDataDefinition(
                    meta_data,
                ),
                permission_tuples=permisions,
            )

            self.assertEqual(dataset_name, ge.DatasetName("MCD12C1_test"))
            self.assertEqual(len(list(ge.list_datasets(name_filter="Land Cover TEST"))), 1)
            dataset_info = ge.dataset_info_by_name(ge.DatasetName("MCD12C1_test"))
            self.assertEqual(dataset_info.name, "MCD12C1_test")
            self.assertEqual(
                dataset_info.description,
                "Land Cover",  # Still the first value, since no overwrite
            )

            # now overwrite
            add_dataset_properties = ge.AddDatasetProperties(
                name="MCD12C1_test",
                display_name="Land Cover TEST",
                source_operator="GdalSource",
                description="Land Cover 3",
                symbology=ge.RasterSymbology(
                    opacity=1.0,
                    raster_colorizer=ge.SingleBandRasterColorizer(
                        band=0,
                        band_colorizer=ge.LinearGradientColorizer(
                            breakpoints=[
                                ge.ColorBreakpoint(value=0, color=(0, 0, 255, 255)),
                                ge.ColorBreakpoint(value=16, color=(255, 0, 0, 255)),
                            ],
                            no_data_color=(0, 0, 0, 0),
                            over_color=(0, 0, 0, 0),
                            under_color=(0, 0, 0, 0),
                        ),
                    ),
                ),
                provenance=[],
            )

            dataset_name = ge.add_or_replace_dataset_with_permissions(
                volume,
                add_dataset_properties,
                geoengine_api_client.MetaDataDefinition(
                    meta_data,
                ),
                permission_tuples=permisions,
                replace_existing=True,
            )

            self.assertEqual(dataset_name, ge.DatasetName("MCD12C1_test"))
            self.assertEqual(len(list(ge.list_datasets(name_filter="Land Cover TEST"))), 1)
            dataset_info = ge.dataset_info_by_name(dataset_name)
            self.assertEqual(dataset_info.name, "MCD12C1_test")
            self.assertEqual(
                dataset_info.description,
                "Land Cover 3",  # Now the third value, replaced with new dataset
            )

    def test_add_multiband_gdal_source(self):
        """Test creating a MultiBandGdalSource dataset, adding tiles and querying it."""

        with GeoEngineTestInstance() as ge_instance:
            ge_instance.wait_for_ready()

            ge.initialize(ge_instance.address(), credentials=("admin@localhost", "adminadmin"))

            pixel_size = 0.2
            tile_z_index = {(0, 0): 0, (0, 1): 1, (1, 0): 1, (1, 1): 2}
            data_dir = Path(os.environ["GEOENGINE_TEST_CODE_PATH"]) / "test_data/raster/multi_tile/data"

            # The tile specs are derived from the actual files (transform and
            # bounds), like stac-import derives them from proj:transform/shape.
            # The time of each file is a single day, as the source requires the
            # file time to match the query time (one step of the time dimension)
            files = []
            for date in [datetime(2025, 1, 1), datetime(2025, 2, 1), datetime(2025, 4, 1)]:
                next_date = date + timedelta(days=1)
                for band in range(2):
                    for x, y in tile_z_index:
                        tiff_path = data_dir / f"{date:%Y-%m-%d}_tile_x{x}_y{y}_b{band}.tif"
                        with rio.open(tiff_path) as source:
                            transform = source.transform
                            bounds = source.bounds
                            files.append(
                                ge.MultiBandGdalFileSpec(
                                    file_path=f"raster/multi_tile/data/{tiff_path.name}",
                                    time=ge.TimeInterval(start=date, end=next_date),
                                    spatial_partition=ge.SpatialPartition2D(
                                        xmin=bounds.left,
                                        ymin=bounds.bottom,
                                        xmax=bounds.right,
                                        ymax=bounds.top,
                                    ),
                                    band=band,
                                    width=source.width,
                                    height=source.height,
                                    geo_transform=ge.GeoTransform(
                                        x_min=transform.c,
                                        y_max=transform.f,
                                        x_pixel_size=transform.a,
                                        y_pixel_size=transform.e,
                                    ),
                                    channel=1,
                                    z_index=tile_z_index[(x, y)],
                                    no_data_value=0.0,
                                    allow_alphaband_as_mask=True,
                                )
                            )

            dataset_name = ge.add_multiband_gdal_source(
                name="multi_band_test",
                display_name="Multi Band Test",
                bands=[
                    RasterBandDescriptor("band 0", ge.UnitlessMeasurement()),
                    RasterBandDescriptor("band 1", ge.UnitlessMeasurement()),
                ],
                data_type=ge.RasterDataType.U16,
                spatial_reference="EPSG:4326",
                files=files,
                data_store="test_data",
                share_with=[REGISTERED_USER_ROLE_ID],
                permission=Permission.READ,
            )

            self.assertEqual(dataset_name, ge.DatasetName("multi_band_test"))

            metadata = ge.dataset_metadata_by_name(dataset_name)
            self.assertIsInstance(metadata.actual_instance, geoengine_api_client.GdalMultiBand)
            result_descriptor = metadata.actual_instance.result_descriptor
            self.assertEqual(result_descriptor.data_type.value, "U16")
            self.assertEqual(result_descriptor.spatial_reference, "EPSG:4326")
            self.assertEqual([band.name for band in result_descriptor.bands], ["band 0", "band 1"])

            grid = ge.SpatialGridDefinition.from_response(result_descriptor.spatial_grid.spatial_grid)
            self.assertEqual(grid.geo_transform.x_min, -180.0)
            self.assertEqual(grid.geo_transform.y_max, 90.0)
            self.assertEqual(grid.geo_transform.x_pixel_size, 0.2)
            self.assertEqual(grid.geo_transform.y_pixel_size, -0.2)

            # The server derives the spatial grid from the tiles; the union of
            # all tiles covers lon -180..180 and lat 90..-90 (within a pixel)
            spatial = grid.spatial_bounds()
            self.assertAlmostEqual(spatial.xmin, -180.0, delta=1.5 * pixel_size)
            self.assertAlmostEqual(spatial.xmax, 180.0, delta=1.5 * pixel_size)
            self.assertAlmostEqual(spatial.ymin, -90.0, delta=1.5 * pixel_size)
            self.assertAlmostEqual(spatial.ymax, 90.0, delta=1.5 * pixel_size)

            permissions = ge.list_permissions(Resource.from_dataset_name(dataset_name))
            user_permissions = [p for p in permissions if p.role.id == REGISTERED_USER_ROLE_ID]
            self.assertEqual(len(user_permissions), 1)
            self.assertEqual(user_permissions[0].permission, Permission.READ)

            # In the central region all four tiles overlap; the tile with the
            # highest z-index (x1y1) must win. Query a single day so the query
            # time matches the file time (as required by the source)
            workflow = ge.register_workflow(wb.MultiBandGdalSource(dataset_name).to_workflow_dict())
            query = ge.QueryRectangle(
                ge.BoundingBox2D(-45.0, -22.4, 45.0, 22.4),
                ge.TimeInterval(start=datetime(2025, 1, 1), end=datetime(2025, 1, 2)),
            )
            array = workflow.get_array(query)

            with rio.open(data_dir / "2025-01-01_tile_x1_y1_b0.tif") as source:
                window = rio.windows.from_bounds(-45.0, -22.4, 45.0, 22.4, source.transform)
                expected = source.read(1, window=window)

            # The server also includes the boundary row whose bottom edge
            # coincides with the query's top edge, so the file window (which
            # starts at that edge) lines up with the second result row
            self.assertEqual(array.shape, (expected.shape[0] + 1, expected.shape[1]))
            self.assertTrue(np.array_equal(array[1:, :], expected))


if __name__ == "__main__":
    unittest.main()
