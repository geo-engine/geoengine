"""Tests for the datasets module."""

import unittest

import geoengine_openapi_client
import geoengine_openapi_client.models
import geoengine_openapi_client.models.spatial_grid_descriptor_state

import geoengine as ge
from geoengine.permissions import REGISTERED_USER_ROLE_ID, Permission, PermissionListing, Role
from geoengine.resource_identifier import Resource
from geoengine.types import RasterBandDescriptor
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

            gdal_params = geoengine_openapi_client.GdalDatasetParameters.from_dict(
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
                    "fileNotFoundHandling": geoengine_openapi_client.FileNotFoundHandling.NODATA,
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
                    descriptor=geoengine_openapi_client.SpatialGridDescriptorState.SOURCE,
                ),
                time=ge.TimeDescriptor(dimension=ge.IrregularTimeDimension(), bounds=None),
            )

            meta_data = geoengine_openapi_client.GdalMetaDataStatic.from_dict(
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

            metadata_for_api = geoengine_openapi_client.MetaDataDefinition(
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

            gdal_params = geoengine_openapi_client.GdalDatasetParameters.from_dict(
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
                    "fileNotFoundHandling": geoengine_openapi_client.FileNotFoundHandling.NODATA,
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
                    descriptor=geoengine_openapi_client.SpatialGridDescriptorState.SOURCE,
                ),
                time=ge.TimeDescriptor(dimension=ge.IrregularTimeDimension(), bounds=None),
            )

            meta_data = geoengine_openapi_client.GdalMetaDataStatic.from_dict(
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
                geoengine_openapi_client.MetaDataDefinition(
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
                geoengine_openapi_client.MetaDataDefinition(
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
                geoengine_openapi_client.MetaDataDefinition(
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


if __name__ == "__main__":
    unittest.main()
