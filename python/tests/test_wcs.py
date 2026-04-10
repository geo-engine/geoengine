"""Tests for WCS calls"""

import unittest
from datetime import datetime

import numpy as np
import owslib.util
import requests_mock
import xarray as xr

import geoengine as ge

from . import UrllibMocker


class WcsTests(unittest.TestCase):
    """WCS test runner"""

    def setUp(self) -> None:
        ge.reset(False)

    def test_ndvi(self):
        with UrllibMocker() as m_urllib:
            m_urllib.post(
                "http://mock-instance/anonymous",
                json={"id": "c4983c3e-9b53-47ae-bda9-382223bd5081", "project": None, "view": None},
            )

            m_urllib.post(
                "http://mock-instance/workflow",
                json={"id": "8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62"},
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            m_urllib.get(
                "http://mock-instance/workflow/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62/metadata",
                json={
                    "type": "raster",
                    "dataType": "U8",
                    "spatialReference": "EPSG:4326",
                    "bands": [{"name": "band", "measurement": {"type": "unitless"}}],
                    "spatialGrid": {
                        "descriptor": "source",
                        "spatialGrid": {
                            "geoTransform": {
                                "originCoordinate": {"x": 0.0, "y": 0.0},
                                "xPixelSize": 1.0,
                                "yPixelSize": -1.0,
                            },
                            "gridBounds": {
                                "topLeftIdx": {"xIdx": 0, "yIdx": 0},
                                "bottomRightIdx": {"xIdx": 10, "yIdx": 20},
                            },
                        },
                    },
                    "time": {
                        "bounds": {"start": 0, "end": 100000},
                        "dimension": {"type": "irregular"},
                    },
                },
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            ge.initialize("http://mock-instance")

            workflow_definition = {
                "type": "Raster",
                "operator": {
                    "type": "GdalSource",
                    "params": {"data": {"type": "internal", "datasetId": "36574dc3-560a-4b09-9d22-d5945f2b8093"}},
                },
            }

            workflow = ge.register_workflow(workflow_definition)

        with requests_mock.Mocker() as m_requests, open("tests/responses/ndvi.tiff", "rb") as ndvi_tiff:
            m_requests.get(
                # pylint: disable=line-too-long
                "http://mock-instance/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62?service=WCS&request=GetCapabilities&version=1.1.1",
                text="""<?xml version="1.0" encoding="UTF-8"?>
    <wcs:Capabilities version="1.1.1"
            xmlns:wcs="http://www.opengis.net/wcs/1.1.1"
            xmlns:xlink="http://www.w3.org/1999/xlink"
            xmlns:ogc="http://www.opengis.net/ogc"
            xmlns:ows="http://www.opengis.net/ows/1.1"
            xmlns:gml="http://www.opengis.net/gml"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wcs/1.1.1 
                http://localhost:3030/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62/schemas/wcs/1.1.1/wcsGetCapabilities.xsd"
                updateSequence="152">
            <ows:ServiceIdentification>
                <ows:Title>Web Coverage Service</ows:Title>
                <ows:ServiceType>WCS</ows:ServiceType>
                <ows:ServiceTypeVersion>1.1.1</ows:ServiceTypeVersion>
                <ows:Fees>NONE</ows:Fees>
                <ows:AccessConstraints>NONE</ows:AccessConstraints>
            </ows:ServiceIdentification>
            <ows:ServiceProvider>
                <ows:ProviderName>Provider Name</ows:ProviderName>
            </ows:ServiceProvider>
            <ows:OperationsMetadata>
                <ows:Operation name="GetCapabilities">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="http://localhost:3030/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
                <ows:Operation name="DescribeCoverage">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="http://localhost:3030/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
                <ows:Operation name="GetCoverage">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="http://localhost:3030/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
            </ows:OperationsMetadata>
            <wcs:Contents>
                <wcs:CoverageSummary>
                    <ows:Title>Workflow 8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62</ows:Title>
                    <ows:WGS84BoundingBox>
                        <ows:LowerCorner>-180.0 -90.0</ows:LowerCorner>
                        <ows:UpperCorner>180.0 90.0</ows:UpperCorner>
                    </ows:WGS84BoundingBox>
                    <wcs:Identifier>8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62</wcs:Identifier>
                </wcs:CoverageSummary>
            </wcs:Contents>
    </wcs:Capabilities>""",
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            m_requests.get(
                # pylint: disable=line-too-long
                "http://mock-instance/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62?version=1.1.1&request=GetCoverage"
                "&service=WCS&identifier=8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62&boundingbox=-90.0,-180.0,90.0,180.0"
                "&timesequence=2014-04-01T12%3A00%3A00.000%2B00%3A00&format=image/tiff&store=False&crs=urn:ogc:def:crs:EPSG::4326&resx=-22.5&resy=45.0",
                body=ndvi_tiff,
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            time = datetime.strptime("2014-04-01T12:00:00.000Z", ge.DEFAULT_ISO_TIME_FORMAT)

            query = ge.QueryRectangle(ge.BoundingBox2D(-180.0, -90.0, 180.0, 90.0), ge.TimeInterval(time))

            array = workflow.get_array(
                query,
                spatial_resolution=ge.SpatialResolution(360.0 / 8, 180.0 / 8),
            )

            self.assertEqual(array.shape, (8, 8))

            expected = np.array(
                [
                    [255, 255, 21, 11, 255, 255, 255, 255],
                    [255, 100, 30, 255, 156, 94, 106, 37],
                    [255, 64, 255, 255, 255, 31, 207, 255],
                    [255, 255, 255, 255, 89, 255, 255, 255],
                    [255, 255, 243, 255, 186, 255, 255, 255],
                    [255, 255, 115, 255, 139, 255, 255, 255],
                    [255, 255, 255, 255, 255, 255, 255, 255],
                    [255, 255, 255, 255, 255, 255, 255, 255],
                ]
            )

            self.assertTrue(np.array_equal(array, expected))

    def test_error(self):
        with UrllibMocker() as m_urllib:
            m_urllib.post(
                "http://mock-instance/anonymous",
                json={"id": "c4983c3e-9b53-47ae-bda9-382223bd5081", "project": None, "view": None},
            )

            m_urllib.post(
                "http://mock-instance/workflow",
                json={"id": "8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62"},
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            m_urllib.get(
                "http://mock-instance/workflow/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62/metadata",
                json={
                    "type": "raster",
                    "dataType": "U8",
                    "spatialReference": "EPSG:4326",
                    "bands": [{"name": "band", "measurement": {"type": "unitless"}}],
                    "spatialGrid": {
                        "descriptor": "source",
                        "spatialGrid": {
                            "geoTransform": {
                                "originCoordinate": {"x": 0.0, "y": 0.0},
                                "xPixelSize": 1.0,
                                "yPixelSize": -1.0,
                            },
                            "gridBounds": {
                                "topLeftIdx": {"xIdx": 0, "yIdx": 0},
                                "bottomRightIdx": {"xIdx": 10, "yIdx": 20},
                            },
                        },
                    },
                    "time": {
                        "bounds": {"start": 0, "end": 100000},
                        "dimension": {"type": "irregular"},
                    },
                },
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            ge.initialize("http://mock-instance")

            workflow_definition = {
                "type": "Raster",
                "operator": {
                    "type": "GdalSource",
                    "params": {"data": {"type": "internal", "datasetId": "36574dc3-560a-4b09-9d22-d5945f2b8093"}},
                },
            }

            workflow = ge.register_workflow(workflow_definition)

        with requests_mock.Mocker() as m_requests:
            m_requests.get(
                # pylint: disable=line-too-long
                "http://mock-instance/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62?service=WCS&request=GetCapabilities&version=1.1.1",
                text="""<?xml version="1.0" encoding="UTF-8"?>
    <wcs:Capabilities version="1.1.1"
            xmlns:wcs="http://www.opengis.net/wcs/1.1.1"
            xmlns:xlink="http://www.w3.org/1999/xlink"
            xmlns:ogc="http://www.opengis.net/ogc"
            xmlns:ows="http://www.opengis.net/ows/1.1"
            xmlns:gml="http://www.opengis.net/gml"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wcs/1.1.1
                http://localhost:3030/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62/schemas/wcs/1.1.1/wcsGetCapabilities.xsd"
                updateSequence="152">
            <ows:ServiceIdentification>
                <ows:Title>Web Coverage Service</ows:Title>
                <ows:ServiceType>WCS</ows:ServiceType>
                <ows:ServiceTypeVersion>1.1.1</ows:ServiceTypeVersion>
                <ows:Fees>NONE</ows:Fees>
                <ows:AccessConstraints>NONE</ows:AccessConstraints>
            </ows:ServiceIdentification>
            <ows:ServiceProvider>
                <ows:ProviderName>Provider Name</ows:ProviderName>
            </ows:ServiceProvider>
            <ows:OperationsMetadata>
                <ows:Operation name="GetCapabilities">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="http://localhost:3030/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
                <ows:Operation name="DescribeCoverage">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="http://localhost:3030/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
                <ows:Operation name="GetCoverage">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="http://localhost:3030/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
            </ows:OperationsMetadata>
            <wcs:Contents>
                <wcs:CoverageSummary>
                    <ows:Title>Workflow 8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62</ows:Title>
                    <ows:WGS84BoundingBox>
                        <ows:LowerCorner>-180.0 -90.0</ows:LowerCorner>
                        <ows:UpperCorner>180.0 90.0</ows:UpperCorner>
                    </ows:WGS84BoundingBox>
                    <wcs:Identifier>8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62</wcs:Identifier>
                </wcs:CoverageSummary>
            </wcs:Contents>
    </wcs:Capabilities>""",
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            m_requests.get(
                # pylint: disable=line-too-long
                "http://mock-instance/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62?version=1.1.1&request=GetCoverage&service=WCS"
                "&identifier=8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62&boundingbox=-90.0,-180.0,90.0,180.0"
                "&timesequence=2014-04-01T12%3A00%3A00.000%2B00%3A00&format=image/tiff&store=False&crs=urn:ogc:def:crs:EPSG::4326&resx=-22.5&resy=45.0",
                json={
                    "error": "Operator",
                    "message": "Operator: Could not open gdal dataset for file path "
                    '"test_data/raster/modis_ndvi/MOD13A2_M_NDVI_2004-04-01.TIFF"',
                },
                status_code=400,
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            time = datetime.strptime("2014-04-01T12:00:00.000Z", ge.DEFAULT_ISO_TIME_FORMAT)

            query = ge.QueryRectangle(
                ge.BoundingBox2D(-180.0, -90.0, 180.0, 90.0),
                ge.TimeInterval(time, time),
            )

            with self.assertRaises(owslib.util.ServiceException) as ctx:
                workflow.get_array(
                    query,
                    spatial_resolution=ge.SpatialResolution(360.0 / 8, 180.0 / 8),
                )

            self.assertEqual(
                str(ctx.exception),
                '{"error": "Operator", "message": "Operator: Could not open gdal dataset for file path '
                '\\"test_data/raster/modis_ndvi/MOD13A2_M_NDVI_2004-04-01.TIFF\\""}',
            )

    def test_ndvi_xarray(self):
        with UrllibMocker() as m_urllib:
            m_urllib.post(
                "http://mock-instance/anonymous",
                json={"id": "c4983c3e-9b53-47ae-bda9-382223bd5081", "project": None, "view": None},
            )

            m_urllib.post(
                "http://mock-instance/workflow",
                json={"id": "8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62"},
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            m_urllib.get(
                "http://mock-instance/workflow/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62/metadata",
                json={
                    "type": "raster",
                    "dataType": "U8",
                    "spatialReference": "EPSG:4326",
                    "bands": [{"name": "band", "measurement": {"type": "unitless"}}],
                    "spatialGrid": {
                        "descriptor": "source",
                        "spatialGrid": {
                            "geoTransform": {
                                "originCoordinate": {"x": 0.0, "y": 0.0},
                                "xPixelSize": 1.0,
                                "yPixelSize": -1.0,
                            },
                            "gridBounds": {
                                "topLeftIdx": {"xIdx": 0, "yIdx": 0},
                                "bottomRightIdx": {"xIdx": 10, "yIdx": 20},
                            },
                        },
                    },
                    "time": {
                        "bounds": {"start": 0, "end": 100000},
                        "dimension": {"type": "irregular"},
                    },
                },
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            ge.initialize("http://mock-instance")

            workflow_definition = {
                "type": "Raster",
                "operator": {
                    "type": "GdalSource",
                    "params": {"data": {"type": "internal", "datasetId": "36574dc3-560a-4b09-9d22-d5945f2b8093"}},
                },
            }

            workflow = ge.register_workflow(workflow_definition)

        with requests_mock.Mocker() as m_requests, open("tests/responses/ndvi.tiff", "rb") as ndvi_tiff:
            m_requests.get(
                # pylint: disable=line-too-long
                "http://mock-instance/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62?version=1.1.1&request=GetCoverage"
                "&service=WCS&identifier=8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62&boundingbox=-90.0,-180.0,90.0,180.0"
                "&timesequence=2014-04-01T12%3A00%3A00.000%2B00%3A00&format=image/tiff&store=False"
                "&crs=urn:ogc:def:crs:EPSG::4326&resx=-22.5&resy=45.0",
                body=ndvi_tiff,
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            m_requests.get(
                # pylint: disable=line-too-long
                "http://mock-instance/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62?service=WCS&request=GetCapabilities&version=1.1.1",
                text="""<?xml version="1.0" encoding="UTF-8"?>
    <wcs:Capabilities version="1.1.1"
            xmlns:wcs="http://www.opengis.net/wcs/1.1.1"
            xmlns:xlink="http://www.w3.org/1999/xlink"
            xmlns:ogc="http://www.opengis.net/ogc"
            xmlns:ows="http://www.opengis.net/ows/1.1"
            xmlns:gml="http://www.opengis.net/gml"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wcs/1.1.1
                http://localhost:3030/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62/schemas/wcs/1.1.1/wcsGetCapabilities.xsd"
                updateSequence="152">
            <ows:ServiceIdentification>
                <ows:Title>Web Coverage Service</ows:Title>
                <ows:ServiceType>WCS</ows:ServiceType>
                <ows:ServiceTypeVersion>1.1.1</ows:ServiceTypeVersion>
                <ows:Fees>NONE</ows:Fees>
                <ows:AccessConstraints>NONE</ows:AccessConstraints>
            </ows:ServiceIdentification>
            <ows:ServiceProvider>
                <ows:ProviderName>Provider Name</ows:ProviderName>
            </ows:ServiceProvider>
            <ows:OperationsMetadata>
                <ows:Operation name="GetCapabilities">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="http://localhost:3030/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
                <ows:Operation name="DescribeCoverage">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="http://localhost:3030/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
                <ows:Operation name="GetCoverage">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="http://localhost:3030/wcs/8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
            </ows:OperationsMetadata>
            <wcs:Contents>
                <wcs:CoverageSummary>
                    <ows:Title>Workflow 8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62</ows:Title>
                    <ows:WGS84BoundingBox>
                        <ows:LowerCorner>-180.0 -90.0</ows:LowerCorner>
                        <ows:UpperCorner>180.0 90.0</ows:UpperCorner>
                    </ows:WGS84BoundingBox>
                    <wcs:Identifier>8df9b0e6-e4b4-586e-90a3-6cf0f08c4e62</wcs:Identifier>
                </wcs:CoverageSummary>
            </wcs:Contents>
    </wcs:Capabilities>""",
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            time = datetime.strptime("2014-04-01T12:00:00.000Z", ge.DEFAULT_ISO_TIME_FORMAT)

            query = ge.QueryRectangle(
                ge.BoundingBox2D(-180.0, -90.0, 180.0, 90.0),
                ge.TimeInterval(time),
            )

            array = workflow.get_xarray(query, spatial_resolution=ge.SpatialResolution(360.0 / 8, 180.0 / 8))

            self.assertEqual(array.shape, (1, 8, 8))

            expected = xr.DataArray(
                np.array(
                    [
                        [
                            [255, 255, 21, 11, 255, 255, 255, 255],
                            [255, 100, 30, 255, 156, 94, 106, 37],
                            [255, 64, 255, 255, 255, 31, 207, 255],
                            [255, 255, 255, 255, 89, 255, 255, 255],
                            [255, 255, 243, 255, 186, 255, 255, 255],
                            [255, 255, 115, 255, 139, 255, 255, 255],
                            [255, 255, 255, 255, 255, 255, 255, 255],
                            [255, 255, 255, 255, 255, 255, 255, 255],
                        ]
                    ],
                    dtype=np.uint8,
                ),
                coords={
                    "band": [1],
                    "x": [-157.5, -112.5, -67.5, -22.5, 22.5, 67.5, 112.5, 157.5],
                    "y": [78.75, 56.25, 33.75, 11.25, -11.25, -33.75, -56.25, -78.75],
                    "spatial_ref": 0,
                },
                dims=["band", "y", "x"],
                attrs={
                    # 'AREA_OR_POINT': 'Area', # TODO: not included in min_version
                    # 'transform': TODO: could be "(45.0, 0.0, -180.0, 0.0, -22.5, 90.0, 0.0, 0.0, 1.0)" or "Affine ..."
                    # 'crs': " TODO: could be "CRS.from_wkt(..." or "CRS.from_epsg(..."
                    "res": (45.0, -22.5),
                    "scale_factor": 1.0,
                    "_FillValue": 0.0,
                    "add_offset": 0.0,
                },
            )

            # test actual array data
            self.assertTrue(np.array_equal(array.data, expected.data), msg=f"{array.data} \n!=\n {expected.data}")

            # test dims
            self.assertEqual(array.dims, expected.dims, msg=f"{array.dims} \n!=\n {expected.dims}")

            # test coords
            self.assertTrue(
                np.array_equal(array.coords, expected.coords), msg=f"{array.coords} \n!=\n {expected.coords}"
            )

            # test attributes
            # Since OWSLib produces different outputs depending on its version, we don't test equality on attributes.
            # Instead, we only check the expected attributes (the ones we need) and ignore the rest.
            # Problematic attributes are:
            #   "AREA_OR_POINT":    This is not available with OWSLib min_version.
            #   "transform":        This used to be a tuple but in newer versions it is an instance of "Affine".
            #   "crs":              OWS "CRS.from_wkt(..." or "CRS.from_epsg(..." depending on the OWSLib version.
            self.assertEqual(array.attrs, array.attrs | expected.attrs, msg=f"{array.attrs} \n!=\n {expected.attrs}")


if __name__ == "__main__":
    unittest.main()
