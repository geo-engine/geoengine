"""Test for WFS calls"""

import json
import textwrap
import unittest
from datetime import datetime
from uuid import UUID

import geoengine_openapi_client
import geopandas as gpd
import geopandas.testing  # pylint: disable=unused-import
from numpy import nan
from shapely.geometry import Point

import geoengine as ge
from tests.util import NOT_FOUND_UUID

from . import UrllibMocker


class WfsTests(unittest.TestCase):
    """WFS test runner"""

    def setUp(self) -> None:
        ge.reset(False)

    def test_geopandas(self):
        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/anonymous",
                json={
                    "id": "e327d9c3-a4f3-4bd7-a5e1-30b26cae8064",
                    "user": {
                        "id": "328ca8d1-15d7-4f59-a989-5d5d72c98744",
                    },
                    "created": "2021-06-08T15:22:22.605891994Z",
                    "validUntil": "2021-06-08T16:22:22.605892183Z",
                    "project": None,
                    "view": None,
                },
            )

            m.post(
                "http://mock-instance/workflow",
                json={"id": "956d3656-2d14-5951-96a0-f962b92371cd"},
                request_headers={"Authorization": "Bearer e327d9c3-a4f3-4bd7-a5e1-30b26cae8064"},
            )

            m.get(
                "http://mock-instance/workflow/956d3656-2d14-5951-96a0-f962b92371cd/metadata",
                json={
                    "type": "vector",
                    "dataType": "MultiPoint",
                    "spatialReference": "EPSG:4326",
                    "columns": {
                        "scalerank": {
                            "dataType": "int",
                            "measurement": {"type": "unitless"},
                        },
                        "NDVI": {
                            "dataType": "int",
                            "measurement": {
                                "type": "continuous",
                                "measurement": "vegetation",
                            },
                        },
                        "featurecla": {
                            "dataType": "text",
                            "measurement": {"type": "unitless"},
                        },
                        "natlscale": {
                            "dataType": "float",
                            "measurement": {"type": "unitless"},
                        },
                        "website": {
                            "dataType": "text",
                            "measurement": {"type": "unitless"},
                        },
                        "name": {
                            "dataType": "text",
                            "measurement": {"type": "unitless"},
                        },
                    },
                },
                request_headers={"Authorization": "Bearer e327d9c3-a4f3-4bd7-a5e1-30b26cae8064"},
            )

            m.get(
                # pylint: disable=line-too-long
                "http://mock-instance/wfs/956d3656-2d14-5951-96a0-f962b92371cd?version=2.0.0&service=WFS&request=GetFeature&typeNames=956d3656-2d14-5951-96a0-f962b92371cd&bbox=-60.0%2C5.0%2C61.0%2C6.0&time=2014-04-01T12%3A00%3A00.000%2B00%3A00&srsName=EPSG%3A4326",
                json={
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [0.007420495, 5.631944444]},
                            "properties": {
                                "scalerank": 7,
                                "website": "www.ghanaports.gov.gh",
                                "NDVI": None,
                                "natlscale": 10.0,
                                "featurecla": "Port",
                                "name": "Tema",
                            },
                            "when": {
                                "start": "2014-04-01T00:00:00+00:00",
                                "end": "2014-05-01T00:00:00+00:00",
                                "type": "Interval",
                            },
                        },
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [-10.05265018, 5.858055556]},
                            "properties": {
                                "scalerank": 7,
                                "website": "www.nationalportauthorityliberia.org",
                                "NDVI": 178,
                                "natlscale": 10.0,
                                "featurecla": "Port",
                                "name": "Buchanan",
                            },
                            "when": {
                                "start": "2014-04-01T00:00:00+00:00",
                                "end": "2014-05-01T00:00:00+00:00",
                                "type": "Interval",
                            },
                        },
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [-57.00176678, 5.951666667]},
                            "properties": {
                                "scalerank": 6,
                                "website": None,
                                "NDVI": 108,
                                "natlscale": 20.0,
                                "featurecla": "Port",
                                "name": "Nieuw Nickerie",
                            },
                            "when": {
                                "start": "2014-04-01T00:00:00+00:00",
                                "end": "2014-05-01T00:00:00+00:00",
                                "type": "Interval",
                            },
                        },
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [-3.966666667, 5.233055556]},
                            "properties": {
                                "scalerank": 5,
                                "website": "www.paa-ci.org",
                                "NDVI": 99,
                                "natlscale": 30.0,
                                "featurecla": "Port",
                                "name": "Abidjan",
                            },
                            "when": {
                                "start": "2014-04-01T00:00:00+00:00",
                                "end": "2014-05-01T00:00:00+00:00",
                                "type": "Interval",
                            },
                        },
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [-52.62426384, 5.158888889]},
                            "properties": {
                                "scalerank": 5,
                                "website": None,
                                "NDVI": 159,
                                "natlscale": 30.0,
                                "featurecla": "Port",
                                "name": "Kourou",
                            },
                            "when": {
                                "start": "2014-04-01T00:00:00+00:00",
                                "end": "2014-05-01T00:00:00+00:00",
                                "type": "Interval",
                            },
                        },
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [-55.13898704, 5.82]},
                            "properties": {
                                "scalerank": 5,
                                "website": None,
                                "NDVI": 128,
                                "natlscale": 30.0,
                                "featurecla": "Port",
                                "name": "Paramaribo",
                            },
                            "when": {
                                "start": "2014-04-01T00:00:00+00:00",
                                "end": "2014-05-01T00:00:00+00:00",
                                "type": "Interval",
                            },
                        },
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [-4.021260306, 5.283333333]},
                            "properties": {
                                "scalerank": 3,
                                "website": "www.paa-ci.org",
                                "NDVI": 126,
                                "natlscale": 75.0,
                                "featurecla": "Port",
                                "name": "Abidjan",
                            },
                            "when": {
                                "start": "2014-04-01T00:00:00+00:00",
                                "end": "2014-05-01T00:00:00+00:00",
                                "type": "Interval",
                            },
                        },
                    ],
                },
                request_headers={"Authorization": "Bearer e327d9c3-a4f3-4bd7-a5e1-30b26cae8064"},
            )

            ge.initialize("http://mock-instance")

            workflow_definition = {
                "type": "Vector",
                "operator": {
                    "type": "RasterVectorJoin",
                    "params": {"names": ["NDVI"], "aggregation": "none"},
                    "sources": {
                        "vector": {
                            "type": "OgrSource",
                            "params": {
                                "data": {"type": "internal", "datasetId": "a9623a5b-b6c5-404b-bc5a-313ff72e4e75"},
                                "attributeProjection": None,
                            },
                        },
                        "rasters": [
                            {
                                "type": "GdalSource",
                                "params": {
                                    "data": {"type": "internal", "datasetId": "36574dc3-560a-4b09-9d22-d5945f2b8093"}
                                },
                            }
                        ],
                    },
                },
            }

            time = datetime.strptime("2014-04-01T12:00:00.000Z", ge.DEFAULT_ISO_TIME_FORMAT)

            workflow = ge.register_workflow(workflow_definition)

            # TODO: remove resolution when not mocked
            df = workflow.get_dataframe(
                ge.QueryRectangle(ge.BoundingBox2D(-60.0, 5.0, 61.0, 6.0), ge.TimeInterval(time, time))
            )

            self.assertEqual(len(m.request_history), 4)

            workflow_request = m.request_history[1]
            self.assertEqual(workflow_request["method"], "POST")
            self.assertEqual(workflow_request["url"], "http://mock-instance/workflow")
            self.assertEqual(json.loads(workflow_request["body"]), workflow_definition)

            # note: the result descriptor is retrieved upon workflow registration (constructor),
            # thus the actual WFS request is in the 4th history slot

            wfs_request = m.request_history[3]
            self.assertEqual(wfs_request["method"], "GET")
            self.assertEqual(
                # pylint: disable=line-too-long
                wfs_request["url"],
                "http://mock-instance/wfs/956d3656-2d14-5951-96a0-f962b92371cd?bbox=-60.0%2C5.0%2C61.0%2C6.0&request=GetFeature&service=WFS&srsName=EPSG%3A4326&time=2014-04-01T12%3A00%3A00.000%2B00%3A00&typeNames=956d3656-2d14-5951-96a0-f962b92371cd&version=2.0.0",
            )

            expected_df = gpd.GeoDataFrame(
                # pylint: disable=line-too-long
                {
                    "geometry": [
                        Point(0.007420495, 5.631944444),
                        Point(-10.05265018, 5.858055556),
                        Point(-57.00176678, 5.951666667),
                        Point(-3.966666667, 5.233055556),
                        Point(-52.62426384, 5.158888889),
                        Point(-55.13898704, 5.82),
                        Point(-4.021260306, 5.283333333),
                    ],
                    "scalerank": [7, 7, 6, 5, 5, 5, 3],
                    "website": [
                        "www.ghanaports.gov.gh",
                        "www.nationalportauthorityliberia.org",
                        None,
                        "www.paa-ci.org",
                        None,
                        None,
                        "www.paa-ci.org",
                    ],
                    "NDVI": [nan, 178.0, 108.0, 99.0, 159.0, 128.0, 126.0],
                    "natlscale": [10.0, 10.0, 20.0, 30.0, 30.0, 30.0, 75.0],
                    "featurecla": ["Port", "Port", "Port", "Port", "Port", "Port", "Port"],
                    "name": ["Tema", "Buchanan", "Nieuw Nickerie", "Abidjan", "Kourou", "Paramaribo", "Abidjan"],
                    "start": [
                        datetime.strptime("2014-04-01T00:00:00.000Z", ge.DEFAULT_ISO_TIME_FORMAT) for _ in range(7)
                    ],
                    "end": [
                        datetime.strptime("2014-05-01T00:00:00.000Z", ge.DEFAULT_ISO_TIME_FORMAT) for _ in range(7)
                    ],
                },
                geometry="geometry",
                crs="EPSG:4326",
            )

            gpd.testing.assert_geodataframe_equal(df, expected_df)

    def test_wfs_error(self):
        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/anonymous",
                json={
                    "id": "e327d9c3-a4f3-4bd7-a5e1-30b26cae8064",
                    "user": {
                        "id": "328ca8d1-15d7-4f59-a989-5d5d72c98744",
                    },
                    "created": "2021-06-08T15:22:22.605891994Z",
                    "validUntil": "2021-06-08T16:22:22.605892183Z",
                    "project": None,
                    "view": None,
                },
            )

            m.post(
                "http://mock-instance/workflow",
                json={"id": "956d3656-2d14-5951-96a0-f962b92371cd"},
                request_headers={"Authorization": "Bearer e327d9c3-a4f3-4bd7-a5e1-30b26cae8064"},
            )

            m.get(
                "http://mock-instance/workflow/956d3656-2d14-5951-96a0-f962b92371cd/metadata",
                json={
                    "type": "vector",
                    "dataType": "MultiPoint",
                    "spatialReference": "EPSG:4326",
                    "columns": {
                        "scalerank": {
                            "dataType": "int",
                            "measurement": {"type": "unitless"},
                        },
                        "NDVI": {
                            "dataType": "int",
                            "measurement": {
                                "type": "continuous",
                                "measurement": "vegetation",
                            },
                        },
                        "featurecla": {
                            "dataType": "text",
                            "measurement": {"type": "unitless"},
                        },
                        "natlscale": {
                            "dataType": "float",
                            "measurement": {"type": "unitless"},
                        },
                        "website": {
                            "dataType": "text",
                            "measurement": {"type": "unitless"},
                        },
                        "name": {
                            "dataType": "text",
                            "measurement": {"type": "unitless"},
                        },
                    },
                },
                request_headers={"Authorization": "Bearer e327d9c3-a4f3-4bd7-a5e1-30b26cae8064"},
            )

            m.get(
                # pylint: disable=line-too-long
                "http://mock-instance/wfs/956d3656-2d14-5951-96a0-f962b92371cd?version=2.0.0&service=WFS&request=GetFeature&typeNames=956d3656-2d14-5951-96a0-f962b92371cd&bbox=-60.0%2C5.0%2C61.0%2C6.0&time=2004-04-01T12%3A00%3A00.000%2B00%3A00&srsName=EPSG%3A4326",
                json={
                    "error": "Operator",
                    "message": "Operator: Could not open gdal dataset for file path "
                    '"test_data/raster/modis_ndvi/MOD13A2_M_NDVI_2004-04-01.TIFF"',
                },
                status_code=400,
                request_headers={"Authorization": "Bearer e327d9c3-a4f3-4bd7-a5e1-30b26cae8064"},
            )

            ge.initialize("http://mock-instance")

            workflow_definition = {
                "type": "Vector",
                "operator": {
                    "type": "RasterVectorJoin",
                    "params": {"names": ["NDVI"], "featureAggregation": "first", "temporalAggregation": "none"},
                    "sources": {
                        "vector": {
                            "type": "OgrSource",
                            "params": {
                                "data": {"type": "internal", "datasetId": "a9623a5b-b6c5-404b-bc5a-313ff72e4e75"},
                                "attributeProjection": None,
                            },
                        },
                        "rasters": [
                            {
                                "type": "GdalSource",
                                "params": {
                                    "data": {"type": "internal", "datasetId": "36574dc3-560a-4b09-9d22-d5945f2b8093"}
                                },
                            }
                        ],
                    },
                },
            }

            time = datetime.strptime("2004-04-01T12:00:00.000Z", ge.DEFAULT_ISO_TIME_FORMAT)

            workflow = ge.register_workflow(workflow_definition)

            with self.assertRaises(ge.BadRequestException) as ctx:
                # TODO: remove resolution when not mocked
                workflow.get_dataframe(
                    ge.QueryRectangle(ge.BoundingBox2D(-60.0, 5.0, 61.0, 6.0), ge.TimeInterval(time))
                )

            self.assertEqual(
                str(ctx.exception),
                "Operator: Operator: Could not open gdal dataset for file path "
                '"test_data/raster/modis_ndvi/MOD13A2_M_NDVI_2004-04-01.TIFF"',
            )

    def test_repr(self):
        uuid = UUID("5fdf8020-d8d9-4f93-81f5-6554ebe6e216")

        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/anonymous",
                json={
                    "id": "e327d9c3-a4f3-4bd7-a5e1-30b26cae8064",
                    "user": {
                        "id": "328ca8d1-15d7-4f59-a989-5d5d72c98744",
                    },
                    "created": "2021-06-08T15:22:22.605891994Z",
                    "validUntil": "2021-06-08T16:22:22.605892183Z",
                    "project": None,
                    "view": None,
                },
            )

            m.get(
                f"http://mock-instance/workflow/{uuid}/metadata",
                json={
                    "type": "vector",
                    "dataType": "MultiPoint",
                    "spatialReference": "EPSG:4326",
                    "columns": {
                        "scalerank": {
                            "dataType": "int",
                            "measurement": {"type": "unitless"},
                        },
                        "NDVI": {
                            "dataType": "int",
                            "measurement": {
                                "type": "continuous",
                                "measurement": "vegetation",
                            },
                        },
                        "featurecla": {
                            "dataType": "text",
                            "measurement": {"type": "unitless"},
                        },
                        "natlscale": {
                            "dataType": "float",
                            "measurement": {"type": "unitless"},
                        },
                        "website": {
                            "dataType": "text",
                            "measurement": {"type": "unitless"},
                        },
                        "name": {
                            "dataType": "text",
                            "measurement": {"type": "unitless"},
                        },
                    },
                },
                request_headers={"Authorization": "Bearer e327d9c3-a4f3-4bd7-a5e1-30b26cae8064"},
            )

            ge.initialize("http://mock-instance")

            workflow = ge.workflow_by_id(uuid)

            self.assertEqual(repr(workflow), str(uuid))

    def test_result_descriptor(self):
        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/anonymous",
                json={
                    "id": "e327d9c3-a4f3-4bd7-a5e1-30b26cae8064",
                    "user": {
                        "id": "328ca8d1-15d7-4f59-a989-5d5d72c98744",
                    },
                    "created": "2021-06-08T15:22:22.605891994Z",
                    "validUntil": "2021-06-08T16:22:22.605892183Z",
                    "project": None,
                    "view": None,
                },
            )

            m.get(
                "http://mock-instance/workflow/4cdf1ffe-cb67-5de2-a1f3-3357ae0112bd/metadata",
                json={
                    "type": "vector",
                    "dataType": "MultiPoint",
                    "spatialReference": "EPSG:4326",
                    "columns": {
                        "scalerank": {
                            "dataType": "int",
                            "measurement": {"type": "unitless"},
                        },
                        "NDVI": {
                            "dataType": "int",
                            "measurement": {
                                "type": "continuous",
                                "measurement": "vegetation",
                            },
                        },
                        "featurecla": {
                            "dataType": "text",
                            "measurement": {"type": "unitless"},
                        },
                        "natlscale": {
                            "dataType": "float",
                            "measurement": {"type": "unitless"},
                        },
                        "website": {
                            "dataType": "text",
                            "measurement": {"type": "unitless"},
                        },
                        "name": {
                            "dataType": "text",
                            "measurement": {"type": "unitless"},
                        },
                    },
                },
                request_headers={"Authorization": "Bearer e327d9c3-a4f3-4bd7-a5e1-30b26cae8064"},
            )

            m.get(
                f"http://mock-instance/workflow/{NOT_FOUND_UUID}/metadata",
                status_code=404,
                json={
                    "error": "NotFound",
                    "message": "Not Found",
                },
                request_headers={"Authorization": "Bearer e327d9c3-a4f3-4bd7-a5e1-30b26cae8064"},
            )

            ge.initialize("http://mock-instance")

            workflow = ge.workflow_by_id("4cdf1ffe-cb67-5de2-a1f3-3357ae0112bd")

            result_descriptor = workflow.get_result_descriptor()

            expected_repr = """\
                Data type:         MultiPoint
                Spatial Reference: EPSG:4326
                Columns:
                  scalerank:
                    Column Type: int
                    Measurement: unitless
                  NDVI:
                    Column Type: int
                    Measurement: vegetation
                  featurecla:
                    Column Type: text
                    Measurement: unitless
                  natlscale:
                    Column Type: float
                    Measurement: unitless
                  website:
                    Column Type: text
                    Measurement: unitless
                  name:
                    Column Type: text
                    Measurement: unitless
            """

            self.assertEqual(repr(result_descriptor), textwrap.dedent(expected_repr))

            with self.assertRaises(ge.NotFoundException) as exception:
                workflow = ge.workflow_by_id(NOT_FOUND_UUID)

                result_descriptor = workflow.get_result_descriptor()

            self.assertEqual(str(exception.exception), "NotFound: Not Found")

    def test_workflow_retrieval(self):
        workflow_definition = {
            "type": "Vector",
            "operator": {
                "type": "RasterVectorJoin",
                "params": {"names": ["NDVI"], "featureAggregation": "first", "temporalAggregation": "none"},
                "sources": {
                    "vector": {
                        "type": "OgrSource",
                        "params": {
                            "data": {"type": "internal", "datasetId": "a9623a5b-b6c5-404b-bc5a-313ff72e4e75"},
                            "attributeProjection": None,
                        },
                    },
                    "rasters": [
                        {
                            "type": "GdalSource",
                            "params": {
                                "data": {"type": "internal", "datasetId": "36574dc3-560a-4b09-9d22-d5945f2b8093"}
                            },
                        }
                    ],
                },
            },
        }

        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/anonymous",
                json={"id": "c4983c3e-9b53-47ae-bda9-382223bd5081", "project": None, "view": None},
            )

            m.post(
                "http://mock-instance/workflow",
                json={"id": "4a2cb6e0-a3e3-53e4-9a0f-ed1cf2e4c3b7"},
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            m.get(
                "http://mock-instance/workflow/4a2cb6e0-a3e3-53e4-9a0f-ed1cf2e4c3b7/metadata",
                json={
                    "type": "vector",
                    "dataType": "MultiPoint",
                    "spatialReference": "EPSG:4326",
                    "columns": {
                        "scalerank": {
                            "dataType": "int",
                            "measurement": {"type": "unitless"},
                        },
                        "NDVI": {
                            "dataType": "int",
                            "measurement": {
                                "type": "continuous",
                                "measurement": "vegetation",
                            },
                        },
                        "featurecla": {
                            "dataType": "text",
                            "measurement": {"type": "unitless"},
                        },
                        "natlscale": {
                            "dataType": "float",
                            "measurement": {"type": "unitless"},
                        },
                        "website": {
                            "dataType": "text",
                            "measurement": {"type": "unitless"},
                        },
                        "name": {
                            "dataType": "text",
                            "measurement": {"type": "unitless"},
                        },
                    },
                },
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            m.get(
                "http://mock-instance/workflow/4a2cb6e0-a3e3-53e4-9a0f-ed1cf2e4c3b7",
                json=workflow_definition,
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            ge.initialize("http://mock-instance")

            workflow = ge.register_workflow(workflow_definition)

            self.assertEqual(workflow.workflow_definition().to_dict(), workflow_definition)

    def test_owslib_user_agent(self):
        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/anonymous",
                json={
                    "id": "e327d9c3-a4f3-4bd7-a5e1-30b26cae8064",
                    "user": {
                        "id": "328ca8d1-15d7-4f59-a989-5d5d72c98744",
                    },
                    "created": "2021-06-08T15:22:22.605891994Z",
                    "validUntil": "2021-06-08T16:22:22.605892183Z",
                    "project": None,
                    "view": None,
                },
            )

            m.post(
                "http://mock-instance/workflow",
                json={"id": "956d3656-2d14-5951-96a0-f962b92371cd"},
                request_headers={"Authorization": "Bearer e327d9c3-a4f3-4bd7-a5e1-30b26cae8064"},
            )

            m.get(
                "http://mock-instance/workflow/956d3656-2d14-5951-96a0-f962b92371cd/metadata",
                json={"type": "vector", "dataType": "MultiPoint", "spatialReference": "EPSG:4326", "columns": {}},
                request_headers={"Authorization": "Bearer e327d9c3-a4f3-4bd7-a5e1-30b26cae8064"},
            )

            m.get(
                # pylint: disable=line-too-long
                "http://mock-instance/wfs/956d3656-2d14-5951-96a0-f962b92371cd?version=2.0.0&service=WFS&request=GetFeature&typeNames=956d3656-2d14-5951-96a0-f962b92371cd&bbox=-60.0%2C5.0%2C61.0%2C6.0&time=2004-04-01T12%3A00%3A00.000%2B00%3A00&srsName=EPSG%3A4326",
                json={
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [-4.021260306, 5.283333333]},
                            "properties": {
                                "scalerank": 3,
                                "website": "www.paa-ci.org",
                                "NDVI": 126,
                                "natlscale": 75.0,
                                "featurecla": "Port",
                                "name": "Abidjan",
                            },
                            "when": {
                                "start": "2014-04-01T00:00:00+00:00",
                                "end": "2014-05-01T00:00:00+00:00",
                                "type": "Interval",
                            },
                        }
                    ],
                },
                request_headers={
                    "Authorization": "Bearer e327d9c3-a4f3-4bd7-a5e1-30b26cae8064",
                    "User-Agent": f"geoengine/openapi-client/python/{geoengine_openapi_client.__version__}",
                },
            )

            ge.initialize("http://mock-instance")

            workflow_definition = {
                "type": "Vector",
                "operator": {
                    "type": "OgrSource",
                    "params": {
                        "data": {"type": "internal", "datasetId": "a9623a5b-b6c5-404b-bc5a-313ff72e4e75"},
                        "attributeProjection": None,
                    },
                },
            }

            time = datetime.strptime("2004-04-01T12:00:00.000Z", ge.DEFAULT_ISO_TIME_FORMAT)

            workflow = ge.register_workflow(workflow_definition)

            df = workflow.get_dataframe(
                ge.QueryRectangle(ge.BoundingBox2D(-60.0, 5.0, 61.0, 6.0), ge.TimeInterval(time))
            )

            self.assertTrue(df is not None)


if __name__ == "__main__":
    unittest.main()
