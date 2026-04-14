"""Tests for the plotting functionality"""

import textwrap
import unittest
from datetime import datetime

import numpy as np
from vega import VegaLite

import geoengine as ge
from tests.ge_test import GeoEngineTestInstance
from tests.util import NOT_FOUND_UUID

from . import UrllibMocker


class PlotTests(unittest.TestCase):
    """Test runner for the plotting functionality"""

    def setUp(self) -> None:
        ge.reset(False)

    def test_ndvi_histogram(self):
        # TODO: use `enterContext(cm)` instead of `with cm:` in Python 3.11
        with GeoEngineTestInstance() as ge_instance:
            ge_instance.wait_for_ready()

            ge.initialize(ge_instance.address())

            workflow_definition = {
                "type": "Plot",
                "operator": {
                    "type": "Histogram",
                    "params": {"attributeName": "ndvi", "bounds": "data", "buckets": {"type": "number", "value": 20}},
                    "sources": {
                        "source": ge.workflow_builder.operators.GdalSource("ndvi").to_workflow_dict()["operator"]
                    },
                },
            }

            workflow = ge.register_workflow(workflow_definition)

            vega_chart = workflow.plot_chart(
                ge.QueryRectangle(
                    ge.BoundingBox2D(-180.0, -90.0, 180.0, 90.0),
                    ge.TimeInterval(np.datetime64("2014-04-01T12:00:00")),
                ),
                spatial_resolution=ge.SpatialResolution(0.1, 0.1),
            )

            self.assertEqual(type(vega_chart), VegaLite)

    def test_result_descriptor(self):
        # pylint: disable=duplicate-code

        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/anonymous",
                json={"id": "c4983c3e-9b53-47ae-bda9-382223bd5081", "project": None, "view": None},
            )

            m.get(
                "http://mock-instance/workflow/5b9508a8-bd34-5a1c-acd6-75bb832d2d38/metadata",
                json={
                    "type": "plot",
                    "spatialReference": "EPSG:4326",
                },
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            m.get(
                f"http://mock-instance/workflow/{NOT_FOUND_UUID}/metadata",
                status_code=404,
                json={
                    "error": "NotFound",
                    "message": "Not Found",
                },
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            ge.initialize("http://mock-instance")

            workflow = ge.workflow_by_id("5b9508a8-bd34-5a1c-acd6-75bb832d2d38")

            result_descriptor = workflow.get_result_descriptor()

            expected_repr = "Plot Result"

            self.assertEqual(repr(result_descriptor), textwrap.dedent(expected_repr))

            with self.assertRaises(ge.NotFoundException) as exception:
                workflow = ge.workflow_by_id(NOT_FOUND_UUID)

                result_descriptor = workflow.get_result_descriptor()

            self.assertEqual(str(exception.exception), "NotFound: Not Found")

    def test_wrong_request(self):
        # pylint: disable=duplicate-code

        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/anonymous",
                json={"id": "c4983c3e-9b53-47ae-bda9-382223bd5081", "project": None, "view": None},
            )

            m.get(
                "http://mock-instance/workflow/5b9508a8-bd34-5a1c-acd6-75bb832d2d38/metadata",
                json={
                    "type": "plot",
                    "spatialReference": "EPSG:4326",
                },
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            m.get(
                "http://mock-instance/workflow/foo/metadata",
                json={
                    "error": "NotFound",
                    "message": "Not Found",
                },
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            ge.initialize("http://mock-instance")

            workflow = ge.workflow_by_id("5b9508a8-bd34-5a1c-acd6-75bb832d2d38")

            time = datetime.strptime("2014-04-01T12:00:00.000Z", ge.DEFAULT_ISO_TIME_FORMAT)

            with self.assertRaises(ge.MethodNotCalledOnVectorException):
                workflow.get_dataframe(
                    ge.QueryRectangle(
                        ge.BoundingBox2D(-180.0, -90.0, 180.0, 90.0),
                        ge.TimeInterval(time),
                    )
                )

    def test_plot_error(self):
        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/anonymous",
                json={"id": "c4983c3e-9b53-47ae-bda9-382223bd5081", "project": None, "view": None},
            )

            m.post(
                "http://mock-instance/workflow",
                json={"id": "5b9508a8-bd34-5a1c-acd6-75bb832d2d38"},
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            m.get(
                "http://mock-instance/workflow/5b9508a8-bd34-5a1c-acd6-75bb832d2d38/metadata",
                json={
                    "type": "plot",
                    "spatialReference": "EPSG:4326",
                },
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            m.get(
                # pylint: disable=line-too-long
                "http://mock-instance/plot/5b9508a8-bd34-5a1c-acd6-75bb832d2d38?bbox=-180.0%2C-90.0%2C180.0%2C90.0&crs=EPSG%3A4326&time=2004-04-01T12%3A00%3A00.000%2B00%3A00&spatialResolution=0.1%2C0.1",
                status_code=400,
                json={
                    "error": "Operator",
                    "message": "Operator: Could not open gdal dataset for file path "
                    '"test_data/raster/modis_ndvi/MOD13A2_M_NDVI_2004-04-01.TIFF"',
                },
                request_headers={"Authorization": "Bearer c4983c3e-9b53-47ae-bda9-382223bd5081"},
            )

            ge.initialize("http://mock-instance")

            workflow_definition = {
                "type": "Plot",
                "operator": {
                    "type": "Histogram",
                    "params": {"bounds": "data", "buckets": 20},
                    "sources": {
                        "source": {
                            "type": "GdalSource",
                            "params": {
                                "data": {"type": "internal", "datasetId": "36574dc3-560a-4b09-9d22-d5945f2b8093"}
                            },
                        }
                    },
                },
            }

            time = datetime.strptime("2004-04-01T12:00:00.000Z", ge.DEFAULT_ISO_TIME_FORMAT)

            workflow = ge.register_workflow(workflow_definition)

            with self.assertRaises(ge.BadRequestException) as ctx:
                workflow.plot_chart(
                    ge.QueryRectangle(
                        ge.BoundingBox2D(-180.0, -90.0, 180.0, 90.0),
                        ge.TimeInterval(time),
                    ),
                    spatial_resolution=ge.SpatialResolution(0.1, 0.1),
                )

            self.assertEqual(
                str(ctx.exception),
                "Operator: Operator: Could not open gdal dataset for file path "
                '"test_data/raster/modis_ndvi/MOD13A2_M_NDVI_2004-04-01.TIFF"',
            )


if __name__ == "__main__":
    unittest.main()
