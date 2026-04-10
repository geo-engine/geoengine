"""Tests regarding upload functionality"""

import unittest

import geopandas
import pandas as pd

import geoengine as ge
from geoengine.datasets import DatasetName, OgrSourceDatasetTimeType, OgrSourceDuration, OgrSourceTimeFormat
from geoengine.types import TimeStepGranularity
from tests.ge_test import GeoEngineTestInstance


class UploadTests(unittest.TestCase):
    """Test runner regarding upload functionality"""

    def setUp(self) -> None:
        ge.reset(logout=False)

    def test_upload(self):
        # TODO: use `enterContext(cm)` instead of `with cm:` in Python 3.11
        with GeoEngineTestInstance() as ge_instance:
            ge_instance.wait_for_ready()

            ge.initialize(ge_instance.address())

            df = pd.DataFrame({"label": ["NA", "DE"], "index": [0, 1], "rnd": [34.34, 567.547]})

            polygons = [
                # pylint: disable=line-too-long
                "Polygon((-121.46484375 47.109375, -99.31640625 17.2265625, -56.42578125 52.03125,-121.46484375 47.109375))",  # noqa: E501
                "Polygon((4.74609375 53.61328125, 5.09765625 43.06640625, 15.1171875 43.76953125, 15.1171875 54.4921875, 4.74609375 53.61328125))",  # noqa: E501
            ]

            gdf = geopandas.GeoDataFrame(df, geometry=geopandas.GeoSeries.from_wkt(polygons), crs="EPSG:4326")

            dataset_name_str = f"{ge.get_session().user_id}:test_upload"
            dataset_name = ge.upload_dataframe(gdf, name=dataset_name_str)

            self.assertEqual(dataset_name, DatasetName(dataset_name_str))

    def test_time_specification(self):
        time = OgrSourceDatasetTimeType.start(
            "start", OgrSourceTimeFormat.auto(), OgrSourceDuration.value(10, TimeStepGranularity.MINUTES)
        )

        self.assertEqual(
            time.to_api_dict().to_dict(),
            {
                "type": "start",
                "startField": "start",
                "startFormat": {"format": "auto"},
                "duration": {"type": "value", "step": 10, "granularity": "minutes"},
            },
        )


if __name__ == "__main__":
    unittest.main()
