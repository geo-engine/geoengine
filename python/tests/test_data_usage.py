"""Tests for WMS calls"""

import unittest
from uuid import UUID

import pandas as pd

import geoengine as ge

from . import UrllibMocker


class DataUsageTests(unittest.TestCase):
    """Test methods for data usage"""

    def setUp(self) -> None:
        ge.reset(False)

    def test_data_usage(self):
        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/login",
                expected_request_body={"email": "admin@localhost", "password": "adminadmin"},
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
                "http://mock-instance/quota/dataUsage?offset=0&limit=10",
                json=[
                    {
                        "timestamp": "2025-01-09T16:40:22.933Z",
                        "userId": "e440bffc-d899-4304-aace-b23fc56828b2",
                        "computationId": "7b08af4a-8793-4299-83c1-39d0c20560f5",
                        "data": "land_cover",
                        "count": 4,
                    },
                    {
                        "timestamp": "2025-01-09T16:40:10.970Z",
                        "userId": "e440bffc-d899-4304-aace-b23fc56828b2",
                        "computationId": "57fba95f-d693-432b-a65b-58ac225a384a",
                        "data": "land_cover",
                        "count": 4,
                    },
                ],
            )

            ge.initialize("http://mock-instance", ("admin@localhost", "adminadmin"))

            df = ge.data_usage(offset=0, limit=10)

            expected = pd.DataFrame(
                {
                    "computationId": [
                        UUID("7b08af4a-8793-4299-83c1-39d0c20560f5"),
                        UUID("57fba95f-d693-432b-a65b-58ac225a384a"),
                    ],
                    "count": [4, 4],
                    "data": ["land_cover", "land_cover"],
                    "timestamp": [
                        pd.Timestamp("2025-01-09 16:40:22.933000+0000", tz="UTC"),
                        pd.Timestamp("2025-01-09 16:40:10.970000+0000", tz="UTC"),
                    ],
                    "userId": [
                        UUID("e440bffc-d899-4304-aace-b23fc56828b2"),
                        UUID("e440bffc-d899-4304-aace-b23fc56828b2"),
                    ],
                }
            )

            pd.testing.assert_frame_equal(df, expected)

    def test_data_usage_summary(self):
        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/login",
                expected_request_body={"email": "admin@localhost", "password": "adminadmin"},
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
                "http://mock-instance/quota/dataUsage/summary?granularity=minutes&offset=0&limit=10",
                json=[{"timestamp": "2025-01-09T16:40:00.000Z", "data": "land_cover", "count": 8}],
            )

            ge.initialize("http://mock-instance", ("admin@localhost", "adminadmin"))

            df = ge.data_usage_summary(granularity=ge.UsageSummaryGranularity.MINUTES, offset=0, limit=10)

            expected = pd.DataFrame(
                {
                    "count": {0: 8},
                    "data": {0: "land_cover"},
                    "timestamp": {0: pd.Timestamp("2025-01-09 16:40:00+0000", tz="UTC")},
                }
            )

            pd.testing.assert_frame_equal(df, expected)
