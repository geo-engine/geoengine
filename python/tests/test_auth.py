"""Tests regarding Geo Engine authentication"""

import os
import unittest
from datetime import datetime

import geoengine_openapi_client

import geoengine as ge
from geoengine.error import GeoEngineException
from geoengine.types import QueryRectangle
from tests.ge_test import GeoEngineTestInstance
from tests.util import NOT_FOUND_UUID

from . import UrllibMocker


class AuthTests(unittest.TestCase):
    """Tests runner regarding Geo Engine authentication"""

    def setUp(self) -> None:
        assert (
            "GEOENGINE_EMAIL" not in os.environ
            and "GEOENGINE_PASSWORD" not in os.environ
            and "GEOENGINE_TOKEN" not in os.environ
        ), "Please unset GEOENGINE_EMAIL, GEOENGINE_PASSWORD and GEOENGINE_TOKEN"
        ge.reset(False)

    def test_uninitialized(self):
        with self.assertRaises(ge.UninitializedException) as exception:
            ge.workflow_by_id(NOT_FOUND_UUID).get_dataframe(
                QueryRectangle(
                    ge.BoundingBox2D(-180, -90, 180, 90),
                    ge.TimeInterval(datetime.now()),
                )
            )

        self.assertEqual(str(exception.exception), "You have to call `initialize` before using other functionality")

    def test_initialize(self):
        def get_session_id(session: ge.Session) -> str:
            return session.auth_header["Authorization"].split(" ")[1]

        # TODO: use `enterContext(cm)` instead of `with cm:` in Python 3.11
        with GeoEngineTestInstance() as ge_instance:
            ge_instance.wait_for_ready()

            # anonymous

            ge.initialize(ge_instance.address())

            self.assertEqual(type(ge.get_session()), ge.Session)

            session_id = get_session_id(ge.get_session())

            ge.reset(False)

            # token as parameter

            ge.initialize(ge_instance.address(), token=session_id)

            self.assertEqual(get_session_id(ge.get_session()), session_id)

            ge.reset(False)

            # token as environment variable

            os.environ["GEOENGINE_TOKEN"] = session_id

            try:
                ge.initialize(ge_instance.address())
            finally:
                del os.environ["GEOENGINE_TOKEN"]

            self.assertEqual(get_session_id(ge.get_session()), session_id)

    def test_initialize_tuple(self):
        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/login",
                expected_request_body={"email": "foo@bar.de", "password": "secret123"},
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

            ge.initialize("http://mock-instance", ("foo@bar.de", "secret123"))

            self.assertEqual(type(ge.get_session()), ge.Session)

    def test_initialize_env(self):
        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/login",
                expected_request_body={"email": "foo@bar.de", "password": "secret123"},
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

            os.environ["GEOENGINE_EMAIL"] = "foo@bar.de"
            os.environ["GEOENGINE_PASSWORD"] = "secret123"

            try:
                ge.initialize("http://mock-instance")
            finally:
                del os.environ["GEOENGINE_EMAIL"]
                del os.environ["GEOENGINE_PASSWORD"]

            self.assertEqual(type(ge.get_session()), ge.Session)

    def test_user_agent(self):
        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/anonymous",
                request_headers={"User-Agent": f"geoengine-python/{geoengine_openapi_client.__version__}"},
                json={
                    "id": "e327d9c3-a4f3-4bd7-a5e1-30b26cae8064",
                    "user": None,
                    "created": "2021-06-08T15:22:22.605891994Z",
                    "validUntil": "2021-06-08T16:22:22.605892183Z",
                    "project": None,
                    "view": None,
                },
            )

            ge.initialize("http://mock-instance")

            self.assertEqual(type(ge.get_session()), ge.Session)

    def test_initialize_credentials_and_token(self):
        self.assertRaises(GeoEngineException, ge.initialize, "http://mock-instance", ("user", "pass"), "token")


if __name__ == "__main__":
    unittest.main()
