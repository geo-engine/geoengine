"""
Provides a Geo Engine instance for unit testing purposes.
"""

import logging
import os
import random
import shutil
import socket
import string
import subprocess
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Optional

import psycopg
from dotenv import load_dotenv

TEST_CODE_PATH_VAR = "GEOENGINE_TEST_CODE_PATH"
TEST_BUILD_TYPE_VAR = "GEOENGINE_TEST_BUILD_TYPE"

POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "geoengine"
POSTGRES_USER = "geoengine"
POSTGRES_PASSWORD = "geoengine"

GE_LOG_SPEC = "info"


@contextmanager
def GeoEngineTestInstance(port: int | None = None) -> Iterator["GeoEngineProcess"]:  # pylint: disable=invalid-name
    """Provides a Geo Engine instance for unit testing purposes."""

    load_dotenv()

    if TEST_CODE_PATH_VAR not in os.environ:
        raise RuntimeError(f"Environment variable {TEST_CODE_PATH_VAR} not set")

    if os.environ.get("GEOENGINE_TEST_BUILD_TYPE", "debug").lower() == "release":
        build_type = BuildType.RELEASE
    else:
        build_type = BuildType.DEBUG

    geo_engine_binaries = GeoEngineBinaries(Path(os.environ[TEST_CODE_PATH_VAR]), build_type)

    try:
        ge = GeoEngineProcess(
            geo_engine_binaries=geo_engine_binaries,
            port=get_open_port() if port is None else port,
            db_schema=generate_test_schema_name(),
        )
        ge._start()  # pylint: disable=protected-access
        yield ge
    finally:
        ge._stop()  # pylint: disable=protected-access


class BuildType(Enum):
    """Build type of the cargo build."""

    DEBUG = "debug"
    RELEASE = "release"


def get_open_port() -> int:
    """Get an open port on the local machine."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def generate_test_schema_name() -> str:
    """Generate a test schema name."""
    schema_name = "pytest_"
    for _ in range(10):
        schema_name += random.choice(string.ascii_letters)
    return schema_name


class GeoEngineBinaries:
    """
    Geo Engine binaries with `cargo` for testing.

    This class is a singleton.
    It builds the Geo Engine binaries from the given code path.
    """

    _instance: Optional["GeoEngineBinaries"] = None
    _lock = threading.Lock()

    _code_path: Path
    _build_type: BuildType
    _server_binary_path: Path
    _cli_binary_path: Path

    def __new__(cls, code_path: Path, build_type: BuildType) -> "GeoEngineBinaries":
        """Create Geo Engine binaries for testing."""

        if cls._instance is not None:
            return cls._instance

        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._code_path = code_path
                cls._instance._build_type = build_type

                cls._instance._build_geo_engine()

        return cls._instance

    def _build_geo_engine(self) -> None:
        """Build the Geo Engine binaries."""

        cargo_bin = shutil.which("cargo")

        if cargo_bin is None:
            raise RuntimeError("Cargo not found")

        logging.info("Building Geo Engine binaries in %s mode… this may take a while.", self._build_type.value)

        subprocess.run(
            [
                cargo_bin,
                "build",
                "--locked",
                "--bins",
            ]
            + (["--release"] if self._build_type == BuildType.RELEASE else []),
            check=True,
            cwd=self._code_path,
        )

        self._server_binary_path = self._code_path / "target" / self._build_type.value / "geoengine-server"
        self._cli_binary_path = self._code_path / "target" / self._build_type.value / "geoengine-cli"

        if not self._server_binary_path.exists():
            raise RuntimeError(f"Server binary not found at {self._server_binary_path}")
        if not self._cli_binary_path.exists():
            raise RuntimeError(f"CLI binary not found at {self._cli_binary_path}")

    @property
    def server_binary_path(self) -> Path:
        """Get the path to the Geo Engine server binary."""
        return self._server_binary_path

    @property
    def cli_binary_path(self) -> Path:
        """Get the path to the Geo Engine CLI binary."""
        return self._cli_binary_path

    @property
    def working_directory(self) -> Path:
        """Get the working directory for the Geo Engine."""

        return self._code_path


class GeoEngineProcess:
    """A Geo Engine process."""

    geo_engine_binaries: GeoEngineBinaries

    port: int
    db_schema: str

    timeout_seconds: int

    process: subprocess.Popen | None = None

    def __init__(
        self, geo_engine_binaries: GeoEngineBinaries, port: int, db_schema: str, timeout_seconds: int = 60
    ) -> None:
        """Initialize a Geo Engine process."""

        self.geo_engine_binaries = geo_engine_binaries

        self.port = port
        self.db_schema = db_schema

        self.timeout_seconds = timeout_seconds

    def _start(self) -> None:
        """Start the Geo Engine process."""

        if self.process is not None:
            raise RuntimeError("Process already started")

        self.process = subprocess.Popen(  # pylint: disable=consider-using-with
            self.geo_engine_binaries.server_binary_path,
            cwd=self.geo_engine_binaries.working_directory,
            env={
                "GEOENGINE__WEB__BIND_ADDRESS": self._bind_address(),
                "GEOENGINE__POSTGRES__HOST": POSTGRES_HOST,
                "GEOENGINE__POSTGRES__PORT": str(POSTGRES_PORT),
                "GEOENGINE__POSTGRES__DATABASE": POSTGRES_DATABASE,
                "GEOENGINE__POSTGRES__USER": POSTGRES_USER,
                "GEOENGINE__POSTGRES__PASSWORD": POSTGRES_PASSWORD,
                "GEOENGINE__POSTGRES__SCHEMA": self.db_schema,
                "GEOENGINE__LOGGING__LOG_SPEC": GE_LOG_SPEC,
                "GEOENGINE__POSTGRES__CLEAR_DATABASE_ON_START": "true",
                "PATH": os.environ["PATH"],
            },
            stderr=subprocess.PIPE,
            text=True,
        )

    def _stop(self) -> None:
        """Stop the Geo Engine process."""

        if self.process is None:
            raise RuntimeError("Process not started")

        terminate_exception: Exception | None = None

        try:
            self.process.terminate()
        except Exception as e:  # pylint: disable=broad-except
            terminate_exception = e

        self._clean_up_schema()

        if terminate_exception is not None:
            raise terminate_exception

    def _clean_up_schema(self) -> None:
        """Clean up the schema."""

        with (
            psycopg.connect(  # pylint: disable=not-context-manager # false-positive
                dbname=POSTGRES_DATABASE,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
            ) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute(f"DROP SCHEMA IF EXISTS {self.db_schema} CASCADE")
            connection.commit()

    def _bind_address(self) -> str:
        return f"127.0.0.1:{self.port}"

    def address(self) -> str:
        return f"http://{self._bind_address()}/api"

    def wait_for_ready(self) -> None:
        """Wait for the Geo Engine to be ready."""

        if self.process is None:
            raise RuntimeError("Process not started")

        try:
            subprocess.run(
                [
                    self.geo_engine_binaries.cli_binary_path,
                    "check-successful-startup",
                    "--timeout",
                    str(self.timeout_seconds),
                    "--output-stdin",
                ],
                cwd=self.geo_engine_binaries.working_directory,
                stdin=self.process.stderr,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError("Geo Engine was not ready… aborting") from e
