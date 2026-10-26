"""Tests for WMS calls"""

import shutil
import unittest
from datetime import datetime

import geoengine_api_client.exceptions
import numpy as np

import geoengine as ge
from geoengine.datasets import DatasetName, StoredDataset
from geoengine.tasks import TaskStatus
from tests.ge_test import GeoEngineTestInstance


class ProcessingGraphStorageTests(unittest.TestCase):
    """Test methods for storing processing graphs as datasets"""

    def setUp(self) -> None:
        ge.reset(False)

    def test_storing_processing_graph(self):
        # TODO: use `enterContext(cm)` instead of `with cm: ` in Python 3.11
        with GeoEngineTestInstance() as ge_instance:
            ge_instance.wait_for_ready()

            ge.initialize(ge_instance.address())

            processing_graph_definition = {
                "type": "Raster",
                "operator": {"type": "GdalSource", "params": {"data": "ndvi"}},
            }

            query = ge.QueryRectangle(
                ge.SpatialPartition2D(xmin=-180.0, ymax=90.0, xmax=180.0, ymin=-90.0),
                ge.TimeInterval(start=datetime(2014, 4, 1, 12, 0), end=None),
            )

            processing_graph = ge.register_processing_graph(processing_graph_definition)

            dataset_name = f"{ge.get_session().user_id}:my_new_dataset"
            task = processing_graph.save_as_dataset(
                query,
                name=dataset_name,
                display_name="Foo",
                description="Bar",
            )
            task.wait_for_finish()
            task_status = task.get_status()
            self.assertEqual(task_status.status, TaskStatus.COMPLETED)

            stored_dataset = StoredDataset.from_response(task_status.info)
            self.assertEqual(stored_dataset.dataset_name, DatasetName(dataset_name))

    def test_exporting_and_importing_a_stored_processing_graph(self):
        with GeoEngineTestInstance() as ge_instance:
            ge_instance.wait_for_ready()

            ge.initialize(ge_instance.address(), credentials=("admin@localhost", "adminadmin"))

            processing_graph = ge.register_processing_graph(
                {"type": "Raster", "operator": {"type": "GdalSource", "params": {"data": "ndvi"}}}
            )
            query = ge.QueryRectangle(
                ge.SpatialPartition2D(xmin=0.0, ymax=20.0, xmax=20.0, ymin=0.0),
                ge.TimeInterval(start=datetime(2014, 4, 1, 12, 0), end=None),
            )

            task = processing_graph.save_as_dataset(query, name=None, display_name="Exported")
            task.wait_for_finish()
            task_status = task.get_status()
            self.assertEqual(task_status.status, TaskStatus.COMPLETED)
            stored_dataset = StoredDataset.from_response(task_status.info)

            # export: copy the upload folder into the `test_data` volume
            code_path = ge_instance.geo_engine_binaries.working_directory
            folder_in_volume = f"import_test_{stored_dataset.upload_id}"
            folder = code_path / "test_data" / folder_in_volume
            shutil.copytree(code_path / "upload" / str(stored_dataset.upload_id), folder)
            try:
                imported = ge.import_dataset(folder, volume="test_data", path_in_volume=folder_in_volume)

                def array_of(dataset: DatasetName):
                    return ge.register_processing_graph(
                        {
                            "type": "Raster",
                            "operator": {"type": "MultiBandGdalSource", "params": {"data": str(dataset)}},
                        }
                    ).get_array(query, ge.SpatialResolution(0.1, 0.1))

                np.testing.assert_array_equal(array_of(imported), array_of(stored_dataset.dataset_name))

                ge.initialize(ge_instance.address())  # anonymous user
                with self.assertRaises(geoengine_api_client.exceptions.ApiException):
                    ge.import_dataset(folder, volume="test_data", path_in_volume=folder_in_volume)
            finally:
                shutil.rmtree(folder)
