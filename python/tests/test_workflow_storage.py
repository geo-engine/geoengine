"""Tests for WMS calls"""

import unittest
from datetime import datetime

import geoengine as ge
from geoengine.datasets import DatasetName, StoredDataset
from geoengine.tasks import TaskStatus
from tests.ge_test import GeoEngineTestInstance


class WorkflowStorageTests(unittest.TestCase):
    """Test methods for storing workflows as datasets"""

    def setUp(self) -> None:
        ge.reset(False)

    def test_storing_workflow(self):
        # TODO: use `enterContext(cm)` instead of `with cm: ` in Python 3.11
        with GeoEngineTestInstance() as ge_instance:
            ge_instance.wait_for_ready()

            ge.initialize(ge_instance.address())

            workflow_definition = {"type": "Raster", "operator": {"type": "GdalSource", "params": {"data": "ndvi"}}}

            query = ge.QueryRectangle(
                ge.SpatialPartition2D(xmin=-180.0, ymax=90.0, xmax=180.0, ymin=-90.0),
                ge.TimeInterval(start=datetime(2014, 4, 1, 12, 0), end=None),
            )

            workflow = ge.register_workflow(workflow_definition)

            dataset_name = f"{ge.get_session().user_id}:my_new_dataset"
            task = workflow.save_as_dataset(
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
