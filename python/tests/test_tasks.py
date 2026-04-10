"""Tests regarding task functionality"""

import datetime
import unittest
from uuid import UUID

import geoengine as ge
from geoengine import DEFAULT_ISO_TIME_FORMAT, BadRequestException, ValidationError
from geoengine.tasks import (
    AbortedTaskStatusInfo,
    CompletedTaskStatusInfo,
    FailedTaskStatusInfo,
    RunningTaskStatusInfo,
    Task,
    TaskId,
    TaskStatus,
)
from tests.ge_test import GeoEngineTestInstance

from . import UrllibMocker


class TaskTests(unittest.TestCase):
    """Test runner regarding task functionality"""

    def setUp(self) -> None:
        ge.reset(False)

    def test_get_task_list_empty(self):
        # TODO: use `enterContext(cm)` instead of `with cm:` in Python 3.11
        with GeoEngineTestInstance() as ge_instance:
            ge_instance.wait_for_ready()

            ge.initialize(ge_instance.address())

            task_list = ge.tasks.get_task_list()

            self.assertEqual(len(task_list), 0)
            self.assertEqual(task_list, [])

    def test_get_task_list_all_types(self):
        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/anonymous",
                json={"id": "26a4c585-8aa5-4de8-9ede-293d3cd3544a", "project": None, "view": None},
            )

            m.get(
                "http://mock-instance/tasks/list",
                json=[
                    {
                        "taskId": "e07aec1e-387a-4d24-8041-fbfba37eae2b",
                        "taskType": "dummy",
                        "description": "No operation",
                        "status": "completed",
                        "info": "generic info",
                        "timeTotal": "00:00:05",
                        "timeStarted": "2023-02-16T15:25:45.390Z",
                    },
                    {
                        "taskId": "a04d2e1b-db24-42cb-a620-1d7803df3abe",
                        "taskType": "dummy",
                        "description": "No operation",
                        "status": "running",
                        "pctComplete": "0.00%",
                        "estimatedTimeRemaining": "? (± ?)",
                        "info": "generic running info",
                        "timeStarted": "2023-02-16T15:25:45.390Z",
                    },
                    {
                        "taskId": "01d68e7b-c69f-4132-b758-538f2f05acf0",
                        "status": "aborted",
                        "cleanUp": {"status": "noCleanUp"},
                        "timeStarted": "2023-02-16T15:25:45.390Z",
                    },
                    {
                        "taskId": "1ccba900-167d-4dcf-9001-5ce3c0b20844",
                        "status": "failed",
                        "error": "TileLimitExceeded",
                        "cleanUp": {"status": "completed", "info": None},
                        "timeStarted": "2023-02-16T15:25:45.390Z",
                    },
                ],
            )

            ge.initialize("http://mock-instance")

            expected_start_time = datetime.datetime.strptime("2023-02-16T15:25:45.390Z", DEFAULT_ISO_TIME_FORMAT)
            expected_result = [
                (
                    Task(TaskId(UUID("e07aec1e-387a-4d24-8041-fbfba37eae2b"))),
                    CompletedTaskStatusInfo(
                        TaskStatus.COMPLETED, expected_start_time, "generic info", "00:00:05", "dummy", "No operation"
                    ),
                ),
                (
                    Task(TaskId(UUID("a04d2e1b-db24-42cb-a620-1d7803df3abe"))),
                    RunningTaskStatusInfo(
                        TaskStatus.RUNNING,
                        expected_start_time,
                        "0.00%",
                        "? (± ?)",
                        "generic running info",
                        "dummy",
                        "No operation",
                    ),
                ),
                (
                    Task(TaskId(UUID("01d68e7b-c69f-4132-b758-538f2f05acf0"))),
                    AbortedTaskStatusInfo(TaskStatus.ABORTED, expected_start_time, {"status": "noCleanUp"}),
                ),
                (
                    Task(TaskId(UUID("1ccba900-167d-4dcf-9001-5ce3c0b20844"))),
                    FailedTaskStatusInfo(
                        TaskStatus.FAILED,
                        expected_start_time,
                        "TileLimitExceeded",
                        {"status": "completed", "info": None},
                    ),
                ),
            ]

            task_list = ge.tasks.get_task_list()

            self.assertEqual(len(task_list), 4)
            self.assertEqual(task_list, expected_result)

    def test_get_task_list_unknown_status(self):
        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/anonymous",
                json={"id": "26a4c585-8aa5-4de8-9ede-293d3cd3544a", "project": None, "view": None},
            )

            m.get(
                "http://mock-instance/tasks/list",
                json=[
                    {
                        "taskId": "e07aec1e-387a-4d24-8041-fbfba37eae2b",
                        "taskType": "dummy",
                        "description": "No operation",
                        "status": "completed",
                        "info": "generic info",
                        "timeTotal": "00:00:05",
                        "timeStarted": "2023-02-16T15:25:45.390Z",
                    },
                    {
                        "taskId": "ee4bc7ca-e637-4427-a617-2d2aa79d1406",
                        "status": "clear",
                        "pctComplete": "0.00%",
                        "estimatedTimeRemaining": "? (± ?)",
                        "info": "generic running info",
                        "timeStarted": "2023-02-16T15:25:45.390Z",
                    },
                ],
            )

            ge.initialize("http://mock-instance")

            self.assertRaises(ValueError, ge.tasks.get_task_list, 0)

    def test_get_task_list_malformed(self):
        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/anonymous",
                json={"id": "26a4c585-8aa5-4de8-9ede-293d3cd3544a", "project": None, "view": None},
            )

            m.get(
                "http://mock-instance/tasks/list",
                json=[
                    {
                        "taskId": "e07aec1e-387a-4d24-8041-fbfba37eae2b",
                        "status": "completed",
                        "info": "generic info",
                        "timeTotal": "00:00:05",
                        "timeStarted": "2023-02-16T15:25:45.390Z",
                    },
                    {
                        "taskId": "ee4f1ed9-fd06-40be-90f5-d6289c154fcd",
                        "status": "running",
                        # Missing pct_complete field
                        "estimatedTimeRemaing": "? (± ?)",
                        "info": "generic running info",
                        "timeStarted": "2023-02-16T15:25:45.390Z",
                    },
                ],
            )

            ge.initialize("http://mock-instance")

            self.assertRaises(ValidationError, ge.tasks.get_task_list, 0)

    def test_get_task_status(self):
        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/anonymous",
                json={"id": "26a4c585-8aa5-4de8-9ede-293d3cd3544a", "project": None, "view": None},
            )

            # Correct results
            m.get(
                "http://mock-instance/tasks/e07aec1e-387a-4d24-8041-fbfba37eae2b/status",
                json={
                    "status": "completed",
                    "taskType": "dummy",
                    "description": "No operation",
                    "info": "generic info",
                    "timeTotal": "00:00:05",
                    "timeStarted": "2023-02-16T15:25:45.390Z",
                },
            )
            m.get(
                "http://mock-instance/tasks/a04d2e1b-db24-42cb-a620-1d7803df3abe/status",
                json={
                    "status": "running",
                    "taskType": "dummy",
                    "description": "No operation",
                    "pctComplete": "0.00%",
                    "estimatedTimeRemaining": "? (± ?)",
                    "info": "generic running info",
                    "timeStarted": "2023-02-16T15:25:45.390Z",
                },
            )
            m.get(
                "http://mock-instance/tasks/01d68e7b-c69f-4132-b758-538f2f05acf0/status",
                json={
                    "status": "aborted",
                    "cleanUp": {"status": "noCleanUp"},
                },
            )
            m.get(
                "http://mock-instance/tasks/1ccba900-167d-4dcf-9001-5ce3c0b20844/status",
                json={
                    "status": "failed",
                    "error": "TileLimitExceeded",
                    "cleanUp": {"status": "completed", "info": None},
                    "timeStarted": "2023-02-16T15:25:45.390Z",
                },
            )

            # Unknown status
            m.get(
                "http://mock-instance/tasks/ee4bc7ca-e637-4427-a617-2d2aa79d1406/status",
                json={
                    "status": "clear",
                    "pctComplete": "0.00%",
                    "estimatedTimeRemaining": "? (± ?)",
                    "info": "generic running info",
                    "timeStarted": "2023-02-16T15:25:45.390Z",
                },
            )

            # Malformed info
            m.get(
                "http://mock-instance/tasks/ee4f1ed9-fd06-40be-90f5-d6289c154fcd/status",
                json={
                    "status": "running",
                    "taskType": "dummy",
                    "description": "No operation",
                    # Missing pct_complete field
                    "estimatedTimeRemaining": "? (± ?)",
                    "info": "generic running info",
                    "timeStarted": "2023-02-16T15:25:45.390Z",
                },
            )

            ge.initialize("http://mock-instance")

            # Correct results
            expected_start_time = datetime.datetime.strptime("2023-02-16T15:25:45.390Z", DEFAULT_ISO_TIME_FORMAT)
            expected_results = [
                CompletedTaskStatusInfo(
                    TaskStatus.COMPLETED, expected_start_time, "generic info", "00:00:05", "dummy", "No operation"
                ),
                RunningTaskStatusInfo(
                    TaskStatus.RUNNING,
                    expected_start_time,
                    "0.00%",
                    "? (± ?)",
                    "generic running info",
                    "dummy",
                    "No operation",
                ),
                AbortedTaskStatusInfo(TaskStatus.ABORTED, expected_start_time, {"status": "noCleanUp"}),
                FailedTaskStatusInfo(
                    TaskStatus.FAILED, expected_start_time, "TileLimitExceeded", {"status": "completed", "info": None}
                ),
            ]

            completed_task = Task(TaskId(UUID("e07aec1e-387a-4d24-8041-fbfba37eae2b")))
            running_task = Task(TaskId(UUID("a04d2e1b-db24-42cb-a620-1d7803df3abe")))
            aborted_task = Task(TaskId(UUID("01d68e7b-c69f-4132-b758-538f2f05acf0")))
            failed_task = Task(TaskId(UUID("1ccba900-167d-4dcf-9001-5ce3c0b20844")))

            self.assertEqual(completed_task.get_status(), expected_results[0])
            self.assertEqual(running_task.get_status(), expected_results[1])
            self.assertEqual(aborted_task.get_status(), expected_results[2])
            self.assertEqual(failed_task.get_status(), expected_results[3])

            # Unknown status
            unknown_status_task = Task(TaskId(UUID("ee4bc7ca-e637-4427-a617-2d2aa79d1406")))
            with self.assertRaises(ValueError):
                unknown_status_task.get_status()

            # Malformed
            malformed_status_task = Task(TaskId(UUID("ee4f1ed9-fd06-40be-90f5-d6289c154fcd")))
            with self.assertRaises(ValueError):
                malformed_status_task.get_status()

    def test_get_abort_task(self):
        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/anonymous",
                json={"id": "26a4c585-8aa5-4de8-9ede-293d3cd3544a", "project": None, "view": None},
            )

            m.delete(
                "http://mock-instance/tasks/a04d2e1b-db24-42cb-a620-1d7803df3abe",
                status_code=200,
            )
            m.delete(
                "http://mock-instance/tasks/9f008e47-645b-48de-a513-748a1d0c2a3f",
                status_code=400,
                json={
                    "error": "TaskError",
                    "message": "TaskError: Task not found with id: 9f008e47-645b-48de-a513-748a1d0c2a3f",
                },
            )

            ge.initialize("http://mock-instance")

            abort_success_task = Task(TaskId(UUID("a04d2e1b-db24-42cb-a620-1d7803df3abe")))
            self.assertEqual(None, abort_success_task.abort())

            abort_failed_task = Task(TaskId(UUID("9f008e47-645b-48de-a513-748a1d0c2a3f")))
            with self.assertRaises(BadRequestException):
                abort_failed_task.abort()


if __name__ == "__main__":
    unittest.main()
