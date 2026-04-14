"""
Module for encapsulating Geo Engine tasks API
"""

from __future__ import annotations

import asyncio
import datetime
import time
from enum import Enum
from uuid import UUID

import geoengine_openapi_client

from geoengine import backports
from geoengine.auth import get_session
from geoengine.error import GeoEngineException, TypeException
from geoengine.types import DEFAULT_ISO_TIME_FORMAT


class TaskId:
    """A wrapper for a task id"""

    def __init__(self, task_id: UUID) -> None:
        self.__task_id = task_id

    @classmethod
    def from_response(cls, response: geoengine_openapi_client.TaskResponse) -> TaskId:
        """Parse a http response to an `TaskId`"""

        return TaskId(response.task_id)

    def __eq__(self, other) -> bool:
        """Checks if two dataset ids are equal"""
        if not isinstance(other, self.__class__):
            return False

        return self.__task_id == other.__task_id  # pylint: disable=protected-access

    def __str__(self) -> str:
        return str(self.__task_id)

    def __repr__(self) -> str:
        return repr(self.__task_id)

    def to_dict(self) -> UUID:
        return self.__task_id


class TaskStatus(Enum):
    """An enum of task status types"""

    RUNNING = "running"
    COMPLETED = "completed"
    ABORTED = "aborted"
    FAILED = "failed"


class TaskStatusInfo:  # pylint: disable=too-few-public-methods
    """A wrapper for a task status type"""

    status: TaskStatus
    time_started: datetime.datetime

    def __init__(self, status, time_started) -> None:
        self.status = status
        self.time_started = time_started

    @classmethod
    def from_response(cls, response: geoengine_openapi_client.TaskStatus) -> TaskStatusInfo:
        """
        Parse a http response to a `TaskStatusInfo`

        The task can be one of:
        RunningTaskStatusInfo, CompletedTaskStatusInfo, AbortedTaskStatusInfo or FailedTaskStatusInfo
        """

        inner = response.actual_instance
        if inner is None:
            raise TypeException("Unknown `TaskStatus` type")

        status = TaskStatus(inner.status)
        time_started = None
        if (
            isinstance(inner, geoengine_openapi_client.TaskStatusRunning | geoengine_openapi_client.TaskStatusCompleted)
            and inner.time_started is not None
        ):
            time_started = datetime.datetime.strptime(inner.time_started, DEFAULT_ISO_TIME_FORMAT)

        if isinstance(inner, geoengine_openapi_client.TaskStatusRunning):
            return RunningTaskStatusInfo(
                status,
                time_started,
                inner.pct_complete,
                inner.estimated_time_remaining,
                inner.info,
                inner.task_type,
                inner.description,
            )
        if isinstance(inner, geoengine_openapi_client.TaskStatusCompleted):
            return CompletedTaskStatusInfo(
                status, time_started, inner.info, inner.time_total, inner.task_type, inner.description
            )
        if isinstance(inner, geoengine_openapi_client.TaskStatusAborted):
            return AbortedTaskStatusInfo(status, time_started, inner.clean_up)
        if isinstance(inner, geoengine_openapi_client.TaskStatusFailed):
            return FailedTaskStatusInfo(status, time_started, inner.error, inner.clean_up)
        raise GeoEngineException(response)


class RunningTaskStatusInfo(TaskStatusInfo):
    """A wrapper for a running task status with information about completion progress"""

    # pylint: disable=too-many-positional-arguments
    def __init__(
        self, status, start_time, pct_complete, estimated_time_remaining, info, task_type, description
    ) -> None:  # pylint: disable=too-many-arguments,line-too-long
        super().__init__(status, start_time)
        self.pct_complete = pct_complete
        self.estimated_time_remaining = estimated_time_remaining
        self.info = info
        self.task_type = task_type
        self.description = description

    def __eq__(self, other):
        """Check if two task statuses are equal"""
        if not isinstance(other, self.__class__):
            return False

        return (
            self.status == other.status
            and self.pct_complete == other.pct_complete
            and self.estimated_time_remaining == other.estimated_time_remaining
            and self.info == other.info
            and self.task_type == other.task_type
            and self.description == other.description
        )

    def __str__(self) -> str:
        return (
            f"status={self.status.value}, time_started={self.time_started}, "
            f"pct_complete={self.pct_complete}, "
            f"estimated_time_remaining={self.estimated_time_remaining}, info={self.info}, "
            f"task_type={self.task_type}, description={self.description}"
        )

    def __repr__(self) -> str:
        return (
            f"TaskStatusInfo(status={self.status.value!r}, pct_complete={self.pct_complete!r}, "
            f"estimated_time_remaining={self.estimated_time_remaining!r}, info={self.info!r}, "
            f"task_type={self.task_type!r}, description={self.description!r})"
        )


class CompletedTaskStatusInfo(TaskStatusInfo):
    """A wrapper for a completed task status with information about the completion"""

    # pylint: disable=too-many-arguments, too-many-positional-arguments
    def __init__(self, status, time_started, info, time_total, task_type, description) -> None:
        super().__init__(status, time_started)
        self.info = info
        self.time_total = time_total
        self.task_type = task_type
        self.description = description

    def __eq__(self, other):
        """Check if two task statuses are equal"""
        if not isinstance(other, self.__class__):
            return False

        return (
            self.status == other.status
            and self.info == other.info
            and self.time_total == other.time_total
            and self.task_type == other.task_type
            and self.description == other.description
        )

    def __str__(self) -> str:
        return (
            f"status={self.status.value}, time_started={self.time_started}, info={self.info}, "
            f"time_total={self.time_total}, task_type={self.task_type}, description={self.description}"
        )

    def __repr__(self) -> str:
        return (
            f"TaskStatusInfo(status={self.status.value!r}, time_started={self.time_started!r}, "
            f"info = {self.info!r}, time_total = {self.time_total!r}, task_type={self.task_type!r}, "
            f"description={self.description!r})"
        )


class AbortedTaskStatusInfo(TaskStatusInfo):
    """A wrapper for an aborted task status with information about the termination"""

    def __init__(self, status, time_started, clean_up) -> None:
        super().__init__(status, time_started)
        self.clean_up = clean_up

    def __eq__(self, other):
        """Check if two task statuses are equal"""
        if not isinstance(other, self.__class__):
            return False

        return self.status == other.status and self.clean_up == other.clean_up

    def __str__(self) -> str:
        return f"status={self.status.value}, time_started={self.time_started}, clean_up={self.clean_up}"

    def __repr__(self) -> str:
        return (
            f"TaskStatusInfo(status={self.status.value!r}, time_started={self.time_started!r}, "
            f"clean_up={self.clean_up!r})"
        )


class FailedTaskStatusInfo(TaskStatusInfo):
    """A wrapper for a failed task status with information about the failure"""

    def __init__(self, status, time_started, error, clean_up) -> None:
        super().__init__(status, time_started)
        self.error = error
        self.clean_up = clean_up

    def __eq__(self, other):
        """Check if two task statuses are equal"""
        if not isinstance(other, self.__class__):
            return False

        return self.status == other.status and self.error == other.error and self.clean_up == other.clean_up

    def __str__(self) -> str:
        return (
            f"status={self.status.value}, time_started={self.time_started}, error={self.error}, "
            f"clean_up={self.clean_up}"
        )

    def __repr__(self) -> str:
        return (
            f"TaskStatusInfo(status={self.status.value!r}, time_started={self.time_started!r}, "
            f"error={self.error!r}, clean_up={self.clean_up!r})"
        )


class Task:
    """
    Holds a task id, allows querying and manipulating the task status
    """

    def __init__(self, task_id: TaskId):
        self.__task_id = task_id

    def __eq__(self, other):
        """Check if two task representations are equal"""
        if not isinstance(other, self.__class__):
            return False

        return self.__task_id == other.__task_id  # pylint: disable=protected-access

    def get_status(self, timeout: int = 3600) -> TaskStatusInfo:
        """
        Returns the status of a task in a Geo Engine instance
        """
        session = get_session()

        task_id = self.__task_id.to_dict()

        with geoengine_openapi_client.ApiClient(session.configuration) as api_client:
            tasks_api = geoengine_openapi_client.TasksApi(api_client)
            response = tasks_api.status_handler(task_id, _request_timeout=timeout)

        return TaskStatusInfo.from_response(response)

    def abort(self, force: bool = False, timeout: int = 3600) -> None:
        """
        Abort a running task in a Geo Engine instance
        """
        session = get_session()

        task_id = self.__task_id.to_dict()

        with geoengine_openapi_client.ApiClient(session.configuration) as api_client:
            tasks_api = geoengine_openapi_client.TasksApi(api_client)
            tasks_api.abort_handler(task_id, None if force is False else True, _request_timeout=timeout)

    def wait_for_finish(
        self, check_interval_seconds: float = 5, request_timeout: int = 3600, print_status: bool = True
    ) -> TaskStatusInfo:
        """
        Wait for the given task in a Geo Engine instance to finish (status either complete, aborted or failed).
        The status is printed after each check-in. Check-ins happen in intervals of check_interval_seconds seconds.
        """
        current_status = self.get_status(request_timeout)

        while current_status.status == TaskStatus.RUNNING:
            current_status = self.get_status(request_timeout)

            if print_status:
                print(current_status)
            if current_status.status == TaskStatus.RUNNING:
                time.sleep(check_interval_seconds)

        return current_status

    def __str__(self) -> str:
        return str(self.__task_id)

    def __repr__(self) -> str:
        return repr(self.__task_id)

    async def as_future(self, request_interval: int = 5, print_status=False) -> TaskStatusInfo:
        """
        Returns a future that will be resolved when the task is finished in the backend.
        """

        def get_status_inner(tasks_api: geoengine_openapi_client.TasksApi, task_id: UUID, timeout: int = 3600):
            return tasks_api.status_handler(task_id, _request_timeout=timeout)

        session = get_session()
        task_id_str = str(self.__task_id)

        last_status = None
        with geoengine_openapi_client.ApiClient(session.configuration) as api_client:
            tasks_api = geoengine_openapi_client.TasksApi(api_client)
            while True:
                response = await backports.to_thread(get_status_inner, tasks_api, task_id_str)

                last_status = TaskStatusInfo.from_response(response)

                if print_status:
                    print(last_status)

                if last_status.status != TaskStatus.RUNNING:
                    return last_status

                await asyncio.sleep(request_interval)


def get_task_list(timeout: int = 3600) -> list[tuple[Task, TaskStatusInfo]]:
    """
    Returns the status of all tasks in a Geo Engine instance
    """
    session = get_session()

    with geoengine_openapi_client.ApiClient(session.configuration) as api_client:
        tasks_api = geoengine_openapi_client.TasksApi(api_client)
        response = tasks_api.list_handler(None, 0, 10, _request_timeout=timeout)

    result = []
    for item in response:
        result.append((Task(TaskId(item.task_id)), TaskStatusInfo.from_response(item)))

    return result
