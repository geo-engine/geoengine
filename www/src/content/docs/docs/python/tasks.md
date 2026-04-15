---
sidebar_label: tasks
title: tasks
---

Module for encapsulating Geo Engine tasks API

## TaskId Objects

```python
class TaskId()
```

A wrapper for a task id

#### from_response

```python
@classmethod
def from_response(cls,
                  response: geoengine_openapi_client.TaskResponse) -> TaskId
```

Parse a http response to an `TaskId`

#### \_\_eq\_\_

```python
def __eq__(other) -> bool
```

Checks if two dataset ids are equal

## TaskStatus Objects

```python
class TaskStatus(Enum)
```

An enum of task status types

## TaskStatusInfo Objects

```python
class TaskStatusInfo()
```

A wrapper for a task status type

#### from_response

```python
@classmethod
def from_response(
        cls, response: geoengine_openapi_client.TaskStatus) -> TaskStatusInfo
```

Parse a http response to a `TaskStatusInfo`

The task can be one of:
RunningTaskStatusInfo, CompletedTaskStatusInfo, AbortedTaskStatusInfo or FailedTaskStatusInfo

## RunningTaskStatusInfo Objects

```python
class RunningTaskStatusInfo(TaskStatusInfo)
```

A wrapper for a running task status with information about completion progress

#### \_\_eq\_\_

```python
def __eq__(other)
```

Check if two task statuses are equal

## CompletedTaskStatusInfo Objects

```python
class CompletedTaskStatusInfo(TaskStatusInfo)
```

A wrapper for a completed task status with information about the completion

#### \_\_eq\_\_

```python
def __eq__(other)
```

Check if two task statuses are equal

## AbortedTaskStatusInfo Objects

```python
class AbortedTaskStatusInfo(TaskStatusInfo)
```

A wrapper for an aborted task status with information about the termination

#### \_\_eq\_\_

```python
def __eq__(other)
```

Check if two task statuses are equal

## FailedTaskStatusInfo Objects

```python
class FailedTaskStatusInfo(TaskStatusInfo)
```

A wrapper for a failed task status with information about the failure

#### \_\_eq\_\_

```python
def __eq__(other)
```

Check if two task statuses are equal

## Task Objects

```python
class Task()
```

Holds a task id, allows querying and manipulating the task status

#### \_\_eq\_\_

```python
def __eq__(other)
```

Check if two task representations are equal

#### get_status

```python
def get_status(timeout: int = 3600) -> TaskStatusInfo
```

Returns the status of a task in a Geo Engine instance

#### abort

```python
def abort(force: bool = False, timeout: int = 3600) -> None
```

Abort a running task in a Geo Engine instance

#### wait_for_finish

```python
def wait_for_finish(check_interval_seconds: float = 5,
                    request_timeout: int = 3600,
                    print_status: bool = True) -> TaskStatusInfo
```

Wait for the given task in a Geo Engine instance to finish (status either complete, aborted or failed).
The status is printed after each check-in. Check-ins happen in intervals of check_interval_seconds seconds.

#### as_future

```python
async def as_future(request_interval: int = 5,
                    print_status=False) -> TaskStatusInfo
```

Returns a future that will be resolved when the task is finished in the backend.

#### get_task_list

```python
def get_task_list(timeout: int = 3600) -> list[tuple[Task, TaskStatusInfo]]
```

Returns the status of all tasks in a Geo Engine instance
