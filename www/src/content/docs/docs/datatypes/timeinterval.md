---
title: Time Interval
---

A time interval consists of two [`TimeInstance`s](/docs/datatypes/timeinstance).
Please be aware, that the interval is defined in close-open semantics.
This means, that the start time is inclusive and the end time of the interval is exclusive.
In mathematical notation, the interval is defined as `[start, end)`.

# Example JSON

Specifying in ISO 8601:

```json
{
    "start": "2010-01-01T00:00:00Z",
    "end": "2011-01-01T00:00:00Z"
}
```

Using the same date as UNIX timestamps in milliseconds:

```json
{
    "start": 1262304000000,
    "end": 1293840000000
}
```
