---
title: Time Step
---

A time step consists of granularity and the number of steps.
For instance, you can specify yearly steps by settings the granularity to `Years` and the number of steps to 1.
Half-yearly steps can be specified by setting the granularity to `Months` and the number of steps to 6.

| Parameter     | Type              | Description                   | Example Value |
| ------------- | ----------------- | ----------------------------- | ------------- |
| `granularity` | `TimeGranularity` | granularity of the time steps | `months`      |
| `step`        | `integer`         | number of time steps          | 1             |

## TimeGranularity

The granularity of the time steps can take one of the following values.

| Variant   | Description  |
| --------- | ------------ |
| `millis`  | milliseconds |
| `seconds` | seconds      |
| `minutes` | minutes      |
| `hours`   | hours        |
| `days`    | days         |
| `months`  | months       |
| `years`   | years        |

# Example JSON

```json
{
    "granularity": "months",
    "step": 1
}
```
