---
sidebar_label: util
title: util
---

Module for utility functions

#### clamp_datetime_ms_ns

```python
def clamp_datetime_ms_ns(value: np.datetime64) -> np.datetime64
```

Clamp a datetime64[ms] to the range of datetime64[ns] used by xarray
