---
sidebar_label: backports
title: backports
---

This module contains backports of newer Python features.

Usually, those can be removed when the minimum required Python version is increased.

#### to_thread

```python
async def to_thread(func, *args, **kwargs)
```

This is a backport of `asyncio.to_thread`.

TODO: Remove this when minimum version of Python is 3.9
