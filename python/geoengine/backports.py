"""
This module contains backports of newer Python features.

Usually, those can be removed when the minimum required Python version is increased.
"""

import asyncio
import contextvars
import functools


# async def to_thread(func, /, *args, **kwargs):
# TODO: the parameters are positional only, but this syntax appeared in Python 3.8
async def to_thread(func, *args, **kwargs):
    """
    This is a backport of `asyncio.to_thread`.

    TODO: Remove this when minimum version of Python is 3.9
    """
    loop = asyncio.events.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)
