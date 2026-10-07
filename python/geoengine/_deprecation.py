"""Helpers for keeping deprecated names importable while warning about their usage."""

from __future__ import annotations

import functools
import importlib
import warnings
from collections.abc import Callable
from typing import Any, TypeVar, cast

F = TypeVar("F", bound=Callable[..., Any])


def warn_deprecated(old: str, new: str, stacklevel: int = 3) -> None:
    """Emit a `DeprecationWarning` that `old` was renamed to `new`."""
    warnings.warn(
        f"`{old}` is deprecated and will be removed in a future release. Use `{new}` instead.",
        DeprecationWarning,
        stacklevel=stacklevel,
    )


def deprecated_module_getattr(
    module_name: str, aliases: dict[str, str], fallback_module: str | None = None
) -> Callable[[str], Any]:
    """
    Create a module-level `__getattr__` (PEP 562) that resolves deprecated names lazily.

    `aliases` maps a deprecated attribute name of `module_name` to its replacement,
    given as the fully qualified name `package.module.attribute` or `package.module`.
    Each access emits a `DeprecationWarning` and returns the replacement.

    All other names are looked up in `fallback_module`, if given.
    """

    def __getattr__(name: str) -> Any:
        if name in aliases:
            replacement = aliases[name]
            warn_deprecated(f"{module_name}.{name}", replacement)

            try:
                return importlib.import_module(replacement)
            except ModuleNotFoundError:
                replacement_module, _, replacement_attribute = replacement.rpartition(".")
                return getattr(importlib.import_module(replacement_module), replacement_attribute)

        if fallback_module is not None and not name.startswith("__"):
            return getattr(importlib.import_module(fallback_module), name)

        raise AttributeError(f"module {module_name!r} has no attribute {name!r}")

    return __getattr__


def warn_deprecated_module(old: str, new: str) -> None:
    """Emit a `DeprecationWarning` when a deprecated module is imported. Call it at the module's top level."""
    # `warnings` skips the import machinery frames, so this points to the user's import statement
    warn_deprecated(old, new, stacklevel=4)


def renamed_parameter(old: str, new: str) -> Callable[[F], F]:
    """
    Decorator that keeps accepting the keyword argument `old` for the renamed parameter `new`.
    Passing `old` emits a `DeprecationWarning`.
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if old in kwargs:
                name = func.__qualname__.removesuffix(".__init__")
                if new in kwargs:
                    raise TypeError(f"{name}() got values for both `{old}` and `{new}`")
                warn_deprecated(f"{name}({old}=...)", f"{name}({new}=...)")
                kwargs[new] = kwargs.pop(old)
            return func(*args, **kwargs)

        return cast(F, wrapper)

    return decorator
