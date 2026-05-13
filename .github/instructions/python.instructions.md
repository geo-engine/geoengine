---
name: Python Development Instructions
applyTo: "**/*.py"
---

# Python Development Instructions

Keep Python work simple, consistent, and testable.

## Setup & Dependencies

- Target Python 3.11+ (match [`pyproject.toml`](../../python/pyproject.toml)).
- Use virtual environments (`just python install-venv`) and activate them in shells/CI.
- Pin dependencies in [`pyproject.toml`](../../python/pyproject.toml); prefer PEP 621 where used.

## Tooling & Quality

- Run `just python format-code` for formatting (PEP 8 style).
- Use `just python lint` for linting and types.
- Type every function (inputs and return values) and public API with type hints
  - use built-in types, collections from `collections` module, and `typing` module.

## Testing & CI

- Use `just python test`.
- Keep tests fast, deterministic, and isolated.
- CI must run formatting, linting, and tests for every PR.

## Development Practices

- Use structured logging (avoid printing in libraries)
- raise clear exceptions
- Never commit secrets—load credentials from environment/configuration.

## Packaging & Docs

- Build with `just python build`
- Write concise docstrings (Google/NumPy style) and generate API docs when helpful.
