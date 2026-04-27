---
name: Jupyter Development Instructions
applyTo: "**/*.ipynb"
---

# Jupyter Notebook Development Instructions

## Getting Started

- Use the `geoengine` library in your notebooks for accessing Geo Engine functionality.
- Explain your code with markdown cells to provide context and reasoning for your analysis.
- Keep notebooks organized and focused on a single topic or analysis.

## Best Practices

- Use clear and descriptive variable names.
- Avoid hardcoding paths or secrets; use environment variables or configuration files.
- Document your data sources, assumptions, and any preprocessing steps clearly in markdown cells.
- Avoid notebooks that are not reproducible; they will be tested in our CI pipeline.

## Useful links

- What Geo Engine operators exist: [docs/operators](../../www/src/content/docs/docs/operators/)
- What Geo Engine plots exist: [docs/plots](../../www/src/content/docs/docs/plots/)
- How the Python API works: [Python API docs](../../www/src/content/docs/docs/python/)
