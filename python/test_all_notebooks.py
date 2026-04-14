#!/usr/bin/env python3

"""Run all Jupyter Notebooks and check for errors."""

import os
import shutil
import subprocess
import sys

COVERAGE_COMMAND_ENV_VAR = "COVERAGE_COMMAND"


def eprint(*args, **kwargs):
    """Print to stderr."""
    print(*args, file=sys.stderr, **kwargs)


def run_test_notebook(notebook_path: str, coverage_args: list[str]) -> bool:
    """Run test_notebook.py for the given notebook."""

    pytest_bin = shutil.which("pytest")

    if pytest_bin is None:
        raise RuntimeError("Python 3 not found")

    result = subprocess.run(
        [pytest_bin, "--ignore=test", *coverage_args, "test_notebook.py"],
        env={
            **os.environ,
            "INPUT_FILE": notebook_path,
        },
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        eprint(f"Notebook {notebook_path} ran successfully.")
        return True
    else:
        eprint(f"Error running notebook {notebook_path}:")
        eprint(result.stderr)
        return False


def parse_coverage_command() -> list[str]:
    """Get coverage command from environment variable."""
    coverage_cmd = os.getenv(COVERAGE_COMMAND_ENV_VAR)
    if not coverage_cmd:
        return []
    return [*coverage_cmd.split(), "--cov-append"]


def main() -> int:
    """Run all Jupyter Notebooks and check for errors."""

    example_folder = "examples"

    if not os.path.isdir(example_folder):
        eprint(f"The folder {example_folder} does not exist.")
        return -1

    coverage_args = parse_coverage_command()
    if coverage_args:
        eprint(f"Using coverage args: {' '.join(coverage_args)}")
    else:
        eprint(f"No coverage args in env {COVERAGE_COMMAND_ENV_VAR} provided.")

    for root, _dirs, files in os.walk(example_folder):
        for file in files:
            if not file.endswith(".ipynb"):
                eprint(f"Skipping non-notebook file {file}")
                continue
            notebook_path = os.path.join(root, file)
            if not run_test_notebook(notebook_path, coverage_args):
                return -1

        break  # skip subdirectories

    return 0


if __name__ == "__main__":
    sys.exit(main())
