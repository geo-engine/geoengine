"""
Utility functions for post-processing
"""

import configparser
from itertools import tee
import logging
from pathlib import Path
from collections.abc import Callable, Generator
from typing import TypeAlias
from difflib import unified_diff

CWD = Path(".generation/")
INI_FILE = CWD / "config.ini"

FileModifier: TypeAlias = Callable[[list[str]], Generator[str, None, None]]


def modify_files(
    modifiers: Generator[tuple[Path, FileModifier], None, None],
    subdir: Path,
    diffdir: Path,
) -> None:
    """Modify files by applying the given modifiers."""

    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

    for file_path, modify_fn in modifiers:
        logging.info("Modifying %s…", file_path)

        file_path = subdir / file_path
        diff_path = diffdir / file_path.with_suffix(file_path.suffix + ".diff")

        # Ensure all parent directories for diff_path exist
        diff_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                file_contents: list[str] = f.readlines()

            new_file_contents: list[str] = list(modify_fn(file_contents))

            diff_file_contents: list[str] = list(
                unified_diff(
                    file_contents,
                    new_file_contents,
                    fromfile=str(file_path),
                    tofile=str(file_path),
                )
            )

            if len(diff_file_contents) == 0:
                logging.error("No changes made to %s.", file_path)

            with open(file_path, "w", encoding="utf-8") as f:
                f.writelines(new_file_contents)

            with open(diff_path, "w", encoding="utf-8") as f:
                f.writelines(diff_file_contents)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logging.error("Error modifying %s: %s", file_path, e)


def pairwise(iterable):
    """from `itertools` 3.10"""
    # pairwise('ABCDEFG') --> AB BC CD DE EF FG
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def version() -> str:
    """Get the version number for the packages from the `config.ini`."""

    config = configparser.ConfigParser()
    config.read(INI_FILE)
    return config["general"]["version"]
