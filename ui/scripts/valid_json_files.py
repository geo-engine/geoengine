#! /usr/bin/env python

"""Check all JSON files in the current directory for validity."""

import os
import json
import sys
from pprint import pprint


def eprint(*args, **kwargs):
    """Print to stderr."""
    print(*args, **kwargs, file=sys.stderr)


def epprint(*args, **kwargs):
    """Pretty print to stderr."""
    pprint(*args, **kwargs, stream=sys.stderr)


invalid_files = []
valid_files = []


def parse():
    """Parse the current directory for JSON files and check their validity."""
    cwd = os.getcwd()

    for filename in os.listdir(cwd):
        if not filename.endswith('.json'):
            continue

        eprint(f'Checking {filename}...')

        with open(cwd + '/' + filename, encoding='utf8') as json_file:
            try:
                json.load(json_file)
                valid_files.append(filename)
            except ValueError as e:
                eprint(f"Error at {filename}: {e}")
                invalid_files.append(filename)

    eprint('\nVALID JSON FILES:')
    epprint(valid_files)

    eprint('\nINVALID JSON FILES:')
    epprint(invalid_files)


if __name__ == '__main__':
    parse()
