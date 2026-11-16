#!/usr/bin/env python3
"""Copy the bench executables from Cargo's JSON messages on stdin to a directory.

Usage: `cargo bench --no-run --message-format=json-render-diagnostics | collect_bench_executables.py DIR`

Each bench is copied as `DIR/bench/<bench target name>`, so that the continuous benchmarking can run
the benches of two commits alternately without rebuilding. The GDAL worker the benches start is
copied as `DIR/gdalsource-process`; the benches find it via `GDALSOURCE_PROCESS_PATH`, as it is no
longer next to them.
"""

import json
import shutil
import sys
from pathlib import Path


def main() -> int:
    target_dir = Path(sys.argv[1])
    (target_dir / "bench").mkdir(parents=True, exist_ok=True)
    copied, worker = 0, False
    for line in sys.stdin:
        try:
            message = json.loads(line)
        except ValueError:
            continue  # not a Cargo message
        if message.get("reason") != "compiler-artifact" or not message.get("executable"):
            continue
        name, kind = message["target"]["name"], message["target"]["kind"]
        if "bench" in kind:
            shutil.copy2(message["executable"], target_dir / "bench" / name)
            copied += 1
        elif name == "gdalsource-process":
            shutil.copy2(message["executable"], target_dir / name)
            worker = True
        else:
            continue
        print(f"{name}: {message['executable']}")
    if copied == 0 or not worker:
        print("::error::no bench executables or no gdalsource-process in the Cargo output")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
