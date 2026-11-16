#!/usr/bin/env python3
"""Compare two Criterion runs of the continuous benchmarking and print a Markdown report.

The base run saves the baseline `base` (`--save-baseline base`), the run of the pull request
compares against it (`--baseline-lenient base`). Criterion then writes, per benchmark,
`<id>/change/estimates.json` with the bootstrapped relative change of the median and its
confidence interval. Both runs share one runner, so the comparison does not depend on the runner.

A benchmark counts as a regression (improvement) only if the whole confidence interval of the
median change lies above (below) zero and the change exceeds the threshold.
"""

import argparse
import json
import sys
from pathlib import Path

MARKER = "<!-- geoengine-continuous-benchmarking -->"


def load(path: Path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def duration(ns: float) -> str:
    for unit, factor in (("s", 1e9), ("ms", 1e6), ("µs", 1e3)):
        if ns >= factor:
            return f"{ns / factor:.2f} {unit}"
    return f"{ns:.0f} ns"


def cpu_model() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return "unknown"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("criterion_dir", type=Path, help="`target/criterion` of both runs")
    parser.add_argument("--baseline", default="base", help="name of the saved base baseline")
    parser.add_argument("--threshold", type=float, default=0.10, help="relative change, e.g. 0.10")
    parser.add_argument("--base-ref", default="base", help="label of the base commit")
    parser.add_argument("--output", type=Path, required=True, help="file for the Markdown report")
    args = parser.parse_args()

    rows, regressions, improvements, unpaired = [], 0, 0, []
    for benchmark_file in sorted(args.criterion_dir.glob("**/new/benchmark.json")):
        bench_dir = benchmark_file.parent.parent
        benchmark = load(benchmark_file) or {}
        name = benchmark.get("full_id", str(bench_dir.relative_to(args.criterion_dir)))
        change = load(bench_dir / "change" / "estimates.json")
        base = load(bench_dir / args.baseline / "estimates.json")
        new = load(bench_dir / "new" / "estimates.json")
        if change is None or base is None or new is None:
            # only in the pull request (new) or only in the base (removed or no longer run)
            unpaired.append((name, "only in base" if base is not None else "new"))
            continue

        median = change["median"]
        point = median["point_estimate"]
        lower = median["confidence_interval"]["lower_bound"]
        upper = median["confidence_interval"]["upper_bound"]
        if lower > 0 and point > args.threshold:
            verdict, regressions = "🔴 slower", regressions + 1
        elif upper < 0 and point < -args.threshold:
            verdict, improvements = "🟢 faster", improvements + 1
        elif lower > 0 or upper < 0:
            verdict = "small"
        else:
            verdict = "≈"
        rows.append(
            f"| `{name}` | {duration(base['median']['point_estimate'])} "
            f"| {duration(new['median']['point_estimate'])} "
            f"| {point:+.1%} | [{lower:+.1%}, {upper:+.1%}] | {verdict} |"
        )

    out = [
        MARKER,
        "## Continuous Benchmarking",
        "",
        f"Base `{args.base_ref}` and this pull request, measured one after the other on the same runner "
        f"(`{cpu_model()}`). Change of the median with its 95 % confidence interval; "
        f"🔴/🟢 if the interval excludes zero and the change exceeds {args.threshold:.0%}.",
        "",
        f"**{regressions} slower, {improvements} faster, {len(rows) - regressions - improvements} unchanged.**",
        "",
        "<details><summary>All benchmarks</summary>",
        "",
        "| Benchmark | Base | PR | Change | 95 % CI | |",
        "| --- | ---: | ---: | ---: | --- | --- |",
        *rows,
        "",
        "</details>",
    ]
    if unpaired:
        out += ["", "Not compared: " + ", ".join(f"`{name}` ({why})" for name, why in unpaired)]
    args.output.write_text("\n".join(out) + "\n")

    # workflow commands are read from stdout
    if regressions:
        print(f"::warning::{regressions} benchmark(s) slower than the base")
    return 0


if __name__ == "__main__":
    sys.exit(main())
