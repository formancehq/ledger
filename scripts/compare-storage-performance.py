#!/usr/bin/env python3
"""Summarize paired Go benchmark samples without hiding the raw output."""

import pathlib
import re
import statistics
import sys


BENCHMARK = re.compile(r"^(Benchmark\S+)\s+\d+\s+([\d.]+) ns/op(?:\s+\d+ B/op\s+\d+ allocs/op)?$")


def samples(path: pathlib.Path) -> dict[str, list[float]]:
    found: dict[str, list[float]] = {}
    for line in path.read_text().splitlines():
        match = BENCHMARK.match(line)
        if match:
            found.setdefault(match.group(1), []).append(float(match.group(2)))
    return found


def main(directory: pathlib.Path) -> None:
    lines = ["# Storage engine benchmark comparison", "", "Lower ns/op is better. Medians of five measured runs on one runner; warmups excluded.", ""]
    lines.append("| Benchmark | Pebble ns/op | RocksDB ns/op | RocksDB delta |")
    lines.append("|---|---:|---:|---:|")
    for suite in ("dal", "index"):
        pebble = samples(directory / f"pebble-{suite}.txt")
        rocksdb = samples(directory / f"rocksdb-{suite}.txt")
        if not pebble or pebble.keys() != rocksdb.keys():
            raise SystemExit(f"missing or mismatched {suite} benchmark cases: {pebble.keys()} vs {rocksdb.keys()}")
        for name in sorted(pebble):
            if len(pebble[name]) != 5 or len(rocksdb[name]) != 5:
                raise SystemExit(f"expected five samples per engine for {name}")
            baseline = statistics.median(pebble[name])
            candidate = statistics.median(rocksdb[name])
            delta = (candidate / baseline - 1) * 100
            lines.append(f"| `{name}` | {baseline:,.0f} | {candidate:,.0f} | {delta:+.1f}% |")
    lines += ["", "These are storage microbenchmarks, not HTTP latency or production capacity measurements."]
    report = "\n".join(lines) + "\n"
    (directory / "summary.md").write_text(report)
    print(report)


if __name__ == "__main__":
    main(pathlib.Path(sys.argv[1]))
