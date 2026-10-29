#!/usr/bin/env python3
"""Order released Ledger tags reachable from one release/vX.Y branch."""

import re
import sys


BRANCH = re.compile(r"^release/v([0-9]+)\.([0-9]+)$")
TAG = re.compile(r"^v([0-9]+)\.([0-9]+)\.([0-9]+)(?:-rc\.([0-9]+))?$")


def candidates(branch: str, tags: list[str]) -> list[str]:
    match = BRANCH.fullmatch(branch)
    if match is None:
        raise ValueError("branch must have the form release/vX.Y")
    line = (int(match[1]), int(match[2]))
    ranked = []
    for tag in tags:
        tag = tag.strip()
        version = TAG.fullmatch(tag)
        if version is None or (int(version[1]), int(version[2])) != line:
            continue
        # Stable is later than every RC with the same major/minor/patch.
        rank = (int(version[3]), version[4] is None, int(version[4] or 0))
        ranked.append((rank, tag))
    return [tag for _, tag in sorted(ranked, reverse=True)]


if __name__ == "__main__":
    try:
        print("\n".join(candidates(sys.argv[1], sys.stdin.readlines())))
    except (IndexError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
