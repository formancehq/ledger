#!/usr/bin/env python3
"""Generate the checked-in public protobuf projection from misc/proto."""

from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "misc" / "proto"
DESTINATION = ROOT / "pkg" / "client" / "v3" / "proto"

PUBLIC_FILES = ("common.proto", "signature.proto", "audit.proto", "bucket.proto", "cluster.proto", "restore.proto")
PRIVATE_MESSAGES = (
    "PersistedClusterState",
    "TransactionState",
    "IdempotencyKeyValue",
    "IdempotencyFailure",
    "TransactionReferenceValue",
    "NumscriptVersionValue",
    "PersistedConfig",
)
PUBLIC_GO_PACKAGE = "github.com/formancehq/ledger/pkg/client/v3/grpc"


def brace_end(text: str, start: int) -> int:
    depth = 0
    in_line_comment = False
    in_block_comment = False
    in_string = False
    escaped = False

    for index in range(start, len(text)):
        char = text[index]
        next_char = text[index + 1] if index + 1 < len(text) else ""

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
            continue
        if in_block_comment:
            if char == "*" and next_char == "/":
                in_block_comment = False
            continue
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue

        if char == "/" and next_char == "/":
            in_line_comment = True
            continue
        if char == "/" and next_char == "*":
            in_block_comment = True
            continue
        if char == '"':
            in_string = True
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return index + 1

    raise ValueError("unterminated protobuf declaration")


def remove_message(text: str, name: str) -> str:
    pattern = re.compile(rf"(?m)^message\s+{re.escape(name)}\s*\{{")
    match = pattern.search(text)
    if match is None:
        raise ValueError(f"private message {name} is missing from misc/proto/common.proto")

    end = brace_end(text, match.start())
    start = match.start()
    line_start = text.rfind("\n", 0, start) + 1
    cursor = line_start
    while cursor > 0:
        previous_end = cursor - 1
        previous_start = text.rfind("\n", 0, previous_end) + 1
        previous_line = text[previous_start:previous_end].strip()
        if previous_line and not previous_line.startswith("//"):
            break
        start = previous_start
        cursor = previous_start

    return text[:start].rstrip() + "\n\n" + text[end:].lstrip()


def project(name: str) -> str:
    source = (SOURCE / name).read_text(encoding="utf-8")
    if name == "common.proto":
        for message in PRIVATE_MESSAGES:
            source = remove_message(source, message)

    source = re.sub(
        r'(?m)^option go_package\s*=\s*"[^"]+";\s*$',
        f'option go_package = "{PUBLIC_GO_PACKAGE}";',
        source,
    )
    header = f"// Code generated from misc/proto/{name}. DO NOT EDIT.\n\n"
    return header + source


def main() -> None:
    DESTINATION.mkdir(parents=True, exist_ok=True)
    for name in PUBLIC_FILES:
        (DESTINATION / name).write_text(project(name), encoding="utf-8")


if __name__ == "__main__":
    main()
