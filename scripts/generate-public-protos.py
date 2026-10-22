#!/usr/bin/env python3
"""Generate the checked-in public protobuf projection from misc/proto."""

from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "misc" / "proto"
DESTINATION = ROOT / "pkg" / "client" / "v3" / "proto"

PUBLIC_FILES = ("common.proto", "signature.proto", "audit.proto", "bucket.proto", "cluster.proto", "restore.proto")
PUBLIC_GO_PACKAGE = "github.com/formancehq/ledger/pkg/client/v3/grpc"


def project(name: str) -> str:
    source = (SOURCE / name).read_text(encoding="utf-8")
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
