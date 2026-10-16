#!/usr/bin/env python3
"""Keep the published client independent from Ledger's server implementation."""

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
CLIENT = ROOT / "pkg" / "client" / "v3"
FORBIDDEN_IMPORTS = (
    "github.com/formancehq/ledger/v3/internal/",
    "github.com/formancehq/go-libs/",
    "github.com/cockroachdb/pebble",
    "github.com/holiman/uint256",
    "github.com/invopop/jsonschema",
    "database/sql",
)
IMPORT_RE = re.compile(r'"([^"\\]+)"')


def main() -> int:
    violations: list[str] = []
    for path in sorted(CLIENT.rglob("*.go")):
        text = path.read_text(encoding="utf-8")
        for imported in IMPORT_RE.findall(text):
            if any(imported == forbidden or imported.startswith(forbidden) for forbidden in FORBIDDEN_IMPORTS):
                violations.append(f"{path.relative_to(ROOT)} imports {imported}")
    if violations:
        print("public client imports server-only dependencies:", file=sys.stderr)
        print("\n".join(violations), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
