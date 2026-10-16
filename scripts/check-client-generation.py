#!/usr/bin/env python3
"""Regenerate in place and fail if any tracked contract output drifts."""

import hashlib
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parents[1]
def snapshot():
    paths = [root / "pkg/client/v3/go.mod", root / "pkg/client/v3/go.sum", root / "pkg/client/v3/contract.json"]
    paths.extend((root / "pkg/client/v3/grpc").glob("*.go"))
    paths.extend((root / "pkg/client/v3/proto").glob("*"))
    paths.extend((root / "internal/proto").rglob("*.pb.go"))
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in paths if path.is_file()}


before = snapshot()
subprocess.run(["just", "generate-proto"], cwd=root, check=True)
after = snapshot()
changed = sorted(key for key in before.keys() | after.keys() if before.get(key) != after.get(key))
if changed:
    raise SystemExit("generated contract drift:\n" + "\n".join(changed))
