#!/usr/bin/env python3
"""Verify the server's exact client selection against a downloaded Go module."""

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

CLIENT_MODULE = "github.com/formancehq/ledger/pkg/client/v3"
VERSION = re.compile(r"v3\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")
DIGEST = re.compile(r"[0-9a-f]{64}$")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_json(path):
    data = json.loads(path.read_text())
    require(isinstance(data, dict), f"{path} must contain an object")
    return data


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify(root, server_tag, downloaded_client, client_tag, client_commit,
           server_commit, module_checksum, *, local_contract=False):
    server = read_json(root / "misc/release/public-client.json")
    local_client = read_json(root / "pkg/client/v3/contract.json")
    published_client = read_json(downloaded_client / "contract.json")
    require(VERSION.fullmatch(server_tag), "invalid server release version")
    require(server.get("serverVersion") == server_tag,
            "server compatibility metadata does not name this release")
    require(server.get("clientModule") == CLIENT_MODULE,
            "server compatibility metadata names the wrong client module")
    client_version = server.get("clientVersion")
    require(isinstance(client_version, str) and VERSION.fullmatch(client_version),
            "server compatibility metadata has an invalid client version")
    require(client_tag == f"pkg/client/{client_version}",
            "downloaded client tag differs from server compatibility metadata")
    require(server.get("compatibilityBasis") == "identical-contract-and-protocol",
            "server compatibility metadata has no verified basis")
    require(local_client == published_client,
            "checkout client metadata differs from the tagged module")
    require(published_client.get("clientModule") == CLIENT_MODULE,
            "tagged module manifest names the wrong client module")
    require(published_client.get("clientVersion") == client_version,
            "tagged module manifest has a different client version")
    known_versions = published_client.get("compatibleServerVersions")
    require(isinstance(known_versions, list) and known_versions and
            all(isinstance(v, str) and VERSION.fullmatch(v) for v in known_versions),
            "tagged module has no valid published server compatibility list")
    expected_digest = server.get("contractDescriptorSHA256")
    require(isinstance(expected_digest, str) and DIGEST.fullmatch(expected_digest),
            "server compatibility metadata has an invalid descriptor digest")
    require(digest(root / "pkg/client/v3/proto/ledger-public.protoset") == expected_digest,
            "server descriptor differs from compatibility metadata")
    require(digest(downloaded_client / "proto/ledger-public.protoset") == expected_digest,
            "tagged client descriptor differs from the server release")
    require(published_client.get("contractDescriptorSHA256") == expected_digest,
            "tagged client manifest has a different descriptor digest")

    protocol_source = (root / "pkg/grpcprotocol/protocol.go").read_text()
    match = re.search(r'\bVersion\s*=\s*"(\d+)"', protocol_source)
    require(match is not None, "server protocol revision is missing")
    protocol = match.group(1)
    require(server.get("protocolVersion") == protocol,
            "server protocol revision differs from compatibility metadata")
    require(published_client.get("protocolVersion") == protocol,
            "tagged client protocol revision differs from the server")
    require(module_checksum.startswith("h1:") and len(module_checksum) > 3,
            "downloaded module has no Go checksum")
    if not local_contract:
        result = subprocess.run(
            ["go", "run", "./scripts/clientrelease", str(root.resolve()),
             client_commit, client_version, str(downloaded_client.resolve()),
             module_checksum],
            cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True)
        require(result.returncode == 0,
                "tagged module source verification failed: " + result.stderr.strip())

    return {
        "serverTag": server_tag,
        "serverCommit": server_commit,
        "clientTag": client_tag,
        "clientCommit": client_commit,
        "moduleChecksum": module_checksum,
        "contractDescriptorSHA256": expected_digest,
        "protocolVersion": protocol,
        "compatibilityBasis": server["compatibilityBasis"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("server_tag")
    parser.add_argument("downloaded_client", type=Path)
    parser.add_argument("client_tag")
    parser.add_argument("client_commit")
    parser.add_argument("server_commit")
    parser.add_argument("module_checksum")
    parser.add_argument("--local-contract", action="store_true",
                        help="check in-progress local metadata without a published client tag")
    args = parser.parse_args()
    try:
        result = verify(args.root, args.server_tag, args.downloaded_client,
                        args.client_tag, args.client_commit, args.server_commit,
                        args.module_checksum, local_contract=args.local_contract)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"client release verification: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
