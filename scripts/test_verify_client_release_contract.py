import base64
import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from verify_client_release_contract import verify


class ReleaseContractTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name) / "release"
        self.published = Path(self.directory.name) / "published"
        (self.root / "misc/release").mkdir(parents=True)
        (self.root / "pkg/client/v3/proto").mkdir(parents=True)
        (self.root / "pkg/client/v3/grpc").mkdir(parents=True)
        (self.root / "pkg/grpcprotocol").mkdir(parents=True)
        (self.published / "proto").mkdir(parents=True)
        (self.published / "grpc").mkdir(parents=True)
        self.descriptor = b"immutable-public-descriptor"
        self.digest = hashlib.sha256(self.descriptor).hexdigest()
        (self.root / "pkg/client/v3/proto/ledger-public.protoset").write_bytes(self.descriptor)
        (self.published / "proto/ledger-public.protoset").write_bytes(self.descriptor)
        module_file = "module github.com/formancehq/ledger/pkg/client/v3\n\ngo 1.26.0\n"
        (self.root / "pkg/client/v3/go.mod").write_text(module_file)
        (self.published / "go.mod").write_text(module_file)
        client_source = "package grpc\n\nfunc Client() {}\n"
        (self.root / "pkg/client/v3/grpc/client.go").write_text(client_source)
        (self.published / "grpc/client.go").write_text(client_source)
        (self.root / "pkg/grpcprotocol/protocol.go").write_text('package grpcprotocol\nconst Version = "14"\n')
        self.server = {
            "serverVersion": "v3.0.0-beta.6",
            "clientModule": "github.com/formancehq/ledger/pkg/client/v3",
            "clientVersion": "v3.0.0-beta.6",
            "contractDescriptorSHA256": self.digest,
            "protocolVersion": "14",
            "compatibilityBasis": "identical-contract-and-protocol",
        }
        self.client = {
            "clientModule": "github.com/formancehq/ledger/pkg/client/v3",
            "clientVersion": "v3.0.0-beta.6",
            "contractDescriptorSHA256": self.digest,
            "protocolVersion": "14",
            "compatibleServerVersions": ["v3.0.0-beta.6"],
        }
        self.save()
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)

    def git(self, *args, capture=False):
        result = subprocess.run(["git", *args], cwd=self.root, check=True,
                                capture_output=capture, text=True)
        return result.stdout.strip() if capture else None

    def module_checksum(self):
        prefix = f"github.com/formancehq/ledger/pkg/client/v3@{self.client['clientVersion']}"
        entries = []
        for path in self.published.rglob("*"):
            if path.is_file():
                name = f"{prefix}/{path.relative_to(self.published).as_posix()}"
                entries.append((name, hashlib.sha256(path.read_bytes()).hexdigest()))
        content = "".join(f"{digest}  {name}\n" for name, digest in sorted(entries))
        return "h1:" + base64.b64encode(hashlib.sha256(content.encode()).digest()).decode()

    def save(self):
        (self.root / "misc/release/public-client.json").write_text(json.dumps(self.server))
        (self.root / "pkg/client/v3/contract.json").write_text(json.dumps(self.client))
        (self.published / "contract.json").write_text(json.dumps(self.client))

    def check(self, server_version="v3.0.0-beta.6", server_commit=None):
        self.git("add", "pkg/client/v3")
        if subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=self.root).returncode:
            self.git("-c", "user.name=Ledger Test", "-c", "user.email=ledger@example.test",
                     "commit", "-qm", "client fixture")
        client_commit = self.git("rev-parse", "HEAD", capture=True)
        return verify(self.root, server_version, self.published,
                      f"pkg/client/{self.client['clientVersion']}",
                      client_commit, server_commit or client_commit,
                      self.module_checksum())

    def test_initial_pair_may_share_version_and_commit(self):
        provenance = self.check()
        self.assertEqual(provenance["clientTag"], "pkg/client/v3.0.0-beta.6")
        self.assertEqual(provenance["clientCommit"], provenance["serverCommit"])
        self.assertEqual(provenance["contractDescriptorSHA256"], self.digest)

    def test_server_only_release_reuses_immutable_client(self):
        self.server["serverVersion"] = "v3.0.0-beta.7"
        self.save()
        provenance = self.check("v3.0.0-beta.7", "server-commit")
        self.assertEqual(provenance["serverTag"], "v3.0.0-beta.7")
        self.assertEqual(provenance["clientTag"], "pkg/client/v3.0.0-beta.6")
        self.assertNotEqual(provenance["clientCommit"], provenance["serverCommit"])
        self.assertEqual(self.client["compatibleServerVersions"], ["v3.0.0-beta.6"])

    def test_client_version_can_advance_independently(self):
        self.server["serverVersion"] = "v3.0.0-beta.7"
        self.server["clientVersion"] = "v3.0.0-beta.8"
        self.client["clientVersion"] = "v3.0.0-beta.8"
        self.save()
        self.assertEqual(self.check("v3.0.0-beta.7")["clientTag"],
                         "pkg/client/v3.0.0-beta.8")

    def test_rejects_descriptor_drift(self):
        (self.published / "proto/ledger-public.protoset").write_bytes(b"other descriptor")
        with self.assertRaisesRegex(ValueError, "tagged client descriptor differs"):
            self.check()

    def test_rejects_altered_downloaded_go_source(self):
        (self.published / "grpc/client.go").write_text("package grpc\n\nfunc Client() { panic(\"changed\") }\n")
        with self.assertRaisesRegex(ValueError, "downloaded module source differs from client tag"):
            self.check()

    def test_rejects_extra_downloaded_go_source(self):
        (self.published / "grpc/extra.go").write_text("package grpc\n")
        with self.assertRaisesRegex(ValueError, "downloaded module source differs from client tag"):
            self.check()

    def test_accepts_go_module_packaging_exclusions(self):
        vendor = self.root / "pkg/client/v3/vendor/example.org/ignored"
        vendor.mkdir(parents=True)
        (vendor / "ignored.go").write_text("package ignored\n")
        nested = self.root / "pkg/client/v3/nested"
        nested.mkdir()
        (nested / "go.mod").write_text("module example.org/nested\n\ngo 1.26.0\n")
        (nested / "nested.go").write_text("package nested\n")
        self.assertEqual(self.check()["clientTag"], "pkg/client/v3.0.0-beta.6")

    def test_rejects_incorrect_server_compatibility(self):
        self.server["serverVersion"] = "v3.0.0-beta.7"
        self.server["protocolVersion"] = "15"
        self.save()
        with self.assertRaisesRegex(ValueError, "server protocol revision differs"):
            self.check("v3.0.0-beta.7")

    def test_rejects_invalid_client_compatibility_list(self):
        self.client["compatibleServerVersions"] = []
        self.save()
        with self.assertRaisesRegex(ValueError, "no valid published server compatibility list"):
            self.check()

    def test_rejects_unverified_compatibility_basis(self):
        self.server["compatibilityBasis"] = "unspecified"
        self.save()
        with self.assertRaisesRegex(ValueError, "no verified basis"):
            self.check()

    def test_matching_version_does_not_require_prior_client_listing(self):
        self.server["serverVersion"] = "v3.0.0-beta.8"
        self.server["clientVersion"] = "v3.0.0-beta.8"
        self.client["clientVersion"] = "v3.0.0-beta.8"
        self.client["compatibleServerVersions"] = ["v3.0.0-beta.7"]
        self.save()
        self.assertEqual(self.check("v3.0.0-beta.8")["clientTag"],
                         "pkg/client/v3.0.0-beta.8")


if __name__ == "__main__":
    unittest.main()
