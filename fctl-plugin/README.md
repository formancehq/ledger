# Ledger fctl plugin

Profiles: **CLI** and **library**. This separate Go 1.26 module owns Ledger's
HTTP commands for fctl v4. It depends on the public `pluginsdk` module only;
it does not import Ledger server internals, Cobra, or the fctl core.

`fctl-plugin-ledger` is a native executable. fctl starts it with the SDK
transport, renders its declarative command manifest and forms, and supplies an
HTTP client through the host broker. Authentication, endpoint selection,
request diagnostics, profiles, file input, and output styling belong to fctl.
The same Go package can be embedded through `New` or `NewVersion`.

The plugin entry point uses the SDK-owned protocol and standard `flag` package
for its two offline metadata flags. It has no human command tree or operational
listener, so the CLI profile's Cobra and Fx composition requirements do not
apply. Owner: Ledger maintainers. Revisit this exception if the executable
acquires independent user commands or a service lifecycle.

## Version identity

| Value | Meaning |
| --- | --- |
| Service version | Exact version of the co-released Ledger service, such as `3.0.0-beta.5` |
| Plugin revision | Positive integer identifying a CLI-only revision for that service version |
| Protocol version | SDK runtime protocol understood by fctl and the executable |

The SDK manifest's `version` is the service version. Plugin revision is separate
metadata; it must not be appended to the service version. `NewVersion(client,
serviceVersion)` holds the version on each instance and returns fresh manifests.
`New(client)` retains the initial `3.0.0` embedded contract.

An independent CLI fix for Ledger `3.0.0` increments plugin revision from `1`
to `2`. A Ledger `3.0.1` release starts with its own revision `1`. Each platform
artifact is immutable and resolves to a digest; never replace an already
advertised artifact under the same service version and revision.

## Build and test

From the repository root, use the pinned Nix development environment:

```sh
nix develop --command just test-fctl-plugin
nix develop --command just build-fctl-plugin 3.0.0-beta.5 1
./build/fctl-plugin-ledger --version
./build/fctl-plugin-ledger --manifest
nix develop --command just fctl-plugin-manifest
```

The default local build reports service version `dev` and revision `1`. Set the
two recipe arguments to test the exact service version deployed on your target.
`--version` prints JSON with `name`, `serviceVersion`, `revision`, and
`protocolVersion`. `--manifest` prints the complete SDK command manifest and
does not start the plugin transport or access a server. The manifest recipe
exports it to `build/fctl-plugin-manifest.json`.

Without metadata flags, the executable serves the SDK runtime protocol and is
started by fctl. It is not a replacement for the `fctl ledger ...` command tree.
Use fctl to select an authenticated Cloud stack or a local unauthenticated
endpoint, then execute Ledger commands through the selected plugin.

All contributor tooling comes from the root Nix environment and its existing
`flake.lock`. The SDK contains the protocol implementation; plugin builds do
not require a host `protoc` installation. The root Nix environment also
provides the ORAS tool used for OCI publication; no host installation is needed.

## Package without publishing

```sh
nix develop --command just package-fctl-plugin 3.0.0-beta.5 1
```

GoReleaser builds six static executables: Linux, macOS, and Windows, each for
amd64 and arm64. Archives and SHA-256 checksums are written to
`build/fctl-plugin/`. Unix archives use tar.gz; Windows archives use zip.
Names include the exact Ledger service version and plugin revision. This
recipe uses snapshot mode and skips all publication.

The release builds preserve injected version and revision in Go build
metadata. Do not add `-trimpath`: Go omits recorded linker flags with this
option, so the publisher cannot verify a foreign-platform executable's
service version and plugin revision and will reject it before upload.

The root GoReleaser configuration also builds the separate module during each
Ledger release. It injects the actual release version and `PLUGIN_REVISION`
(default `1`) and attaches the platform archives to the Ledger GitHub release
alongside the server and ledgerctl archives. The release workflow runs the
plugin tests before building, and the normal CI test job validates the nested
module. Root lint, tidy, and agent baseline checks include this module.

## Catalogue and OCI publication

The SDK manifest from `--manifest` is the command contract used by the fctl
catalogue. It preserves argument rules, request-body bindings, and declarative
forms without duplicating them in a handwritten catalogue file.

The publisher combines this SDK manifest with each raw executable's actual
SHA-256 checksum and the digest returned by ORAS. It emits the shared fctl
catalogue schema `1`: `releases` entries contain `service`, `serviceVersion`,
`revision`, `platform` (`os`, `arch`), `artifact` (`registry`, `repository`,
`digest`), `sha256`, and `manifest`.

Each OCI image manifest has artifact type
`application/vnd.formance.fctl.plugin.v1`, an empty JSON config with media type
`application/vnd.oci.empty.v1+json`, and exactly one raw executable layer with
media type `application/vnd.formance.fctl.plugin.executable.v1`. It uses OCI
image specification `v1.1`. The executable is uploaded directly, without an
archive or container image index. Catalogue entries use immutable digests.

Prepare a matching native manifest and six platform binaries, then exercise
ORAS locally before publishing:

```sh
nix develop --command just build-fctl-plugin 3.0.0-beta.10 1 fctl-plugin-manifest
nix develop --command just package-fctl-plugin 3.0.0-beta.10 1
nix develop --command just fctl-plugin-oci-layout 1
```

The layout recipe writes real artifacts under `build/fctl-plugin-oci/` and
the six-platform catalogue to `build/fctl-plugin-catalogue.json`. It contacts
no registry. The catalogue still names the configured registry: a local OCI
layout is a serialization fixture, not a hosted download endpoint.

For an explicitly authorized upload, authenticate ORAS using the publishing
account, then run:

```sh
nix develop --command oras login ghcr.io --username PUBLISHER --password-stdin
nix develop --command just publish-fctl-plugin https://ghcr.io formancehq/fctl-plugin-ledger 1
```

This recipe reads the already prepared snapshot, uploads all six raw binaries,
and replaces `build/fctl-plugin-catalogue.json` only after every upload succeeds.
It checks every executable's embedded service version, revision, product
entry point, and target platform before the first ORAS call. A stale manifest
or mixed release set produces no upload and no catalogue.
For a tagged co-release, the same helper accepts the root GoReleaser
`artifacts.json` and distribution directory through `--artifacts` and
`--source-root`. Export the native binary's manifest from that exact release.
Promotion into an official catalogue is a separate operation after upload and
anonymous download verification.

GitHub creates GHCR packages privately by default. After the first upload,
set the package visibility to **Public** in its GitHub package settings. For
a private source repository, remove inherited repository permissions when
needed before changing package visibility. Keep sources private while allowing
anonymous artifact downloads. This is a one-time registry setup, independent
of source visibility; see [GitHub package permissions](https://docs.github.com/en/packages/learn-github-packages/about-permissions-for-github-packages).
