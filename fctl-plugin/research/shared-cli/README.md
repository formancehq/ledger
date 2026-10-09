# Shared Ledger command feasibility experiment

Result: **yes for sharing the manifest and request validation across two
transports**. This experiment does not establish a complete migration of every
business command, option, payload schema, or human rendering convention.

The Ledger plugin exposes `NewWithExecutor`. Both the normal HTTP executor and
an experimental gRPC executor pass through its existing command manifest,
argument rules, flag normalization, identifier validation, JSON syntax/size
validation, and request option validation. No Cobra, terminal, credential, or
Ledger server dependency was added to the independently released plugin module.

| Shared product package | fctl host | ledgerctl host |
| --- | --- | --- |
| Manifest, operation identity, declarative input descriptions, request validation | External plugin process, HTTP client broker, profiles/auth, input sources, forms, output | Existing Cobra bindings, connection/signing, gRPC/protobuf conversion, input sources, prompts, native JSON projection |

Sharing the existing `RunE` functions directly is unsuitable: they also own
global terminal state, file reads, credentials, gRPC envelopes, and output.
The prototype keeps these responsibilities in each host. Existing ledgerctl
Cobra declarations are still host-specific. Only the ledger-name field title
is consumed from the common manifest by its prototype prompt; the complete
five-field form is rendered by fctl.

## Executed tests

Source baseline: Ledger `885e135ef98a232aa9e4c05a5cb2895ce11c4238`, descended
from `release/v3.0`; fctl `7ae581642ae47042c4489d8faf1fbc55a0f6dce8`.
Date: 2026-10-09. Ledger server build: `3.0.0-beta.10+local.885e135ef98a`,
gRPC protocol revision 26. All listeners were bound to loopback, without
authentication or TLS, with isolated data/WAL and CLI configuration directories.
The server was stopped after testing. No Cloud stack or user profile was changed.

The three representative paths were ledger list, ledger creation, and JSON
transaction creation. The opt-in ledgerctl handler used the same product
manifest/validation as the actual independently running fctl Ledger plugin.

The real-server matrix passed **25 CLI invocations plus 4 cleanup calls**:

- Both hosts listed and created ledgers. Creation metadata containing
  `9007199254740993` was read back exactly from the server.
- Each host submitted three transactions through inline JSON, `@file`, and
  stdin. All three persisted postings were read back with that exact amount.
- Malformed JSON and missing files failed with empty stdout and diagnostics on
  stderr. Transaction counts did not change.
- The complete shared gRPC list had the same JSON value as the original
  ledgerctl list. fctl retained its own response envelope. Both human lists ran;
  identical table layout was not asserted.
- Non-TTY creation required an explicit name. A real 100-column PTY completed
  fctl's five-step form and ledgerctl's manifest-titled name prompt. Both
  produced valid JSON on stdout, without prompt text or ANSI controls.
- Ctrl+C at each prompt returned nonzero, emitted no JSON, and created nothing.
- All four owned ledgers were deleted and the server contained no leftovers.

The archived launcher and parameterized harness were replayed against a second
fresh local Ledger. The same 25 command cases and 4 cleanup calls passed, and
that owned server was stopped automatically afterward.

[results.json](results.json) records the cases, exit status, output sizes,
assertions, cleanup, and scope limits. Raw terminal capture remains a local
test artifact; it contains no additional architectural conclusion.

The shared product package passed its complete race suite and Nix lint. The
opt-in adapter's targeted race tests and vet passed. Synthetic gRPC tests
verified complete/native JSON equivalence, a row followed by an injected stream
error, cancellation, unsupported options rejected before RPC, file/stdin reads,
and exact numbers. Product executor tests cover defaults, explicit false,
23 invalid requests rejected before dispatch, in-flight cancellation,
partial data plus errors, and one invocation without automatic retries.

An independent review found no blocking issue within this bounded experiment.
Jev Review was unavailable because `JEV_API_KEY` was absent. The optional
alternate-root-modfile golangci-lint invocation failed during package loading;
the prototype was checked with race tests, vet, and formatting instead. This
does not qualify the archived root adapter as a production-ready change.

## Original ledgerctl boundaries confirmed separately

The untouched original ledgerctl passed targeted race suites and eight
additional I/O boundary tests. Ten real binary invocations included two PTY
prompts against a synthetic loopback gRPC fixture.

Confirmed problems relevant to an extraction:

- Automatic ledger selection can print `INFO` before the JSON result.
- The native ledger-name prompt can put text and cursor escapes on JSON stdout.
- The top-level pterm error printer can put errors on stdout. Setting Cobra's
  writers or pterm's default writer does not redirect already-created prefix
  printers. The opt-in patch uses explicit stderr for its prompt and errors.
- Native structured encoding writes to `os.Stdout`, bypassing Cobra `SetOut`.
- The native root context does not bind OS signals to graceful cancellation.
  Ctrl+C process termination and context cancellation are separate guarantees.
- Native streaming helpers discard earlier rows when a later receive fails.
  The experiment deliberately preserves data plus the error. A migration must
  choose and document its public presentation policy rather than silently
  change native output.
- A result-file error can follow a successful mutation and complete JSON
  stdout. The sink requires an existing file and is JSON-only. The shared
  executor must not replay a write because the host failed to deliver output.

## Scope limits and next extraction

The gRPC adapter supports only the three named paths. Ledger creation maps
metadata and enforcement mode; schema, account types, mirror creation,
pagination, checkpoint selection, and legacy transaction input options are
outside this experiment. Unsupported explicit flags fail before RPC.
Transport-specific payload decoding still exists in both adapters; this does
not prove every semantic validation has a single implementation.

Blocked stdin context cancellation, full graceful signal handling, signed or
authenticated RPCs, full error-category parity, account-volume projections,
all renderers, and all command variants were not validated by the real-server
matrix. Keep ledgerctl operational commands (storage, repair, backup/restore,
administration) autonomous and bundled with the server.

The next step is a production command-operation package with explicit input,
typed result/error, cursor/partial-result policy, and transport interfaces.
Generate or bind both business command trees from that contract and add
compatibility tests before moving commands. This prototype is evidence for
that boundary, not a replacement for this extraction.

## Reproduce without changing default CLI sources

Use a disposable Ledger worktree at this branch and the repository-pinned Nix
environment. `ledgerctl.patch` is intentionally not applied by default. Its
zero-context diff avoids treating diff context as source whitespace.

```sh
git apply --check --unidiff-zero fctl-plugin/research/shared-cli/ledgerctl.patch
git apply --unidiff-zero fctl-plugin/research/shared-cli/ledgerctl.patch
mkdir -p build/shared-cli-poc
cp go.mod build/shared-cli-poc/ledger.mod
cp go.sum build/shared-cli-poc/ledger.sum
go mod edit -modfile=build/shared-cli-poc/ledger.mod \
  -require=github.com/formancehq/fctl/pkg/pluginsdk@v0.0.0-20261009101655-a5cfb893d64d \
  -require=github.com/formancehq/ledger/fctl-plugin@v0.0.0-20261009103441-885e135ef98a \
  -replace=github.com/formancehq/ledger/fctl-plugin=./fctl-plugin
go mod tidy -modfile=build/shared-cli-poc/ledger.mod
go test -modfile=build/shared-cli-poc/ledger.mod -race ./cmd/ledgerctl \
  -run '^TestSharedPOC' -count=1
go vet -modfile=build/shared-cli-poc/ledger.mod ./cmd/ledgerctl ./cmd/ledgerctl/cmdutil
go build -modfile=build/shared-cli-poc/ledger.mod \
  -o build/shared-cli-poc/ledgerctl ./cmd/ledgerctl
```

The alternate modfile isolates dependency selection from the production root
module. Tidy raises some transitive gRPC/protobuf/HTTP instrumentation versions
to the SDK-selected versions. Do not promote those changes without review.

Build the external plugin with the exact test-server version, for example
`just build-fctl-plugin 3.0.0-beta.10+local.885e135ef98a 1`. Build fctl from the
stated baseline with an alternate modfile whose
`github.com/formancehq/ledger/fctl-plugin` replacement points to this worktree's
`fctl-plugin`. Its SDK replacement must point to the fctl checkout's
`pkg/pluginsdk`. The normal fctl checkout and its configuration can remain intact.

To start the isolated real Ledger, copy `server.go.txt` to
`build/shared-cli-runtime/main.go` and compile it from the Ledger root. Inject
`internal/pkg/version.Version` and `Commit` through the existing linker flags.
Set `SHARED_CLI_RUNTIME_DIR` to a **new absolute directory** and run the binary.
It prebinds all three listeners to loopback, writes `config/runtime.json`,
bootstraps one local node, and refuses to reuse a runtime directory. The 95%
disk health threshold is a local test setting, not a deployment recommendation.
Wait for `/readyz` to succeed before invoking the harness.

```sh
python3 fctl-plugin/research/shared-cli/probe.py \
  --runtime /absolute/local/runtime/config/runtime.json \
  --work-dir /absolute/new/probe-directory \
  --fctl-binary /absolute/fctl \
  --ledgerctl-binary /absolute/ledgerctl \
  --plugin-binary /absolute/fctl-plugin-ledger
```

The harness refuses non-loopback targets, requires an initially empty Ledger,
uses a fresh fctl configuration, deletes only its owned ledger names, and writes
a report after cleanup. It does not start or stop the server: stop the owned
server process afterward. Never point this mutation harness at an existing
development or Cloud stack.
