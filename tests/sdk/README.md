# Generated TypeScript SDK contract tests

## Bulk SDK contract probes

Bulk SDK contracts require evidence through the actual generated TypeScript bulk operation,
in addition to OpenAPI validation. These probes accept a compiled consumer SDK;
they do not maintain a second SDK or hand-written response decoder in Ledger.

Regenerate the consumer SDK from this checkout's `openapi.yml` using its existing
generation configuration and dependencies. Apply `bulk-generation-overlay.yml`
to a copy of the specification before generation. Speakeasy 1.762.0 treats the
whole 401 response as unmodeled when JSON and non-object text are both declared;
the overlay selects JSON for typed errors. Plain-text invalid-token responses
still reach the default SDK error with their original body. The canonical
specification retains both media types, and the probes verify both behaviors.
Place the generated SDK under
`build/bulk-sdk`, install its dependencies and run its build. Record the exact
specification revision, generator configuration/version and dependency lock in
the validation report. Generated files and fixtures stay under ignored `build/`.

Export real router responses with the pinned Ledger development environment:

```bash
LEDGER_BULK_SDK_FIXTURE_DIR="$PWD/build/bulk-sdk-fixtures" \
  bash scripts/agent-validation-env --ephemeral nix develop --command \
  go test ./internal/adapter/http -run '^TestHandleBulk_.*Contract$' -count=1
nix develop .#sdk --command node tests/sdk/bulk-contract.mjs \
  build/bulk-sdk build/bulk-sdk-fixtures
```

Typecheck `bulk-idempotency.ts` against the generated SDK using that SDK's
TypeScript compiler with strict checking, NodeNext module resolution and
ES2022/DOM libraries. Both supplied and omitted batch identity must compile.
The runtime probe verifies that the typed input becomes the actual HTTP header.

The Go tests validate each response against its declared operation schema before
exporting it. The SDK probe requires the critical cases, compares all public
fields against those bytes, checks 503 headers, and ensures defaults do not retry
failed non-atomic batches. It covers mixed successful/failed/aborted outcomes,
atomic failures, malformed requests, authentication, both size limits and panic
recovery. Local generation/testing proves the source contract; publishing a
regenerated consumer SDK is a separate delivery step.

## Large-integer response SDK contract tests

Run `nix develop .#sdk --command just test-sdk-bigint` with Speakeasy authentication
(`SPEAKEASY_API_KEY` or an existing CLI login).
The explicit target pins Speakeasy CLI 1.762.0 (generator 2.882.0), matching the
generator metadata in the committed standalone Ledger SDK at
`platform-ui/packages/sdks/ledger`. It regenerates from this checkout's
`openapi.yml`, installs locked TypeScript/Zod dependencies, and compiles both the
SDK and the typed operation tests covering all 12 operations that currently
return Uint256, BigUint or SignedBigInt values. Negotiation applies by response type, including future nonmonetary fields;
the operation inventory describes current fields rather than limiting the
contract. Generated files live under `build/sdk-bigint`.
No platform-ui checkout or changes are required.

The tagged Go fixture in
`internal/adapter/http/handlers_sdk_bigint_test.go` serves the actual Ledger HTTP
router with a generated mock controller. The SDK calls transaction creation, retrieval, listing and reversal; account
retrieval/listing; bulk operations with a successful result beside a business failure;
ledger log listing and single system log retrieval; and prepared-query cursors
for transactions, accounts and logs, plus dedicated and prepared-query volume
aggregation and transaction analysis. Analysis statistics cover both numeric
defaults and exact strings, while transaction counts remain numeric. Paginated transaction/account/log operations preserve next and
previous cursor tokens and `hasMore` while returning exact monetary strings.
Assertions cover numeric defaults, exact decimal string posting inputs and
responses, typed header parameters, post-commit volumes, and unsigned/signed
account volumes. Ordinary integer IDs/counts, metadata and explicitly
string-encoded counters retain their existing representation. String posting
inputs also work without response negotiation; response negotiation does not
expand request decoding.
This covers the transport and generated client contract; it does not execute the
storage or FSM. Backend expectations verify the decoded posting amount.

SDK generation requires an external Speakeasy credential, so this target is
separate from default CI validation. TypeScript/Zod dependencies are locked in
`package-lock.json`; Speakeasy reports the embedded generator version in the
artifacts it writes under `build/sdk-bigint/.speakeasy`.

## Compact write response SDK probes

`response-suppression.mjs` checks actual generated SDK operations against raw
responses exported by registered HTTP router tests. It covers single writes in
omitted/false/true modes and sequential/atomic bulk with complete skip details.
Generated files stay under ignored `build/`:

```bash
mkdir -p build
nix develop .#sdk --command speakeasy overlay apply --strict \
  --schema openapi.yml --overlay tests/sdk/response-suppression-overlay.yml \
  --out build/response-sdk-input.yml
nix develop .#sdk --command speakeasy generate sdk \
  --schema build/response-sdk-input.yml --out build/response-sdk --lang typescript --auto-yes
cp tests/sdk/package.json tests/sdk/package-lock.json build/response-sdk/
nix develop .#sdk --command npm --prefix build/response-sdk ci --ignore-scripts --no-audit --no-fund
(cd build/response-sdk && nix develop ../..#sdk --command node node_modules/typescript/bin/tsc)
LEDGER_WRITE_SDK_FIXTURE_DIR="$PWD/build/write-fixtures" \
LEDGER_BULK_SDK_FIXTURE_DIR="$PWD/build/bulk-fixtures" \
  bash scripts/agent-validation-env --ephemeral nix develop .#ci --command \
  go test ./internal/adapter/http -run '^TestWriteResponse_(SingleTransaction|OtherMutations|Bulk)$' -count=1
nix develop .#sdk --command node tests/sdk/response-suppression.mjs \
  build/response-sdk build/write-fixtures build/bulk-fixtures
```

The overlay retains canonical OpenAPI unchanged. The pinned Speakeasy CLI 1.762.0 / generator 2.882.0 cannot
model the existing Databricks `not` constraint and otherwise skips sink creation
entirely. Its removal from generation input permits a NATS operation/response
probe, without establishing Databricks auth validation. The bulk media overlay
has the existing JSON/text limitation described above. Bind the exact schema,
overlay, generator configuration/version and dependency lock in the report.

Sequence is an unsigned JSON number on the wire. The pinned TypeScript
generator produces `number`, so it rounds `9007199254740993` to
`9007199254740992`, and uint64 maximum to `18446744073709552000`. The probes
establish union and field preservation, and separately compare the compact
sequence raw token with the decoded number to report rounding. Equality after
JSON.parse alone is not full-width precision evidence; these probes do not
certify full-width numeric SDK decoding. Full-range clients need lossless
JSON decoding or the raw sequence token. Bigint response negotiation does not
change ordinary IDs or this sequence.
