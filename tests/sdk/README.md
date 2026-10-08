# Bulk SDK contract probes

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
nix develop --command node tests/sdk/bulk-contract.mjs \
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
