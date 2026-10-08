# SystemLog SDK regression (EN-2781)

The frontend needs non-apply system log payloads. SystemLog.payload explicitly
allows additional properties while retaining the typed apply.log branch. All
other variants remain open-ended JSON; this does not implement credential
redaction (EN-1634) or change server/audit bytes.

`system-log.mjs` loads the actual built TypeScript SDK operation response schemas
for getLog, listLedgerLogs and executePreparedQuery. It checks complete envelope
and payload equality, nested values, signature fields, query cursor fields,
transaction/metadata apply data, a future sibling, and typed apply validation.
The SDK's existing conversion of apply.log.date to Date is included in the oracle.

The Go `TestOpenAPISpec_SystemLogPayloadPreservation` test reconciles fixture
names with every live LogPayload oneof descriptor and its custom JSON codec.
Non-apply fixtures also round-trip through the real codec with exact equality.
Empty fixtures inventory the remaining variants; representative populated
fixtures exercise nested createLedger, deleteLedger, promoteLedger and
savedLedgerMetadata values. Fixtures are synthetic and contain no credentials.
A new variant must extend the shared fixture inventory.

Run against a TypeScript SDK generated from this repository's exact openapi.yml
and built with that SDK's package build command:

```sh
node tests/sdk/system-log.mjs /absolute/path/to/built-sdk
```

The SDK directory must contain `esm/models/operations/` (the standard Speakeasy
TypeScript ESM build layout). No substitute decoder or SDK source edits are
used. Generated SDKs and dependencies remain outside the tracked source tree.

## Reproduction and generated evidence

Source base: release/v3.0 at `0f4656d1efbcac42705839daccb34012e70d783a`.
Discovery: NO_EXISTING_WORK for EN-2781; EN-2685/PR #2183 is RELATED_BUT_DIFFERENT.
BEFORE_FIX: BUG_REPRODUCED. AFTER_FIX: PASS.

The same generated SDK and operation regression produced 63 field-loss failures
before the schema correction and passed all 23 fixtures across 3 operations
after it. The Go schema regression independently failed before the correction
and passed afterwards. Non-apply keys changed from an empty-object decoder to
`z.catchall(z.object({ apply: ... }), z.any())`; typed apply.log remains present.

Generation: Speakeasy CLI/generator `1.762.0`, generated SDK version `0.0.1`,
default TypeScript configuration. Runtime: Node `22.22.1`, Zod `4.6.5`;
build: TypeScript native preview `7.0.0-dev.20260302.1`, TypeScript `5.8.3`.
This is a fresh SDK generated from the exact Ledger spec, rather than a
regeneration of the separate platform-ui PR #1615 checkout or its overlays.

SHA-256 evidence:

| Artifact | SHA-256 |
|---|---|
| Corrected openapi.yml | `28d33e753926aeb8d42cdf2a9e6a80de04ada81728e96c6c1afff01b5bbc3c66` |
| Generated src/models/system-log.ts | `d141c88391ee123f3c864f837b7b5b498bc709924e7fcade19faa876155dd948` |
| Generated .speakeasy/gen.yaml | `964cd89de0c0647031843191878dbc02774f006f5734236cb47bfd32982ecc2c` |
| Generated package-lock.json | `fa0afef0bd9102cd2985ae95b21c5c90c5f29bb27e5f55fb5be423fcdb5f0ae8` |

Inventory at this revision: the three OpenAPI references are the single-log
response, list-log items and ExecutePreparedQueryResponse.cursor.logData.
SystemLog also documents the shared JSON events envelope. Events need no
separate decoder contract; event delivery behavior is unchanged.
