# Ledger v3 plugin — exclusions

All 108 executable commands are accounted for: `108 = 54 included + 13
host/local + 34 operator + 7 signing/event-sink`. Every non-product bucket is
an explicit exclusion or a separately granted facet, never a silent omission.

## A. fctl host/local concerns (13) — excluded, host-owned

| Command | Reason |
| --- | --- |
| `auth login`, `auth logout`, `auth status`, `auth generate-token`, `auth generate-key` | Credentials and token lifecycle are host-owned. fctl resolves `auth.*` capabilities through the profile's preferred provider. |
| `profile create`, `profile delete`, `profile list`, `profile show`, `profile use` | Profiles are core fctl concepts. |
| `upgrade` | Self-update. The programme plan explicitly bars copying Ledger's self-update into fctl. |
| `version` | Host reports its own and the product's version via the host-owned `/_info` probe. |
| `signing generate-key` | Local Ed25519 key generation. **Verified local-only**: `cmd/ledgerctl/signing/generate_key.go` contains no gRPC client, `servicepb`, or connection reference. It is a credential-material concern, so it belongs to the host, not to the deferred server-side signing bucket. |

The placement of `signing generate-key` here rather than in bucket D is what
makes the split 13/7 rather than 12/8, and it is evidence-based.

## B. Operator / storage / cluster concerns (34) — excluded from the business facet

Kept outside the business facet per the programme plan. Any operator facet is a
**separate** grant, not part of this plugin's catalogue.

| Group | Count | Commands |
| --- | --- | --- |
| `cluster` | 8 | `add-learner`, `disk-usage`, `maintenance`, `promote-learner`, `remove-node`, `status`, `transfer-leader`, `watch` |
| `store` | 12 | `backup`, `bootstrap`, `cache-stats`, `check`, `checkpoint`, `dump`, `incremental-backup`, `rebuild-audit-index`, `primary compact`, `primary metrics`, `secondary compact`, `secondary metrics` |
| `query-checkpoint` | 7 | `create`, `delete`, `delete-schedule`, `get-schedule`, `info`, `list`, `set-schedule` |
| `restore` | 4 | `download`, `finalize`, `preview`, `validate` |
| `provision` | 2 | `list`, `run` |
| `ledgers` | 1 | `promote` — promotes a mirror ledger to normal mode (`ledgers/promote.go:18`), a replication-topology action |
| **Total** | **34** | |

`ledgers promote` is the single command that sits in an otherwise-product group
but is operator by behaviour. It is called out so the `ledgers` family reads
13 product commands, not 14.

## C. Signing and event-sink control plane (7) — deferred

Sensitive control-plane decisions, deferred pending an explicit contract.

| Command | Required granular scope |
| --- | --- |
| `signing register-key` | `ledger:OpsWrite` |
| `signing revoke-key` | `ledger:OpsWrite` |
| `signing require` | `ledger:OpsWrite` (`SetSigningConfig`) |
| `signing list-keys` | read (`ListSigningKeys`, streaming) |
| `events add-sink` | `ledger:OpsWrite` |
| `events remove-sink` | `ledger:OpsWrite` |
| `events list` | read (`GetEventsSinks`) |

Scopes read from `internal/adapter/auth/request_scope.go` at the pin.

Two reasons for deferral:

1. **Server-side signing key management is not client request signing.** RFC
   0009 grants exactly `sign.ledger.apply-batch`: the host signs one
   `ApplyBatch` with a target-scoped key. Registering, revoking, or *requiring*
   signatures server-side is a different authority and needs its own contract.
2. **Event sinks take sensitive inputs.** Sink configuration can carry
   credentials and endpoints. Delivering those through a product plugin needs
   the host's sensitive-input and redaction contract first.

`signing require` is the highest-risk command in this bucket: it can make
signatures mandatory server-side and lock out every unsigned client, including
fctl profiles with no activated signer.

## D. Open decisions — recorded, not resolved

These are decisions the programme plan requires **before** freezing the v3
catalogue. None is settled by this preparation.

| # | Open decision |
| --- | --- |
| O1 | Whether a separately granted operator facet exists at all, and if so which of the 34 commands it carries. |
| O2 | Compound configuration-apply failure semantics. `ledgers configuration apply` is executable *and* has subcommands; partial-failure behaviour across a compound apply is unspecified for fctl. |
| O3 | Deterministic ledger selection. ledgerctl prompts interactively when `--ledger` is omitted and several ledgers exist (`accounts/list.go`). fctl must be non-interactive and deterministic; the fallback rule is undecided. |
| O4 | Global vocabulary: `show` vs `get`, `run` vs `execute`, `analyze` vs `analyse`. ledgerctl ships both spellings as aliases (`accounts get` aliases `show`; `queries execute` aliases `run`; `accounts analyze` aliases `analyse`). fctl must choose one canonical form across all plugins. |
| O5 | Whether response-signature verification is ever adopted. The product already supports it (`signature.SignedLog`, server-signed responses verified via the `Discovery` RPC), but RFC 0009 deliberately keeps it out of scope. Adopting it needs a new capability. |

## E. Not the v3 HTTP spec

`openapi/v3.yaml` exists on Ledger `origin/main` and is titled *"a
work-in-progress specification for Ledger API v3"*. It is **not** this plugin's
contract:

- Its operations still carry `v2*` operationIds (`v2GetInfo`, `v2ListAccounts`, …).
- The `Justfile openapi` recipe merges only `v1.yaml`, `v2.yaml` and
  `overlay.yaml`, so `v3.yaml` never reaches the generated `pkg/client`.
- It lives on `origin/main`, a different module and branch from the pinned v3
  source.

Two unrelated artifacts are both called "v3". This plugin's contract is the
gRPC `ledger.BucketService` at `bb0297cc`. Binding it to `openapi/v3.yaml`
would silently target the v2 HTTP server.

## F. Capabilities and behaviours excluded

| Excluded | Reason |
| --- | --- |
| Generic request signing | RFC 0009 grants exactly one capability, for one RPC, at one product major. |
| Response-signature verification | Out of RFC 0009 scope (see O5). |
| Direct server, TLS, raw token, `--insecure`, active-ledger fallback, `--result-file`, consistency flags | ledgerctl root persistent flags. Transport, credentials and output sinks are host-owned; the programme plan bars copying these into generic fctl core. |
| `cmdutil.DrainAllPages` | Unbounded page draining, explicitly barred. |
| Proto bindings in fctl-v2 | No transport RFC is accepted; `proto/` stays a draft. |

## G. Not shared with `ledger-v2`

No catalogue entry, manifest field, command path, or operation identifier is
shared. The v2 plugin's source module is `github.com/formancehq/ledger`
(`origin/main`, HTTP + Speakeasy client); this plugin's is
`github.com/formancehq/ledger/v3` (`release/v3.0`, gRPC). Neither branch
contains the other's contract, so a merged catalogue is not constructible.
