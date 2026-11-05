# Account expansions

The v1 and v2 account list endpoints and the v2 account read endpoint accept
an `expand` query parameter that adds computed volume data to each returned
account. The v1 account read endpoint ignores any client-provided `expand`
value and always requests the `volumes` expansion internally.

## Supported values

Only two values are accepted:

| Value              | Required feature flag and setting                          |
|--------------------|------------------------------------------------------------|
| `volumes`          | `MOVES_HISTORY=ON`                                         |
| `effectiveVolumes` | `MOVES_HISTORY_POST_COMMIT_EFFECTIVE_VOLUMES=SYNC`         |

Any other value is rejected before SQL for that expansion is built.

## Validation behavior

The validation applies to every route that accepts an `expand` value: the v1
and v2 account list endpoints and the v2 account read endpoint. Cursor
validation applies to the list routes:

- An unsupported client-provided `expand` value returns HTTP 400 with the
  `VALIDATION` error code.
- Expansion values carried inside a pagination cursor are validated by the
  same rule as values passed directly in the query string. A cursor whose
  embedded expand value is unsupported also returns HTTP 400 `VALIDATION`.
- When a required feature flag is not set to the expected value, the request
  is rejected the same way, with HTTP 400 and the `VALIDATION` error code.
- The v1 account read endpoint can also return HTTP 400 `VALIDATION` for its
  internal `volumes` expansion — for example when `MOVES_HISTORY` is not set
  to `ON` — even though it does not accept a client-provided `expand` value.

## Implementation notes

- The allow list lives in the storage layer
  (`internal/storage/ledger/resource_accounts.go`, `Expand` handler). Every
  unsupported value fails with `ErrInvalidQuery` before SQL for that
  expansion is built.
  The v1 account read controller hardcodes `Expand: []string{"volumes"}`
  (`internal/api/v1/controllers_accounts_read.go`) and never reads the
  `expand` query parameter.
- The API layer maps `ErrInvalidQuery` to HTTP 400 `VALIDATION` in
  `internal/api/common/errors.go` (`HandleCommonErrors`) and in
  `HandleCommonPaginationErrors`, which covers the v1 and v2 list routes and
  the v2 read route.
