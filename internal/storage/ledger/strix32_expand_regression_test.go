//go:build it

package ledger_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"testing"
	libtime "time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/query"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/formancehq/ledger/pkg/features"
)

// STRIX-32 / 29a87ddc-2677-41dd-b904-db140254e3de — corrected regression contract.
//
// Broken invariant: client-controlled "expand" input (query parameter on the
// v1/v2 account list + read routes, and the unsigned base64 cursor payload)
// must never execute SQL expressions nor disclose another ledger's data.
//
// accountsResourceHandler.Expand interpolates the expand property into a raw
// SQL alias via strcase.SnakeCase(property) with no allow-list. strcase
// preserves every byte except '-', '_' and spaces, so a payload written
// without those characters reaches the "as <alias>" position of the expand
// CTE verbatim. On the vulnerable base the payload below executes: it reads
// the accounts table of ANOTHER ledger in the shared bucket and aliases the
// result as "volumes", so the foreign row is scanned into the response.
//
// Every security test in this file asserts the SAFE invariant, so each one is
// RED on the vulnerable base for the security reason and can only be GREEN
// once unsupported expand values are rejected with common.ErrInvalidQuery:
//   - unsupported/injection expand values return ErrInvalidQuery (no SQL run);
//   - the foreign-ledger sentinel is never disclosed in Volumes;
//   - the pg_sleep payload is never executed (query returns fast).
// Legitimate expand values and feature gates are covered separately and must
// stay green on both base and fixed code.

const (
	strix32ForeignLedger = "strx32_foreign_ledger"
	strix32SentinelAddr  = "strx32:foreign:sentinel"
	strix32SentinelMeta  = "STRIX32_CROSS_LEDGER_SENTINEL"
	strix32VictimAddr    = "users:alice"
)

// strix32CrossLedgerPayload builds an expand value whose strcase.SnakeCase
// output is byte-identical to the input (no spaces, '-', '_' or upper-case
// letters), so the expression reaches PostgreSQL verbatim on the vulnerable
// base. It appends a second column to the expand CTE: a scalar sub-select
// reading the accounts table of ANOTHER ledger in the shared "_default"
// bucket, aliased as "volumes" so it would be scanned into the response.
func strix32CrossLedgerPayload() string {
	return "x,/**/(select/**/jsonb_build_object(a.address,/**/jsonb_build_object('input',/**/42,/**/'output',/**/0))/**/from/**/\"_default\".accounts/**/as/**/a/**/where/**/a.ledger='" +
		strix32ForeignLedger + "'/**/limit/**/1)as/**/volumes"
}

// strix32SleepPayload smuggles a pg_sleep(2) into the SQL alias position.
func strix32SleepPayload() string {
	return "x,/**/(select/**/pg_sleep(2))as/**/y"
}

// requireExpandRejected asserts the SAFE contract: an unsupported expand value
// must be rejected with the repository's invalid-query error before any SQL
// built from it is executed.
func requireExpandRejected(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err, "unsupported expand value must be rejected, not executed")
	require.ErrorAs(t, err, &common.ErrInvalidQuery{}, "unsupported expand value must return ErrInvalidQuery, got %v", err)
}

func strix32Setup(t *testing.T, store *ledgerstore.Store) context.Context {
	t.Helper()
	ctx := logging.TestingContext()

	now := time.Now()
	tx := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", strix32VictimAddr, "USD", big.NewInt(100))).
		WithTimestamp(now).
		WithInsertedAt(now)
	require.NoError(t, commitTransactionAndUpsertAccounts(ctx, store, pointer.For(tx)))

	return ctx
}

// strix32PlantForeignLedger inserts, with raw SQL, a sentinel account row that
// belongs to a DIFFERENT ledger inside the same shared "_default" bucket. A
// correct implementation scopes every read to the requesting ledger and must
// never return this row through the victim ledger's API.
func strix32PlantForeignLedger(t *testing.T, store *ledgerstore.Store, ctx context.Context) {
	t.Helper()
	_, err := store.GetDB().ExecContext(ctx, `
		insert into "_default".accounts (ledger, address, address_array, insertion_date, updated_at, metadata)
		values (?, ?, ?::jsonb, now(), now(), ?::jsonb)
		on conflict (ledger, address) do nothing
	`, strix32ForeignLedger, strix32SentinelAddr, `["strx32:foreign:sentinel"]`,
		`{"strx32":"`+strix32SentinelMeta+`"}`)
	require.NoError(t, err)
}

// requireNoForeignSentinel asserts the SAFE contract: the response must never
// contain the sentinel planted in the foreign ledger.
func requireNoForeignSentinel(t *testing.T, volumes ledger.VolumesByAssets) {
	t.Helper()
	_, leaked := volumes[strix32SentinelAddr]
	require.False(t, leaked, "cross-ledger sentinel account %q must NOT be disclosed, got volumes=%v", strix32SentinelAddr, volumes)
}

// TestSTRIX32GetOneExpandInjection: individual account read (v2
// GET /{ledger}/accounts/{address} path, Expand from the query string).
// SAFE contract: the injection expand value is rejected with ErrInvalidQuery
// and no foreign data is disclosed. RED on the vulnerable base because the
// injected sub-select executes and leaks the foreign-ledger sentinel.
func TestSTRIX32GetOneExpandInjection(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	ctx := strix32Setup(t, store)
	strix32PlantForeignLedger(t, store, ctx)

	acc, err := store.Accounts().GetOne(ctx, common.ResourceQuery[any]{
		Builder: query.Match("address", strix32VictimAddr),
		Expand:  []string{strix32CrossLedgerPayload()},
	})
	requireExpandRejected(t, err)
	if acc != nil {
		requireNoForeignSentinel(t, acc.Volumes)
	}
}

// TestSTRIX32ListExpandInjection: v2 list path (GET /{ledger}/accounts?expand=...).
// SAFE contract: same rejection + no disclosure. RED on the vulnerable base.
func TestSTRIX32ListExpandInjection(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	ctx := strix32Setup(t, store)
	strix32PlantForeignLedger(t, store, ctx)

	cursor, err := store.Accounts().Paginate(ctx, common.InitialPaginatedQuery[any]{
		Column:   "address",
		Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
		PageSize: 15,
		Options: common.ResourceQuery[any]{
			Expand: []string{strix32CrossLedgerPayload()},
		},
	})
	requireExpandRejected(t, err)
	if cursor != nil {
		for _, acc := range cursor.Data {
			requireNoForeignSentinel(t, acc.Volumes)
		}
	}
}

// TestSTRIX32TamperedCursorExpandInjection: the list route also accepts a
// base64 cursor whose embedded "filters.expand" is fully attacker-controlled
// (storagecommon.UnmarshalCursor decodes it without any signature). A server
// minted cursor can be decoded, modified and re-encoded. SAFE contract:
// same rejection + no disclosure. RED on the vulnerable base.
func TestSTRIX32TamperedCursorExpandInjection(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	ctx := strix32Setup(t, store)
	strix32PlantForeignLedger(t, store, ctx)

	raw, err := json.Marshal(common.OffsetPaginatedQuery[any]{
		InitialPaginatedQuery: common.InitialPaginatedQuery[any]{
			Column:   "address",
			Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
			PageSize: 15,
			Options: common.ResourceQuery[any]{
				Expand: []string{strix32CrossLedgerPayload()},
			},
		},
	})
	require.NoError(t, err)
	tamperedCursor := base64.RawURLEncoding.EncodeToString(raw)

	q, err := common.UnmarshalCursor[any](tamperedCursor)
	require.NoError(t, err, "tampered cursor must still decode (the trust boundary is the expand validation, not the cursor)")

	cursor, err := store.Accounts().Paginate(ctx, q)
	requireExpandRejected(t, err)
	if cursor != nil {
		for _, acc := range cursor.Data {
			requireNoForeignSentinel(t, acc.Volumes)
		}
	}
}

// TestSTRIX32V1RouteExpandInjection: the v1 list route reads expand from the
// query string / cursor identically (v1 getResourceQuery uses
// r.URL.Query()["expand"]). SAFE contract: same rejection + no disclosure.
// RED on the vulnerable base.
func TestSTRIX32V1RouteExpandInjection(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	ctx := strix32Setup(t, store)
	strix32PlantForeignLedger(t, store, ctx)

	cursor, err := store.Accounts().Paginate(ctx, common.InitialPaginatedQuery[any]{
		Column:   "address",
		Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
		PageSize: 15,
		Options: common.ResourceQuery[any]{
			Expand: []string{strix32CrossLedgerPayload()},
		},
	})
	requireExpandRejected(t, err)
	if cursor != nil {
		for _, acc := range cursor.Data {
			requireNoForeignSentinel(t, acc.Volumes)
		}
	}
}

// TestSTRIX32SleepPayloadNotExecuted: proves the injected expression is never
// EXECUTED on a secure implementation. SAFE contract: the pg_sleep(2) payload
// is rejected with ErrInvalidQuery and the query returns quickly. RED on the
// vulnerable base because pg_sleep(2) really runs and delays the query by
// >= 2s. The margin is 1.5s: a secure implementation answers in well under
// 500ms, so the bound cannot flake on a fast machine, while the vulnerable
// base is delayed by a full 2s.
func TestSTRIX32SleepPayloadNotExecuted(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	ctx := strix32Setup(t, store)

	start := libtime.Now()
	_, err := store.Accounts().GetOne(ctx, common.ResourceQuery[any]{
		Builder: query.Match("address", strix32VictimAddr),
		Expand:  []string{strix32SleepPayload()},
	})
	elapsed := libtime.Since(start)

	requireExpandRejected(t, err)
	require.Less(t, elapsed, 1500*libtime.Millisecond,
		"injected pg_sleep(2) must not execute: query took %s (rejected expands must not run any SQL)", elapsed)
}

// TestSTRIX32LegitimateExpandPreserved: the legitimate expand values must keep
// working (volumes / effectiveVolumes with their feature gates). This test is
// GREEN on both the vulnerable base and a correct fix.
func TestSTRIX32LegitimateExpandPreserved(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	ctx := strix32Setup(t, store)

	for _, expand := range []string{"volumes", "effectiveVolumes"} {
		acc, err := store.Accounts().GetOne(ctx, common.ResourceQuery[any]{
			Builder: query.Match("address", strix32VictimAddr),
			Expand:  []string{expand},
		})
		require.NoError(t, err, "legitimate expand %q must keep working", expand)
		require.NotNil(t, acc)
		require.Equal(t, strix32VictimAddr, acc.Address)
		requireNoForeignSentinel(t, acc.Volumes)
	}
}

// TestSTRIX32ExpandWithFeatureOff: with MOVES_HISTORY=OFF the "volumes"
// expand must be rejected with ErrInvalidQuery, proving the feature gate
// stays in front of the sink. GREEN on both base and fix.
func TestSTRIX32ExpandWithFeatureOff(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t, func(cfg *ledger.Configuration) {
		cfg.Features = features.DefaultFeatures.With(features.FeatureMovesHistory, "OFF")
	})
	ctx := strix32Setup(t, store)

	_, err := store.Accounts().GetOne(ctx, common.ResourceQuery[any]{
		Builder: query.Match("address", strix32VictimAddr),
		Expand:  []string{"volumes"},
	})
	requireExpandRejected(t, err)
}
