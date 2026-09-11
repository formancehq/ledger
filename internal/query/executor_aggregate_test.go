package query_test

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

type seededVolume struct {
	account, asset, color string
	input, output         uint64
}

func seedVolumes(t *testing.T, store *dal.Store, attrs *attributes.Attributes, ledger string, entries ...seededVolume) {
	t.Helper()

	batch := store.OpenWriteSession()
	for _, e := range entries {
		_, err := attrs.Volume.Set(batch, domain.NewVolumeKey(ledger, e.account, e.asset, e.color).Bytes(), &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(e.input),
			Output: commonpb.NewUint256FromUint64(e.output),
		})
		require.NoError(t, err)
	}
	require.NoError(t, batch.Commit())
}

// An exactly unfiltered prepared aggregation must produce the same result as
// the direct, ledger-wide volume scan, because the prepared path selects the
// shared fast path (one volume iterator) rather than enumerating accounts and
// scanning per-account volume iterators.
func TestExecute_NilFilterAggregateMatchesDirectLedgerWideScan(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		seed         []seededVolume
		metadataOnly bool
	}{
		{name: "empty ledger"},
		{name: "metadata-only accounts", metadataOnly: true},
		{
			name: "zero volumes",
			seed: []seededVolume{
				{account: "a", asset: "USD/2", input: 0, output: 0},
				{account: "b", asset: "EUR/4", input: 0, output: 0},
			},
		},
		{
			name: "multiple assets precisions and colors",
			seed: []seededVolume{
				{account: "world", asset: "USD/2", input: 100, output: 0},
				{account: "bank", asset: "USD/2", input: 100, output: 50},
				{account: "bank", asset: "USD/4", input: 5, output: 0},
				{account: "user", asset: "USD/2", color: "RED", input: 30, output: 0},
				{account: "user", asset: "EUR/3", input: 0, output: 7},
				{account: "z", asset: "GOLD", color: "BLUE", input: 1, output: 2},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := newTestStore(t)
			registerLedger(t, store, "l")

			rs := newTestReadStore(t)

			attrs := attributes.New()
			seedVolumes(t, store, attrs, "l", tc.seed...)
			if tc.metadataOnly {
				batch := store.OpenWriteSession()
				for _, account := range []string{"a", "b"} {
					key := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "l", Account: account}, Key: "label"}
					_, err := attrs.Metadata.Set(batch, key.Bytes(), commonpb.NewStringValue("metadata only"))
					require.NoError(t, err)
				}
				require.NoError(t, batch.Commit())
			}
			seedPreparedQuery(t, store, attrs, "l", "q", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, nil)

			req := &servicepb.ExecutePreparedQueryRequest{
				Ledger:    "l",
				QueryName: "q",
				Mode:      commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES,
			}

			profile := &query.QueryProfile{}
			resp, err := query.Execute(context.Background(), rs, store, attrs.Volume, attrs.PreparedQuery, attrs.Index, req, profile, nil)
			require.NoError(t, err)

			got := resp.GetAggregate()
			require.NotNil(t, got)
			if tc.metadataOnly {
				require.Empty(t, got.GetVolumes())
				require.Empty(t, got.GetGroups())
			}

			// The direct unfiltered aggregation path reads the same ledger-wide
			// volume stream through the same handle shape.
			handle, releaseHold, err := query.OpenQueryHandle(rs, store, nil, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
			require.NoError(t, err)
			defer releaseHold()
			defer func() { _ = handle.Close() }()

			want, err := query.AggregateAllVolumes(handle, attrs.Volume, "l", query.AggregateOptions{})
			require.NoError(t, err)

			require.True(t, proto.Equal(want, got),
				"prepared nil-filter aggregate must match the direct ledger-wide scan\nwant: %v\ngot:  %v",
				want, got)

			require.Nil(t, profile.Root,
				"an unfiltered aggregate must not compile an account-universe iterator")
		})
	}
}

// A parameterized (non-nil) filter is NOT the unfiltered fast path: it must
// still compile through the account-iterator strategy and aggregate only the
// matched account. No empty or parameterized filter is treated as nil.
func TestExecute_ParameterizedFilterAggregateStillUsesAccountIterator(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	registerLedger(t, store, "l")

	rs := newTestReadStore(t)

	attrs := attributes.New()
	seedVolumes(t, store, attrs, "l",
		seededVolume{account: "users:alice", asset: "USD/2", input: 10, output: 0},
		seededVolume{account: "merchants:shop", asset: "USD/2", input: 999, output: 0},
	)

	filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{
		Address: &commonpb.AddressMatch{
			Match: &commonpb.AddressMatch_ParamExact{ParamExact: "account"},
		},
	}}
	seedPreparedQuery(t, store, attrs, "l", "q", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, filter)

	req := &servicepb.ExecutePreparedQueryRequest{
		Ledger:    "l",
		QueryName: "q",
		Mode:      commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES,
		Parameters: map[string]*commonpb.ParameterValue{
			"account": {Value: &commonpb.ParameterValue_StringValue{StringValue: "users:alice"}},
		},
	}

	profile := &query.QueryProfile{}
	resp, err := query.Execute(context.Background(), rs, store, attrs.Volume, attrs.PreparedQuery, attrs.Index, req, profile, nil)
	require.NoError(t, err)

	require.NotNil(t, profile.Root,
		"a non-nil (parameterized) filter must still compile through the account iterator")

	got := resp.GetAggregate()
	require.NotNil(t, got)
	require.Len(t, got.GetVolumes(), 1)
	require.Equal(t, "USD/2", got.GetVolumes()[0].GetAsset())

	var input uint256.Int
	got.GetVolumes()[0].GetInput().IntoUint256(&input)
	require.Equal(t, uint256.NewInt(10), &input,
		"only the matched account's volumes may be aggregated")
}

// Overflow must propagate through Execute without returning a partial aggregate.
func TestExecute_NilFilterAggregateOverflow(t *testing.T) {
	t.Parallel()

	for _, side := range []string{"input", "output"} {
		t.Run(side, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t)
			registerLedger(t, store, "l")
			rs := newTestReadStore(t)
			attrs := attributes.New()
			batch := store.OpenWriteSession()
			for i, amount := range []*uint256.Int{new(uint256.Int).SetAllOne(), uint256.NewInt(1)} {
				pair := &raftcmdpb.VolumePair{}
				if side == "input" {
					pair.Input = commonpb.NewUint256(amount)
				} else {
					pair.Output = commonpb.NewUint256(amount)
				}
				_, err := attrs.Volume.Set(batch, domain.NewVolumeKey("l", []string{"a", "b"}[i], "USD/2", "").Bytes(), pair)
				require.NoError(t, err)
			}
			require.NoError(t, batch.Commit())
			seedPreparedQuery(t, store, attrs, "l", "q", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, nil)

			profile := &query.QueryProfile{}
			resp, err := query.Execute(context.Background(), rs, store, attrs.Volume, attrs.PreparedQuery, attrs.Index,
				&servicepb.ExecutePreparedQueryRequest{Ledger: "l", QueryName: "q", Mode: commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES}, profile, nil)
			require.Nil(t, resp)
			var overflow *query.ErrAggregateOverflow
			require.ErrorAs(t, err, &overflow)
			require.Equal(t, "accumulate", overflow.Stage)
			require.Equal(t, side, overflow.Side)
			require.Nil(t, profile.Root, "overflow must originate from the unfiltered fast path")

			handle, err := store.NewReadHandle()
			require.NoError(t, err)
			defer func() { _ = handle.Close() }()
			direct, err := query.AggregateAllVolumes(handle, attrs.Volume, "l", query.AggregateOptions{})
			require.Nil(t, direct)
			var directOverflow *query.ErrAggregateOverflow
			require.ErrorAs(t, err, &directOverflow)
			require.Equal(t, overflow, directOverflow)
		})
	}
}

// The fast path must use the definition, ledger and volumes from the reserved
// snapshot even if a writer changes the live store immediately after it opens.
func TestExecute_NilFilterAggregateUsesPinnedSnapshot(t *testing.T) {
	t.Parallel()

	for _, mutation := range []string{"query deleted", "target changed", "ledger deleted"} {
		t.Run(mutation, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t)
			registerLedger(t, store, "l")
			rs := newTestReadStore(t)
			attrs := attributes.New()
			seedPreparedQuery(t, store, attrs, "l", "q", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, nil)
			seedVolumes(t, store, attrs, "l", seededVolume{account: "a", asset: "USD/2", input: 10})

			opener := &mutatingQueryHandleStore{store: store, afterOpen: func() {
				// The reservation must already protect history before this callback.
				require.Zero(t, rs.Leases().BeginGC(100))
				switch mutation {
				case "query deleted":
					batch := store.OpenWriteSession()
					require.NoError(t, attrs.PreparedQuery.Delete(batch, domain.PreparedQueryKey{LedgerName: "l", Name: "q"}.Bytes()))
					require.NoError(t, batch.Commit())
				case "target changed":
					seedPreparedQuery(t, store, attrs, "l", "q", commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, nil)
				case "ledger deleted":
					batch := store.OpenWriteSession()
					require.NoError(t, state.SaveLedger(batch, "l", &commonpb.LedgerInfo{Name: "l", DeletedAt: &commonpb.Timestamp{}}))
					require.NoError(t, batch.Commit())
				}
				seedVolumes(t, store, attrs, "l", seededVolume{account: "a", asset: "USD/2", input: 999})
			}}
			profile := &query.QueryProfile{}
			resp, err := query.Execute(t.Context(), rs, opener, attrs.Volume, attrs.PreparedQuery, attrs.Index,
				&servicepb.ExecutePreparedQueryRequest{Ledger: "l", QueryName: "q", Mode: commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES}, profile, nil)
			require.NoError(t, err)
			require.Len(t, resp.GetAggregate().GetVolumes(), 1)
			volume := resp.GetAggregate().GetVolumes()[0]
			require.Equal(t, "USD/2", volume.GetAsset())
			require.True(t, proto.Equal(commonpb.NewUint256FromUint64(10), volume.GetInput()))
			require.Nil(t, profile.Root)
			require.Equal(t, uint64(100), rs.Leases().BeginGC(100), "the reservation must be released after the fast path")
		})
	}
}

func TestExecute_NilFilterAggregateValidatesPinnedTarget(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	registerLedger(t, store, "l")
	rs := newTestReadStore(t)
	attrs := attributes.New()
	seedPreparedQuery(t, store, attrs, "l", "q", commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, nil)
	opener := &mutatingQueryHandleStore{store: store, afterOpen: func() {
		seedPreparedQuery(t, store, attrs, "l", "q", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, nil)
	}}
	resp, err := query.Execute(t.Context(), rs, opener, attrs.Volume, attrs.PreparedQuery, attrs.Index,
		&servicepb.ExecutePreparedQueryRequest{Ledger: "l", QueryName: "q", Mode: commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES}, nil, nil)
	require.Nil(t, resp)
	require.EqualError(t, err, "AGGREGATE_VOLUMES mode is only valid for ACCOUNTS target queries")
	require.Equal(t, uint64(100), rs.Leases().BeginGC(100), "validation failure must release the reservation")
}
