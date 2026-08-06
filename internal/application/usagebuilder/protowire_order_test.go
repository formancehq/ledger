package usagebuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestParseUsageCreateOrder(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		order *raftcmdpb.CreateTransactionOrder
		want  usageCreateOrder
	}{
		{
			name: "postings and metadata are skipped",
			order: &raftcmdpb.CreateTransactionOrder{
				Postings: []*commonpb.Posting{{Source: "world", Destination: "bank", Asset: "USD"}},
				Metadata: map[string]*commonpb.MetadataValue{
					"large": commonpb.NewStringValue("unused by usage projection"),
				},
				Force: true,
			},
			want: usageCreateOrder{ledger: "ledger"},
		},
		{
			name: "plain script and reference",
			order: &raftcmdpb.CreateTransactionOrder{
				Script:    &commonpb.Script{Plain: "send [USD 1] (source = @world destination = @bank)"},
				Reference: "ref",
			},
			want: usageCreateOrder{ledger: "ledger", hasReference: true, isScripted: true},
		},
		{
			name: "stored numscript and client timestamp",
			order: &raftcmdpb.CreateTransactionOrder{
				Timestamp: &commonpb.Timestamp{Data: 42},
				NumscriptReference: &raftcmdpb.NumscriptReference{
					Name:    "payout",
					Version: "1.2.3",
					Vars:    map[string]string{"destination": "users:1"},
				},
			},
			want: usageCreateOrder{
				ledger:       "ledger",
				isScripted:   true,
				template:     "payout",
				usesTemplate: true,
				timestamp:    &commonpb.Timestamp{Data: 42},
			},
		},
		{
			name: "present empty numscript and timestamp retain presence semantics",
			order: &raftcmdpb.CreateTransactionOrder{
				Timestamp:          &commonpb.Timestamp{},
				NumscriptReference: &raftcmdpb.NumscriptReference{},
			},
			want: usageCreateOrder{
				ledger:       "ledger",
				isScripted:   true,
				usesTemplate: true,
				timestamp:    &commonpb.Timestamp{},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			raw, err := createTxOrder("ledger", tc.order).MarshalVT()
			require.NoError(t, err)

			got, matched, err := parseUsageCreateOrder(raw)
			require.NoError(t, err)
			require.True(t, matched)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseUsageCreateOrderFallsBackForOtherOrderKinds(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		order *raftcmdpb.Order
	}{
		{
			name: "revert",
			order: revertTxOrder("ledger", &raftcmdpb.RevertTransactionOrder{
				TransactionId: 12,
			}),
		},
		{
			name: "delete ledger",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger:  "ledger",
					Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedger{DeleteLedger: &raftcmdpb.DeleteLedgerOrder{}},
				},
			}},
		},
		{
			name: "system scoped",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{
					Payload: &raftcmdpb.SystemScopedOrder_CloseChapter{CloseChapter: &raftcmdpb.CloseChapterOrder{}},
				},
			}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			raw, err := tc.order.MarshalVT()
			require.NoError(t, err)

			_, matched, err := parseUsageCreateOrder(raw)
			require.NoError(t, err)
			assert.False(t, matched)
		})
	}
}

func TestParseUsageCreateOrderHonorsFinalOneofMember(t *testing.T) {
	t.Parallel()

	raw, err := createTxOrder("ledger", &raftcmdpb.CreateTransactionOrder{}).MarshalVT()
	require.NoError(t, err)

	// Append a system-scoped member after the ledger-scoped one. Protobuf oneof
	// decoding keeps the final member, so the fast path must not treat this as a
	// native create transaction.
	raw = protowire.AppendTag(raw, 3, protowire.BytesType)
	raw = protowire.AppendBytes(raw, nil)

	_, matched, err := parseUsageCreateOrder(raw)
	require.NoError(t, err)
	assert.False(t, matched)
}

func TestParseUsageCreateOrderAcceptsUnknownFields(t *testing.T) {
	t.Parallel()

	raw, err := createTxOrder("ledger", &raftcmdpb.CreateTransactionOrder{Reference: "ref"}).MarshalVT()
	require.NoError(t, err)
	raw = protowire.AppendTag(raw, 99, protowire.VarintType)
	raw = protowire.AppendVarint(raw, 123)

	got, matched, err := parseUsageCreateOrder(raw)
	require.NoError(t, err)
	require.True(t, matched)
	assert.Equal(t, usageCreateOrder{ledger: "ledger", hasReference: true}, got)
}

func TestUsageCreateTransactionFallsBackForDuplicateMessages(t *testing.T) {
	t.Parallel()

	timestamp, err := (&commonpb.Timestamp{Data: 42}).MarshalVT()
	require.NoError(t, err)
	raw := appendUsageBytesField(nil, 3, timestamp)
	raw = appendUsageBytesField(raw, 3, timestamp)

	_, matched, err := usageCreateTransaction(raw)
	require.NoError(t, err)
	assert.False(t, matched, "the generated decoder must retain authority over singular-message merge semantics")
}

func TestParseUsageCreateOrderRejectsMalformedWire(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		raw  []byte
	}{
		{name: "truncated ledger scoped order", raw: []byte{0x12, 0x80}},
		{name: "wrong technical wire type", raw: appendUsageVarintField(nil, 1, 1)},
		{name: "wrong posting wire type", raw: wrapUsageCreateTransaction(appendUsageVarintField(nil, 1, 1))},
		{name: "wrong force wire type", raw: wrapUsageCreateTransaction(appendUsageBytesField(nil, 7, nil))},
		{name: "wrong script vars wire type", raw: wrapUsageCreateTransaction(
			appendUsageBytesField(nil, 2, appendUsageVarintField(nil, 2, 1)),
		)},
		{name: "wrong numscript vars wire type", raw: wrapUsageCreateTransaction(
			appendUsageBytesField(nil, 8, appendUsageVarintField(nil, 3, 1)),
		)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, matched, err := parseUsageCreateOrder(tc.raw)
			require.Error(t, err)
			assert.False(t, matched)
		})
	}
}

func wrapUsageCreateTransaction(create []byte) []byte {
	apply := appendUsageBytesField(nil, 1, create)
	scoped := appendUsageBytesField(nil, 1, []byte("ledger"))
	scoped = appendUsageBytesField(scoped, 2, apply)

	return appendUsageBytesField(nil, 2, scoped)
}

func appendUsageVarintField(dst []byte, num protowire.Number, value uint64) []byte {
	dst = protowire.AppendTag(dst, num, protowire.VarintType)

	return protowire.AppendVarint(dst, value)
}
