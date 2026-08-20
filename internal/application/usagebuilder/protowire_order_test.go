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
	raw = protowire.AppendTag(raw, 2, protowire.BytesType)
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
		{name: "truncated ledger scoped order", raw: []byte{0x0a, 0x80}},
		{name: "wrong technical wire type", raw: appendUsageVarintField(nil, 3, 1)},
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

func TestUsageOrderWireScannersRejectMalformedFields(t *testing.T) {
	t.Parallel()

	invalidTag := []byte{0}
	truncatedBytes := func(num protowire.Number) []byte {
		return append(protowire.AppendTag(nil, num, protowire.BytesType), 0x80)
	}
	truncatedVarint := func(num protowire.Number) []byte {
		return protowire.AppendTag(nil, num, protowire.VarintType)
	}

	testCases := []struct {
		name string
		raw  []byte
		scan func([]byte) error
	}{
		{name: "order invalid tag", raw: invalidTag, scan: scanUsageOrderLedgerScoped},
		{name: "order ledger wrong type", raw: appendUsageVarintField(nil, 1, 1), scan: scanUsageOrderLedgerScoped},
		{name: "order ledger truncated", raw: truncatedBytes(1), scan: scanUsageOrderLedgerScoped},
		{name: "order technical truncated", raw: truncatedBytes(3), scan: scanUsageOrderLedgerScoped},
		{name: "order unknown truncated", raw: truncatedBytes(99), scan: scanUsageOrderLedgerScoped},
		{name: "ledger scoped invalid tag", raw: invalidTag, scan: scanUsageLedgerApply},
		{name: "ledger name wrong type", raw: appendUsageVarintField(nil, 1, 1), scan: scanUsageLedgerApply},
		{name: "ledger name truncated", raw: truncatedBytes(1), scan: scanUsageLedgerApply},
		{name: "ledger payload wrong type", raw: appendUsageVarintField(nil, 2, 1), scan: scanUsageLedgerApply},
		{name: "ledger payload truncated", raw: truncatedBytes(2), scan: scanUsageLedgerApply},
		{name: "ledger unknown truncated", raw: truncatedBytes(99), scan: scanUsageLedgerApply},
		{name: "apply invalid tag", raw: invalidTag, scan: scanUsageLedgerCreateTransaction},
		{name: "apply payload wrong type", raw: appendUsageVarintField(nil, 1, 1), scan: scanUsageLedgerCreateTransaction},
		{name: "apply payload truncated", raw: truncatedBytes(1), scan: scanUsageLedgerCreateTransaction},
		{name: "apply unknown truncated", raw: truncatedBytes(99), scan: scanUsageLedgerCreateTransaction},
		{name: "create invalid tag", raw: invalidTag, scan: scanUsageCreateTransaction},
		{name: "create script wrong type", raw: appendUsageVarintField(nil, 2, 1), scan: scanUsageCreateTransaction},
		{name: "create script truncated", raw: truncatedBytes(2), scan: scanUsageCreateTransaction},
		{name: "create postings wrong type", raw: appendUsageVarintField(nil, 1, 1), scan: scanUsageCreateTransaction},
		{name: "create postings truncated", raw: truncatedBytes(1), scan: scanUsageCreateTransaction},
		{name: "create force truncated", raw: truncatedVarint(7), scan: scanUsageCreateTransaction},
		{name: "create unknown truncated", raw: truncatedBytes(99), scan: scanUsageCreateTransaction},
		{name: "script invalid tag", raw: invalidTag, scan: scanUsageScriptPlain},
		{name: "script plain wrong type", raw: appendUsageVarintField(nil, 1, 1), scan: scanUsageScriptPlain},
		{name: "script plain truncated", raw: truncatedBytes(1), scan: scanUsageScriptPlain},
		{name: "script vars wrong type", raw: appendUsageVarintField(nil, 2, 1), scan: scanUsageScriptPlain},
		{name: "script vars truncated", raw: truncatedBytes(2), scan: scanUsageScriptPlain},
		{name: "script unknown truncated", raw: truncatedBytes(99), scan: scanUsageScriptPlain},
		{name: "numscript invalid tag", raw: invalidTag, scan: scanUsageNumscriptName},
		{name: "numscript name wrong type", raw: appendUsageVarintField(nil, 1, 1), scan: scanUsageNumscriptName},
		{name: "numscript name truncated", raw: truncatedBytes(1), scan: scanUsageNumscriptName},
		{name: "numscript vars wrong type", raw: appendUsageVarintField(nil, 2, 1), scan: scanUsageNumscriptName},
		{name: "numscript vars truncated", raw: truncatedBytes(2), scan: scanUsageNumscriptName},
		{name: "numscript unknown truncated", raw: truncatedBytes(99), scan: scanUsageNumscriptName},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tc.scan(tc.raw))
		})
	}
}

func scanUsageOrderLedgerScoped(raw []byte) error {
	_, _, err := usageOrderLedgerScoped(raw)

	return err
}

func scanUsageLedgerApply(raw []byte) error {
	_, _, _, err := usageLedgerApply(raw)

	return err
}

func scanUsageLedgerCreateTransaction(raw []byte) error {
	_, _, err := usageLedgerCreateTransaction(raw)

	return err
}

func scanUsageCreateTransaction(raw []byte) error {
	_, _, err := usageCreateTransaction(raw)

	return err
}

func scanUsageScriptPlain(raw []byte) error {
	_, err := usageScriptPlain(raw)

	return err
}

func scanUsageNumscriptName(raw []byte) error {
	_, err := usageNumscriptName(raw)

	return err
}

func wrapUsageCreateTransaction(create []byte) []byte {
	apply := appendUsageBytesField(nil, 1, create)
	scoped := appendUsageBytesField(nil, 1, []byte("ledger"))
	scoped = appendUsageBytesField(scoped, 2, apply)

	return appendUsageBytesField(nil, 1, scoped)
}

func appendUsageVarintField(dst []byte, num protowire.Number, value uint64) []byte {
	dst = protowire.AppendTag(dst, num, protowire.VarintType)

	return protowire.AppendVarint(dst, value)
}
