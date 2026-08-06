package usagebuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestParseUsageLogMatchesGeneratedDecoder(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		log  *commonpb.Log
	}{
		{
			name: "created transaction",
			log: usageApplyLog(&commonpb.LedgerLog{
				Data: &commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
						CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{
							Postings: []*commonpb.Posting{
								{Source: "world", Destination: "bank", Asset: "USD"},
								{Source: "bank", Destination: "users:1", Asset: "USD"},
							},
							Metadata: map[string]*commonpb.MetadataValue{
								"ignored": commonpb.NewStringValue("large value ignored by usage"),
							},
							Timestamp: &commonpb.Timestamp{Data: 42},
						}},
					},
				},
				PurgedVolumes: []*commonpb.TouchedVolume{
					{Account: "drained", Asset: "USD", Color: "RED"},
				},
				NewKeptVolumes: []*commonpb.TouchedVolume{
					{Account: "bank", Asset: "USD"},
					{Account: "users:1", Asset: "USD", Color: "BLUE"},
				},
				EphemeralVolumes: []*commonpb.TouchedVolume{
					{Account: "temporary", Asset: "EUR"},
				},
			}),
		},
		{
			name: "reverted transaction",
			log: usageApplyLog(&commonpb.LedgerLog{
				Data: &commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
						RevertedTransaction: &commonpb.RevertedTransaction{
							RevertedTransactionId: 12,
							RevertTransaction: &commonpb.Transaction{
								Postings:  []*commonpb.Posting{{Source: "users:1", Destination: "world", Asset: "USD"}},
								Timestamp: &commonpb.Timestamp{Data: 88},
							},
						},
					},
				},
				PurgedVolumes: []*commonpb.TouchedVolume{{Account: "users:1", Asset: "USD"}},
			}),
		},
		{
			name: "skipped order keeps volume annotations but is not a transaction",
			log: usageApplyLog(&commonpb.LedgerLog{
				Data: &commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_OrderSkipped{
						OrderSkipped: &commonpb.OrderSkippedLog{},
					},
				},
				EphemeralVolumes: []*commonpb.TouchedVolume{{Account: "temporary", Asset: "USD"}},
			}),
		},
		{
			name: "non apply log",
			log: &commonpb.Log{Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{}},
			}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			raw, err := tc.log.MarshalVT()
			require.NoError(t, err)

			decoded := &commonpb.Log{}
			require.NoError(t, decoded.UnmarshalVT(raw))

			var got logVolumeAnnotations
			require.NoError(t, parseUsageLog(raw, &got))
			assert.Equal(t, expectedUsageLog(decoded), got)
		})
	}
}

func TestParseUsageLogResetsScratch(t *testing.T) {
	t.Parallel()

	populated := usageApplyLog(&commonpb.LedgerLog{
		Data: &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
				CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{
					Postings:  []*commonpb.Posting{{}},
					Timestamp: &commonpb.Timestamp{Data: 42},
				}},
			},
		},
		PurgedVolumes:    []*commonpb.TouchedVolume{{Account: "a", Asset: "USD"}},
		NewKeptVolumes:   []*commonpb.TouchedVolume{{Account: "b", Asset: "USD"}},
		EphemeralVolumes: []*commonpb.TouchedVolume{{Account: "c", Asset: "USD"}},
	})
	rawPopulated, err := populated.MarshalVT()
	require.NoError(t, err)

	empty := &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{}},
	}}
	rawEmpty, err := empty.MarshalVT()
	require.NoError(t, err)

	var scratch logVolumeAnnotations
	require.NoError(t, parseUsageLog(rawPopulated, &scratch))
	require.NoError(t, parseUsageLog(rawEmpty, &scratch))

	assert.Zero(t, scratch.postings)
	assert.Empty(t, scratch.purged)
	assert.Empty(t, scratch.newKept)
	assert.Empty(t, scratch.ephemeral)
	assert.Nil(t, scratch.txTimestamp)
	assert.False(t, scratch.isCreatedTx)
	assert.False(t, scratch.isRevertedTx)
}

func TestParseUsageLogAcceptsUnknownFields(t *testing.T) {
	t.Parallel()

	log := usageApplyLog(&commonpb.LedgerLog{
		Data: &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
				CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{}},
			},
		},
	})
	raw, err := log.MarshalVT()
	require.NoError(t, err)
	raw = protowire.AppendTag(raw, 99, protowire.VarintType)
	raw = protowire.AppendVarint(raw, 123)

	var got logVolumeAnnotations
	require.NoError(t, parseUsageLog(raw, &got))
	assert.True(t, got.isCreatedTx)
}

func TestParseUsageLogRejectsMalformedWire(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		raw  []byte
	}{
		{name: "truncated log payload", raw: []byte{0x12, 0x80}},
		{name: "wrong touched volume string type", raw: malformedUsageTouchedVolumeLog()},
		{name: "wrong timestamp type", raw: malformedUsageTimestampLog()},
		{name: "multiple log payload oneof members", raw: multipleUsageLogPayloadMembers()},
		{name: "multiple ledger log payload oneof members", raw: multipleUsageLedgerLogPayloadMembers()},
		{name: "duplicate log payload", raw: duplicateUsageLogPayload()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var got logVolumeAnnotations
			require.Error(t, parseUsageLog(tc.raw, &got))
		})
	}
}

func usageApplyLog(ledgerLog *commonpb.LedgerLog) *commonpb.Log {
	return &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
			LedgerName: "ledger",
			Log:        ledgerLog,
		}},
	}}
}

func expectedUsageLog(log *commonpb.Log) logVolumeAnnotations {
	ledgerLog := log.GetPayload().GetApply().GetLog()
	if ledgerLog == nil {
		return logVolumeAnnotations{}
	}

	out := logVolumeAnnotations{
		purged:    ledgerLog.GetPurgedVolumes(),
		newKept:   ledgerLog.GetNewKeptVolumes(),
		ephemeral: ledgerLog.GetEphemeralVolumes(),
	}

	var tx *commonpb.Transaction
	switch payload := ledgerLog.GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		out.isCreatedTx = true
		tx = payload.CreatedTransaction.GetTransaction()
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		out.isRevertedTx = true
		tx = payload.RevertedTransaction.GetRevertTransaction()
	}

	if tx != nil {
		out.postings = len(tx.GetPostings())
		out.txTimestamp = tx.GetTimestamp()
	}

	return out
}

func malformedUsageTouchedVolumeLog() []byte {
	touchedVolume := protowire.AppendTag(nil, 1, protowire.VarintType)
	touchedVolume = protowire.AppendVarint(touchedVolume, 1)
	ledgerLog := appendUsageBytesField(nil, 4, touchedVolume)

	return wrapUsageLedgerLog(ledgerLog)
}

func malformedUsageTimestampLog() []byte {
	timestamp := protowire.AppendTag(nil, 1, protowire.VarintType)
	timestamp = protowire.AppendVarint(timestamp, 42)
	transaction := appendUsageBytesField(nil, 3, timestamp)
	created := appendUsageBytesField(nil, 1, transaction)
	ledgerPayload := appendUsageBytesField(nil, 1, created)
	ledgerLog := appendUsageBytesField(nil, 1, ledgerPayload)

	return wrapUsageLedgerLog(ledgerLog)
}

func multipleUsageLogPayloadMembers() []byte {
	payload := appendUsageBytesField(nil, 1, nil)
	payload = appendUsageBytesField(payload, 3, nil)

	return appendUsageBytesField(nil, 2, payload)
}

func multipleUsageLedgerLogPayloadMembers() []byte {
	ledgerPayload := appendUsageBytesField(nil, 1, nil)
	ledgerPayload = appendUsageBytesField(ledgerPayload, 2, nil)
	ledgerLog := appendUsageBytesField(nil, 1, ledgerPayload)

	return wrapUsageLedgerLog(ledgerLog)
}

func duplicateUsageLogPayload() []byte {
	raw := appendUsageBytesField(nil, 2, nil)

	return appendUsageBytesField(raw, 2, nil)
}

func wrapUsageLedgerLog(ledgerLog []byte) []byte {
	apply := appendUsageBytesField(nil, 2, ledgerLog)
	payload := appendUsageBytesField(nil, 3, apply)

	return appendUsageBytesField(nil, 2, payload)
}

func appendUsageBytesField(dst []byte, num protowire.Number, value []byte) []byte {
	dst = protowire.AppendTag(dst, num, protowire.BytesType)

	return protowire.AppendBytes(dst, value)
}
