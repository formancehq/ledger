package http

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestDecodeAtomicApplyBatch(t *testing.T) {
	t.Parallel()
	t.Run("KV commands preserve opaque bytes", func(t *testing.T) {
		batch, err := decodeAtomicApplyBatch([]byte(`{"requests":[{"putKv":{"ledger":"a","key":"k","value":"AQID"}},{"deleteKv":{"ledger":"a","key":"k"}}]}`))
		require.NoError(t, err)
		require.Equal(t, []byte{1, 2, 3}, batch.GetRequests()[0].GetPutKv().GetValue())
		require.Equal(t, "k", batch.GetRequests()[1].GetDeleteKv().GetKey())
	})
	t.Run("exact amounts timestamps and typed metadata", func(t *testing.T) {
		batch, err := decodeAtomicApplyBatch([]byte(`{"requests":[{"apply":{"ledger":"a","action":{"createTransaction":{"postings":[{"source":"world","destination":"a","asset":"USD","amount":9007199254740993}],"timestamp":"2026-01-01T00:00:00Z","metadata":{"large":{"intValue":"9007199254740993"},"text":{"stringValue":"9007199254740993"}}}}}}]}`))
		require.NoError(t, err)
		tx := batch.GetRequests()[0].GetApply().GetAction().GetCreateTransaction()
		require.Equal(t, "9007199254740993", tx.GetPostings()[0].GetAmount().ToBigInt().String())
		require.Equal(t, uint64(1767225600000000), tx.GetTimestamp().GetData())
		require.Equal(t, int64(9007199254740993), tx.GetMetadata()["large"].GetIntValue())
		require.Equal(t, "9007199254740993", tx.GetMetadata()["text"].GetStringValue())
	})
	t.Run("every request variant", func(t *testing.T) {
		fields := (&servicepb.Request{}).ProtoReflect().Descriptor().Fields()
		require.Equal(t, 29, fields.Len())
		for i := range fields.Len() {
			field := fields.Get(i)
			value := `{}`
			if field.JSONName() == "apply" {
				value = `{"action":{"addMetadata":{}}}`
			}
			_, err := decodeAtomicApplyBatch([]byte(fmt.Sprintf(`{"requests":[{%q:%s}]}`, field.JSONName(), value)))
			require.NoError(t, err, field.JSONName())
		}
	})
	t.Run("every ledger action", func(t *testing.T) {
		fields := (&servicepb.LedgerAction{}).ProtoReflect().Descriptor().Fields()
		for i := range fields.Len() {
			_, err := decodeAtomicApplyBatch([]byte(fmt.Sprintf(`{"requests":[{"apply":{"action":{%q:{}}}}]}`, fields.Get(i).JSONName())))
			require.NoError(t, err, fields.Get(i).JSONName())
		}
	})
	t.Run("timestamps reject internal and pre epoch forms", func(t *testing.T) {
		for _, timestamp := range []string{`{"data":"1"}`, `"1969-12-31T23:59:59Z"`, `"bad"`} {
			_, err := decodeAtomicApplyBatch([]byte(fmt.Sprintf(`{"requests":[{"apply":{"action":{"createTransaction":{"timestamp":%s}}}}]}`, timestamp)))
			require.Error(t, err, timestamp)
		}
	})

	t.Run("posting amount boundaries", func(t *testing.T) {
		for _, amount := range []string{`"9007199254740993"`, `0`, `"0"`, `"115792089237316195423570985008687907853269984665640564039457584007913129639935"`} {
			_, err := decodeAtomicApplyBatch([]byte(fmt.Sprintf(`{"requests":[{"apply":{"action":{"createTransaction":{"postings":[{"amount":%s}]}}}}]}`, amount)))
			require.NoError(t, err, amount)
		}
		for _, amount := range []string{`"01"`, `"-1"`, `1.5`, `1e3`, `"0x10"`, `"115792089237316195423570985008687907853269984665640564039457584007913129639936"`, `{"v0":"1"}`} {
			_, err := decodeAtomicApplyBatch([]byte(fmt.Sprintf(`{"requests":[{"apply":{"action":{"createTransaction":{"postings":[{"amount":%s}]}}}}]}`, amount)))
			require.Error(t, err, amount)
		}
	})
	t.Run("skippable reasons retain grpc semantics", func(t *testing.T) {
		batch, err := decodeAtomicApplyBatch([]byte(`{"requests":[{"apply":{"action":{"createTransaction":{}},"skippableReasons":["ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT"]}}]}`))
		require.NoError(t, err)
		require.Len(t, batch.GetRequests()[0].GetApply().GetSkippableReasons(), 1)
	})
	t.Run("undefined enum values are rejected before admission", func(t *testing.T) {
		for _, body := range []string{
			`{"requests":[{"createLedger":{"name":"a","mode":99}}]}`,
			`{"requests":[{"createLedger":{"name":"a","defaultEnforcementMode":99}}]}`,
			`{"requests":[{"apply":{"action":{"createTransaction":{}},"skippableReasons":[99]}}]}`,
		} {
			_, err := decodeAtomicApplyBatch([]byte(body))
			require.ErrorContains(t, err, "undefined enum value")
		}
	})

	for _, body := range []string{
		`null`, `{}`, `{"requests":null}`, `{"requests":[null]}`, `{"requests":[{}]}`,
		`{"requests":[{"createLedger":null}]}`, `{"requests":[{"apply":{}}]}`,
		`{"requests":[{"apply":{"action":{}}}]}`, `{"requests":[],"idempotencyKey":""}`,
		`{"requests":[],"forwarded":false}`, `{"requests":[],"requests":[]}`,
		`{"requests":[{"create_ledger":{}}]}`, `{"requests":[{"createLedger":{},"deleteLedger":{}}]}`,
		`{"requests":[{"createLedger":{"name":"a","name":"b"}}]}`,
		`{"requests":[]} {}`, `{"requests":[{"createLedger":{"unknown":1}}]}`,
		`{"requests":[{"saveLedgerMetadata":{"metadata":{"a":1}}}]}`,
		`{"requests":[{"saveLedgerMetadata":{"metadata":{"a":{"intValue":"1","stringValue":"1"}}}}]}`,
		`{"requests":[{"apply":{"action":{"createTransaction":{},"addMetadata":{}}}}]}`,
		`{"requests":[{"saveLedgerMetadata":{"metadata":{"a":{"int_value":"1"}}}}]}`,
		strings.Repeat(`[`, 65) + strings.Repeat(`]`, 65),
	} {
		t.Run(body, func(t *testing.T) { _, err := decodeAtomicApplyBatch([]byte(body)); require.Error(t, err) })
	}
}

func TestAtomicJSONDepthBound(t *testing.T) {
	t.Parallel()
	for _, depth := range []int{64, 65} {
		decoder := json.NewDecoder(strings.NewReader(strings.Repeat("[", depth) + strings.Repeat("]", depth)))
		_, err := readAtomicJSONValue(decoder, 0)
		if depth == 64 {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, "nesting exceeds 64")
		}
	}
}
