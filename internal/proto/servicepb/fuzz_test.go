package servicepb

import (
	"testing"
)

// FuzzBulkElementUnmarshalJSON fuzzes the BulkElement JSON decoder.
// This targets the action-based dispatch (CREATE_TRANSACTION, ADD_METADATA,
// REVERT_TRANSACTION, DELETE_METADATA) with arbitrary JSON payloads.
func FuzzBulkElementUnmarshalJSON(f *testing.F) {
	// Seed corpus: valid bulk elements for each action type.
	f.Add([]byte(`{"action":"CREATE_TRANSACTION","data":{"postings":[{"source":"world","destination":"users:alice","asset":"USD/2","amount":1000}]}}`))
	f.Add([]byte(`{"action":"CREATE_TRANSACTION","ik":"tx-001","data":{"postings":[{"source":"world","destination":"bank","asset":"EUR/2","amount":500}],"metadata":{"ref":"order-123"}}}`))
	f.Add([]byte(`{"action":"ADD_METADATA","data":{"targetType":"ACCOUNT","targetId":"users:alice","metadata":{"role":"admin"}}}`))
	f.Add([]byte(`{"action":"ADD_METADATA","data":{"targetType":"TRANSACTION","targetId":42,"metadata":{"status":"settled"}}}`))
	f.Add([]byte(`{"action":"REVERT_TRANSACTION","data":{"id":1,"force":true}}`))
	f.Add([]byte(`{"action":"REVERT_TRANSACTION","data":{"id":1,"atEffectiveDate":true,"metadata":{"reason":"fraud"}}}`))
	f.Add([]byte(`{"action":"DELETE_METADATA","data":{"targetType":"ACCOUNT","targetId":"users:alice","key":"role"}}`))
	f.Add([]byte(`{"action":"DELETE_METADATA","data":{"targetType":"TRANSACTION","targetId":42,"key":"status"}}`))
	// Edge cases
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"action":"UNKNOWN"}`))
	f.Add([]byte(`{"action":"CREATE_TRANSACTION"}`))
	f.Add([]byte(`{"action":"CREATE_TRANSACTION","data":null}`))
	f.Add([]byte(`{"action":"ADD_METADATA","data":{"targetType":"INVALID","targetId":"x","metadata":{}}}`))
	f.Add([]byte(`not json`))
	f.Add([]byte(`null`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`""`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var elem BulkElement
		// Must not panic on any input.
		_ = elem.UnmarshalJSON(data)
	})
}

// FuzzLedgerActionUnmarshalJSON fuzzes the LedgerAction JSON decoder.
func FuzzLedgerActionUnmarshalJSON(f *testing.F) {
	f.Add([]byte(`{"action":"CREATE_TRANSACTION","data":{"postings":[{"source":"world","destination":"users:bob","asset":"USD/2","amount":100}]}}`))
	f.Add([]byte(`{"action":"ADD_METADATA","data":{"targetType":"ACCOUNT","targetId":"x","metadata":{"k":"v"}}}`))
	f.Add([]byte(`{"action":"REVERT_TRANSACTION","data":{"id":0}}`))
	f.Add([]byte(`{"action":"DELETE_METADATA","data":{"targetType":"ACCOUNT","targetId":"x","key":"k"}}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"action":""}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var req LedgerAction
		_ = req.UnmarshalJSON(data)
	})
}
