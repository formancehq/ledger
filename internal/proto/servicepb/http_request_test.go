package servicepb_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func TestDecodeHTTPRequest(t *testing.T) {
	t.Parallel()
	metadata := map[string]*commonpb.MetadataValue{
		"large": commonpb.NewUintValue(9007199254740993),
		"flag":  commonpb.NewBoolValue(true),
	}
	tests := []struct {
		name   string
		method string
		target string
		body   string
		action *servicepb.LedgerAction
	}{
		{
			name: "create preserves camelCase and exact metadata numbers", method: http.MethodPost,
			target: "/v3/main/transactions",
			body:   `{"reference":"r","force":true,"scriptReference":{"name":"transfer","version":"1"},"metadata":{"large":9007199254740993,"flag":true},"accountMetadata":{"users:1":{"count":42}}}`,
			action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: &servicepb.CreateTransactionPayload{
				Reference: "r", Force: true, ScriptReference: &servicepb.ScriptReference{Name: "transfer", Version: "1"}, Metadata: metadata,
				AccountMetadata: map[string]*commonpb.MetadataMap{"users:1": {Values: map[string]*commonpb.MetadataValue{"count": commonpb.NewUintValue(42)}}},
			}}},
		},
		{
			name: "revert preserves options and metadata", method: http.MethodPost,
			target: "/v3/main/transactions/18446744073709551615/revert",
			body:   `{"force":true,"atEffectiveDate":true,"metadata":{"large":9007199254740993,"flag":true}}`,
			action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RevertTransaction{RevertTransaction: &servicepb.RevertTransactionPayload{
				TransactionId: ^uint64(0), Force: true, AtEffectiveDate: true, Metadata: metadata,
			}}},
		},
		{
			name: "empty revert uses defaults", method: http.MethodPost, target: "/v3/main/transactions/0/revert",
			action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RevertTransaction{RevertTransaction: &servicepb.RevertTransactionPayload{TransactionId: 0}}},
		},
		{
			name: "null revert uses defaults", method: http.MethodPost, target: "/v3/main/transactions/12/revert", body: "null",
			action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RevertTransaction{RevertTransaction: &servicepb.RevertTransactionPayload{TransactionId: 12}}},
		},
		{
			name: "save typed metadata", method: http.MethodPost, target: "/v3/main/transactions/12/metadata",
			body: `{"large":9007199254740993,"flag":true}`,
			action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{AddMetadata: &commonpb.SaveMetadataCommand{
				Target: &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: 12}}, Metadata: metadata,
			}}},
		},
		{
			name: "escaped metadata namespace", method: http.MethodDelete, target: "/v3/main/transactions/12/metadata/team%2Fstate",
			action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{DeleteMetadata: &commonpb.DeleteMetadataCommand{
				Target: &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: 12}}, Key: "team/state",
			}}},
		},
		{
			name: "double escape decodes once", method: http.MethodDelete, target: "/v3/main/transactions/12/metadata/team%252Fstate",
			action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{DeleteMetadata: &commonpb.DeleteMetadataCommand{
				Target: &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: 12}}, Key: "team%2Fstate",
			}}},
		},
		{
			name: "escaped colon decodes once", method: http.MethodDelete, target: "/v3/main/transactions/12/metadata/team%3Astate",
			action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{DeleteMetadata: &commonpb.DeleteMetadataCommand{
				Target: &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: 12}}, Key: "team:state",
			}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := &signaturepb.SignedHTTPRequest{Method: test.method, RequestTarget: test.target, Body: []byte(test.body), IdempotencyKey: "batch-key"}
			got, err := servicepb.DecodeHTTPRequest(request)
			require.NoError(t, err)
			want := &servicepb.ApplyBatch{IdempotencyKey: "batch-key", Requests: []*servicepb.Request{{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
				Ledger: "main", Action: test.action,
			}}}}}
			require.True(t, proto.Equal(want, got), "want %v, got %v", want, got)
			peeked, err := servicepb.PeekBatch(&servicepb.ApplyRequest{Variant: &servicepb.ApplyRequest_HttpSigned{HttpSigned: request}})
			require.NoError(t, err)
			require.True(t, proto.Equal(want, peeked))
		})
	}
}

func TestDecodeHTTPRequestAtomicBulk(t *testing.T) {
	t.Parallel()
	request := &signaturepb.SignedHTTPRequest{
		Method: http.MethodPost, RequestTarget: "/v3/main/bulk?atomic=true&continueOnFailure=true", IdempotencyKey: "whole-batch",
		Body: []byte(`[{"action":"CREATE_TRANSACTION","ik":"ignored-element-key","data":{"reference":"first"},"skippableReasons":["TRANSACTION_REFERENCE_CONFLICT"]},{"action":"REVERT_TRANSACTION","data":{"id":12,"force":true,"metadata":{"large":9007199254740993}}}]`),
	}
	got, err := servicepb.DecodeHTTPRequest(request)
	require.NoError(t, err)
	want := &servicepb.ApplyBatch{IdempotencyKey: "whole-batch", Requests: []*servicepb.Request{
		{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
			Ledger: "main", SkippableReasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: &servicepb.CreateTransactionPayload{Reference: "first"}}},
		}}},
		{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
			Ledger: "main", Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RevertTransaction{RevertTransaction: &servicepb.RevertTransactionPayload{
				TransactionId: 12, Force: true, Metadata: map[string]*commonpb.MetadataValue{"large": commonpb.NewUintValue(9007199254740993)},
			}}},
		}}},
	}}
	require.True(t, proto.Equal(want, got), "want %v, got %v", want, got)
}

func TestDecodeHTTPRequestRejectsInvalidRequests(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, method, target, body, message string
	}{
		{"absolute target", "POST", "https://ledger.test/v3/main/transactions", `{}`, "origin-form"},
		{"authority target", "POST", "//ledger.test/v3/main/transactions", `{}`, "origin-form"},
		{"invalid escape", "POST", "/v3/main/transactions/%ZZ/revert", `{}`, "invalid HTTP request target"},
		{"invalid UTF-8", "POST", "/v3/main/transactions/1/metadata/%FF", `{}`, "not UTF-8"},
		{"unknown version", "POST", "/v2/main/transactions", `{}`, "unsupported signed HTTP route"},
		{"unknown operation", "POST", "/v3/main/accounts", `{}`, "unsupported signed HTTP route"},
		{"wrong method", "PUT", "/v3/main/transactions", `{}`, "unsupported signed HTTP route"},
		{"wrong suffix", "POST", "/v3/main/transactions/1/revert/extra", `{}`, "unsupported signed HTTP route"},
		{"invalid id", "POST", "/v3/main/transactions/no/revert", `{}`, "invalid HTTP transaction id"},
		{"negative id", "POST", "/v3/main/transactions/-1/revert", `{}`, "invalid HTTP transaction id"},
		{"overflow id", "POST", "/v3/main/transactions/18446744073709551616/revert", `{}`, "invalid HTTP transaction id"},
		{"unknown query", "POST", "/v3/main/transactions?force=true", `{}`, "unknown HTTP query parameter"},
		{"malformed query", "POST", "/v3/main/transactions?x=%ZZ", `{}`, "invalid HTTP query string"},
		{"empty create", "POST", "/v3/main/transactions", "", "expected a single JSON value"},
		{"trailing JSON", "POST", "/v3/main/transactions", `{} {}`, "expected a single JSON value"},
		{"wrong revert bool", "POST", "/v3/main/transactions/1/revert", `{"force":"true"}`, "invalid HTTP revert body"},
		{"whitespace revert", "POST", "/v3/main/transactions/1/revert", " ", "expected a single JSON value"},
		{"nested metadata", "POST", "/v3/main/transactions/1/metadata", `{"nested":{}}`, "invalid HTTP metadata"},
		{"fraction metadata", "POST", "/v3/main/transactions/1/revert", `{"metadata":{"fraction":1.5}}`, "invalid HTTP revert metadata"},
		{"empty key", "DELETE", "/v3/main/transactions/1/metadata/", "", "HTTP metadata key is required"},
		{"delete body", "DELETE", "/v3/main/transactions/1/metadata/key", "null", "requires an empty body"},
		{"non atomic bulk", "POST", "/v3/main/bulk", `[]`, "requires atomic=true"},
		{"boolean atomic syntax", "POST", "/v3/main/bulk?atomic=1", `[]`, "requires atomic=true"},
		{"first duplicate atomic wins", "POST", "/v3/main/bulk?atomic=false&atomic=true", `[]`, "requires atomic=true"},
		{"empty bulk", "POST", "/v3/main/bulk?atomic=true", `[]`, "requires at least one element"},
		{"null bulk", "POST", "/v3/main/bulk?atomic=true", `null`, "requires at least one element"},
		{"null bulk entry", "POST", "/v3/main/bulk?atomic=true", `[null]`, "element 0 is null"},
		{"unknown bulk action", "POST", "/v3/main/bulk?atomic=true", `[{"action":"BOGUS","data":{}}]`, "unsupported action"},
		{"unknown skip reason", "POST", "/v3/main/bulk?atomic=true", `[{"action":"CREATE_TRANSACTION","data":{},"skippableReasons":["BOGUS"]}]`, "unknown ErrorReason"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := servicepb.DecodeHTTPRequest(&signaturepb.SignedHTTPRequest{Method: test.method, RequestTarget: test.target, Body: []byte(test.body)})
			require.ErrorContains(t, err, test.message)
		})
	}
	t.Run("nil request", func(t *testing.T) {
		t.Parallel()
		_, err := servicepb.DecodeHTTPRequest(nil)
		require.ErrorContains(t, err, "empty signed HTTP request")
	})
}
