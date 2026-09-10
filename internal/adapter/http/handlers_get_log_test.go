package http

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleGetLog_Success(t *testing.T) {
	t.Parallel()

	wantLog := &commonpb.LedgerLog{
		Id:   3,
		Date: &commonpb.Timestamp{Data: 1_700_000_000_000_000},
		Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{
					Id:        1,
					Reference: "order-123",
					Postings: []*commonpb.Posting{
						commonpb.NewColoredPosting("world", "alice", "USD/2", "pending", big.NewInt(1000)),
					},
					Metadata: map[string]*commonpb.MetadataValue{"note": commonpb.NewStringValue("checkout")},
				},
				AccountMetadata: map[string]*commonpb.MetadataMap{
					"alice": {Values: map[string]*commonpb.MetadataValue{"tier": commonpb.NewStringValue("gold")}},
				},
			},
		}},
	}
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLog(gomock.Any(), uint64(7)).DoAndReturn(
		func(_ context.Context, _ uint64) (*commonpb.Log, error) {
			return &commonpb.Log{
				Sequence: 7,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							LedgerName: "ledger1",
							Log:        wantLog,
						},
					},
				},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/logs/7", nil, map[string]string{
		"sequence": "7",
	})

	srv.handleGetLog(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	// The swap to writeOKChecked routes the log through Log.MarshalJSON, which
	// emits the synthetic `type` discriminator LedgerLog.MarshalJSON injects.
	// protojson cannot emit it at all (it is not a proto field), so before this
	// change the body carried no discriminator and disagreed with the logs-list
	// route, which has always used sonic for the same commonpb.Log type.
	//
	// The direct data payload is shared by the logs-list route, prepared
	// queries, and JSON event sinks.
	body := w.Body.String()
	require.Contains(t, body, `"type":"NEW_TRANSACTION"`)
	require.Contains(t, body, `"sequence":7`)
	require.NotContains(t, body, `"sequence":"7"`)
	require.NotContains(t, body, `"createdTransaction":`)
	require.Contains(t, body, `"data":{"transaction":`)
	require.NotContains(t, body, `"id":"1"`)
	var response struct {
		Data struct {
			Payload struct {
				Apply struct {
					Log json.RawMessage `json:"log"`
				} `json:"apply"`
			} `json:"payload"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	require.JSONEq(t, `{
		"id":3,"date":"2023-11-14T22:13:20Z","type":"NEW_TRANSACTION",
		"data":{
			"transaction":{"id":1,"reference":"order-123","reverted":false,
				"postings":[{"source":"world","destination":"alice","asset":"USD/2","color":"pending","amount":1000}],
				"metadata":{"note":"checkout"}},
			"accountMetadata":{"alice":{"tier":"gold"}}
		}
	}`, string(response.Data.Payload.Apply.Log))
}

// TestHandleGetLog_SerializesThroughMarshalJSON pins the property EN-1622 is
// about: the single-log route must serialize its commonpb.Log through
// Log.MarshalJSON, matching the shape the logs-list route also produces. It
// diverged because get-log used protojson while the list used sonic, so the
// same type had two wire shapes depending on which route you asked.
func TestHandleGetLog_SerializesThroughMarshalJSON(t *testing.T) {
	t.Parallel()

	logValue := func() *commonpb.Log {
		return &commonpb.Log{
			Sequence: 7,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "ledger1",
						Log: &commonpb.LedgerLog{
							Data: &commonpb.LedgerLogPayload{
								Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
									CreatedTransaction: &commonpb.CreatedTransaction{
										Transaction: &commonpb.Transaction{Id: 1},
									},
								},
							},
						},
					},
				},
			},
		}
	}

	direct, err := logValue().MarshalJSON()
	require.NoError(t, err)

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLog(gomock.Any(), uint64(7)).DoAndReturn(
		func(_ context.Context, _ uint64) (*commonpb.Log, error) {
			return logValue(), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/logs/7", nil, map[string]string{"sequence": "7"})

	srv.handleGetLog(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"data":`+string(direct)+`}`, w.Body.String())
}

func TestHandleGetLog_InvalidSequence(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/logs/abc", nil, map[string]string{
		"sequence": "abc",
	})

	srv.handleGetLog(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetLog_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLog(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ uint64) (*commonpb.Log, error) {
			return nil, commonpb.NewNotFoundError("log %d not found", 9999)
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/logs/9999", nil, map[string]string{
		"sequence": "9999",
	})

	srv.handleGetLog(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}
