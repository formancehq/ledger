package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleGetTransaction_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: "ledger1"}, nil
		}).AnyTimes()
	backend.EXPECT().GetTransaction(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, txID uint64) (*commonpb.Transaction, *string, error) {
			return &commonpb.Transaction{Id: txID}, nil, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/transactions/42", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "42",
	})

	srv.handleGetTransaction(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	// The response envelope pairs the transaction with the receipt (EN-1510).
	require.Contains(t, w.Body.String(), `"transaction":`)
	require.Contains(t, w.Body.String(), `"id":42`)
}

// TestHandleGetTransaction_ReceiptPresent verifies the backend receipt is
// surfaced verbatim in data.receipt (EN-1510).
func TestHandleGetTransaction_ReceiptPresent(t *testing.T) {
	t.Parallel()

	const receipt = "eyJhbGciOiJIUzI1NiJ9.receipt-token"

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: "ledger1"}, nil
		}).AnyTimes()
	backend.EXPECT().GetTransaction(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, txID uint64) (*commonpb.Transaction, *string, error) {
			r := receipt

			return &commonpb.Transaction{Id: txID}, &r, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/transactions/42", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "42",
	})

	srv.handleGetTransaction(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Data struct {
			Receipt     string          `json:"receipt"`
			Transaction json.RawMessage `json:"transaction"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, receipt, resp.Data.Receipt)
	require.NotEmpty(t, resp.Data.Transaction)
	require.Contains(t, string(resp.Data.Transaction), `"id":42`)
}

// TestHandleGetTransaction_ReceiptEmpty verifies that a transaction with no
// receipt (empty string or nil pointer) renders a stable, always-present
// receipt field set to "" (EN-1510).
func TestHandleGetTransaction_ReceiptEmpty(t *testing.T) {
	t.Parallel()

	emptyReceipt := ""
	tests := map[string]*string{
		"empty string": &emptyReceipt,
		"nil pointer":  nil,
	}

	for name, receipt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
					return &commonpb.LedgerInfo{Name: "ledger1"}, nil
				}).AnyTimes()
			backend.EXPECT().GetTransaction(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, _ string, txID uint64) (*commonpb.Transaction, *string, error) {
					return &commonpb.Transaction{Id: txID}, receipt, nil
				}).AnyTimes()
			srv := newTestServer(t, backend)

			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodGet, "/ledger1/transactions/42", nil, map[string]string{
				"ledgerName":    "ledger1",
				"transactionId": "42",
			})

			srv.handleGetTransaction(w, r)

			require.Equal(t, http.StatusOK, w.Code)
			// The receipt key is always present, even when empty.
			require.Contains(t, w.Body.String(), `"receipt":""`)

			var resp struct {
				Data struct {
					Receipt string `json:"receipt"`
				} `json:"data"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			require.Equal(t, "", resp.Data.Receipt)
		})
	}
}

func TestHandleGetTransaction_RevertRelationshipFields(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: "ledger1"}, nil
		}).AnyTimes()
	// Transaction 1 was reverted by transaction 2, which in turn reverts 1.
	backend.EXPECT().GetTransaction(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, txID uint64) (*commonpb.Transaction, *string, error) {
			if txID == 1 {
				return &commonpb.Transaction{
					Id:                    1,
					Reverted:              true,
					RevertedByTransaction: 2,
					RevertedAt:            &commonpb.Timestamp{Data: 1_700_000_000_000_000},
				}, nil, nil
			}

			return &commonpb.Transaction{Id: 2, RevertsTransaction: 1}, nil, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	// The reverted original exposes the forward link and reverted_at.
	wOrig := httptest.NewRecorder()
	srv.handleGetTransaction(wOrig, newRequest(t, http.MethodGet, "/ledger1/transactions/1", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	}))
	require.Equal(t, http.StatusOK, wOrig.Code)
	require.Contains(t, wOrig.Body.String(), `"reverted":true`)
	require.Contains(t, wOrig.Body.String(), `"revertedByTransactionId":2`)
	require.Contains(t, wOrig.Body.String(), `"revertedAt":`)

	// The compensating transaction exposes the back link.
	wRevert := httptest.NewRecorder()
	srv.handleGetTransaction(wRevert, newRequest(t, http.MethodGet, "/ledger1/transactions/2", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "2",
	}))
	require.Equal(t, http.StatusOK, wRevert.Code)
	require.Contains(t, wRevert.Body.String(), `"revertsTransactionId":1`)
}

func TestHandleGetTransaction_InvalidTxID(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/transactions/abc", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "abc",
	})

	srv.handleGetTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetTransaction_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: "ledger1"}, nil
		}).AnyTimes()
	backend.EXPECT().GetTransaction(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint64) (*commonpb.Transaction, *string, error) {
			return nil, nil, &domain.ErrTransactionNotFound{TransactionID: 999}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/transactions/999", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "999",
	})

	srv.handleGetTransaction(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleGetTransaction_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/", nil, map[string]string{
		"ledgerName":    "",
		"transactionId": "1",
	})

	srv.handleGetTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

// TestHandleGetTransaction_StringAmountWire pins both amount wires of the detail
// route (EN-1779). The default body is asserted byte-for-byte against what the
// route emitted before the opt-in existed, and the opt-in body against a literal
// that requireOnlyAmountQuotingDiffers proves differs from it in the amount
// alone — including the HTML escaping and the trailing newline this route's
// ConfigStd encoder produces, which is what a change of writer would break.
//
// Amount assertions read the raw body bytes and never decode: the repository's
// sonic wrapper has no UseNumber, so decoding silently truncates 2^53+1.
func TestHandleGetTransaction_StringAmountWire(t *testing.T) {
	t.Parallel()

	const (
		defaultBody = `{"data":{"transaction":{"postings":[{"source":"world\u003c\u0026\u003e","destination":"alice\u0026\u003cbob","amount":9007199254740993,"asset":"USD/2","color":""}],"metadata":{},"id":42,"reverted":false},"receipt":""}}` + "\n"
		optInBody   = `{"data":{"transaction":{"postings":[{"source":"world\u003c\u0026\u003e","destination":"alice\u0026\u003cbob","amount":"9007199254740993","asset":"USD/2","color":""}],"metadata":{},"id":42,"reverted":false},"receipt":""}}` + "\n"
	)

	requireOnlyAmountQuotingDiffers(t, defaultBody, optInBody)

	type testCase struct {
		name        string
		headerValue string // empty means the header is not sent at all
		wantBody    string
		wantAmount  string // the amount rendering that must appear
		notAmount   string // the other mode's rendering, which must not
	}

	testCases := []testCase{
		{
			name:       "default wire keeps the bare number",
			wantBody:   defaultBody,
			wantAmount: `"amount":` + aboveJSNumberLimit,
			notAmount:  `"amount":"` + aboveJSNumberLimit + `"`,
		},
		{
			name:        "opt-in wire quotes the decimal",
			headerValue: "true",
			wantBody:    optInBody,
			wantAmount:  `"amount":"` + aboveJSNumberLimit + `"`,
			notAmount:   `"amount":` + aboveJSNumberLimit,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
					return &commonpb.LedgerInfo{Name: "ledger1"}, nil
				}).AnyTimes()
			backend.EXPECT().GetTransaction(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, _ string, txID uint64) (*commonpb.Transaction, *string, error) {
					return bigAmountTransaction(t, txID), nil, nil
				}).AnyTimes()
			srv := newTestServer(t, backend)

			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodGet, "/ledger1/transactions/42", nil, map[string]string{
				"ledgerName":    "ledger1",
				"transactionId": "42",
			})

			if tc.headerValue != "" {
				r.Header.Set("Formance-Bigint-As-String", tc.headerValue)
			}

			srv.handleGetTransaction(w, r)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Equal(t, tc.wantBody, w.Body.String())
			require.Contains(t, w.Body.String(), tc.wantAmount)
			require.NotContains(t, w.Body.String(), tc.notAmount)
		})
	}
}
