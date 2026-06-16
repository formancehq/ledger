package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleBulk_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`not json`)
	r := newRequest(t, http.MethodPost, "/ledger1/bulk", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleBulk(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleBulk_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`[]`)
	r := newRequest(t, http.MethodPost, "/bulk", body, map[string]string{
		"ledgerName": "",
	})

	srv.handleBulk(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleBulk_SizeLimitExceeded(t *testing.T) {
	t.Parallel()

	srv := newTestServerWithBulkLimit(t, NewMockBackend(gomock.NewController(t)), 1)

	// Two elements but limit is 1
	w := httptest.NewRecorder()
	body := strings.NewReader(`[
		{"action":"CREATE_TRANSACTION","data":{"postings":[{"source":"world","destination":"bank","amount":100,"asset":"USD/2"}]}},
		{"action":"CREATE_TRANSACTION","data":{"postings":[{"source":"world","destination":"bank","amount":100,"asset":"USD/2"}]}}
	]`)
	r := newRequest(t, http.MethodPost, "/ledger1/bulk", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleBulk(w, r)

	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
}

func TestHandleBulk_EmptyArray(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`[]`)
	r := newRequest(t, http.MethodPost, "/ledger1/bulk", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleBulk(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestRunBulkAtomic_AllFail(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("atomic failure")
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ ...*servicepb.Envelope) ([]*commonpb.Log, error) {
			return nil, expectedErr
		}).AnyTimes()
	srv := newTestServer(t, backend)

	requests := []*servicepb.Request{{}, {}}
	results := srv.runBulkAtomic(context.Background(), requests)

	require.Len(t, results, 2)

	for _, r := range results {
		require.ErrorIs(t, r.err, expectedErr)
	}
}

func TestRunBulkAtomic_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ ...*servicepb.Envelope) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Id: 1}}}}},
				{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Id: 2}}}}},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	requests := []*servicepb.Request{{}, {}}
	results := srv.runBulkAtomic(context.Background(), requests)

	require.Len(t, results, 2)

	for _, r := range results {
		require.NoError(t, r.err)
		require.NotNil(t, r.log)
	}
}

func TestRunBulkSequential_StopOnError(t *testing.T) {
	t.Parallel()

	callCount := 0
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ ...*servicepb.Envelope) ([]*commonpb.Log, error) {
			callCount++
			if callCount == 1 {
				return nil, errors.New("first fails")
			}

			return []*commonpb.Log{
				{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{}}}}},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	requests := []*servicepb.Request{{}, {}, {}}
	results := srv.runBulkSequential(context.Background(), requests, false)

	require.Len(t, results, 3)
	require.Error(t, results[0].err)
	require.ErrorIs(t, results[1].err, context.Canceled)
	require.ErrorIs(t, results[2].err, context.Canceled)
}

func TestRunBulkSequential_ContinueOnFailure(t *testing.T) {
	t.Parallel()

	callCount := 0
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ ...*servicepb.Envelope) ([]*commonpb.Log, error) {
			callCount++
			if callCount == 1 {
				return nil, errors.New("first fails")
			}

			return []*commonpb.Log{
				{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{}}}}},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	requests := []*servicepb.Request{{}, {}}
	results := srv.runBulkSequential(context.Background(), requests, true)

	require.Len(t, results, 2)
	require.Error(t, results[0].err)
	require.NoError(t, results[1].err)
}
