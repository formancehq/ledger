package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleCreateLedger_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_CreateLedger{
							CreateLedger: &commonpb.CreatedLedgerLog{
								Name: "test-ledger",
							},
						},
					},
				},
			}, nil
		})
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/test-ledger", nil, map[string]string{
		"ledgerName": "test-ledger",
	})

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
}

func TestHandleCreateLedger_InitialSchemaAndAccountTypes(t *testing.T) {
	t.Parallel()

	var captured *servicepb.CreateLedgerRequest

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			captured = req.GetUnsigned().GetRequests()[0].GetCreateLedger()

			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_CreateLedger{
							CreateLedger: &commonpb.CreatedLedgerLog{Name: "test-ledger"},
						},
					},
				},
			}, nil
		})
	srv := newTestServer(t, backend)

	body := `{
		"initialSchema": [
			{"targetType": "account", "key": "color", "type": "string"},
			{"targetType": "transaction", "key": "seq", "type": "int64"}
		],
		"accountTypes": {
			"user-checking": {
				"name": "user-checking",
				"pattern": "users:{id}:checking",
				"persistence": "EPHEMERAL",
				"segmentTypes": {"id": {"type": "uint64"}}
			}
		}
	}`

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/test-ledger", strings.NewReader(body), map[string]string{
		"ledgerName": "test-ledger",
	})

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
	require.NotNil(t, captured)

	require.Len(t, captured.GetInitialSchema(), 2)
	require.Equal(t, commonpb.TargetType_TARGET_TYPE_ACCOUNT, captured.GetInitialSchema()[0].GetTargetType())
	require.Equal(t, "color", captured.GetInitialSchema()[0].GetKey())
	require.Equal(t, commonpb.MetadataType_METADATA_TYPE_STRING, captured.GetInitialSchema()[0].GetType())
	require.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, captured.GetInitialSchema()[1].GetType())

	at, ok := captured.GetAccountTypes()["user-checking"]
	require.True(t, ok)
	require.Equal(t, "users:{id}:checking", at.GetPattern())
	require.Equal(t, commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL, at.GetPersistence())
	require.IsType(t, &commonpb.SegmentType_Uint64{}, at.GetSegmentTypes()["id"].GetConstraint())
}

func TestHandleCreateLedger_InvalidInitialSchema(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/test-ledger",
		strings.NewReader(`{"initialSchema":[{"targetType":"bogus","key":"x","type":"string"}]}`), map[string]string{
			"ledgerName": "test-ledger",
		})

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreateLedger_InvalidAccountTypePersistence(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/test-ledger",
		strings.NewReader(`{"accountTypes":{"x":{"name":"x","pattern":"x","persistence":"WRONG"}}}`), map[string]string{
			"ledgerName": "test-ledger",
		})

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreateLedger_MissingName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

// TestHandleCreateLedger_LogContractViolations locks in the exact-one typed-log
// contract: a create-ledger request yields exactly one non-nil CreateLedger log.
// Any other cardinality, a nil sole log, or a mismatched payload type is an
// impossible backend response and must fail loudly through unreachable (the
// jsonRecoverer middleware turns the panic into a sanitized 500 in production).
func TestHandleCreateLedger_LogContractViolations(t *testing.T) {
	t.Parallel()

	created := &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "test-ledger"}},
	}}
	wrongPayload := &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_PromoteLedger{PromoteLedger: &commonpb.PromotedLedgerLog{Name: "test-ledger"}},
	}}

	cases := []struct {
		name string
		logs []*commonpb.Log
	}{
		{"zero logs", []*commonpb.Log{}},
		{"two logs", []*commonpb.Log{created, created}},
		{"nil sole log", []*commonpb.Log{nil}},
		{"wrong payload type", []*commonpb.Log{wrongPayload}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t, backendReturningLogs(t, tc.logs))

			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodPost, "/test-ledger", nil, map[string]string{
				"ledgerName": "test-ledger",
			})

			require.Panics(t, func() {
				srv.handleCreateLedger(w, r)
			})
		})
	}
}

func TestHandleCreateLedger_AlreadyExists(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, &domain.ErrLedgerAlreadyExists{Name: "test-ledger"}
		})
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/test-ledger", nil, map[string]string{
		"ledgerName": "test-ledger",
	})

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusConflict, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "LEDGER_ALREADY_EXISTS", resp.ErrorCode)
}
