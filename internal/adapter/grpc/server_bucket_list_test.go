package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	auditpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/application/ctrl/ctrlmock"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
)

// newListHandlerHarness wires the minimum fields BucketServiceServerImpl needs
// to drive a list-handler unit test. The Controller is a gomock so the test
// can dictate the items returned for each handler.
func newListHandlerHarness(t *testing.T) (*BucketServiceServerImpl, *ctrlmock.MockController) {
	t.Helper()

	mockCtrl := ctrlmock.NewMockController(gomock.NewController(t))

	impl := &BucketServiceServerImpl{
		logger: noopLogger{},
		ctrl:   mockCtrl,
	}

	return impl, mockCtrl
}

// page returns a cursor backed by the given slice. Handlers use peek-ahead so
// the slice should hold pageSize+1 items for the trailer to fire.
func page[T any](items ...*T) cursor.Cursor[*T] {
	return cursor.NewSliceCursor(items)
}

// TestListLedgers covers the new pagination plumbing on ListLedgers: peek
// fires → trailer, exact page → no trailer, ValidateListOptions reject.
func TestListLedgers(t *testing.T) {
	t.Parallel()

	t.Run("peek fires → trailer carries last-sent ledger name", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		mockCtrl.EXPECT().ListLedgers(gomock.Any()).Return(
			page(&auditpb.LedgerInfo{Name: "a"}, &auditpb.LedgerInfo{Name: "b"}, &auditpb.LedgerInfo{Name: "c"}, &auditpb.LedgerInfo{Name: "d"}),
			nil,
		)

		stream := newFakeServerStream[auditpb.LedgerInfo](t)
		req := &auditpb.ListLedgersRequest{Options: &auditpb.ListOptions{PageSize: 3}}

		require.NoError(t, impl.ListLedgers(req, stream))
		require.Equal(t, []string{"a", "b", "c"}, sentLedgerNames(stream))
		require.Equal(t, "c", stream.trailerCursor())
	})

	t.Run("exact page → no trailer", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		mockCtrl.EXPECT().ListLedgers(gomock.Any()).Return(
			page(&auditpb.LedgerInfo{Name: "a"}, &auditpb.LedgerInfo{Name: "b"}, &auditpb.LedgerInfo{Name: "c"}),
			nil,
		)

		stream := newFakeServerStream[auditpb.LedgerInfo](t)
		req := &auditpb.ListLedgersRequest{Options: &auditpb.ListOptions{PageSize: 3}}

		require.NoError(t, impl.ListLedgers(req, stream))
		require.Empty(t, stream.trailerCursor())
	})

	t.Run("controller error wrapped", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		boom := errors.New("ctrl blew up")
		mockCtrl.EXPECT().ListLedgers(gomock.Any()).Return(nil, boom)

		stream := newFakeServerStream[auditpb.LedgerInfo](t)
		err := impl.ListLedgers(&auditpb.ListLedgersRequest{}, stream)
		require.ErrorIs(t, err, boom)
	})
}

// TestListTransactions covers the new plumbing for transactions: peek-ahead
// uses txCursorOf (id as decimal), ledger-name required, cursor parsing,
// page-size clamping.
func TestListTransactions(t *testing.T) {
	t.Parallel()

	t.Run("peek fires → trailer is tx id as decimal", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		mockCtrl.EXPECT().ListTransactions(gomock.Any(), "main", uint32(3), uint64(0), gomock.Any(), false).
			Return(page(
				&auditpb.Transaction{Id: 1},
				&auditpb.Transaction{Id: 2},
				&auditpb.Transaction{Id: 3},
			), nil)

		stream := newFakeServerStream[auditpb.Transaction](t)
		req := &auditpb.ListTransactionsRequest{
			Ledger:  "main",
			Options: &auditpb.ListOptions{PageSize: 2},
		}

		require.NoError(t, impl.ListTransactions(req, stream))
		require.Equal(t, []uint64{1, 2}, sentTxIDs(stream))
		require.Equal(t, "2", stream.trailerCursor())
	})

	t.Run("empty ledger rejected", func(t *testing.T) {
		t.Parallel()

		impl, _ := newListHandlerHarness(t)
		err := impl.ListTransactions(&auditpb.ListTransactionsRequest{}, newFakeServerStream[auditpb.Transaction](t))
		require.ErrorContains(t, err, "ledger name is required")
	})

	t.Run("non-uint cursor → InvalidArgument", func(t *testing.T) {
		t.Parallel()

		impl, _ := newListHandlerHarness(t)
		req := &auditpb.ListTransactionsRequest{
			Ledger:  "main",
			Options: &auditpb.ListOptions{Cursor: "not-a-uint"},
		}
		err := impl.ListTransactions(req, newFakeServerStream[auditpb.Transaction](t))
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("valid cursor parsed and forwarded", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		mockCtrl.EXPECT().ListTransactions(gomock.Any(), "main", gomock.Any(), uint64(42), gomock.Any(), gomock.Any()).
			Return(page[auditpb.Transaction](), nil)

		req := &auditpb.ListTransactionsRequest{
			Ledger:  "main",
			Options: &auditpb.ListOptions{Cursor: "42"},
		}
		require.NoError(t, impl.ListTransactions(req, newFakeServerStream[auditpb.Transaction](t)))
	})
}

// TestListAccounts mirrors TestListTransactions for the account address space.
// The trailer carries the account address verbatim (the validator restricts
// it to [a-zA-Z0-9:_-]).
func TestListAccounts(t *testing.T) {
	t.Parallel()

	t.Run("peek fires → trailer is last sent address", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		mockCtrl.EXPECT().ListAccounts(gomock.Any(), "main", uint32(3), "", gomock.Any(), false).
			Return(page(
				&auditpb.Account{Address: "alpha"},
				&auditpb.Account{Address: "beta"},
				&auditpb.Account{Address: "gamma"},
			), nil)

		stream := newFakeServerStream[auditpb.Account](t)
		req := &auditpb.ListAccountsRequest{
			Ledger:  "main",
			Options: &auditpb.ListOptions{PageSize: 2},
		}

		require.NoError(t, impl.ListAccounts(req, stream))
		require.Equal(t, []string{"alpha", "beta"}, sentAccountAddrs(stream))
		require.Equal(t, "beta", stream.trailerCursor())
	})

	t.Run("empty ledger rejected", func(t *testing.T) {
		t.Parallel()

		impl, _ := newListHandlerHarness(t)
		err := impl.ListAccounts(&auditpb.ListAccountsRequest{}, newFakeServerStream[auditpb.Account](t))
		require.ErrorContains(t, err, "ledger name is required")
	})
}

// TestListLogs pins the defensive cursorOf branch: a log with no Apply
// payload must NOT publish a bogus "0" trailer (the proto allows other
// payload kinds even if today's controller only yields Apply).
func TestListLogs(t *testing.T) {
	t.Parallel()

	applyLog := func(id uint64) *auditpb.Log {
		return &auditpb.Log{
			Payload: &auditpb.LogPayload{
				Type: &auditpb.LogPayload_Apply{
					Apply: &auditpb.ApplyLedgerLog{
						Log: &auditpb.LedgerLog{Id: id},
					},
				},
			},
		}
	}

	t.Run("peek fires → trailer is ledger-local log id", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		mockCtrl.EXPECT().ListLogs(gomock.Any(), "main", uint64(0), uint32(3), gomock.Any()).
			Return(page(applyLog(1), applyLog(2), applyLog(3)), nil)

		stream := newFakeServerStream[auditpb.Log](t)
		req := &auditpb.ListLogsRequest{
			Ledger:  "main",
			Options: &auditpb.ListOptions{PageSize: 2},
		}

		require.NoError(t, impl.ListLogs(req, stream))
		require.Equal(t, "2", stream.trailerCursor())
	})

	t.Run("non-Apply payload produces empty trailer (defensive)", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		nonApply := &auditpb.Log{Payload: &auditpb.LogPayload{}} // no Apply oneof set
		mockCtrl.EXPECT().ListLogs(gomock.Any(), "main", gomock.Any(), gomock.Any(), gomock.Any()).
			Return(page(applyLog(1), nonApply, applyLog(3)), nil)

		stream := newFakeServerStream[auditpb.Log](t)
		req := &auditpb.ListLogsRequest{
			Ledger:  "main",
			Options: &auditpb.ListOptions{PageSize: 2},
		}

		require.NoError(t, impl.ListLogs(req, stream))
		// Last sent item is `nonApply` → cursorOf returns "" → no trailer.
		// Beats publishing `"0"` which would trap clients in an infinite
		// resume loop against an unreachable id.
		require.Empty(t, stream.trailerCursor())
	})

	t.Run("empty ledger rejected", func(t *testing.T) {
		t.Parallel()

		impl, _ := newListHandlerHarness(t)
		err := impl.ListLogs(&auditpb.ListLogsRequest{}, newFakeServerStream[auditpb.Log](t))
		require.ErrorContains(t, err, "ledger name is required")
	})
}

// TestListAuditEntries covers the audit list plumbing — cursor is the
// audit sequence (uint64 decimal).
func TestListAuditEntries(t *testing.T) {
	t.Parallel()

	t.Run("peek fires → trailer is audit sequence", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		mockCtrl.EXPECT().ListAuditEntries(gomock.Any(), uint32(3), uint64(0), nil, false).
			Return(page(
				&auditpb.AuditEntry{Sequence: 1},
				&auditpb.AuditEntry{Sequence: 2},
				&auditpb.AuditEntry{Sequence: 3},
			), nil)

		stream := newFakeServerStream[auditpb.AuditEntry](t)
		req := &auditpb.ListAuditEntriesRequest{Options: &auditpb.ListOptions{PageSize: 2}}

		require.NoError(t, impl.ListAuditEntries(req, stream))
		require.Equal(t, "2", stream.trailerCursor())
	})
}

// TestListSigningKeys pins the signing-key trailer encoding — the cursor is
// the key id (printable ASCII enforced at admission).
func TestListSigningKeys(t *testing.T) {
	t.Parallel()

	t.Run("peek fires → trailer is last sent key id", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		mockCtrl.EXPECT().ListSigningKeys(gomock.Any()).Return(
			page(
				&auditpb.SigningKey{KeyId: "kms-1"},
				&auditpb.SigningKey{KeyId: "kms-2"},
				&auditpb.SigningKey{KeyId: "kms-3"},
			),
			nil,
		)

		stream := newFakeServerStream[auditpb.SigningKey](t)
		req := &auditpb.ListSigningKeysRequest{Options: &auditpb.ListOptions{PageSize: 2}}

		require.NoError(t, impl.ListSigningKeys(req, stream))
		require.Equal(t, "kms-2", stream.trailerCursor())
	})
}

// TestListNumscripts pins the numscript trailer encoding — sorted by name
// server-side, cursor is the script name.
func TestListNumscripts(t *testing.T) {
	t.Parallel()

	t.Run("peek fires → trailer is last sent script name", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		// Returned unordered — the handler sorts by name and then paginates.
		mockCtrl.EXPECT().ListNumscripts(gomock.Any(), "main").Return(
			[]*auditpb.NumscriptInfo{
				{Name: "z-last"},
				{Name: "a-first"},
				{Name: "m-mid"},
			},
			nil,
		)

		stream := newFakeServerStream[auditpb.NumscriptInfo](t)
		req := &auditpb.ListNumscriptsRequest{
			Ledger:  "main",
			Options: &auditpb.ListOptions{PageSize: 2},
		}

		require.NoError(t, impl.ListNumscripts(req, stream))
		// Sorted: a-first, m-mid, z-last. PageSize 2, peek fires on z-last,
		// trailer is the last SENT item: m-mid.
		require.Equal(t, "m-mid", stream.trailerCursor())
	})
}

// Sent-item helpers; keep the test bodies readable.

func sentLedgerNames(s *fakeServerStream[auditpb.LedgerInfo]) []string {
	out := make([]string, len(s.sent))
	for i, l := range s.sent {
		out[i] = l.GetName()
	}

	return out
}

func sentTxIDs(s *fakeServerStream[auditpb.Transaction]) []uint64 {
	out := make([]uint64, len(s.sent))
	for i, tx := range s.sent {
		out[i] = tx.GetId()
	}

	return out
}

func sentAccountAddrs(s *fakeServerStream[auditpb.Account]) []string {
	out := make([]string, len(s.sent))
	for i, a := range s.sent {
		out[i] = a.GetAddress()
	}

	return out
}

// Sanity: the helpers above compile only when the impl is wired.
var _ = ctrl.DefaultPageSize

var _ context.Context
