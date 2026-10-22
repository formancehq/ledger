package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/application/ctrl/ctrlmock"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
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
			page(&ledgerpb.LedgerInfo{Name: "a"}, &ledgerpb.LedgerInfo{Name: "b"}, &ledgerpb.LedgerInfo{Name: "c"}, &ledgerpb.LedgerInfo{Name: "d"}),
			nil,
		)

		stream := newFakeServerStream[ledgerpb.LedgerInfo](t)
		req := &servicepb.ListLedgersRequest{Options: &ledgerpb.ListOptions{PageSize: 3}}

		require.NoError(t, impl.ListLedgers(req, stream))
		require.Equal(t, []string{"a", "b", "c"}, sentLedgerNames(stream))
		require.Equal(t, "c", stream.trailerCursor())
	})

	t.Run("exact page → no trailer", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		mockCtrl.EXPECT().ListLedgers(gomock.Any()).Return(
			page(&ledgerpb.LedgerInfo{Name: "a"}, &ledgerpb.LedgerInfo{Name: "b"}, &ledgerpb.LedgerInfo{Name: "c"}),
			nil,
		)

		stream := newFakeServerStream[ledgerpb.LedgerInfo](t)
		req := &servicepb.ListLedgersRequest{Options: &ledgerpb.ListOptions{PageSize: 3}}

		require.NoError(t, impl.ListLedgers(req, stream))
		require.Empty(t, stream.trailerCursor())
	})

	t.Run("controller error wrapped", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		boom := errors.New("ctrl blew up")
		mockCtrl.EXPECT().ListLedgers(gomock.Any()).Return(nil, boom)

		stream := newFakeServerStream[ledgerpb.LedgerInfo](t)
		err := impl.ListLedgers(&servicepb.ListLedgersRequest{}, stream)
		require.ErrorIs(t, err, boom)
	})

	t.Run("malformed token is rejected before any cursor is opened", func(t *testing.T) {
		t.Parallel()

		// No ListLedgers expectation: the strict mock fails the test if the
		// handler opens the storage-backed cursor it would then have to close.
		impl, _ := newListHandlerHarness(t)

		stream := newFakeServerStream[ledgerpb.LedgerInfo](t)
		err := impl.ListLedgers(&servicepb.ListLedgersRequest{Options: &ledgerpb.ListOptions{Cursor: "not-a-token"}}, stream)
		require.Equal(t, codes.InvalidArgument, status.Code(convertToGRPCError(err, testLogger())))
		require.Empty(t, stream.sent)
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
				&ledgerpb.Transaction{Id: 1},
				&ledgerpb.Transaction{Id: 2},
				&ledgerpb.Transaction{Id: 3},
			), nil)

		stream := newFakeServerStream[ledgerpb.Transaction](t)
		req := &servicepb.ListTransactionsRequest{
			Ledger:  "main",
			Options: &ledgerpb.ListOptions{PageSize: 2},
		}

		require.NoError(t, impl.ListTransactions(req, stream))
		require.Equal(t, []uint64{1, 2}, sentTxIDs(stream))
		require.Equal(t, "2", stream.trailerCursor())
	})

	t.Run("empty ledger rejected", func(t *testing.T) {
		t.Parallel()

		impl, _ := newListHandlerHarness(t)
		err := impl.ListTransactions(&servicepb.ListTransactionsRequest{}, newFakeServerStream[ledgerpb.Transaction](t))
		require.ErrorContains(t, err, "ledger name is required")
	})

	t.Run("non-uint cursor → InvalidArgument", func(t *testing.T) {
		t.Parallel()

		impl, _ := newListHandlerHarness(t)
		req := &servicepb.ListTransactionsRequest{
			Ledger:  "main",
			Options: &ledgerpb.ListOptions{Cursor: "not-a-uint"},
		}
		err := impl.ListTransactions(req, newFakeServerStream[ledgerpb.Transaction](t))
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(convertToGRPCError(err, testLogger())))
	})

	t.Run("non-decimal key → InvalidArgument", func(t *testing.T) {
		t.Parallel()

		impl, _ := newListHandlerHarness(t)
		req := &servicepb.ListTransactionsRequest{
			Ledger:  "main",
			Options: &ledgerpb.ListOptions{Cursor: pagecursor.Cursor{Key: "users:1"}.Encode()},
		}
		err := impl.ListTransactions(req, newFakeServerStream[ledgerpb.Transaction](t))
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(convertToGRPCError(err, testLogger())))
	})

	t.Run("back cursor reads the opposite order from its key", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		// The default transaction order is reverse=false at the API; a back
		// page reads it the other way, then returns the rows flipped back.
		mockCtrl.EXPECT().ListTransactions(gomock.Any(), "main", uint32(3), uint64(10), gomock.Any(), true).
			Return(page(&ledgerpb.Transaction{Id: 9}, &ledgerpb.Transaction{Id: 8}, &ledgerpb.Transaction{Id: 7}), nil)

		stream := newFakeServerStream[ledgerpb.Transaction](t)
		req := &servicepb.ListTransactionsRequest{
			Ledger:  "main",
			Options: &ledgerpb.ListOptions{PageSize: 2, Cursor: pagecursor.Cursor{Key: "10", Back: true}.Encode()},
		}
		require.NoError(t, impl.ListTransactions(req, stream))
		require.Equal(t, []uint64{8, 9}, []uint64{stream.sent[0].GetId(), stream.sent[1].GetId()})

		previous, ok := stream.previous(t)
		require.True(t, ok)
		require.Equal(t, pagecursor.Cursor{Key: "8", Back: true}, previous)
		require.Equal(t, "9", stream.trailerCursor())
	})

	t.Run("valid cursor parsed and forwarded", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		mockCtrl.EXPECT().ListTransactions(gomock.Any(), "main", gomock.Any(), uint64(42), gomock.Any(), gomock.Any()).
			Return(page[ledgerpb.Transaction](), nil)

		req := &servicepb.ListTransactionsRequest{
			Ledger:  "main",
			Options: &ledgerpb.ListOptions{Cursor: pagecursor.Cursor{Key: "42"}.Encode()},
		}
		require.NoError(t, impl.ListTransactions(req, newFakeServerStream[ledgerpb.Transaction](t)))
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
				&ledgerpb.Account{Address: "alpha"},
				&ledgerpb.Account{Address: "beta"},
				&ledgerpb.Account{Address: "gamma"},
			), nil)

		stream := newFakeServerStream[ledgerpb.Account](t)
		req := &servicepb.ListAccountsRequest{
			Ledger:  "main",
			Options: &ledgerpb.ListOptions{PageSize: 2},
		}

		require.NoError(t, impl.ListAccounts(req, stream))
		require.Equal(t, []string{"alpha", "beta"}, sentAccountAddrs(stream))
		require.Equal(t, "beta", stream.trailerCursor())
	})

	t.Run("empty ledger rejected", func(t *testing.T) {
		t.Parallel()

		impl, _ := newListHandlerHarness(t)
		err := impl.ListAccounts(&servicepb.ListAccountsRequest{}, newFakeServerStream[ledgerpb.Account](t))
		require.ErrorContains(t, err, "ledger name is required")
	})
}

// TestListLogs pins the defensive cursorOf branch: a log with no Apply
// payload must NOT publish a bogus "0" trailer (the proto allows other
// payload kinds even if today's controller only yields Apply).
func TestListLogs(t *testing.T) {
	t.Parallel()

	applyLog := func(id uint64) *ledgerpb.Log {
		return &ledgerpb.Log{
			Payload: &ledgerpb.LogPayload{
				Type: &ledgerpb.LogPayload_Apply{
					Apply: &ledgerpb.ApplyLedgerLog{
						Log: &ledgerpb.LedgerLog{Id: id},
					},
				},
			},
		}
	}

	t.Run("peek fires → trailer is ledger-local log id", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		mockCtrl.EXPECT().ListLogs(gomock.Any(), "main", uint64(0), uint32(3), gomock.Any(), false).
			Return(page(applyLog(1), applyLog(2), applyLog(3)), nil)

		stream := newFakeServerStream[ledgerpb.Log](t)
		req := &servicepb.ListLogsRequest{
			Ledger:  "main",
			Options: &ledgerpb.ListOptions{PageSize: 2},
		}

		require.NoError(t, impl.ListLogs(req, stream))
		require.Equal(t, "2", stream.trailerCursor())
	})

	t.Run("non-Apply payload produces empty trailer (defensive)", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		nonApply := &ledgerpb.Log{Payload: &ledgerpb.LogPayload{}} // no Apply oneof set
		mockCtrl.EXPECT().ListLogs(gomock.Any(), "main", gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(page(applyLog(1), nonApply, applyLog(3)), nil)

		stream := newFakeServerStream[ledgerpb.Log](t)
		req := &servicepb.ListLogsRequest{
			Ledger:  "main",
			Options: &ledgerpb.ListOptions{PageSize: 2},
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
		err := impl.ListLogs(&servicepb.ListLogsRequest{}, newFakeServerStream[ledgerpb.Log](t))
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
		req := &servicepb.ListAuditEntriesRequest{Options: &ledgerpb.ListOptions{PageSize: 2}}

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
				&ledgerpb.SigningKey{KeyId: "kms-1"},
				&ledgerpb.SigningKey{KeyId: "kms-2"},
				&ledgerpb.SigningKey{KeyId: "kms-3"},
			),
			nil,
		)

		stream := newFakeServerStream[ledgerpb.SigningKey](t)
		req := &servicepb.ListSigningKeysRequest{Options: &ledgerpb.ListOptions{PageSize: 2}}

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
			[]*ledgerpb.NumscriptInfo{
				{Name: "z-last"},
				{Name: "a-first"},
				{Name: "m-mid"},
			},
			nil,
		)

		stream := newFakeServerStream[ledgerpb.NumscriptInfo](t)
		req := &servicepb.ListNumscriptsRequest{
			Ledger:  "main",
			Options: &ledgerpb.ListOptions{PageSize: 2},
		}

		require.NoError(t, impl.ListNumscripts(req, stream))
		// Sorted: a-first, m-mid, z-last. PageSize 2, peek fires on z-last,
		// trailer is the last SENT item: m-mid.
		require.Equal(t, "m-mid", stream.trailerCursor())
	})
}

// Sent-item helpers; keep the test bodies readable.

func sentLedgerNames(s *fakeServerStream[ledgerpb.LedgerInfo]) []string {
	out := make([]string, len(s.sent))
	for i, l := range s.sent {
		out[i] = l.GetName()
	}

	return out
}

func sentTxIDs(s *fakeServerStream[ledgerpb.Transaction]) []uint64 {
	out := make([]uint64, len(s.sent))
	for i, tx := range s.sent {
		out[i] = tx.GetId()
	}

	return out
}

func sentAccountAddrs(s *fakeServerStream[ledgerpb.Account]) []string {
	out := make([]string, len(s.sent))
	for i, a := range s.sent {
		out[i] = a.GetAddress()
	}

	return out
}

// Sanity: the helpers above compile only when the impl is wired.
var _ = ctrl.DefaultPageSize

var _ context.Context
