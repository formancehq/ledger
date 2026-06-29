package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestDeleteLedgerData_RemovesAccountMarkers is the regression test for PR #564
// finding [C]: SubAttrAccount (EN-1276 per-account existence markers) must be
// included in ledgerScopedAttrTypes so the chapter-purge range delete tombstones
// them. A stale marker left behind after a ledger is deleted is reported by the
// checker's compareAccounts as an ACCOUNT_MISMATCH (and could suppress
// default-metadata application if a ledger name were ever reused).
func TestDeleteLedgerData_RemovesAccountMarkers(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	attrs := attributes.New()

	markerKey := func(ledger, account string) []byte {
		return domain.AccountKey{LedgerName: ledger, Account: account}.Bytes()
	}

	// Seed account existence markers for two ledgers.
	batch := s.OpenWriteSession()
	_, err := attrs.Account.Set(batch, markerKey("doomed", "users:1"),
		&commonpb.AccountState{InsertionDate: commonpb.NewTimestamp(libtime.Now())})
	require.NoError(t, err)
	_, err = attrs.Account.Set(batch, markerKey("keep", "users:2"),
		&commonpb.AccountState{InsertionDate: commonpb.NewTimestamp(libtime.Now())})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Delete the doomed ledger's per-ledger data.
	batch = s.OpenWriteSession()
	require.NoError(t, deleteLedgerData(batch, "doomed"))
	require.NoError(t, batch.Commit())

	// The doomed ledger's marker is gone; another ledger's marker survives
	// (range delete is scoped by the padded ledger-name prefix).
	gone, err := attrs.Account.Get(s, markerKey("doomed", "users:1"))
	require.NoError(t, err)
	require.Nil(t, gone, "EN-1276: account markers must be range-deleted on ledger cleanup")

	survivor, err := attrs.Account.Get(s, markerKey("keep", "users:2"))
	require.NoError(t, err)
	require.NotNil(t, survivor, "another ledger's account markers must survive the cleanup")
}
