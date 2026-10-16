package ctrl

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestScanAccountReturnsMaterializationDate(t *testing.T) {
	store := newCtrlTestStore(t)
	attrs := attributes.New()
	key := domain.AccountKey{LedgerName: "ledger", Account: "account:1"}
	batch := store.OpenWriteSession()
	_, err := attrs.AccountInventory.Set(batch, key.Bytes(), &raftcmdpb.AccountInventoryState{CreatedAt: 12345, MetadataCount: 1})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	account, err := scanAccount(reader, attrs, key.LedgerName, key.Account, false)
	require.NoError(t, err)
	require.Equal(t, uint64(12345), account.GetInsertionDate().GetData())
}
