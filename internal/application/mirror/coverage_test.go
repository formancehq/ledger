package mirror

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestExtractMirrorNeedsCoversIngestVariants(t *testing.T) {
	t.Parallel()
	const ledger = "mirrored"
	account := func(name string) []byte { return (domain.AccountKey{LedgerName: ledger, Account: name}).Bytes() }
	volume := func(name string) []byte {
		return (domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: ledger, Account: name}, Asset: "USD", Color: "GREEN"}).Bytes()
	}
	metadata := func(name, key string) []byte {
		return (domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: ledger, Account: name}, Key: key}).Bytes()
	}
	transaction := func(id uint64) []byte { return (domain.TransactionKey{LedgerName: ledger, ID: id}).Bytes() }
	accountTarget := func(name string) *commonpb.Target {
		return &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: name}}}
	}
	transactionTarget := func(id uint64) *commonpb.Target {
		return &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: id}}
	}

	tests := []struct {
		name     string
		entry    *raftcmdpb.MirrorLogEntry
		keys     map[byte][][]byte
		accounts []string
	}{
		{
			name: "created transaction with colored postings and account metadata",
			entry: &raftcmdpb.MirrorLogEntry{Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
				Postings:        []*commonpb.Posting{{Source: "world", Destination: "alice", Asset: "USD", Color: "GREEN"}},
				AccountMetadata: map[string]*commonpb.MetadataMap{"alice": {Values: map[string]*commonpb.MetadataValue{"owner": {}}}},
			}}},
			keys:     map[byte][][]byte{dal.SubAttrVolume: {volume("world"), volume("alice")}, dal.SubAttrMetadata: {metadata("alice", "owner")}},
			accounts: []string{"world", "alice"},
		},
		{
			name: "reverted transaction",
			entry: &raftcmdpb.MirrorLogEntry{Data: &raftcmdpb.MirrorLogEntry_RevertedTransaction{RevertedTransaction: &raftcmdpb.MirrorRevertedTransaction{
				RevertedTransactionId: 7,
				ReversePostings:       []*commonpb.Posting{{Source: "alice", Destination: "world", Asset: "USD", Color: "GREEN"}},
			}}},
			keys:     map[byte][][]byte{dal.SubAttrVolume: {volume("alice"), volume("world")}, dal.SubAttrTransaction: {transaction(7)}},
			accounts: []string{"alice", "world"},
		},
		{
			name: "saved account metadata",
			entry: &raftcmdpb.MirrorLogEntry{Data: &raftcmdpb.MirrorLogEntry_SavedMetadata{SavedMetadata: &raftcmdpb.MirrorSavedMetadata{
				Target: accountTarget("alice"), Metadata: map[string]*commonpb.MetadataValue{"tag": {}},
			}}},
			keys:     map[byte][][]byte{dal.SubAttrMetadata: {metadata("alice", "tag")}},
			accounts: []string{"alice"},
		},
		{
			name: "deleted account metadata",
			entry: &raftcmdpb.MirrorLogEntry{Data: &raftcmdpb.MirrorLogEntry_DeletedMetadata{DeletedMetadata: &raftcmdpb.MirrorDeletedMetadata{
				Target: accountTarget("alice"), Key: "tag",
			}}},
			keys:     map[byte][][]byte{dal.SubAttrMetadata: {metadata("alice", "tag")}},
			accounts: []string{"alice"},
		},
		{
			name:  "saved transaction metadata",
			entry: &raftcmdpb.MirrorLogEntry{Data: &raftcmdpb.MirrorLogEntry_SavedMetadata{SavedMetadata: &raftcmdpb.MirrorSavedMetadata{Target: transactionTarget(8)}}},
			keys:  map[byte][][]byte{dal.SubAttrTransaction: {transaction(8)}},
		},
		{
			name:  "deleted transaction metadata",
			entry: &raftcmdpb.MirrorLogEntry{Data: &raftcmdpb.MirrorLogEntry_DeletedMetadata{DeletedMetadata: &raftcmdpb.MirrorDeletedMetadata{Target: transactionTarget(9), Key: "tag"}}},
			keys:  map[byte][][]byte{dal.SubAttrTransaction: {transaction(9)}},
		},
		{
			name:  "fill gap",
			entry: &raftcmdpb.MirrorLogEntry{Data: &raftcmdpb.MirrorLogEntry_FillGap{FillGap: &raftcmdpb.MirrorFillGap{}}},
		},
	}

	worker := &Worker{ledgerName: ledger}
	for _, tc := range tests {
		for _, enabled := range []bool{false, true} {
			t.Run(tc.name+"/incarnations="+map[bool]string{false: "off", true: "on"}[enabled], func(t *testing.T) {
				t.Parallel()
				order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: ledger, Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: tc.entry}},
				}}}
				aggregate, perOrder := worker.extractMirrorNeeds(commands.NewCommand(order), enabled)
				require.Len(t, perOrder, 1)
				require.Equal(t, perOrder[0].Attributes, aggregate.Attributes)
				ledgerKey := (domain.LedgerKey{Name: ledger}).Bytes()
				require.True(t, aggregate.Has(dal.SubAttrLedger, ledgerKey))
				require.True(t, aggregate.Has(dal.SubAttrBoundary, ledgerKey))
				for code, keys := range tc.keys {
					require.Equal(t, len(keys), aggregate.Count(code))
					for _, key := range keys {
						require.True(t, aggregate.Has(code, key))
					}
				}
				wantKeys := 2 // ledger and boundary
				for _, keys := range tc.keys {
					wantKeys += len(keys)
				}
				if enabled {
					wantKeys += len(tc.accounts)
					require.Equal(t, len(tc.accounts), aggregate.Count(dal.SubAttrAccountIncarnation))
					for _, name := range tc.accounts {
						require.True(t, aggregate.Has(dal.SubAttrAccountIncarnation, account(name)))
					}
				} else {
					require.Zero(t, aggregate.Count(dal.SubAttrAccountIncarnation))
				}
				require.Equal(t, wantKeys, aggregate.AttributeKeysCount())
			})
		}
	}
}
