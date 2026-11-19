// Package coverage declares application-level preload keys shared by proposal producers.
package coverage

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// AddVolumeNeed declares a volume and, when enabled, the account incarnation
// touched by a posting. Color is part of the volume key.
func AddVolumeNeed(p *plan.Coverage, incarnationWrites bool, ledgerName, account, asset, color string) {
	p.Add(dal.SubAttrVolume, domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: account},
		Asset:      asset,
		Color:      color,
	}.Bytes())
	if incarnationWrites {
		p.Add(dal.SubAttrAccountIncarnation, (domain.AccountKey{LedgerName: ledgerName, Account: account}).Bytes())
	}
}

// AddAccountMetadataNeed declares a metadata row and the account incarnation
// when writes to incarnations are active. Deletions need the metadata key too:
// the FSM uses it to create a cache tombstone.
func AddAccountMetadataNeed(p *plan.Coverage, incarnationWrites bool, ledgerName, account, key string) {
	p.Add(dal.SubAttrMetadata, domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: account},
		Key:        key,
	}.Bytes())
	if incarnationWrites {
		p.Add(dal.SubAttrAccountIncarnation, (domain.AccountKey{LedgerName: ledgerName, Account: account}).Bytes())
	}
}

// AddTransactionTargetNeed declares the state read for a transaction target.
func AddTransactionTargetNeed(p *plan.Coverage, ledgerName string, txID uint64) {
	p.Add(dal.SubAttrTransaction, domain.TransactionKey{LedgerName: ledgerName, ID: txID}.Bytes())
}

// AddMirrorIngestNeeds declares the payload-specific reads of a MirrorIngest
// order. Both public admission and the mirror worker add their own wrapper
// needs (ledger and boundary) before calling this function.
func AddMirrorIngestNeeds(p *plan.Coverage, ledgerName string, mi *raftcmdpb.MirrorIngestOrder, incarnationWrites bool) {
	var postings []*commonpb.Posting
	if ct := mi.GetEntry().GetCreatedTransaction(); ct != nil {
		postings = ct.GetPostings()
	} else if rt := mi.GetEntry().GetRevertedTransaction(); rt != nil {
		postings = rt.GetReversePostings()
	}
	for _, posting := range postings {
		AddVolumeNeed(p, incarnationWrites, ledgerName, posting.GetSource(), posting.GetAsset(), posting.GetColor())
		AddVolumeNeed(p, incarnationWrites, ledgerName, posting.GetDestination(), posting.GetAsset(), posting.GetColor())
	}

	if ct := mi.GetEntry().GetCreatedTransaction(); ct != nil {
		for account, metadata := range ct.GetAccountMetadata() {
			for key := range metadata.GetValues() {
				AddAccountMetadataNeed(p, incarnationWrites, ledgerName, account, key)
			}
		}
	}

	if saved := mi.GetEntry().GetSavedMetadata(); saved != nil {
		switch target := saved.GetTarget().GetTarget().(type) {
		case *commonpb.Target_Account:
			for key := range saved.GetMetadata() {
				AddAccountMetadataNeed(p, incarnationWrites, ledgerName, target.Account.GetAddr(), key)
			}
		case *commonpb.Target_TransactionId:
			AddTransactionTargetNeed(p, ledgerName, target.TransactionId)
		}
	}

	if deleted := mi.GetEntry().GetDeletedMetadata(); deleted != nil {
		switch target := deleted.GetTarget().GetTarget().(type) {
		case *commonpb.Target_Account:
			AddAccountMetadataNeed(p, incarnationWrites, ledgerName, target.Account.GetAddr(), deleted.GetKey())
		case *commonpb.Target_TransactionId:
			AddTransactionTargetNeed(p, ledgerName, target.TransactionId)
		}
	}

	if reverted := mi.GetEntry().GetRevertedTransaction(); reverted != nil {
		AddTransactionTargetNeed(p, ledgerName, reverted.GetRevertedTransactionId())
	}
}
