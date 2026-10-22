package main

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// validateReversal checks the acknowledged effect against the submitted intent.
// Shared accounts can change between the create and revert, so their final
// volumes cannot be inferred by subtracting these two response snapshots.
// This checks response contents, not the absence of another persisted reversal.
func validateReversal(resp *servicepb.ApplyResponse, ledger string, txID uint64, postings []*commonpb.Posting) error {
	if len(resp.GetLogs()) != 1 {
		return fmt.Errorf("expected one revert log, got %d", len(resp.GetLogs()))
	}
	apply := resp.GetLogs()[0].GetPayload().GetApply()
	if apply == nil || apply.GetLedgerName() != ledger {
		return fmt.Errorf("missing apply log for ledger %q", ledger)
	}
	reverted := apply.GetLog().GetData().GetRevertedTransaction()
	if reverted == nil || reverted.GetRevertedTransactionId() != txID || reverted.GetRevertTransaction() == nil {
		return fmt.Errorf("missing reversal of transaction %d", txID)
	}
	tx := reverted.GetRevertTransaction()
	if tx.GetId() == txID || tx.GetRevertsTransaction() != txID {
		return fmt.Errorf("invalid reversal identity or target")
	}
	if len(tx.GetPostings()) != len(postings) {
		return fmt.Errorf("expected %d reversed postings, got %d", len(postings), len(tx.GetPostings()))
	}
	for i, original := range postings {
		actual := tx.GetPostings()[i]
		if actual == nil || original == nil || actual.GetAmount() == nil || original.GetAmount() == nil {
			return fmt.Errorf("posting %d is incomplete", i)
		}
		want := original.GetAmount().ToBigInt()
		got := actual.GetAmount().ToBigInt()
		if want.Cmp(got) != 0 ||
			actual.GetSource() != original.GetDestination() || actual.GetDestination() != original.GetSource() ||
			actual.GetAsset() != original.GetAsset() || actual.GetColor() != original.GetColor() {
			return fmt.Errorf("posting %d does not exactly reverse the original", i)
		}
	}
	return nil
}
