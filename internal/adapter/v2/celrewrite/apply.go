package celrewrite

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// cloneEntry deep-copies the entry so Apply can mutate freely without touching
// the caller's log. proto.Clone is the correct choice here — it copies through
// oneof interfaces and nested maps.
func cloneEntry(entry *raftcmdpb.MirrorLogEntry) *raftcmdpb.MirrorLogEntry {
	return proto.Clone(entry).(*raftcmdpb.MirrorLogEntry)
}

// hasRewritableVariant reports whether the entry's data oneof carries a
// variant the rewrite engine can address. Fill-gap and nil pass through
// untouched (Apply exits early on these).
func hasRewritableVariant(entry *raftcmdpb.MirrorLogEntry) bool {
	switch entry.GetData().(type) {
	case *raftcmdpb.MirrorLogEntry_CreatedTransaction,
		*raftcmdpb.MirrorLogEntry_RevertedTransaction,
		*raftcmdpb.MirrorLogEntry_SavedMetadata,
		*raftcmdpb.MirrorLogEntry_DeletedMetadata:
		return true
	}

	return false
}

// validateAddresses is the final safety net after all rules have run: every
// address slot that a rewrite could have touched is validated against the
// platform's account-address charset. An invalid rewrite (or an unlucky
// collision that leaves an address empty) fails the batch loudly, so the
// worker retries rather than shipping bad data.
func validateAddresses(entry *raftcmdpb.MirrorLogEntry) error {
	switch data := entry.GetData().(type) {
	case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
		if err := validatePostingAddresses(data.CreatedTransaction.GetPostings()); err != nil {
			return err
		}

		for acc := range data.CreatedTransaction.GetAccountMetadata() {
			if err := validateAccountAddress(acc); err != nil {
				return fmt.Errorf("account-metadata address %q invalid: %w", acc, err)
			}
		}

	case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
		if err := validatePostingAddresses(data.RevertedTransaction.GetReversePostings()); err != nil {
			return err
		}

	case *raftcmdpb.MirrorLogEntry_SavedMetadata:
		if err := validateOptionalTargetAddress(data.SavedMetadata.GetTarget()); err != nil {
			return err
		}

	case *raftcmdpb.MirrorLogEntry_DeletedMetadata:
		if err := validateOptionalTargetAddress(data.DeletedMetadata.GetTarget()); err != nil {
			return err
		}
	}

	return nil
}

func validatePostingAddresses(postings []*commonpb.Posting) error {
	for i, p := range postings {
		if err := validateAccountAddress(p.GetSource()); err != nil {
			return fmt.Errorf("posting %d source %q invalid: %w", i, p.GetSource(), err)
		}

		if err := validateAccountAddress(p.GetDestination()); err != nil {
			return fmt.Errorf("posting %d destination %q invalid: %w", i, p.GetDestination(), err)
		}
	}

	return nil
}

// validateOptionalTargetAddress re-validates the target address of a metadata
// op if it addresses an account. TransactionId targets carry no address; a
// nil target is only produced by invalid upstream code and is left to the
// caller (typically the FSM) to reject.
func validateOptionalTargetAddress(t *commonpb.Target) error {
	if t == nil {
		return nil
	}

	acc := t.GetAccount()
	if acc == nil {
		return nil
	}

	if err := validateAccountAddress(acc.GetAddr()); err != nil {
		return fmt.Errorf("target address %q invalid: %w", acc.GetAddr(), err)
	}

	return nil
}
