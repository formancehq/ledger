package ledger

import (
	"errors"
	"fmt"
	"math"
	"slices"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/accounts"
)

// Imported IDs must leave room for the first live write's max(id)+1 sequence
// reset. Zero and gaps are valid: older ledgers start at zero, and PostgreSQL
// sequence allocations are not rolled back with a failed transaction.
func validateImportID(id *uint64) error {
	if id == nil || *id >= math.MaxInt64 {
		return errors.New("identifier must be present and less than 9223372036854775807")
	}
	return nil
}

func validateImportTransaction(tx ledger.Transaction) error {
	if err := validateImportID(tx.ID); err != nil {
		return err
	}
	if tx.RevertedAt != nil {
		return errors.New("a new transaction cannot already be reverted")
	}
	if len(tx.Postings) == 0 {
		return ErrNoPostings
	}
	// Use the live posting contract, including zero amounts. Restoration cannot
	// reconstruct Numscript overdraft authorizations from postings alone.
	if i, err := tx.Postings.Validate(); err != nil {
		return fmt.Errorf("posting %d: %w", i, err)
	}
	return nil
}

func validateImportTarget(targetType string, targetID any) error {
	switch targetType {
	case ledger.MetaTargetTypeTransaction:
		id, ok := targetID.(uint64)
		if !ok {
			return errors.New("invalid transaction metadata target")
		}
		return validateImportID(&id)
	case ledger.MetaTargetTypeAccount:
		address, ok := targetID.(string)
		if !ok || !accounts.ValidateAddress(address) {
			return errors.New("invalid account metadata target")
		}
		return nil
	default:
		return errors.New("unknown metadata target type")
	}
}

func validateImportLog(log ledger.Log) error {
	if err := validateImportID(log.ID); err != nil {
		return err
	}
	if log.Data == nil {
		return errors.New("missing log payload")
	}
	switch payload := log.Data.(type) {
	case ledger.CreatedTransaction:
		if err := validateImportTransaction(payload.Transaction); err != nil {
			return err
		}
	case ledger.RevertedTransaction:
		if err := validateImportID(payload.RevertedTransaction.ID); err != nil {
			return err
		}
		if payload.RevertedTransaction.RevertedAt == nil || payload.RevertedTransaction.RevertedAt.IsZero() {
			return errors.New("missing revert date")
		}
		if err := validateImportTransaction(payload.RevertTransaction); err != nil {
			return err
		}
	case ledger.SavedMetadata:
		if err := validateImportTarget(payload.TargetType, payload.TargetID); err != nil {
			return err
		}
	case ledger.DeletedMetadata:
		if err := validateImportTarget(payload.TargetType, payload.TargetID); err != nil {
			return err
		}
	case ledger.InsertedSchema:
	default:
		return errors.New("unsupported log payload")
	}
	if log.Type != log.Data.Type() {
		return errors.New("log type does not match its payload")
	}
	return nil
}

func equalImportPostings(a, b ledger.Postings) bool {
	return slices.EqualFunc(a, b, func(a, b ledger.Posting) bool {
		return a.Source == b.Source && a.Destination == b.Destination &&
			a.Asset == b.Asset && a.Amount != nil && b.Amount != nil && a.Amount.Cmp(b.Amount) == 0
	})
}
