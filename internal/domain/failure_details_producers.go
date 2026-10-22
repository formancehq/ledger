package domain

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
)

func mustFailureDetails(code string, facts map[string]string) *ledgerpb.FailureDetails {
	details, err := ledgerpb.NewFailureDetails(code, facts)
	if err != nil {
		panic(fmt.Errorf("invalid failure details for %s: %w", code, err))
	}

	return details
}

func (e *validationSentinel) FailureDetails() *ledgerpb.FailureDetails {
	return mustFailureDetails(e.code, nil)
}

func (errMetadataLimitsUnconfigured) FailureDetails() *ledgerpb.FailureDetails {
	return nil // Technical failures are never persisted as audited outcomes.
}

func (e *ErrTransactionReferenceConflict) FailureDetails() *ledgerpb.FailureDetails {
	details := &ledgerpb.TransactionReferenceConflictDetails{
		Ledger: new(e.Ledger), Reference: new(e.Reference), ExistingTransactionId: new(e.ExistingTransactionID),
	}

	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_TransactionReferenceConflict{
		TransactionReferenceConflict: details,
	}}
}

func (e *ErrInsufficientFunds) FailureDetails() *ledgerpb.FailureDetails {
	if !e.ColorKnown {
		return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptInsufficientFunds{
			NumscriptInsufficientFunds: &ledgerpb.NumscriptInsufficientFundsDetails{
				Asset: new(e.Asset), Amount: new(e.Amount), Balance: new(e.Balance),
			},
		}}
	}
	details := &ledgerpb.InsufficientFundsDetails{
		Account: new(e.Account), Asset: new(e.Asset),
		Amount: new(e.Amount), Balance: new(e.Balance), Color: new(e.Color),
	}

	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_InsufficientFunds{InsufficientFunds: details}}
}

func (e *ErrNumscriptNotFound) FailureDetails() *ledgerpb.FailureDetails {
	if e.Version != "" {
		return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptVersionNotFound{
			NumscriptVersionNotFound: &ledgerpb.NumscriptVersionNotFoundDetails{Name: new(e.Name), Version: new(e.Version)},
		}}
	}

	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptNotFound{
		NumscriptNotFound: &ledgerpb.NumscriptNotFoundDetails{Name: new(e.Name)},
	}}
}

func (e *ErrMetadataKeyValidation) FailureDetails() *ledgerpb.FailureDetails {
	switch cause := e.Cause.FailureDetails().GetKind().(type) {
	case *ledgerpb.FailureDetails_MetadataLimitExceeded:
		d := cause.MetadataLimitExceeded

		return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_MetadataKeyLimitExceeded{
			MetadataKeyLimitExceeded: &ledgerpb.MetadataKeyLimitExceededDetails{
				Dimension: d.Dimension, Limit: d.Limit, Actual: d.Actual, Key: new(e.Key),
			},
		}}
	case *ledgerpb.FailureDetails_MetadataKeyInvalidChar:
		return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_MetadataKeyInvalidChar{
			MetadataKeyInvalidChar: &ledgerpb.MetadataKeyInvalidCharDetails{Key: new(e.Key)},
		}}
	case *ledgerpb.FailureDetails_MetadataValueContainsNullByte:
		return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_MetadataValueContainsNullByte{
			MetadataValueContainsNullByte: &ledgerpb.MetadataValueContainsNullByteDetails{Key: new(e.Key)},
		}}
	default:
		panic(fmt.Errorf("metadata key context cannot wrap %T", cause))
	}
}

func (e *ErrAccountValidation) FailureDetails() *ledgerpb.FailureDetails {
	switch cause := e.Cause.FailureDetails().GetKind().(type) {
	case *ledgerpb.FailureDetails_MetadataKeyEmpty:
		return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_AccountMetadataKeyEmpty{
			AccountMetadataKeyEmpty: &ledgerpb.AccountMetadataKeyEmptyDetails{Account: new(e.Account)},
		}}
	case *ledgerpb.FailureDetails_MetadataKeyInvalidChar:
		return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_AccountMetadataKeyInvalidChar{
			AccountMetadataKeyInvalidChar: &ledgerpb.AccountMetadataKeyInvalidCharDetails{
				Key: cause.MetadataKeyInvalidChar.Key, Account: new(e.Account),
			},
		}}
	case *ledgerpb.FailureDetails_MetadataValueContainsNullByte:
		return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_AccountMetadataValueContainsNullByte{
			AccountMetadataValueContainsNullByte: &ledgerpb.AccountMetadataValueContainsNullByteDetails{
				Key: cause.MetadataValueContainsNullByte.Key, Account: new(e.Account),
			},
		}}
	case *ledgerpb.FailureDetails_MetadataLimitExceeded:
		d := cause.MetadataLimitExceeded

		return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_AccountMetadataLimitExceeded{
			AccountMetadataLimitExceeded: &ledgerpb.AccountMetadataLimitExceededDetails{
				Dimension: d.Dimension, Limit: d.Limit, Actual: d.Actual, Account: new(e.Account),
			},
		}}
	case *ledgerpb.FailureDetails_MetadataKeyLimitExceeded:
		d := cause.MetadataKeyLimitExceeded

		return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_AccountMetadataKeyLimitExceeded{
			AccountMetadataKeyLimitExceeded: &ledgerpb.AccountMetadataKeyLimitExceededDetails{
				Dimension: d.Dimension, Limit: d.Limit, Actual: d.Actual, Key: d.Key, Account: new(e.Account),
			},
		}}
	default:
		panic(fmt.Errorf("account metadata context cannot wrap %T", cause))
	}
}

func (e *ReplayedFailure) FailureDetails() *ledgerpb.FailureDetails {
	if e.Details == nil {
		return nil
	}

	return proto.Clone(e.Details).(*ledgerpb.FailureDetails)
}

func (e *ErrNumscriptCompile) FailureDetails() *ledgerpb.FailureDetails {
	if e.Details != nil {
		return proto.Clone(e.Details).(*ledgerpb.FailureDetails)
	}

	return nil // The unclassified fallback has no auditable business code.
}

func (*unclassifiedNumscriptCompile) FailureDetails() *ledgerpb.FailureDetails {
	return nil
}

func (e *ErrNumscriptExecution) FailureDetails() *ledgerpb.FailureDetails {
	if e.Details != nil {
		return proto.Clone(e.Details).(*ledgerpb.FailureDetails)
	}

	return nil // The unclassified fallback has no auditable business code.
}

func (e *ErrNumscriptRuntime) FailureDetails() *ledgerpb.FailureDetails {
	return nil // Technical failures are never persisted as audited outcomes.
}

func (*ErrStorageOperation) FailureDetails() *ledgerpb.FailureDetails {
	return nil // Technical failures are never persisted as audited outcomes.
}

func (e *ErrSequenceExhausted) FailureDetails() *ledgerpb.FailureDetails {
	return nil // Technical failures are never persisted as audited outcomes.
}

func (errStaleProposal) FailureDetails() *ledgerpb.FailureDetails {
	return nil // Technical failures are never persisted as audited outcomes.
}

func (errStaleInputsResolution) FailureDetails() *ledgerpb.FailureDetails {
	return nil // Technical failures are never persisted as audited outcomes.
}

func (e *ErrLedgerAlreadyExists) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_LedgerAlreadyExists{LedgerAlreadyExists: &ledgerpb.LedgerAlreadyExistsDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrLedgerNotFound) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_LedgerNotFound{LedgerNotFound: &ledgerpb.LedgerNotFoundDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrLedgerDeleted) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_LedgerDeleted{LedgerDeleted: &ledgerpb.LedgerDeletedDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrIdempotencyKeyConflict) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_IdempotencyKeyConflict{IdempotencyKeyConflict: &ledgerpb.IdempotencyKeyConflictDetails{
		Key: new(e.Key),
	}}}
}

func (e *ErrInvalidSkippableReason) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_InvalidSkippableReason{InvalidSkippableReason: &ledgerpb.InvalidSkippableReasonDetails{
		ProvidedReason: new(ReasonString(e.Provided)),
	}}}
}

func (e *ErrTransactionNotFound) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_TransactionNotFound{TransactionNotFound: &ledgerpb.TransactionNotFoundDetails{
		TransactionId: new(e.TransactionID),
	}}}
}

func (e *ErrTransactionReferenceNotFound) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_TransactionReferenceNotFound{TransactionReferenceNotFound: &ledgerpb.TransactionReferenceNotFoundDetails{
		Reference: new(e.Reference),
	}}}
}

func (e *ErrTransactionAlreadyReverted) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_TransactionAlreadyReverted{TransactionAlreadyReverted: &ledgerpb.TransactionAlreadyRevertedDetails{
		TransactionId: new(e.TransactionID),
	}}}
}

func (e *ErrRevertTargetCreatedInBatch) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_RevertTargetCreatedInBatch{RevertTargetCreatedInBatch: &ledgerpb.RevertTargetCreatedInBatchDetails{
		TransactionId: new(e.TransactionID),
	}}}
}

func (e *ErrVolumeOverflow) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_VolumeOverflow{VolumeOverflow: &ledgerpb.VolumeOverflowDetails{
		Account: new(e.Account),
		Asset:   new(e.Asset),
		Color:   new(e.Color),
		Side:    new(e.Side),
		Amount:  new(e.Amount),
		Current: new(e.Current),
	}}}
}

func (e *ErrBalanceNotFound) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_BalanceNotFound{BalanceNotFound: &ledgerpb.BalanceNotFoundDetails{
		Account: new(e.Account),
		Asset:   new(e.Asset),
	}}}
}

func (e *ErrSinkAlreadyExists) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_SinkAlreadyExists{SinkAlreadyExists: &ledgerpb.SinkAlreadyExistsDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrSinkBatchSizeTooLarge) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_SinkBatchSizeTooLarge{SinkBatchSizeTooLarge: &ledgerpb.SinkBatchSizeTooLargeDetails{
		Name:      new(e.Name),
		BatchSize: new(uint64(e.BatchSize)),
		Max:       new(uint64(e.Max)),
	}}}
}

func (e *ErrMetadataNotFound) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_MetadataNotFound{MetadataNotFound: &ledgerpb.MetadataNotFoundDetails{
		Target: new(e.Target),
		Key:    new(e.Key),
	}}}
}

func (e *ErrSinkNotFound) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_SinkNotFound{SinkNotFound: &ledgerpb.SinkNotFoundDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrSinkControllerMismatch) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_SinkControllerMismatch{SinkControllerMismatch: &ledgerpb.SinkControllerMismatchDetails{
		Name:         new(e.Name),
		ControllerId: new(e.ControllerID),
	}}}
}

func (e *ErrInvalidCronExpression) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_InvalidCronExpression{InvalidCronExpression: &ledgerpb.InvalidCronExpressionDetails{
		Expression: new(e.Expression),
	}}}
}

func (e *ErrLedgerInMirrorMode) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_LedgerInMirrorMode{LedgerInMirrorMode: &ledgerpb.LedgerInMirrorModeDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrLedgerNotInMirrorMode) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_LedgerNotInMirrorMode{LedgerNotInMirrorMode: &ledgerpb.LedgerNotInMirrorModeDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrMirrorV2LogIDGap) FailureDetails() *ledgerpb.FailureDetails {
	return nil // Technical failures are never persisted as audited outcomes.
}

func (e *ErrMirrorV2LogIDInvalid) FailureDetails() *ledgerpb.FailureDetails {
	return nil // Technical failures are never persisted as audited outcomes.
}

func (e *ErrPreparedQueryAlreadyExists) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_PreparedQueryAlreadyExists{PreparedQueryAlreadyExists: &ledgerpb.PreparedQueryAlreadyExistsDetails{
		Ledger: new(e.Ledger),
		Name:   new(e.Name),
	}}}
}

func (e *ErrPreparedQueryNotFound) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_PreparedQueryNotFound{PreparedQueryNotFound: &ledgerpb.PreparedQueryNotFoundDetails{
		Ledger: new(e.Ledger),
		Name:   new(e.Name),
	}}}
}

func (e *ErrIndexAlreadyExists) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_IndexAlreadyExists{IndexAlreadyExists: &ledgerpb.IndexAlreadyExistsDetails{
		Index: new(e.Index),
	}}}
}

func (e *ErrIndexNotFound) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_IndexNotFound{IndexNotFound: &ledgerpb.IndexNotFoundDetails{
		Index: new(e.Index),
	}}}
}

func (e *ErrMetadataFieldNotInSchema) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_MetadataFieldNotInSchema{MetadataFieldNotInSchema: &ledgerpb.MetadataFieldNotInSchemaDetails{
		Target: new(e.Target),
		Key:    new(e.Key),
	}}}
}

func (e *ErrNumscriptVersionAlreadyExists) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptVersionAlreadyExists{NumscriptVersionAlreadyExists: &ledgerpb.NumscriptVersionAlreadyExistsDetails{
		Name:    new(e.Name),
		Version: new(e.Version),
	}}}
}

func (e *ErrNumscriptInvalidVersion) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptInvalidVersion{NumscriptInvalidVersion: &ledgerpb.NumscriptInvalidVersionDetails{
		Version: new(e.Version),
	}}}
}

func (e *ErrAccountNotMatchingType) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_AccountNotMatchingType{AccountNotMatchingType: &ledgerpb.AccountNotMatchingTypeDetails{
		Address: new(e.Address),
	}}}
}

func (e *ErrAccountTypeNotFound) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_AccountTypeNotFound{AccountTypeNotFound: &ledgerpb.AccountTypeNotFoundDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrAccountTypeAlreadyExists) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_AccountTypeAlreadyExists{AccountTypeAlreadyExists: &ledgerpb.AccountTypeAlreadyExistsDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrAccountTypeConflict) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_AccountTypeConflict{AccountTypeConflict: &ledgerpb.AccountTypeConflictDetails{
		Pattern:         new(e.NewPattern),
		ExistingName:    new(e.ExistingName),
		ExistingPattern: new(e.ExistingPattern),
	}}}
}

func (e *ErrInvalidPattern) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_InvalidPattern{InvalidPattern: &ledgerpb.InvalidPatternDetails{
		Pattern: new(e.Pattern),
	}}}
}

func (e *ErrAccountTypeHasAccounts) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_AccountTypeHasAccounts{AccountTypeHasAccounts: &ledgerpb.AccountTypeHasAccountsDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrNumscriptParse) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_NumscriptParseError{NumscriptParseError: &ledgerpb.NumscriptParseErrorDetails{}}}
}

func (*ErrFilterCompilation) FailureDetails() *ledgerpb.FailureDetails {
	return nil // Filter validation is outside the audited order outcome.
}

func (e *ErrBalanceNotPreloaded) FailureDetails() *ledgerpb.FailureDetails {
	return nil // Technical failures are never persisted as audited outcomes.
}

func (e *ErrTransientAccountNonZero) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_TransientAccountNonZero{TransientAccountNonZero: &ledgerpb.TransientAccountNonZeroDetails{
		Accounts: new(e.joinedAccounts()),
	}}}
}

func (e *ErrInvalidOrderType) FailureDetails() *ledgerpb.FailureDetails {
	return nil // Technical failures are never persisted as audited outcomes.
}

func (e *ErrStaleClusterPolicy) FailureDetails() *ledgerpb.FailureDetails {
	return nil // Technical failures are never persisted as audited outcomes.
}

func (e *ErrClusterPolicyRevisionConflict) FailureDetails() *ledgerpb.FailureDetails {
	return nil // This internal proposal rejection is never an audited outcome.
}

func (e *ErrClusterPolicyInvalid) FailureDetails() *ledgerpb.FailureDetails {
	return nil // This internal proposal rejection is never an audited outcome.
}

func (e *ErrInvalidApplyType) FailureDetails() *ledgerpb.FailureDetails {
	return nil // Technical failures are never persisted as audited outcomes.
}

func (e *ErrTransactionStateInconsistent) FailureDetails() *ledgerpb.FailureDetails {
	return nil // Technical failures are never persisted as audited outcomes.
}

func (errCheckpointIDRequired) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_CheckpointIdRequired{CheckpointIdRequired: &ledgerpb.CheckpointIdRequiredDetails{}}}
}

func (e *ErrCheckpointLimitReached) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_CheckpointLimitReached{CheckpointLimitReached: &ledgerpb.CheckpointLimitReachedDetails{
		Limit: new(e.Limit),
	}}}
}

func (e *ErrMetadataLimitExceeded) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_MetadataLimitExceeded{MetadataLimitExceeded: &ledgerpb.MetadataLimitExceededDetails{
		Dimension: new(e.Dimension),
		Limit:     new(e.Limit),
		Actual:    new(e.Actual),
	}}}
}

func (e *ErrCheckpointNotFound) FailureDetails() *ledgerpb.FailureDetails {
	return &ledgerpb.FailureDetails{Kind: &ledgerpb.FailureDetails_CheckpointNotFound{CheckpointNotFound: &ledgerpb.CheckpointNotFoundDetails{
		CheckpointId: new(e.CheckpointID),
	}}}
}

func (e *ErrVolumeNotMaterialized) FailureDetails() *ledgerpb.FailureDetails {
	return nil // Technical failures are never persisted as audited outcomes.
}

func (e *ErrInvalidExecutionPlan) FailureDetails() *ledgerpb.FailureDetails {
	return nil // Technical failures are never persisted as audited outcomes.
}
