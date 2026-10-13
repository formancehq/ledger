package domain

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func mustFailureDetails(code string, facts map[string]string) *commonpb.FailureDetails {
	details, err := commonpb.NewFailureDetails(code, facts)
	if err != nil {
		panic(fmt.Errorf("invalid failure details for %s: %w", code, err))
	}

	return details
}

func (e *validationSentinel) FailureDetails() *commonpb.FailureDetails {
	return mustFailureDetails(e.code, nil)
}

func (errMetadataLimitsUnconfigured) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_MetadataLimitsUnconfigured{
		MetadataLimitsUnconfigured: &commonpb.MetadataLimitsUnconfiguredDetails{},
	}}
}

func (e *ErrTransactionReferenceConflict) FailureDetails() *commonpb.FailureDetails {
	details := &commonpb.TransactionReferenceConflictDetails{
		Ledger: new(e.Ledger), Reference: new(e.Reference), ExistingTransactionId: new(e.ExistingTransactionID),
	}

	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_TransactionReferenceConflict{
		TransactionReferenceConflict: details,
	}}
}

func (e *ErrInsufficientFunds) FailureDetails() *commonpb.FailureDetails {
	if !e.ColorKnown {
		return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_NumscriptInsufficientFunds{
			NumscriptInsufficientFunds: &commonpb.NumscriptInsufficientFundsDetails{
				Asset: new(e.Asset), Amount: new(e.Amount), Balance: new(e.Balance),
			},
		}}
	}
	details := &commonpb.InsufficientFundsDetails{
		Account: new(e.Account), Asset: new(e.Asset),
		Amount: new(e.Amount), Balance: new(e.Balance), Color: new(e.Color),
	}

	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_InsufficientFunds{InsufficientFunds: details}}
}

func (e *ErrNumscriptNotFound) FailureDetails() *commonpb.FailureDetails {
	if e.Version != "" {
		return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_NumscriptVersionNotFound{
			NumscriptVersionNotFound: &commonpb.NumscriptVersionNotFoundDetails{Name: new(e.Name), Version: new(e.Version)},
		}}
	}

	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_NumscriptNotFound{
		NumscriptNotFound: &commonpb.NumscriptNotFoundDetails{Name: new(e.Name)},
	}}
}

func (e *ErrMetadataKeyValidation) FailureDetails() *commonpb.FailureDetails {
	switch cause := e.Cause.FailureDetails().GetKind().(type) {
	case *commonpb.FailureDetails_MetadataLimitExceeded:
		d := cause.MetadataLimitExceeded

		return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_MetadataKeyLimitExceeded{
			MetadataKeyLimitExceeded: &commonpb.MetadataKeyLimitExceededDetails{
				Dimension: d.Dimension, Limit: d.Limit, Actual: d.Actual, Key: new(e.Key),
			},
		}}
	case *commonpb.FailureDetails_MetadataKeyInvalidChar:
		return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_MetadataKeyInvalidChar{
			MetadataKeyInvalidChar: &commonpb.MetadataKeyInvalidCharDetails{Key: new(e.Key)},
		}}
	case *commonpb.FailureDetails_MetadataValueContainsNullByte:
		return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_MetadataValueContainsNullByte{
			MetadataValueContainsNullByte: &commonpb.MetadataValueContainsNullByteDetails{Key: new(e.Key)},
		}}
	default:
		panic(fmt.Errorf("metadata key context cannot wrap %T", cause))
	}
}

func (e *ErrAccountValidation) FailureDetails() *commonpb.FailureDetails {
	switch cause := e.Cause.FailureDetails().GetKind().(type) {
	case *commonpb.FailureDetails_MetadataKeyEmpty:
		return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_AccountMetadataKeyEmpty{
			AccountMetadataKeyEmpty: &commonpb.AccountMetadataKeyEmptyDetails{Account: new(e.Account)},
		}}
	case *commonpb.FailureDetails_MetadataKeyInvalidChar:
		return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_AccountMetadataKeyInvalidChar{
			AccountMetadataKeyInvalidChar: &commonpb.AccountMetadataKeyInvalidCharDetails{
				Key: cause.MetadataKeyInvalidChar.Key, Account: new(e.Account),
			},
		}}
	case *commonpb.FailureDetails_MetadataValueContainsNullByte:
		return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_AccountMetadataValueContainsNullByte{
			AccountMetadataValueContainsNullByte: &commonpb.AccountMetadataValueContainsNullByteDetails{
				Key: cause.MetadataValueContainsNullByte.Key, Account: new(e.Account),
			},
		}}
	case *commonpb.FailureDetails_MetadataLimitsUnconfigured:
		return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_AccountMetadataLimitsUnconfigured{
			AccountMetadataLimitsUnconfigured: &commonpb.AccountMetadataLimitsUnconfiguredDetails{Account: new(e.Account)},
		}}
	case *commonpb.FailureDetails_MetadataLimitExceeded:
		d := cause.MetadataLimitExceeded

		return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_AccountMetadataLimitExceeded{
			AccountMetadataLimitExceeded: &commonpb.AccountMetadataLimitExceededDetails{
				Dimension: d.Dimension, Limit: d.Limit, Actual: d.Actual, Account: new(e.Account),
			},
		}}
	case *commonpb.FailureDetails_MetadataKeyLimitExceeded:
		d := cause.MetadataKeyLimitExceeded

		return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_AccountMetadataKeyLimitExceeded{
			AccountMetadataKeyLimitExceeded: &commonpb.AccountMetadataKeyLimitExceededDetails{
				Dimension: d.Dimension, Limit: d.Limit, Actual: d.Actual, Key: d.Key, Account: new(e.Account),
			},
		}}
	default:
		panic(fmt.Errorf("account metadata context cannot wrap %T", cause))
	}
}

func (e *ReplayedFailure) FailureDetails() *commonpb.FailureDetails {
	if e.Details == nil {
		return nil
	}

	return proto.Clone(e.Details).(*commonpb.FailureDetails)
}

func (e *ErrNumscriptCompile) FailureDetails() *commonpb.FailureDetails {
	if e.Details != nil {
		return proto.Clone(e.Details).(*commonpb.FailureDetails)
	}

	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_NumscriptCompileError{
		NumscriptCompileError: &commonpb.NumscriptCompileErrorDetails{},
	}}
}

func (e *ErrNumscriptExecution) FailureDetails() *commonpb.FailureDetails {
	if e.Details != nil {
		return proto.Clone(e.Details).(*commonpb.FailureDetails)
	}

	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_NumscriptExecutionError{
		NumscriptExecutionError: &commonpb.NumscriptExecutionErrorDetails{},
	}}
}

func (e *ErrNumscriptRuntime) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_NumscriptRuntime{
		NumscriptRuntime: &commonpb.NumscriptRuntimeDetails{},
	}}
}

func (*ErrStorageOperation) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_StorageOperationFailed{
		StorageOperationFailed: &commonpb.StorageOperationFailedDetails{},
	}}
}

func (e *ErrSequenceExhausted) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_SequenceExhausted{SequenceExhausted: &commonpb.SequenceExhaustedDetails{
		Counter: new(string(e.Counter)),
	}}}
}

func (errStaleProposal) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_StaleProposal{StaleProposal: &commonpb.StaleProposalDetails{}}}
}

func (errStaleInputsResolution) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_StaleInputsResolution{StaleInputsResolution: &commonpb.StaleInputsResolutionDetails{}}}
}

func (e *ErrLedgerAlreadyExists) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_LedgerAlreadyExists{LedgerAlreadyExists: &commonpb.LedgerAlreadyExistsDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrLedgerNotFound) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_LedgerNotFound{LedgerNotFound: &commonpb.LedgerNotFoundDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrLedgerDeleted) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_LedgerDeleted{LedgerDeleted: &commonpb.LedgerDeletedDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrIdempotencyKeyConflict) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_IdempotencyKeyConflict{IdempotencyKeyConflict: &commonpb.IdempotencyKeyConflictDetails{
		Key: new(e.Key),
	}}}
}

func (e *ErrInvalidSkippableReason) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_InvalidSkippableReason{InvalidSkippableReason: &commonpb.InvalidSkippableReasonDetails{
		ProvidedReason: new(ReasonString(e.Provided)),
	}}}
}

func (e *ErrTransactionNotFound) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_TransactionNotFound{TransactionNotFound: &commonpb.TransactionNotFoundDetails{
		TransactionId: new(e.TransactionID),
	}}}
}

func (e *ErrTransactionReferenceNotFound) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_TransactionReferenceNotFound{TransactionReferenceNotFound: &commonpb.TransactionReferenceNotFoundDetails{
		Reference: new(e.Reference),
	}}}
}

func (e *ErrTransactionAlreadyReverted) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_TransactionAlreadyReverted{TransactionAlreadyReverted: &commonpb.TransactionAlreadyRevertedDetails{
		TransactionId: new(e.TransactionID),
	}}}
}

func (e *ErrRevertTargetCreatedInBatch) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_RevertTargetCreatedInBatch{RevertTargetCreatedInBatch: &commonpb.RevertTargetCreatedInBatchDetails{
		TransactionId: new(e.TransactionID),
	}}}
}

func (e *ErrVolumeOverflow) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_VolumeOverflow{VolumeOverflow: &commonpb.VolumeOverflowDetails{
		Account: new(e.Account),
		Asset:   new(e.Asset),
		Color:   new(e.Color),
		Side:    new(e.Side),
		Amount:  new(e.Amount),
		Current: new(e.Current),
	}}}
}

func (e *ErrBalanceNotFound) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_BalanceNotFound{BalanceNotFound: &commonpb.BalanceNotFoundDetails{
		Account: new(e.Account),
		Asset:   new(e.Asset),
	}}}
}

func (e *ErrSinkAlreadyExists) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_SinkAlreadyExists{SinkAlreadyExists: &commonpb.SinkAlreadyExistsDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrSinkBatchSizeTooLarge) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_SinkBatchSizeTooLarge{SinkBatchSizeTooLarge: &commonpb.SinkBatchSizeTooLargeDetails{
		Name:      new(e.Name),
		BatchSize: new(uint64(e.BatchSize)),
		Max:       new(uint64(e.Max)),
	}}}
}

func (e *ErrMetadataNotFound) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_MetadataNotFound{MetadataNotFound: &commonpb.MetadataNotFoundDetails{
		Target: new(e.Target),
		Key:    new(e.Key),
	}}}
}

func (e *ErrSinkNotFound) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_SinkNotFound{SinkNotFound: &commonpb.SinkNotFoundDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrSinkControllerMismatch) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_SinkControllerMismatch{SinkControllerMismatch: &commonpb.SinkControllerMismatchDetails{
		Name:         new(e.Name),
		ControllerId: new(e.ControllerID),
	}}}
}

func (e *ErrInvalidCronExpression) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_InvalidCronExpression{InvalidCronExpression: &commonpb.InvalidCronExpressionDetails{
		Expression: new(e.Expression),
	}}}
}

func (e *ErrLedgerInMirrorMode) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_LedgerInMirrorMode{LedgerInMirrorMode: &commonpb.LedgerInMirrorModeDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrLedgerNotInMirrorMode) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_LedgerNotInMirrorMode{LedgerNotInMirrorMode: &commonpb.LedgerNotInMirrorModeDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrMirrorV2LogIDGap) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_MirrorV2LogIdGap{MirrorV2LogIdGap: &commonpb.MirrorV2LogIdGapDetails{
		Name:     new(e.Name),
		Got:      new(e.Got),
		Expected: new(e.Expected),
	}}}
}

func (e *ErrMirrorV2LogIDInvalid) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_MirrorV2LogIdInvalid{MirrorV2LogIdInvalid: &commonpb.MirrorV2LogIdInvalidDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrPreparedQueryAlreadyExists) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_PreparedQueryAlreadyExists{PreparedQueryAlreadyExists: &commonpb.PreparedQueryAlreadyExistsDetails{
		Ledger: new(e.Ledger),
		Name:   new(e.Name),
	}}}
}

func (e *ErrPreparedQueryNotFound) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_PreparedQueryNotFound{PreparedQueryNotFound: &commonpb.PreparedQueryNotFoundDetails{
		Ledger: new(e.Ledger),
		Name:   new(e.Name),
	}}}
}

func (e *ErrIndexAlreadyExists) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_IndexAlreadyExists{IndexAlreadyExists: &commonpb.IndexAlreadyExistsDetails{
		Index: new(e.Index),
	}}}
}

func (e *ErrIndexNotFound) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_IndexNotFound{IndexNotFound: &commonpb.IndexNotFoundDetails{
		Index: new(e.Index),
	}}}
}

func (e *ErrMetadataFieldNotInSchema) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_MetadataFieldNotInSchema{MetadataFieldNotInSchema: &commonpb.MetadataFieldNotInSchemaDetails{
		Target: new(e.Target),
		Key:    new(e.Key),
	}}}
}

func (e *ErrIndexBuilding) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_IndexBuilding{IndexBuilding: &commonpb.IndexBuildingDetails{
		Index: new(e.Index),
	}}}
}

func (e *ErrCheckpointNotReady) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_CheckpointNotReady{CheckpointNotReady: &commonpb.CheckpointNotReadyDetails{
		CheckpointId: new(e.CheckpointID),
	}}}
}

func (e *ErrIndexInconsistent) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_IndexInconsistent{IndexInconsistent: &commonpb.IndexInconsistentDetails{
		Index: new(e.Index),
	}}}
}

func (e *ErrNumscriptVersionAlreadyExists) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_NumscriptVersionAlreadyExists{NumscriptVersionAlreadyExists: &commonpb.NumscriptVersionAlreadyExistsDetails{
		Name:    new(e.Name),
		Version: new(e.Version),
	}}}
}

func (e *ErrNumscriptInvalidVersion) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_NumscriptInvalidVersion{NumscriptInvalidVersion: &commonpb.NumscriptInvalidVersionDetails{
		Version: new(e.Version),
	}}}
}

func (e *ErrAccountNotMatchingType) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_AccountNotMatchingType{AccountNotMatchingType: &commonpb.AccountNotMatchingTypeDetails{
		Address: new(e.Address),
	}}}
}

func (e *ErrAccountTypeNotFound) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_AccountTypeNotFound{AccountTypeNotFound: &commonpb.AccountTypeNotFoundDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrAccountTypeAlreadyExists) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_AccountTypeAlreadyExists{AccountTypeAlreadyExists: &commonpb.AccountTypeAlreadyExistsDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrAccountTypeConflict) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_AccountTypeConflict{AccountTypeConflict: &commonpb.AccountTypeConflictDetails{
		Pattern:         new(e.NewPattern),
		ExistingName:    new(e.ExistingName),
		ExistingPattern: new(e.ExistingPattern),
	}}}
}

func (e *ErrInvalidPattern) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_InvalidPattern{InvalidPattern: &commonpb.InvalidPatternDetails{
		Pattern: new(e.Pattern),
	}}}
}

func (e *ErrAccountTypeHasAccounts) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_AccountTypeHasAccounts{AccountTypeHasAccounts: &commonpb.AccountTypeHasAccountsDetails{
		Name: new(e.Name),
	}}}
}

func (e *ErrNumscriptParse) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_NumscriptParseError{NumscriptParseError: &commonpb.NumscriptParseErrorDetails{}}}
}

func (e *ErrBalanceNotPreloaded) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_BalanceNotPreloaded{BalanceNotPreloaded: &commonpb.BalanceNotPreloadedDetails{
		Account: new(e.Account),
		Asset:   new(e.Asset),
		Color:   new(e.Color),
	}}}
}

func (e *ErrTransientAccountNonZero) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_TransientAccountNonZero{TransientAccountNonZero: &commonpb.TransientAccountNonZeroDetails{
		Accounts: new(e.joinedAccounts()),
	}}}
}

func (e *ErrFilterCompilation) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_FilterCompilationError{FilterCompilationError: &commonpb.FilterCompilationErrorDetails{}}}
}

func (e *ErrInvalidOrderType) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_InvalidOrderType{InvalidOrderType: &commonpb.InvalidOrderTypeDetails{}}}
}

func (e *ErrStaleClusterPolicy) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_StaleClusterPolicy{StaleClusterPolicy: &commonpb.StaleClusterPolicyDetails{
		ProposedRevision: new(e.ProposedRevision),
		AppliedRevision:  new(e.AppliedRevision),
	}}}
}

func (e *ErrClusterPolicyRevisionConflict) FailureDetails() *commonpb.FailureDetails {
	return nil // This internal proposal rejection is never an audited outcome.
}

func (e *ErrClusterPolicyInvalid) FailureDetails() *commonpb.FailureDetails {
	return nil // This internal proposal rejection is never an audited outcome.
}

func (e *ErrInvalidApplyType) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_InvalidApplyType{InvalidApplyType: &commonpb.InvalidApplyTypeDetails{}}}
}

func (e *ErrTransactionStateInconsistent) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_TransactionStateInconsistent{TransactionStateInconsistent: &commonpb.TransactionStateInconsistentDetails{
		TransactionId: new(e.TransactionID),
	}}}
}

func (errCheckpointIDRequired) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_CheckpointIdRequired{CheckpointIdRequired: &commonpb.CheckpointIdRequiredDetails{}}}
}

func (e *ErrCheckpointLimitReached) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_CheckpointLimitReached{CheckpointLimitReached: &commonpb.CheckpointLimitReachedDetails{
		Limit: new(e.Limit),
	}}}
}

func (e *ErrMetadataLimitExceeded) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_MetadataLimitExceeded{MetadataLimitExceeded: &commonpb.MetadataLimitExceededDetails{
		Dimension: new(e.Dimension),
		Limit:     new(e.Limit),
		Actual:    new(e.Actual),
	}}}
}

func (e *ErrCheckpointNotFound) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_CheckpointNotFound{CheckpointNotFound: &commonpb.CheckpointNotFoundDetails{
		CheckpointId: new(e.CheckpointID),
	}}}
}

func (e *ErrVolumeNotMaterialized) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_VolumeNotMaterialized{VolumeNotMaterialized: &commonpb.VolumeNotMaterializedDetails{
		Account: new(e.Account),
		Asset:   new(e.Asset),
		Color:   new(e.Color),
		Side:    new(e.Side),
	}}}
}

func (e *ErrInvalidExecutionPlan) FailureDetails() *commonpb.FailureDetails {
	return &commonpb.FailureDetails{Kind: &commonpb.FailureDetails_InvalidExecutionPlan{InvalidExecutionPlan: &commonpb.InvalidExecutionPlanDetails{}}}
}
