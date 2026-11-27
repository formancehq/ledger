package admission

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/inputvalidation"
)

// These mappings preserve existing public errors while protobuf owns the
// structural rule. Every admission caller uses this table instead of inferring
// a cause from the fact that ValidateInput returned an error.
var inputValidationErrors = map[inputvalidation.Key]domain.Describable{
	{Message: "ledger.UpdatePreparedQueryRequest", Path: "target", Rule: "required"}:   domain.ErrPreparedQueryTargetRequired,
	{Message: "raft.LedgerScopedOrder", Path: "ledger", Rule: "non_zero"}:              domain.ErrLedgerNameRequired,
	{Message: "raft.RemoveAccountTypeOrder", Path: "name", Rule: "max_bytes"}:          domain.ErrAccountTypeNameTooLong,
	{Message: "raft.CreateTransactionOrder", Path: "reference", Rule: "max_bytes"}:     domain.ErrTransactionReferenceTooLong,
	{Message: "raft.ExecuteNumscriptOrder", Path: "reference", Rule: "max_bytes"}:      domain.ErrTransactionReferenceTooLong,
	{Message: "common.Posting", Path: "source", Rule: "non_zero"}:                      domain.ErrAccountAddressEmpty,
	{Message: "common.Posting", Path: "destination", Rule: "non_zero"}:                 domain.ErrAccountAddressEmpty,
	{Message: "common.Posting", Path: "amount", Rule: "required"}:                      ErrPostingAmountRequired,
	{Message: "common.Posting", Path: "asset", Rule: "non_zero"}:                       domain.ErrAssetInvalid,
	{Message: "raft.CreatePreparedQueryOrder", Path: "query", Rule: "required"}:        domain.ErrPreparedQueryRequired,
	{Message: "common.PreparedQuery", Path: "name", Rule: "non_zero"}:                  domain.ErrPreparedQueryNameRequired,
	{Message: "raft.UpdatePreparedQueryOrder", Path: "name", Rule: "non_zero"}:         domain.ErrPreparedQueryNameRequired,
	{Message: "raft.DeletePreparedQueryOrder", Path: "name", Rule: "non_zero"}:         domain.ErrPreparedQueryNameRequired,
	{Message: "common.PostgresAwsIamAuth", Path: "region", Rule: "non_zero"}:           ErrMirrorIAMRegionRequired,
	{Message: "raft.RegisterSigningKeyOrder", Path: "public_key", Rule: "exact_bytes"}: ErrSigningKeyInvalidLength,
	{Message: "raft.AddEventsSinkOrder", Path: "config", Rule: "required"}:             ErrSinkConfigIncomplete,
	{Message: "common.SinkConfig", Path: "name", Rule: "non_zero"}:                     ErrSinkConfigIncomplete,
}

func mapInputValidation(err error) domain.Describable {
	return inputvalidation.Resolve(err, inputValidationErrors)
}
