package grpc

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/inputvalidation"
)

var inputValidationErrors = map[inputvalidation.Key]error{
	{Message: "ledger.GetAccountRequest", Path: "ledger", Rule: "non_zero"}:              domain.ErrLedgerNameRequired,
	{Message: "ledger.GetTransactionRequest", Path: "ledger", Rule: "non_zero"}:          domain.ErrLedgerNameRequired,
	{Message: "ledger.ListTransactionsRequest", Path: "ledger", Rule: "non_zero"}:        domain.ErrLedgerNameRequired,
	{Message: "ledger.ListAccountsRequest", Path: "ledger", Rule: "non_zero"}:            domain.ErrLedgerNameRequired,
	{Message: "ledger.GetLedgerRequest", Path: "ledger", Rule: "non_zero"}:               domain.ErrLedgerNameRequired,
	{Message: "ledger.ListLogsRequest", Path: "ledger", Rule: "non_zero"}:                domain.ErrLedgerNameRequired,
	{Message: "ledger.AnalyzeAccountsRequest", Path: "ledger", Rule: "non_zero"}:         domain.ErrLedgerNameRequired,
	{Message: "ledger.AnalyzeTransactionsRequest", Path: "ledger", Rule: "non_zero"}:     domain.ErrLedgerNameRequired,
	{Message: "ledger.GetLedgerStatsRequest", Path: "ledger", Rule: "non_zero"}:          domain.ErrLedgerNameRequired,
	{Message: "ledger.AggregateVolumesRequest", Path: "ledger", Rule: "non_zero"}:        domain.ErrLedgerNameRequired,
	{Message: "ledger.InspectIndexRequest", Path: "ledger", Rule: "non_zero"}:            domain.ErrLedgerNameRequired,
	{Message: "cluster.BeginDrainRequest", Path: "epoch", Rule: "non_zero"}:              status.Error(codes.InvalidArgument, "drain epoch must contain 1 to 128 bytes"),
	{Message: "cluster.EndDrainRequest", Path: "epoch", Rule: "non_zero"}:                status.Error(codes.InvalidArgument, "drain epoch must contain 1 to 128 bytes"),
	{Message: "cluster.TransferLeadershipRequest", Path: "transferee", Rule: "non_zero"}: errors.New("transferee node ID must be non-zero"),
}

func mapInputValidation(err error) error {
	return inputvalidation.Resolve(err, inputValidationErrors)
}
