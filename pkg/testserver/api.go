package testserver

import (
	"bytes"
	"context"
	"io"
	"math/big"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/stack/ledger/client/models/components"
	"github.com/formancehq/stack/ledger/client/models/operations"
	"github.com/formancehq/stack/ledger/client/models/sdkerrors"
)

func CreateLedger(ctx context.Context, srv *Server, request operations.V2CreateLedgerRequest) error {
	_, err := srv.Client().Ledger.V2.CreateLedger(ctx, request)
	return mapSDKError(err)
}

func GetInfo(ctx context.Context, srv *Server) (*operations.V2GetInfoResponse, error) {
	return srv.Client().Ledger.V2.GetInfo(ctx)
}

func CreateTransaction(ctx context.Context, srv *Server, request operations.V2CreateTransactionRequest) (*components.V2Transaction, error) {
	response, err := srv.Client().Ledger.V2.CreateTransaction(ctx, request)

	if err != nil {
		return nil, mapSDKError(err)
	}

	return &response.V2CreateTransactionResponse.Data, nil
}

func AddMetadataToAccount(ctx context.Context, srv *Server, request operations.V2AddMetadataToAccountRequest) error {
	_, err := srv.Client().Ledger.V2.AddMetadataToAccount(ctx, request)
	return mapSDKError(err)
}

func AddMetadataToTransaction(ctx context.Context, srv *Server, request operations.V2AddMetadataOnTransactionRequest) error {
	_, err := srv.Client().Ledger.V2.AddMetadataOnTransaction(ctx, request)
	return mapSDKError(err)
}

func DeleteAccountMetadata(ctx context.Context, srv *Server, request operations.V2DeleteAccountMetadataRequest) error {
	_, err := srv.Client().Ledger.V2.DeleteAccountMetadata(ctx, request)
	return mapSDKError(err)
}

func DeleteTransactionMetadata(ctx context.Context, srv *Server, request operations.V2DeleteTransactionMetadataRequest) error {
	_, err := srv.Client().Ledger.V2.DeleteTransactionMetadata(ctx, request)
	return mapSDKError(err)
}

func RevertTransaction(ctx context.Context, srv *Server, request operations.V2RevertTransactionRequest) (*components.V2Transaction, error) {
	response, err := srv.Client().Ledger.V2.RevertTransaction(ctx, request)

	if err != nil {
		return nil, mapSDKError(err)
	}

	return &response.V2RevertTransactionResponse.Data, nil
}

func GetTransaction(ctx context.Context, srv *Server, request operations.V2GetTransactionRequest) (*components.V2ExpandedTransaction, error) {
	response, err := srv.Client().Ledger.V2.GetTransaction(ctx, request)

	if err != nil {
		return nil, mapSDKError(err)
	}

	return &response.V2GetTransactionResponse.Data, nil
}

func GetAccount(ctx context.Context, srv *Server, request operations.V2GetAccountRequest) (*components.V2Account, error) {
	response, err := srv.Client().Ledger.V2.GetAccount(ctx, request)

	if err != nil {
		return nil, mapSDKError(err)
	}

	return &response.V2AccountResponse.Data, nil
}

func ListTransactions(ctx context.Context, srv *Server, request operations.V2ListTransactionsRequest) (*components.V2TransactionsCursorResponseCursor, error) {
	response, err := srv.Client().Ledger.V2.ListTransactions(ctx, request)

	if err != nil {
		return nil, mapSDKError(err)
	}

	return &response.V2TransactionsCursorResponse.Cursor, nil
}

func ListLedgers(ctx context.Context, srv *Server, request operations.V2ListLedgersRequest) (*components.V2LedgerListResponseCursor, error) {
	response, err := srv.Client().Ledger.V2.ListLedgers(ctx, request)

	if err != nil {
		return nil, mapSDKError(err)
	}

	return &response.V2LedgerListResponse.Cursor, nil
}

func GetAggregatedBalances(ctx context.Context, srv *Server, request operations.V2GetBalancesAggregatedRequest) (map[string]*big.Int, error) {
	response, err := srv.Client().Ledger.V2.GetBalancesAggregated(ctx, request)

	if err != nil {
		return nil, mapSDKError(err)
	}

	return response.GetV2AggregateBalancesResponse().Data, nil
}

func Export(ctx context.Context, srv *Server, request operations.V2ExportLogsRequest) (io.Reader, error) {
	response, err := srv.Client().Ledger.V2.ExportLogs(ctx, request)
	if err != nil {
		return nil, mapSDKError(err)
	}

	data, err := io.ReadAll(response.HTTPMeta.Response.Body)
	if err != nil {
		return nil, err
	}

	return bytes.NewBuffer(data), nil
}

func Import(ctx context.Context, srv *Server, request operations.V2ImportLogsRequest) error {
	_, err := srv.Client().Ledger.V2.ImportLogs(ctx, request)
	return mapSDKError(err)
}

func mapSDKError(err error) error {
	switch err := err.(type) {
	case *sdkerrors.V2ErrorResponse:
		return api.ErrorResponse{
			ErrorCode:    string(err.ErrorCode),
			ErrorMessage: err.ErrorMessage,
			Details: func() string {
				if err.Details == nil {
					return ""
				}
				return *err.Details
			}(),
		}
	default:
		return err
	}
}
