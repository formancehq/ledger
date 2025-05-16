package testserver

import (
	"bytes"
	"context"
	"io"
	"math/big"
	"strconv"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/pkg/client/models/sdkerrors"
)

func CreateLedger(ctx context.Context, srv *Server, request operations.V2CreateLedgerRequest) error {
	_, err := srv.Client().Ledger.V2.CreateLedger(ctx, request)
	return mapSDKError(err)
}

func GetLedger(ctx context.Context, srv *Server, request operations.V2GetLedgerRequest) (*components.V2Ledger, error) {
	ret, err := srv.Client().Ledger.V2.GetLedger(ctx, request)
	if err := mapSDKError(err); err != nil {
		return nil, err
	}
	return &ret.V2GetLedgerResponse.Data, nil
}

func GetInfo(ctx context.Context, srv *Server) (*operations.V2GetInfoResponse, error) {
	return srv.Client().Ledger.V2.GetInfo(ctx)
}

func GetLedgerInfo(ctx context.Context, srv *Server, request operations.V2GetLedgerInfoRequest) (*operations.V2GetLedgerInfoResponse, error) {
	return srv.Client().Ledger.V2.GetLedgerInfo(ctx, request)
}

func CreateTransaction(ctx context.Context, srv *Server, request operations.V2CreateTransactionRequest) (*components.V2Transaction, error) {
	response, err := srv.Client().Ledger.V2.CreateTransaction(ctx, request)

	if err != nil {
		return nil, mapSDKError(err)
	}

	return &response.V2CreateTransactionResponse.Data, nil
}

func CreateBulk(ctx context.Context, srv *Server, request operations.V2CreateBulkRequest) ([]components.V2BulkElementResult, error) {
	response, err := srv.Client().Ledger.V2.CreateBulk(ctx, request)
	if err != nil {
		return nil, mapSDKError(err)
	}
	return response.V2BulkResponse.Data, nil
}

func GetBalancesAggregated(ctx context.Context, srv *Server, request operations.V2GetBalancesAggregatedRequest) (map[string]*big.Int, error) {
	response, err := srv.Client().Ledger.V2.GetBalancesAggregated(ctx, request)
	if err != nil {
		return nil, mapSDKError(err)
	}
	return response.V2AggregateBalancesResponse.Data, nil
}

func GetVolumesWithBalances(ctx context.Context, srv *Server, request operations.V2GetVolumesWithBalancesRequest) (*components.V2VolumesWithBalanceCursorResponseCursor, error) {
	response, err := srv.Client().Ledger.V2.GetVolumesWithBalances(ctx, request)
	if err != nil {
		return nil, mapSDKError(err)
	}
	return &response.V2VolumesWithBalanceCursorResponse.Cursor, nil
}

func UpdateLedgerMetadata(ctx context.Context, srv *Server, request operations.V2UpdateLedgerMetadataRequest) error {
	_, err := srv.Client().Ledger.V2.UpdateLedgerMetadata(ctx, request)
	return mapSDKError(err)
}

func DeleteLedgerMetadata(ctx context.Context, srv *Server, request operations.V2DeleteLedgerMetadataRequest) error {
	_, err := srv.Client().Ledger.V2.DeleteLedgerMetadata(ctx, request)
	return mapSDKError(err)
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

func DeleteMetadataFromAccount(ctx context.Context, srv *Server, request operations.V2DeleteAccountMetadataRequest) error {
	return DeleteAccountMetadata(ctx, srv, request)
}

func DeleteTransactionMetadata(ctx context.Context, srv *Server, request operations.V2DeleteTransactionMetadataRequest) error {
	_, err := srv.Client().Ledger.V2.DeleteTransactionMetadata(ctx, request)
	return mapSDKError(err)
}

func DeleteMetadataFromTransaction(ctx context.Context, srv *Server, request operations.V2DeleteTransactionMetadataRequest) error {
	return DeleteTransactionMetadata(ctx, srv, request)
}

func RevertTransaction(ctx context.Context, srv *Server, request operations.V2RevertTransactionRequest) (*components.V2Transaction, error) {
	response, err := srv.Client().Ledger.V2.RevertTransaction(ctx, request)

	if err != nil {
		return nil, mapSDKError(err)
	}

	return &response.V2RevertTransactionResponse.Data, nil
}

func GetTransaction(ctx context.Context, srv *Server, request operations.V2GetTransactionRequest) (*components.V2Transaction, error) {
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

func CountTransactions(ctx context.Context, srv *Server, request operations.V2CountTransactionsRequest) (int, error) {
	response, err := srv.Client().Ledger.V2.CountTransactions(ctx, request)

	if err != nil {
		return 0, mapSDKError(err)
	}

	ret, err := strconv.ParseInt(response.Headers["Count"][0], 10, 64)
	if err != nil {
		return 0, err
	}

	return int(ret), nil
}

func ListAccounts(ctx context.Context, srv *Server, request operations.V2ListAccountsRequest) (*components.V2AccountsCursorResponseCursor, error) {
	response, err := srv.Client().Ledger.V2.ListAccounts(ctx, request)

	if err != nil {
		return nil, mapSDKError(err)
	}

	return &response.V2AccountsCursorResponse.Cursor, nil
}

func ListLogs(ctx context.Context, srv *Server, request operations.V2ListLogsRequest) (*components.V2LogsCursorResponseCursor, error) {
	response, err := srv.Client().Ledger.V2.ListLogs(ctx, request)

	if err != nil {
		return nil, mapSDKError(err)
	}

	return &response.V2LogsCursorResponse.Cursor, nil
}

func CountAccounts(ctx context.Context, srv *Server, request operations.V2CountAccountsRequest) (int, error) {
	response, err := srv.Client().Ledger.V2.CountAccounts(ctx, request)

	if err != nil {
		return 0, mapSDKError(err)
	}

	ret, err := strconv.ParseInt(response.Headers["Count"][0], 10, 64)
	if err != nil {
		return 0, err
	}

	return int(ret), nil
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
	// notes: *sdkerrors.V2ErrorResponse does not implements errors.Is
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
