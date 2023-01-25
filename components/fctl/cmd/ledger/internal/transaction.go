package internal

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/formancehq/formance-sdk-go"
)

func TransactionIDOrLastN(ctx context.Context, ledgerClient *formance.APIClient, ledger, id string) (int64, error) {
	if strings.HasPrefix(id, "last") {
		id = strings.TrimPrefix(id, "last")
		sub := int64(0)
		if id != "" {
			var err error
			sub, err = strconv.ParseInt(id, 10, 64)
			if err != nil {
				return 0, err
			}
		}
		response, _, err := ledgerClient.TransactionsApi.
			ListTransactions(ctx, ledger).
			PageSize(1).
			Execute()
		if err != nil {
			return 0, err
		}
		if len(response.Cursor.Data) == 0 {
			return 0, errors.New("no transaction found")
		}
		return response.Cursor.Data[0].Txid + sub, nil
	}

	return strconv.ParseInt(id, 10, 64)
}
