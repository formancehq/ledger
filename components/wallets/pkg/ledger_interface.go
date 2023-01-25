package wallet

import (
	"context"

	sdk "github.com/formancehq/formance-sdk-go"
	"github.com/formancehq/go-libs/metadata"
)

type ListAccountsQuery struct {
	Cursor   string
	Limit    int
	Metadata map[string]any
}

type ListTransactionsQuery struct {
	Cursor      string
	Limit       int
	Metadata    map[string]any
	Destination string
	Source      string
	Account     string
}

type Ledger interface {
	AddMetadataToAccount(ctx context.Context, ledger, account string, metadata metadata.Metadata) error
	GetAccount(ctx context.Context, ledger, account string) (*sdk.AccountWithVolumesAndBalances, error)
	ListAccounts(ctx context.Context, ledger string, query ListAccountsQuery) (*sdk.AccountsCursorResponseCursor, error)
	ListTransactions(ctx context.Context, ledger string, query ListTransactionsQuery) (*sdk.TransactionsCursorResponseCursor, error)
	RunScript(ctx context.Context, ledger string, script sdk.Script) (*sdk.ScriptResponse, error)
}

type DefaultLedger struct {
	client *sdk.APIClient
}

func (d DefaultLedger) ListTransactions(ctx context.Context, ledger string, query ListTransactionsQuery) (*sdk.TransactionsCursorResponseCursor, error) {
	var (
		ret *sdk.TransactionsCursorResponse
		err error
	)
	if query.Cursor == "" {
		//nolint:bodyclose
		ret, _, err = d.client.TransactionsApi.ListTransactions(ctx, ledger).
			Metadata(query.Metadata).
			PageSize(int64(query.Limit)).
			Destination(query.Destination).
			Account(query.Account).
			Source(query.Source).
			Execute()
	} else {
		//nolint:bodyclose
		ret, _, err = d.client.TransactionsApi.ListTransactions(ctx, ledger).
			Cursor(query.Cursor).
			Execute()
	}
	if err != nil {
		return nil, err
	}

	return &ret.Cursor, nil
}

func (d DefaultLedger) AddMetadataToAccount(ctx context.Context, ledger, account string, metadata metadata.Metadata) error {
	//nolint:bodyclose
	_, err := d.client.AccountsApi.AddMetadataToAccount(ctx, ledger, account).RequestBody(metadata).Execute()
	return err
}

func (d DefaultLedger) GetAccount(ctx context.Context, ledger, account string) (*sdk.AccountWithVolumesAndBalances, error) {
	//nolint:bodyclose
	ret, _, err := d.client.AccountsApi.GetAccount(ctx, ledger, account).Execute()
	return &ret.Data, err
}

func (d DefaultLedger) ListAccounts(ctx context.Context, ledger string, query ListAccountsQuery) (*sdk.AccountsCursorResponseCursor, error) {
	var (
		ret *sdk.AccountsCursorResponse
		err error
	)
	if query.Cursor == "" {
		//nolint:bodyclose
		ret, _, err = d.client.AccountsApi.ListAccounts(ctx, ledger).
			Metadata(query.Metadata).
			PageSize(int64(query.Limit)).
			Execute()
	} else {
		//nolint:bodyclose
		ret, _, err = d.client.AccountsApi.ListAccounts(ctx, ledger).
			Cursor(query.Cursor).
			Execute()
	}
	if err != nil {
		return nil, err
	}

	return &ret.Cursor, nil
}

func (d DefaultLedger) CreateTransaction(ctx context.Context, ledger string, transaction sdk.PostTransaction) error {
	//nolint:bodyclose
	_, _, err := d.client.TransactionsApi.
		CreateTransaction(ctx, ledger).
		PostTransaction(transaction).
		Execute()
	return err
}

func (d DefaultLedger) RunScript(ctx context.Context, ledger string, script sdk.Script) (*sdk.ScriptResponse, error) {
	//nolint:bodyclose
	ret, _, err := d.client.ScriptApi.RunScript(ctx, ledger).Script(script).Execute()
	return ret, err
}

var _ Ledger = &DefaultLedger{}

func NewDefaultLedger(client *sdk.APIClient) *DefaultLedger {
	return &DefaultLedger{
		client: client,
	}
}
