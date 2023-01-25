package wallet

import (
	"context"

	sdk "github.com/formancehq/formance-sdk-go"
	"github.com/formancehq/go-libs/metadata"
	"github.com/pkg/errors"
)

type ListResponse[T any] struct {
	Data           []T
	Next, Previous string
	HasMore        bool
}

type Pagination struct {
	Limit           int
	PaginationToken string
}

type ListQuery[T any] struct {
	Pagination
	Payload T
}

type mapper[SRC any, DST any] func(src SRC) DST

func newListResponse[SRC any, DST any](cursor interface {
	GetData() []SRC
	GetNext() string
	GetPrevious() string
	GetHasMore() bool
}, mapper mapper[SRC, DST],
) *ListResponse[DST] {
	ret := make([]DST, 0)
	for _, item := range cursor.GetData() {
		ret = append(ret, mapper(item))
	}

	return &ListResponse[DST]{
		Data:     ret,
		Next:     cursor.GetNext(),
		Previous: cursor.GetPrevious(),
		HasMore:  cursor.GetHasMore(),
	}
}

type ListHolds struct {
	WalletID string
	Metadata map[string]any
}

type ListBalances struct {
	WalletID string
	Metadata map[string]any
}

type ListTransactions struct {
	WalletID string
}

func BalancesMetadataFilter(walletID string) map[string]interface{} {
	return map[string]interface{}{
		MetadataKeyWalletBalance: TrueValue,
		MetadataKeyWalletID:      walletID,
	}
}

type Manager struct {
	client     Ledger
	chart      *Chart
	ledgerName string
}

func NewManager(
	ledgerName string,
	client Ledger,
	chart *Chart,
) *Manager {
	return &Manager{
		client:     client,
		chart:      chart,
		ledgerName: ledgerName,
	}
}

//nolint:cyclop
func (m *Manager) Debit(ctx context.Context, debit Debit) (*DebitHold, error) {
	if err := debit.Validate(); err != nil {
		return nil, err
	}

	dest := debit.getDestination()

	var hold *DebitHold
	if debit.Pending {
		hold = Ptr(debit.newHold())
		holdAccount := m.chart.GetHoldAccount(hold.ID)
		if err := m.client.AddMetadataToAccount(ctx, m.ledgerName, holdAccount, hold.LedgerMetadata(m.chart)); err != nil {
			return nil, errors.Wrap(err, "adding metadata to account")
		}

		dest = NewLedgerAccountSubject(holdAccount)
	}

	sources := make([]string, 0)
	var err error
	switch {
	case len(debit.Balances) == 0:
		sources = append(sources, m.chart.GetMainBalanceAccount(debit.WalletID))
	case len(debit.Balances) == 1 && debit.Balances[0] == "*":
		sources, err = fetchAndMapAllAccounts[string](ctx, m, BalancesMetadataFilter(debit.WalletID), Account.GetAddress)
		if err != nil {
			return nil, err
		}
	default:
		for _, balance := range debit.Balances {
			if balance == "*" {
				return nil, ErrInvalidBalanceSpecified
			}
			sources = append(sources, m.chart.GetBalanceAccount(debit.WalletID, balance))
		}
	}

	script := sdk.Script{
		Plain: BuildDebitWalletScript(sources...),
		Vars: map[string]interface{}{
			"destination": dest.getAccount(m.chart),
			"amount": map[string]any{
				// @todo: upgrade this to proper int after sdk is updated
				"amount": debit.Amount.Amount.Uint64(),
				"asset":  debit.Amount.Asset,
			},
		},
		Metadata: TransactionMetadata(debit.Metadata),
		//nolint:godox
		// TODO: Add set account metadata for hold when released on ledger (v1.9)
	}
	if debit.Reference != "" {
		script.Reference = &debit.Reference
	}

	return hold, m.runScript(ctx, script)
}

func (m *Manager) ConfirmHold(ctx context.Context, debit ConfirmHold) error {
	account, err := m.client.GetAccount(ctx, m.ledgerName, m.chart.GetHoldAccount(debit.HoldID))
	if err != nil {
		return errors.Wrap(err, "getting account")
	}
	if !IsHold(account) {
		return ErrHoldNotFound
	}

	hold := ExpandedDebitHoldFromLedgerAccount(account)
	if hold.Remaining.Uint64() == 0 {
		return ErrClosedHold
	}

	amount, err := debit.resolveAmount(hold)
	if err != nil {
		return err
	}

	return m.runScript(
		ctx,
		sdk.Script{
			Plain: BuildConfirmHoldScript(debit.Final, hold.Asset),
			Vars: map[string]interface{}{
				"hold": m.chart.GetHoldAccount(debit.HoldID),
				"amount": map[string]any{
					"amount": amount,
					"asset":  hold.Asset,
				},
			},
			Metadata: TransactionMetadata(metadata.Metadata{}),
		},
	)
}

func (m *Manager) VoidHold(ctx context.Context, void VoidHold) error {
	account, err := m.client.GetAccount(ctx, m.ledgerName, m.chart.GetHoldAccount(void.HoldID))
	if err != nil {
		return errors.Wrap(err, "getting account")
	}

	hold := ExpandedDebitHoldFromLedgerAccount(account)
	if hold.IsClosed() {
		return ErrClosedHold
	}

	return m.runScript(ctx, sdk.Script{
		Plain: BuildCancelHoldScript(hold.Asset),
		Vars: map[string]interface{}{
			"hold": m.chart.GetHoldAccount(void.HoldID),
		},
		Metadata: TransactionMetadata(metadata.Metadata{}),
	})
}

func (m *Manager) Credit(ctx context.Context, credit Credit) error {
	if err := credit.Validate(); err != nil {
		return err
	}

	if credit.Balance != "" {
		if _, err := m.GetBalance(ctx, credit.WalletID, credit.Balance); err != nil {
			return err
		}
	}

	script := sdk.Script{
		Plain: BuildCreditWalletScript(credit.Sources.ResolveAccounts(m.chart)...),
		Vars: map[string]interface{}{
			"destination": credit.destinationAccount(m.chart),
			"amount": map[string]any{
				// @todo: upgrade this to proper int after sdk is updated
				"amount": credit.Amount.Amount.Uint64(),
				"asset":  credit.Amount.Asset,
			},
		},
		Metadata: TransactionMetadata(credit.Metadata),
	}
	if credit.Reference != "" {
		script.Reference = &credit.Reference
	}

	return m.runScript(ctx, script)
}

func (m *Manager) runScript(ctx context.Context, script sdk.Script) error {
	ret, err := m.client.RunScript(ctx, m.ledgerName, script)
	if err != nil {
		return err
	}
	if ret.ErrorCode == nil {
		return nil
	}
	if *ret.ErrorCode == sdk.INSUFFICIENT_FUND {
		return ErrInsufficientFundError
	}
	if ret.ErrorMessage != nil {
		return errors.New(*ret.ErrorMessage)
	}
	return errors.New(string(*ret.ErrorCode))
}

func (m *Manager) ListWallets(ctx context.Context, query ListQuery[ListWallets]) (*ListResponse[Wallet], error) {
	return mapAccountList(ctx, m, mapAccountListQuery{
		Pagination: query.Pagination,
		Metadata: func() metadata.Metadata {
			metadata := map[string]interface{}{
				MetadataKeyWalletSpecType: PrimaryWallet,
			}
			if query.Payload.Metadata != nil && len(query.Payload.Metadata) > 0 {
				for k, v := range query.Payload.Metadata {
					metadata[MetadataKeyWalletCustomData+"."+k] = v
				}
			}
			if query.Payload.Name != "" {
				metadata[MetadataKeyWalletName] = query.Payload.Name
			}
			return metadata
		},
	}, func(account Account) Wallet {
		return FromAccount(m.ledgerName, account)
	})
}

func (m *Manager) ListHolds(ctx context.Context, query ListQuery[ListHolds]) (*ListResponse[DebitHold], error) {
	return mapAccountList(ctx, m, mapAccountListQuery{
		Pagination: query.Pagination,
		Metadata: func() metadata.Metadata {
			metadata := metadata.Metadata{
				MetadataKeyWalletSpecType: HoldWallet,
			}
			if query.Payload.WalletID != "" {
				metadata[MetadataKeyHoldWalletID] = query.Payload.WalletID
			}
			if query.Payload.Metadata != nil && len(query.Payload.Metadata) > 0 {
				for k, v := range query.Payload.Metadata {
					metadata[MetadataKeyWalletCustomData+"."+k] = v
				}
			}
			return metadata
		},
	}, DebitHoldFromLedgerAccount)
}

func (m *Manager) ListBalances(ctx context.Context, query ListQuery[ListBalances]) (*ListResponse[Balance], error) {
	return mapAccountList(ctx, m, mapAccountListQuery{
		Metadata: func() metadata.Metadata {
			metadata := BalancesMetadataFilter(query.Payload.WalletID)
			if query.Payload.Metadata != nil && len(query.Payload.Metadata) > 0 {
				for k, v := range query.Payload.Metadata {
					metadata[MetadataKeyWalletCustomData+"."+k] = v
				}
			}
			return metadata
		},
		Pagination: query.Pagination,
	}, BalanceFromAccount)
}

func (m *Manager) ListTransactions(ctx context.Context, query ListQuery[ListTransactions]) (*ListResponse[sdk.Transaction], error) {
	var (
		response *sdk.TransactionsCursorResponseCursor
		err      error
	)
	if query.PaginationToken == "" {
		response, err = m.client.ListTransactions(ctx, m.ledgerName, ListTransactionsQuery{
			Limit: query.Limit,
			Account: func() string {
				if query.Payload.WalletID != "" {
					return m.chart.GetMainBalanceAccount(query.Payload.WalletID)
				}
				return ""
			}(),
			Metadata: TransactionBaseMetadataFilter(),
		})
	} else {
		response, err = m.client.ListTransactions(ctx, m.ledgerName, ListTransactionsQuery{
			Cursor: query.PaginationToken,
		})
	}
	if err != nil {
		return nil, errors.Wrap(err, "listing transactions")
	}

	return newListResponse[sdk.Transaction, sdk.Transaction](response, func(tx sdk.Transaction) sdk.Transaction {
		return tx
	}), nil
}

func (m *Manager) CreateWallet(ctx context.Context, data *CreateRequest) (*Wallet, error) {
	wallet := NewWallet(data.Name, m.ledgerName, data.Metadata)

	if err := m.client.AddMetadataToAccount(
		ctx,
		m.ledgerName,
		m.chart.GetMainBalanceAccount(wallet.ID),
		wallet.LedgerMetadata(),
	); err != nil {
		return nil, errors.Wrap(err, "adding metadata to account")
	}

	return &wallet, nil
}

func (m *Manager) UpdateWallet(ctx context.Context, id string, data *PatchRequest) error {
	account, err := m.client.GetAccount(ctx, m.ledgerName, m.chart.GetMainBalanceAccount(id))
	if err != nil {
		return ErrWalletNotFound
	}

	if !IsPrimary(account) {
		return ErrWalletNotFound
	}

	newCustomMetadata := metadata.Metadata{}
	existingCustomMetadata := GetMetadata(account, MetadataKeyWalletCustomData)
	if existingCustomMetadata != nil {
		newCustomMetadata = newCustomMetadata.Merge(existingCustomMetadata.(map[string]any))
	}
	newCustomMetadata = newCustomMetadata.Merge(data.Metadata)

	meta := account.GetMetadata()
	meta[MetadataKeyWalletCustomData] = newCustomMetadata

	if err := m.client.AddMetadataToAccount(ctx, m.ledgerName, m.chart.GetMainBalanceAccount(id), meta); err != nil {
		return errors.Wrap(err, "adding metadata to account")
	}

	return nil
}

func (m *Manager) GetWallet(ctx context.Context, id string) (*WithBalances, error) {
	account, err := m.client.GetAccount(
		ctx,
		m.ledgerName,
		m.chart.GetMainBalanceAccount(id),
	)
	if err != nil {
		return nil, errors.Wrap(err, "getting account")
	}

	if !IsPrimary(account) {
		return nil, ErrWalletNotFound
	}

	return Ptr(WithBalancesFromAccount(m.ledgerName, account)), nil
}

func (m *Manager) GetHold(ctx context.Context, id string) (*ExpandedDebitHold, error) {
	account, err := m.client.GetAccount(ctx, m.ledgerName, m.chart.GetHoldAccount(id))
	if err != nil {
		return nil, err
	}

	return Ptr(ExpandedDebitHoldFromLedgerAccount(account)), nil
}

func (m *Manager) CreateBalance(ctx context.Context, data *CreateBalance) (*Balance, error) {
	if err := data.Validate(); err != nil {
		return nil, err
	}
	ret, err := m.client.GetAccount(ctx, m.ledgerName, m.chart.GetBalanceAccount(data.WalletID, data.Name))
	if err != nil {
		return nil, err
	}
	if ret.Metadata != nil && ret.Metadata[MetadataKeyWalletBalance] == TrueValue {
		return nil, ErrBalanceAlreadyExists
	}

	balance := NewBalance(data.Name)

	if err := m.client.AddMetadataToAccount(
		ctx,
		m.ledgerName,
		m.chart.GetBalanceAccount(data.WalletID, balance.Name),
		balance.LedgerMetadata(data.WalletID),
	); err != nil {
		return nil, errors.Wrap(err, "adding metadata to account")
	}

	return &balance, nil
}

func (m *Manager) GetBalance(ctx context.Context, walletID string, balanceName string) (*ExpandedBalance, error) {
	account, err := m.client.GetAccount(ctx, m.ledgerName, m.chart.GetBalanceAccount(walletID, balanceName))
	if err != nil {
		return nil, err
	}
	if account.Metadata[MetadataKeyWalletBalance] != TrueValue {
		return nil, ErrBalanceNotExists
	}

	return Ptr(ExpandedBalanceFromAccount(account)), nil
}

type mapAccountListQuery struct {
	Pagination
	Metadata func() metadata.Metadata
}

func mapAccountList[TO any](ctx context.Context, r *Manager, query mapAccountListQuery, mapper mapper[Account, TO]) (*ListResponse[TO], error) {
	var (
		response *sdk.AccountsCursorResponseCursor
		err      error
	)
	if query.PaginationToken == "" {
		response, err = r.client.ListAccounts(ctx, r.ledgerName, ListAccountsQuery{
			Limit:    query.Limit,
			Metadata: query.Metadata(),
		})
	} else {
		response, err = r.client.ListAccounts(ctx, r.ledgerName, ListAccountsQuery{
			Cursor: query.PaginationToken,
		})
	}
	if err != nil {
		return nil, err
	}

	return newListResponse[sdk.Account, TO](response, func(item sdk.Account) TO {
		return mapper(&item)
	}), nil
}

const maxPageSize = 100

func fetchAndMapAllAccounts[TO any](ctx context.Context, r *Manager, md metadata.Metadata, mapper mapper[Account, TO]) ([]TO, error) {
	ret := make([]TO, 0)
	query := mapAccountListQuery{
		Metadata: func() metadata.Metadata {
			return md
		},
		Pagination: Pagination{
			Limit: maxPageSize,
		},
	}
	for {
		listResponse, err := mapAccountList(ctx, r, query, mapper)
		if err != nil {
			return nil, err
		}
		ret = append(ret, listResponse.Data...)
		if listResponse.Next == "" {
			return ret, nil
		}
		query = mapAccountListQuery{
			Pagination: Pagination{
				PaginationToken: listResponse.Next,
			},
		}
	}
}
