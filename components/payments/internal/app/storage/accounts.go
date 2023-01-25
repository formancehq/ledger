package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/formancehq/payments/internal/app/models"
)

func (s *Storage) UpsertAccounts(ctx context.Context, provider models.ConnectorProvider, accounts []models.Account) error {
	if len(accounts) == 0 {
		return nil
	}

	accountsMap := make(map[string]models.Account)
	for _, account := range accounts {
		accountsMap[account.Reference] = account
	}

	accounts = make([]models.Account, 0, len(accountsMap))
	for _, account := range accountsMap {
		accounts = append(accounts, account)
	}

	_, err := s.db.NewInsert().
		Model(&accounts).
		On("CONFLICT (reference) DO UPDATE").
		Set("provider = EXCLUDED.provider").
		Set("type = EXCLUDED.type").
		Exec(ctx)
	if err != nil {
		return e("failed to create accounts", err)
	}

	return nil
}

func (s *Storage) ListAccounts(ctx context.Context, pagination Paginator) ([]*models.Account, PaginationDetails, error) {
	var accounts []*models.Account

	query := s.db.NewSelect().
		Model(&accounts)

	query = pagination.apply(query, "account.created_at")

	err := query.Scan(ctx)
	if err != nil {
		return nil, PaginationDetails{}, fmt.Errorf("failed to list payments: %w", err)
	}

	var (
		hasMore                       = len(accounts) > pagination.pageSize
		firstReference, lastReference string
	)

	if hasMore {
		accounts = accounts[:pagination.pageSize]
	}

	if len(accounts) > 0 {
		firstReference = accounts[0].CreatedAt.Format(time.RFC3339Nano)
		lastReference = accounts[len(accounts)-1].CreatedAt.Format(time.RFC3339Nano)
	}

	paginationDetails, err := pagination.paginationDetails(hasMore, firstReference, lastReference)
	if err != nil {
		return nil, PaginationDetails{}, fmt.Errorf("failed to get pagination details: %w", err)
	}

	return accounts, paginationDetails, nil
}
