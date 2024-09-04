package system

import (
	"context"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/platform/postgres"
	"github.com/formancehq/go-libs/pointer"
	"github.com/formancehq/go-libs/time"
	ledger "github.com/formancehq/ledger/internal"

	"github.com/formancehq/go-libs/bun/bunpaginate"

	"github.com/uptrace/bun"
)

type Ledger struct {
	bun.BaseModel `bun:"_system.ledgers,alias:ledgers"`

	ID       int               `bun:"id,type:int,scanonly"`
	Name     string            `bun:"name,type:varchar(255),pk"` // Primary key
	AddedAt  time.Time         `bun:"addedat,type:timestamp"`
	Bucket   string            `bun:"bucket,type:varchar(255)"`
	Metadata map[string]string `bun:"metadata,type:jsonb"`
	State    string            `bun:"state,type:varchar(255)"`
	Features map[string]string `bun:"features,type:jsonb"`
}

func (l Ledger) toCore() ledger.Ledger {
	return ledger.Ledger{
		Name: l.Name,
		Configuration: ledger.Configuration{
			Bucket:   l.Bucket,
			Metadata: l.Metadata,
			Features: l.Features,
		},
		AddedAt: l.AddedAt,
		State:   l.State,
		ID:      l.ID,
	}
}

func (s *Store) ListLedgers(ctx context.Context, q ledgercontroller.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error) {
	query := s.db.NewSelect().
		Model(&Ledger{}).
		Column("*").
		Order("addedat asc")

	cursor, err := bunpaginate.UsingOffset[ledgercontroller.PaginatedQueryOptions[struct{}], Ledger](
		ctx,
		query,
		bunpaginate.OffsetPaginatedQuery[ledgercontroller.PaginatedQueryOptions[struct{}]](q),
	)
	if err != nil {
		return nil, err
	}

	return bunpaginate.MapCursor(cursor, Ledger.toCore), nil
}

func (s *Store) CreateLedger(ctx context.Context, l *ledger.Ledger) (bool, error) {
	if l.Metadata == nil {
		l.Metadata = metadata.Metadata{}
	}

	mappedLedger := &Ledger{
		BaseModel: bun.BaseModel{},
		Name:      l.Name,
		AddedAt:   l.AddedAt,
		Bucket:    l.Bucket,
		Metadata:  l.Metadata,
		State:     l.State,
		Features:  l.Features,
	}
	ret, err := s.db.NewInsert().
		Model(mappedLedger).
		Ignore().
		Returning("id").
		Exec(ctx)
	if err != nil {
		return false, postgres.ResolveError(err)
	}

	affected, err := ret.RowsAffected()
	if err != nil {
		return false, postgres.ResolveError(err)
	}

	l.ID = mappedLedger.ID

	return affected > 0, nil
}

func (s *Store) GetLedger(ctx context.Context, name string) (*ledger.Ledger, error) {
	ret := &Ledger{}
	if err := s.db.NewSelect().
		Model(ret).
		Column("*").
		Where("name = ?", name).
		Scan(ctx); err != nil {
		return nil, postgres.ResolveError(err)
	}

	return pointer.For(ret.toCore()), nil
}

func (s *Store) UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error {
	_, err := s.db.NewUpdate().
		Model(&Ledger{}).
		Set("metadata = metadata || ?", m).
		Where("name = ?", name).
		Exec(ctx)
	return err
}

func (s *Store) UpdateLedgerState(ctx context.Context, name string, state string) error {
	_, err := s.db.NewUpdate().
		Model(&Ledger{}).
		Set("state = ?", state).
		Where("name = ?", name).
		Exec(ctx)
	return err
}

func (s *Store) DeleteLedgerMetadata(ctx context.Context, name string, key string) error {
	_, err := s.db.NewUpdate().
		Model(&Ledger{}).
		Set("metadata = metadata - ?", key).
		Where("name = ?", name).
		Exec(ctx)
	return err
}
