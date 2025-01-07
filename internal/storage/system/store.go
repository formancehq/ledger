package system

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/pagination"
	"github.com/formancehq/ledger/internal/replication/controller"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/lib/pq"
	errors2 "github.com/pkg/errors"
	"github.com/uptrace/bun"
)

const (
	SchemaSystem = "_system"
)

type DefaultStore struct {
	db *bun.DB
}

func (d *DefaultStore) IsUpToDate(ctx context.Context) (bool, error) {
	return d.GetMigrator().IsUpToDate(ctx)
}

func (d *DefaultStore) GetDistinctBuckets(ctx context.Context) ([]string, error) {
	var buckets []string
	err := d.db.NewSelect().
		DistinctOn("bucket").
		Model(&ledger.Ledger{}).
		Column("bucket").
		Scan(ctx, &buckets)
	if err != nil {
		return nil, fmt.Errorf("getting buckets: %w", postgres.ResolveError(err))
	}

	return buckets, nil
}

func (d *DefaultStore) CreateLedger(ctx context.Context, l *ledger.Ledger) error {

	if l.Metadata == nil {
		l.Metadata = metadata.Metadata{}
	}

	_, err := d.db.NewInsert().
		Model(l).
		Returning("id, added_at").
		Exec(ctx)
	if err != nil {
		if errors.Is(postgres.ResolveError(err), postgres.ErrConstraintsFailed{}) {
			return systemcontroller.ErrLedgerAlreadyExists
		}
		return postgres.ResolveError(err)
	}

	return nil
}

func (d *DefaultStore) UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error {
	_, err := d.db.NewUpdate().
		Model(&ledger.Ledger{}).
		Set("metadata = metadata || ?", m).
		Where("name = ?", name).
		Exec(ctx)
	return err
}

func (d *DefaultStore) DeleteLedgerMetadata(ctx context.Context, name string, key string) error {
	_, err := d.db.NewUpdate().
		Model(&ledger.Ledger{}).
		Set("metadata = metadata - ?", key).
		Where("name = ?", name).
		Exec(ctx)
	return err
}

func (d *DefaultStore) ListLedgers(ctx context.Context, q ledgercontroller.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error) {
	query := d.db.NewSelect().
		Model(&ledger.Ledger{}).
		Column("*").
		Order("added_at asc")

	return bunpaginate.UsingOffset[pagination.PaginatedQueryOptions[struct{}], ledger.Ledger](
		ctx,
		query,
		bunpaginate.OffsetPaginatedQuery[pagination.PaginatedQueryOptions[struct{}]](q),
	)
}

func (d *DefaultStore) GetLedger(ctx context.Context, name string) (*ledger.Ledger, error) {
	ret := &ledger.Ledger{}
	if err := d.db.NewSelect().
		Model(ret).
		Column("*").
		Where("name = ?", name).
		Scan(ctx); err != nil {
		return nil, postgres.ResolveError(err)
	}

	return ret, nil
}

func (d *DefaultStore) Migrate(ctx context.Context, options ...migrations.Option) error {
	return d.GetMigrator(options...).Up(ctx)
}

func (d *DefaultStore) GetMigrator(options ...migrations.Option) *migrations.Migrator {
	return GetMigrator(d.db, options...)
}

func (d *DefaultStore) GetDB() *bun.DB {
	return d.db
}

func New(db *bun.DB) *DefaultStore {
	return &DefaultStore{
		db: db,
	}
}

func (p *DefaultStore) ListConnectors(ctx context.Context) (*bunpaginate.Cursor[ledger.Connector], error) {
	connectors, err := bunpaginate.UsingOffset[struct{}, Connector](
		ctx,
		p.db.NewSelect(),
		bunpaginate.OffsetPaginatedQuery[struct{}]{},
	)
	if err != nil {
		return nil, err
	}
	return bunpaginate.MapCursor(connectors, Connector.ToCore), nil
}

func (p *DefaultStore) CreateConnector(ctx context.Context, connector ledger.Connector) error {
	_, err := p.db.NewInsert().
		Model(&Connector{
			ID:        connector.ID,
			Driver:    connector.Driver,
			Config:    connector.Config,
			CreatedAt: connector.CreatedAt,
		}).
		Exec(ctx)
	return err
}

func (p *DefaultStore) DeleteConnector(ctx context.Context, id string) error {
	ret, err := p.db.NewDelete().
		Model(&Connector{}).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		switch err := err.(type) {
		case *pq.Error:
			if err.Constraint == "pipelines_connector_id_fkey" {
				return controller.NewErrConnectorUsed(id)
			}
			return err
		default:
			return err
		}
	}

	rowsAffected, err := ret.RowsAffected()
	if err != nil {
		panic(err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return err
}

func (p *DefaultStore) GetConnector(ctx context.Context, id string) (*ledger.Connector, error) {
	ret := &Connector{}
	err := p.db.NewSelect().
		Model(ret).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		return nil, err
	}

	return pointer.For(ret.ToCore()), nil
}

func (p *DefaultStore) ListPipelines(ctx context.Context) (*bunpaginate.Cursor[ledger.Pipeline], error) {
	pipelines, err := bunpaginate.UsingOffset[struct{}, Pipeline](
		ctx,
		p.db.NewSelect(),
		bunpaginate.OffsetPaginatedQuery[struct{}]{},
	)
	if err != nil {
		return nil, err
	}
	return bunpaginate.MapCursor(pipelines, Pipeline.ToCore), nil
}

func (p *DefaultStore) CreatePipeline(ctx context.Context, pipeline ledger.Pipeline) error {
	_, err := p.db.NewInsert().
		Model(&Pipeline{
			State:     pipeline.State,
			Module:    pipeline.Ledger,
			Connector: pipeline.ConnectorID,
			ID:        pipeline.ID,
			CreatedAt: pipeline.CreatedAt,
		}).
		Exec(ctx)
	if err != nil {
		// notes(gfyrag): it is not safe to check errors like that
		// but *pq.Error does not implement standard go utils for errors
		// so, we don't have choice
		err := postgres.ResolveError(err)
		if errors.Is(err, postgres.ErrConstraintsFailed{}) {
			return controller.NewErrPipelineAlreadyExists(pipeline.PipelineConfiguration)
		}

		return err
	}
	return nil
}

func (p *DefaultStore) DeletePipeline(ctx context.Context, id string) error {
	ret, err := p.db.NewDelete().
		Model(&Pipeline{}).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return err
	}

	rowsAffected, err := ret.RowsAffected()
	if err != nil {
		panic(err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return err
}

func (p *DefaultStore) GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error) {
	ret := &Pipeline{}
	err := p.db.NewSelect().
		Model(ret).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		return nil, err
	}

	return pointer.For(ret.ToCore()), nil
}

func (p *DefaultStore) ListEnabledPipelines(ctx context.Context) ([]ledger.Pipeline, error) {
	ret := make([]Pipeline, 0)
	if err := p.db.NewSelect().
		Model(&ret).
		Where("state->>'label' <> ?", ledger.StateLabelStop).
		Scan(ctx); err != nil {
		return nil, err
	}
	return collectionutils.Map(ret, Pipeline.ToCore), nil
}

func (p *DefaultStore) StoreState(ctx context.Context, id string, state ledger.State) error {
	ret, err := p.db.NewUpdate().
		Model(&Pipeline{}).
		Where("id = ?", id).
		Set("state = ?", state).
		Exec(ctx)
	if err != nil {
		return errors2.Wrap(err, "updating state in database")
	}
	rowsAffected, err := ret.RowsAffected()
	if err != nil {
		panic(err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func NewDefaultStore(db *bun.DB) *DefaultStore {
	return &DefaultStore{
		db: db,
	}
}

var _ controller.Store = (*DefaultStore)(nil)
var _ drivers.Store = (*DefaultStore)(nil)

type Pipeline struct {
	bun.BaseModel `bun:"table:pipelines"`

	ID        string       `bun:"id,pk"`
	CreatedAt time.Time    `bun:"created_at"`
	State     ledger.State `bun:"state,type:jsonb"`
	Module    string       `bun:"module"`
	Connector string       `bun:"connector_id"`
}

type Connector struct {
	bun.BaseModel `bun:"table:connectors"`

	ID        string          `bun:"id"`
	CreatedAt time.Time       `bun:"created_at"`
	Driver    string          `bun:"driver"`
	Config    json.RawMessage `bun:"config"`
}

func (c Connector) ToCore() ledger.Connector {
	return ledger.Connector{
		ID: c.ID,
		ConnectorConfiguration: ledger.ConnectorConfiguration{
			Driver: c.Driver,
			Config: c.Config,
		},
		CreatedAt: c.CreatedAt,
	}
}

func (p Pipeline) ToCore() ledger.Pipeline {
	return ledger.Pipeline{
		ID:    p.ID,
		State: p.State,
		PipelineConfiguration: ledger.PipelineConfiguration{
			Ledger:      p.Module,
			ConnectorID: p.Connector,
		},
		CreatedAt: p.CreatedAt,
	}
}
