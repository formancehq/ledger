package storage

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/time"
	ingester "github.com/formancehq/ledger/internal/replication"
	"github.com/formancehq/ledger/internal/replication/controller"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/internal/replication/runner"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

type Pipeline struct {
	bun.BaseModel `bun:"table:pipelines"`

	ID        string         `bun:"id,pk"`
	CreatedAt time.Time      `bun:"created_at"`
	State     ingester.State `bun:"state,type:jsonb"`
	Module    string         `bun:"module"`
	Connector string         `bun:"connector_id"`
}

type Connector struct {
	bun.BaseModel `bun:"table:connectors"`

	ID        string          `bun:"id"`
	CreatedAt time.Time       `bun:"created_at"`
	Driver    string          `bun:"driver"`
	Config    json.RawMessage `bun:"config"`
}

func (c Connector) ToCore() ingester.Connector {
	return ingester.Connector{
		ID: c.ID,
		ConnectorConfiguration: ingester.ConnectorConfiguration{
			Driver: c.Driver,
			Config: c.Config,
		},
		CreatedAt: c.CreatedAt,
	}
}

func (p Pipeline) ToCore() ingester.Pipeline {
	return ingester.Pipeline{
		ID:    p.ID,
		State: p.State,
		PipelineConfiguration: ingester.PipelineConfiguration{
			Ledger:      p.Module,
			ConnectorID: p.Connector,
		},
		CreatedAt: p.CreatedAt,
	}
}

type PostgresStore struct {
	db *bun.DB
}

func (p *PostgresStore) ListConnectors(ctx context.Context) (*bunpaginate.Cursor[ingester.Connector], error) {
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

func (p *PostgresStore) CreateConnector(ctx context.Context, connector ingester.Connector) error {
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

func (p *PostgresStore) DeleteConnector(ctx context.Context, id string) error {
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

func (p *PostgresStore) GetConnector(ctx context.Context, id string) (*ingester.Connector, error) {
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

func (p *PostgresStore) ListPipelines(ctx context.Context) (*bunpaginate.Cursor[ingester.Pipeline], error) {
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

func (p *PostgresStore) CreatePipeline(ctx context.Context, pipeline ingester.Pipeline) error {
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
		switch err := err.(type) {
		case *pq.Error:
			if err.Code == "23505" { // Conflict on pair module/connector
				return controller.NewErrPipelineAlreadyExists(pipeline.PipelineConfiguration)
			}
			return err
		default:
			return err
		}
	}
	return nil
}

func (p *PostgresStore) DeletePipeline(ctx context.Context, id string) error {
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

func (p *PostgresStore) GetPipeline(ctx context.Context, id string) (*ingester.Pipeline, error) {
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

func (p *PostgresStore) ListEnabledPipelines(ctx context.Context) ([]ingester.Pipeline, error) {
	ret := make([]Pipeline, 0)
	if err := p.db.NewSelect().
		Model(&ret).
		Where("state->>'label' <> ?", ingester.StateLabelStop).
		Scan(ctx); err != nil {
		return nil, err
	}
	return collectionutils.Map(ret, Pipeline.ToCore), nil
}

func (p *PostgresStore) StoreState(ctx context.Context, id string, state ingester.State) error {
	ret, err := p.db.NewUpdate().
		Model(&Pipeline{}).
		Where("id = ?", id).
		Set("state = ?", state).
		Exec(ctx)
	if err != nil {
		return errors.Wrap(err, "updating state in database")
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

func NewPostgresStore(db *bun.DB) *PostgresStore {
	return &PostgresStore{
		db: db,
	}
}

var _ runner.Store = (*PostgresStore)(nil)
var _ controller.Store = (*PostgresStore)(nil)
var _ drivers.Store = (*PostgresStore)(nil)
