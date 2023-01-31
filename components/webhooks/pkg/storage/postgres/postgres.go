package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/webhooks/cmd/flag"
	webhooks "github.com/formancehq/webhooks/pkg"
	"github.com/formancehq/webhooks/pkg/storage"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"github.com/uptrace/bun/extra/bunotel"
)

type Store struct {
	db *bun.DB
}

var _ storage.Store = &Store{}

func NewStore() (storage.Store, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logging.Infof("postgres.NewStore: connecting to '%s'", viper.GetString(flag.StoragePostgresConnString))
	sqldb := sql.OpenDB(
		pgdriver.NewConnector(
			pgdriver.WithDSN(viper.GetString(flag.StoragePostgresConnString))))
	db := bun.NewDB(sqldb, pgdialect.New())
	db.AddQueryHook(bunotel.NewQueryHook(bunotel.WithDBName("webhooks")))
	if err := db.Ping(); err != nil {
		return nil, errors.Wrap(err, "bun.DB.Ping")
	}

	if _, err := db.NewCreateTable().Model((*webhooks.Config)(nil)).IfNotExists().Exec(ctx); err != nil {
		return nil, errors.Wrap(err, "create table configs")
	}

	if _, err := db.NewCreateTable().Model((*webhooks.Attempt)(nil)).IfNotExists().Exec(ctx); err != nil {
		return nil, errors.Wrap(err, "create table attempts")
	}

	return Store{db: db}, nil
}

func (s Store) FindManyConfigs(ctx context.Context, filters map[string]any) ([]webhooks.Config, error) {
	res := []webhooks.Config{}
	sq := s.db.NewSelect().Model(&res)
	for key, val := range filters {
		switch key {
		case "id":
			sq = sq.Where("id = ?", val)
		case "endpoint":
			sq = sq.Where("endpoint = ?", val)
		case "active":
			sq = sq.Where("active = ?", val)
		case "event_types":
			sq = sq.Where("? = ANY (event_types)", val)
		default:
			panic(key)
		}
	}
	sq.Order("updated_at DESC")
	if err := sq.Scan(ctx); err != nil {
		return nil, errors.Wrap(err, "selecting configs")
	}

	return res, nil
}

func (s Store) InsertOneConfig(ctx context.Context, cfgUser webhooks.ConfigUser) (webhooks.Config, error) {
	cfg := webhooks.NewConfig(cfgUser)
	if _, err := s.db.NewInsert().Model(&cfg).Exec(ctx); err != nil {
		return webhooks.Config{}, errors.Wrap(err, "insert one config")
	}

	return cfg, nil
}

func (s Store) DeleteOneConfig(ctx context.Context, id string) error {
	cfg := webhooks.Config{}
	if err := s.db.NewSelect().Model(&cfg).
		Where("id = ?", id).Scan(ctx); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return storage.ErrConfigNotFound
		}
		return errors.Wrap(err, "selecting one config before deleting")
	}

	if _, err := s.db.NewDelete().Model((*webhooks.Config)(nil)).
		Where("id = ?", id).Exec(ctx); err != nil {
		return errors.Wrap(err, "deleting one config")
	}

	return nil
}

func (s Store) UpdateOneConfigActivation(ctx context.Context, id string, active bool) (webhooks.Config, error) {
	cfg := webhooks.Config{}
	if err := s.db.NewSelect().Model(&cfg).
		Where("id = ?", id).Scan(ctx); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return webhooks.Config{}, storage.ErrConfigNotFound
		}
		return webhooks.Config{}, errors.Wrap(err, "selecting one config before updating activation")
	}
	if cfg.Active == active {
		return webhooks.Config{}, storage.ErrConfigNotModified
	}

	if _, err := s.db.NewUpdate().Model((*webhooks.Config)(nil)).
		Where("id = ?", id).
		Set("active = ?", active).
		Set("updated_at = ?", time.Now().UTC()).
		Exec(ctx); err != nil {
		return webhooks.Config{}, errors.Wrap(err, "updating one config activation")
	}

	cfg.Active = active
	return cfg, nil
}

func (s Store) UpdateOneConfigSecret(ctx context.Context, id, secret string) (webhooks.Config, error) {
	cfg := webhooks.Config{}
	if err := s.db.NewSelect().Model(&cfg).
		Where("id = ?", id).Scan(ctx); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return webhooks.Config{}, storage.ErrConfigNotFound
		}
		return webhooks.Config{}, errors.Wrap(err, "selecting one config before updating secret")
	}
	if cfg.Secret == secret {
		return webhooks.Config{}, storage.ErrConfigNotModified
	}

	if _, err := s.db.NewUpdate().Model((*webhooks.Config)(nil)).
		Where("id = ?", id).
		Set("secret = ?", secret).
		Set("updated_at = ?", time.Now().UTC()).
		Exec(ctx); err != nil {
		return webhooks.Config{}, errors.Wrap(err, "updating one config secret")
	}

	cfg.Secret = secret
	return cfg, nil
}

func (s Store) FindAttemptsToRetryByWebhookID(ctx context.Context, webhookID string) ([]webhooks.Attempt, error) {
	res := []webhooks.Attempt{}
	if err := s.db.NewSelect().Model(&res).
		Where("webhook_id = ?", webhookID).
		Where("status = ?", webhooks.StatusAttemptToRetry).
		Where("next_retry_after < ?", time.Now().UTC()).
		Order("created_at DESC").
		Scan(ctx); err != nil {
		return nil, errors.Wrap(err, "finding attempts to retry")
	}

	return res, nil
}

func (s Store) FindWebhookIDsToRetry(ctx context.Context) ([]string, error) {
	atts := []webhooks.Attempt{}
	if err := s.db.NewSelect().Model(&atts).
		Column("webhook_id").Distinct().
		Where("status = ?", webhooks.StatusAttemptToRetry).
		Where("next_retry_after < ?", time.Now().UTC()).
		Scan(ctx); err != nil {
		return nil, errors.Wrap(err, "finding distinct webhook IDs to retry")
	}

	webhookIDs := []string{}
	for _, att := range atts {
		webhookIDs = append(webhookIDs, att.WebhookID)
	}

	return webhookIDs, nil
}

func (s Store) UpdateAttemptsStatus(ctx context.Context, webhookID, status string) ([]webhooks.Attempt, error) {
	atts := []webhooks.Attempt{}
	if err := s.db.NewSelect().Model(&atts).
		Where("webhook_id = ?", webhookID).Scan(ctx); err != nil {
		return []webhooks.Attempt{}, errors.Wrap(err, "selecting attempts by webhook ID before updating status")
	}
	if len(atts) == 0 {
		return []webhooks.Attempt{}, storage.ErrWebhookIDNotFound
	}

	toUpdate := false
	for _, att := range atts {
		if att.Status != status {
			toUpdate = true
		}
	}
	if !toUpdate {
		return []webhooks.Attempt{}, storage.ErrAttemptsNotModified
	}

	if _, err := s.db.NewUpdate().Model((*webhooks.Attempt)(nil)).
		Where("webhook_id = ?", webhookID).
		Set("status = ?", status).
		Set("updated_at = ?", time.Now().UTC()).
		Exec(ctx); err != nil {
		return []webhooks.Attempt{}, errors.Wrap(err, "updating attempts status")
	}

	for _, att := range atts {
		att.Status = status
	}

	return atts, nil
}

func (s Store) InsertOneAttempt(ctx context.Context, att webhooks.Attempt) error {
	if _, err := s.db.NewInsert().Model(&att).Exec(ctx); err != nil {
		return errors.Wrap(err, "inserting one attempt")
	}

	return nil
}

func (s Store) Close(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	if err := s.db.PingContext(ctx); err == nil {
		if err := s.db.Close(); err != nil {
			return errors.Wrap(err, "closing postgres")
		}
	}

	return nil
}
