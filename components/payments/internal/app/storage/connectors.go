package storage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/formancehq/payments/internal/app/models"
)

func (s *Storage) ListConnectors(ctx context.Context) ([]*models.Connector, error) {
	var res []*models.Connector
	err := s.db.NewSelect().Model(&res).Scan(ctx)
	if err != nil {
		return nil, e("list connectors", err)
	}

	return res, nil
}

func (s *Storage) GetConfig(ctx context.Context, connectorProvider models.ConnectorProvider, destination any) error {
	var connector models.Connector

	err := s.db.NewSelect().Model(&connector).
		Column("config").
		Where("provider = ?", connectorProvider).
		Scan(ctx)
	if err != nil {
		return fmt.Errorf("failed to get config for connector %s: %w", connectorProvider, err)
	}

	err = json.Unmarshal(connector.Config, destination)
	if err != nil {
		return fmt.Errorf("failed to unmarshal config for connector %s: %w", connectorProvider, err)
	}

	return nil
}

func (s *Storage) FindAll(ctx context.Context) ([]models.Connector, error) {
	var connectors []models.Connector

	err := s.db.NewSelect().Model(&connectors).Scan(ctx)
	if err != nil {
		return nil, e("find all connectors", err)
	}

	return connectors, err
}

func (s *Storage) IsInstalled(ctx context.Context, provider models.ConnectorProvider) (bool, error) {
	exists, err := s.db.NewSelect().
		Model(&models.Connector{}).
		Where("provider = ?", provider).
		Exists(ctx)
	if err != nil {
		return false, e("find connector", err)
	}

	return exists, nil
}

func (s *Storage) Install(ctx context.Context, provider models.ConnectorProvider, config json.RawMessage) error {
	connector := models.Connector{
		Provider: provider,
		Enabled:  true,
		Config:   config,
	}

	_, err := s.db.NewInsert().Model(&connector).Exec(ctx)
	if err != nil {
		return e("install connector", err)
	}

	return nil
}

func (s *Storage) Uninstall(ctx context.Context, provider models.ConnectorProvider) error {
	_, err := s.db.NewDelete().
		Model(&models.Connector{}).
		Where("provider = ?", provider).
		Exec(ctx)
	if err != nil {
		return e("uninstall connector", err)
	}

	return nil
}

func (s *Storage) UpdateConfig(ctx context.Context, provider models.ConnectorProvider, config json.RawMessage) error {
	_, err := s.db.NewUpdate().
		Model(&models.Connector{}).
		Set("config = ?", config).
		Where("provider = ?", provider).
		Exec(ctx)
	if err != nil {
		return e("update connector config", err)
	}

	return nil
}

func (s *Storage) Enable(ctx context.Context, provider models.ConnectorProvider) error {
	_, err := s.db.NewUpdate().
		Model(&models.Connector{}).
		Set("enabled = TRUE").
		Where("provider = ?", provider).
		Exec(ctx)
	if err != nil {
		return e("enable connector", err)
	}

	return nil
}

func (s *Storage) Disable(ctx context.Context, provider models.ConnectorProvider) error {
	_, err := s.db.NewUpdate().
		Model(&models.Connector{}).
		Set("enabled = TRUE").
		Where("provider = ?", provider).
		Exec(ctx)
	if err != nil {
		return e("enable connector", err)
	}

	return nil
}

func (s *Storage) IsEnabled(ctx context.Context, provider models.ConnectorProvider) (bool, error) {
	var connector models.Connector

	err := s.db.NewSelect().
		Model(&connector).
		Where("provider = ?", provider).
		Scan(ctx)
	if err != nil {
		return false, e("find connector", err)
	}

	return connector.Enabled, nil
}

func (s *Storage) GetConnector(ctx context.Context, provider models.ConnectorProvider) (*models.Connector, error) {
	var connector models.Connector

	err := s.db.NewSelect().
		Model(&connector).
		Where("provider = ?", provider).
		Scan(ctx)
	if err != nil {
		return nil, e("find connector", err)
	}

	return &connector, nil
}
