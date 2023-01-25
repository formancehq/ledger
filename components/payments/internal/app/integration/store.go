package integration

import (
	"context"
	"encoding/json"

	"github.com/formancehq/payments/internal/app/models"
)

type Repository interface {
	FindAll(ctx context.Context) ([]models.Connector, error)
	IsInstalled(ctx context.Context, name models.ConnectorProvider) (bool, error)
	Install(ctx context.Context, name models.ConnectorProvider, config json.RawMessage) error
	Uninstall(ctx context.Context, name models.ConnectorProvider) error
	UpdateConfig(ctx context.Context, name models.ConnectorProvider, config json.RawMessage) error
	Enable(ctx context.Context, name models.ConnectorProvider) error
	Disable(ctx context.Context, name models.ConnectorProvider) error
	IsEnabled(ctx context.Context, name models.ConnectorProvider) (bool, error)
	GetConnector(ctx context.Context, name models.ConnectorProvider) (*models.Connector, error)
}
