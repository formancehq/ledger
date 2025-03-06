package ledger

import (
	"encoding/json"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/time"
	"github.com/google/uuid"
)

type ConnectorConfiguration struct {
	Driver string          `json:"driver" bun:"driver"`
	Config json.RawMessage `json:"config" bun:"config"`
}

func NewConnectorConfiguration(driver string, config json.RawMessage) ConnectorConfiguration {
	return ConnectorConfiguration{
		Driver: driver,
		Config: config,
	}
}

type Connector struct {
	bun.BaseModel `bun:"table:_system.connectors"`

	ID        string    `json:"id" bun:"id,pk"`
	CreatedAt time.Time `json:"createdAt" bun:"created_at"`
	ConnectorConfiguration
}

func NewConnector(configuration ConnectorConfiguration) Connector {
	return Connector{
		ConnectorConfiguration: configuration,
		ID:                     uuid.NewString(),
		CreatedAt:              time.Now(),
	}
}
