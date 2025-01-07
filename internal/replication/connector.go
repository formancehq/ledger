package ingester

import (
	"encoding/json"

	"github.com/formancehq/go-libs/v2/time"
	"github.com/google/uuid"
)

type ConnectorConfiguration struct {
	Driver string          `json:"driver"`
	Config json.RawMessage `json:"config"`
}

func NewConnectorConfiguration(driver string, config json.RawMessage) ConnectorConfiguration {
	return ConnectorConfiguration{
		Driver: driver,
		Config: config,
	}
}

type Connector struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"createdAt"`
	ConnectorConfiguration
}

func NewConnector(configuration ConnectorConfiguration) Connector {
	return Connector{
		ConnectorConfiguration: configuration,
		ID:                     uuid.NewString(),
		CreatedAt:              time.Now(),
	}
}
