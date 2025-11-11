package ledger

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/time"
)

type ExporterConfiguration struct {
	Driver string          `json:"driver" bun:"driver"`
	Config json.RawMessage `json:"config" bun:"config"`
}

func NewExporterConfiguration(driver string, config json.RawMessage) ExporterConfiguration {
	return ExporterConfiguration{
		Driver: driver,
		Config: config,
	}
}

type Exporter struct {
	bun.BaseModel `bun:"table:_system.exporters"`

	ID        string    `json:"id" bun:"id,pk"`
	CreatedAt time.Time `json:"createdAt" bun:"created_at"`
	ExporterConfiguration
}

func NewExporter(configuration ExporterConfiguration) Exporter {
	return Exporter{
		ExporterConfiguration: configuration,
		ID:                    uuid.NewString(),
		CreatedAt:             time.Now(),
	}
}
