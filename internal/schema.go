package ledger

import (
	"github.com/formancehq/go-libs/v3/time"
	"github.com/uptrace/bun"
)

type SchemaData struct {
	Chart *ChartOfAccounts `json:"chart" bun:"chart"`
}

type Schema struct {
	bun.BaseModel `bun:"table:schemas,alias:schemas"`
	SchemaData

	Version   string    `json:"version" bun:"version"`
	CreatedAt time.Time `json:"createdAt" bun:"created_at,nullzero"`
}

func NewSchema(version string, data SchemaData) Schema {
	return Schema{
		Version:    version,
		SchemaData: data,
	}
}
