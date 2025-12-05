package ledger

import (
	"errors"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/time"
)

type SchemaData struct {
	Chart        ChartOfAccounts      `json:"chart" bun:"chart"`
	Transactions TransactionTemplates `json:"transactions" bun:"transactions"`
}

type Schema struct {
	bun.BaseModel `bun:"table:schemas,alias:schemas"`
	SchemaData

	Version   string    `json:"version" bun:"version"`
	CreatedAt time.Time `json:"createdAt" bun:"created_at,nullzero"`
}

func NewSchema(version string, data SchemaData) (Schema, error) {
	if data.Chart == nil {
		return Schema{}, ErrInvalidSchema{
			err: errors.New("missing chart of accounts"),
		}
	}
	return Schema{
		Version:    version,
		SchemaData: data,
	}, nil
}
