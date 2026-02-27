package ledger

import (
	"errors"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v4/time"
)

type SchemaData struct {
	Chart        ChartOfAccounts      `json:"chart" bun:"chart"`
	Transactions TransactionTemplates `json:"transactions,omitempty" bun:"transactions"`
	Queries      QueryTemplates       `json:"queries,omitempty" bun:"queries"`
}

type Schema struct {
	bun.BaseModel `bun:"table:schemas,alias:schemas"`
	SchemaData

	Version   string    `json:"version" bun:"version"`
	CreatedAt time.Time `json:"createdAt" bun:"created_at,nullzero"`
}

func NewSchema(version string, data SchemaData) (Schema, error) {
	if data.Chart == nil {
		return Schema{}, NewErrInvalidSchema(errors.New("missing chart of accounts"))
	}
	if err := data.Transactions.Validate(); err != nil {
		return Schema{}, NewErrInvalidSchema(err)
	}
	if err := data.Queries.Validate(); err != nil {
		return Schema{}, NewErrInvalidSchema(err)
	}
	return Schema{
		Version:    version,
		SchemaData: data,
	}, nil
}
