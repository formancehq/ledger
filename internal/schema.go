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
		return Schema{}, NewErrInvalidSchema(errors.New("missing chart of accounts"))
	}
	if data.Transactions == nil {
		return Schema{}, NewErrInvalidSchema(errors.New("missing transaction templates"))
	}
	if err := data.Transactions.Validate(); err != nil {
		return Schema{}, NewErrInvalidSchema(err)
	}
	return Schema{
		Version:    version,
		SchemaData: data,
	}, nil
}
