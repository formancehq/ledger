package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

type Account struct {
	bun.BaseModel `bun:"accounts.account"`

	ID        uuid.UUID `bun:",pk,nullzero"`
	CreatedAt time.Time `bun:",nullzero"`
	Reference string
	Provider  string
	Type      AccountType

	Payments []*Payment `bun:"rel:has-many,join:id=account_id"`
}

type AccountType string

const (
	AccountTypeSource  AccountType = "SOURCE"
	AccountTypeTarget  AccountType = "TARGET"
	AccountTypeUnknown AccountType = "UNKNOWN"
)
