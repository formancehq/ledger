package models

import (
	"encoding/json"
	"time"

	"github.com/uptrace/bun"

	"github.com/google/uuid"
)

type Adjustment struct {
	bun.BaseModel `bun:"payments.adjustment"`

	ID        uuid.UUID `bun:",pk,nullzero"`
	PaymentID uuid.UUID `bun:",pk,nullzero"`
	CreatedAt time.Time `bun:",nullzero"`
	Reference string
	Amount    int64
	Status    PaymentStatus
	Absolute  bool

	RawData json.RawMessage

	Payment *Payment `bun:"rel:has-one,join:payment_id=id"`
}
