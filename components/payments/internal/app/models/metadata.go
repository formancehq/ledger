package models

import (
	"time"

	"github.com/uptrace/bun"

	"github.com/google/uuid"
)

type Metadata struct {
	bun.BaseModel `bun:"payments.metadata"`

	PaymentID uuid.UUID `bun:",pk,nullzero"`
	CreatedAt time.Time `bun:",nullzero"`
	Key       string    `bun:",pk,nullzero"`
	Value     string

	Changelog []MetadataChangelog `bun:",pk,nullzero"`
	Payment   *Payment            `bun:"rel:has-one,join:payment_id=id"`
}

type MetadataChangelog struct {
	CreatedAt time.Time
	Value     string
}
