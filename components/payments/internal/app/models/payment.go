package models

import (
	"encoding/json"
	"time"

	"github.com/uptrace/bun"

	"github.com/google/uuid"
)

type Payment struct {
	bun.BaseModel `bun:"payments.payment"`

	ID          uuid.UUID `bun:",pk,nullzero"`
	ConnectorID uuid.UUID `bun:",nullzero"`
	CreatedAt   time.Time `bun:",nullzero"`
	Reference   string
	Amount      int64
	Type        PaymentType
	Status      PaymentStatus
	Scheme      PaymentScheme
	Asset       PaymentAsset

	RawData json.RawMessage

	AccountID uuid.UUID `bun:",nullzero"`

	Account     *Account      `bun:"rel:has-one,join:account_id=id"`
	Adjustments []*Adjustment `bun:"rel:has-many,join:id=payment_id"`
	Metadata    []*Metadata   `bun:"rel:has-many,join:id=payment_id"`
	Connector   *Connector    `bun:"rel:has-one,join:connector_id=id"`
}

type (
	PaymentType   string
	PaymentStatus string
	PaymentScheme string
	PaymentAsset  string
)

const (
	PaymentTypePayIn    PaymentType = "PAY-IN"
	PaymentTypePayOut   PaymentType = "PAYOUT"
	PaymentTypeTransfer PaymentType = "TRANSFER"
	PaymentTypeOther    PaymentType = "OTHER"
)

const (
	PaymentStatusPending   PaymentStatus = "PENDING"
	PaymentStatusSucceeded PaymentStatus = "SUCCEEDED"
	PaymentStatusCancelled PaymentStatus = "CANCELLED"
	PaymentStatusFailed    PaymentStatus = "FAILED"
	PaymentStatusOther     PaymentStatus = "OTHER"
)

const (
	PaymentSchemeUnknown PaymentScheme = "unknown"
	PaymentSchemeOther   PaymentScheme = "other"

	PaymentSchemeCardVisa       PaymentScheme = "visa"
	PaymentSchemeCardMasterCard PaymentScheme = "mastercard"
	PaymentSchemeCardAmex       PaymentScheme = "amex"
	PaymentSchemeCardDiners     PaymentScheme = "diners"
	PaymentSchemeCardDiscover   PaymentScheme = "discover"
	PaymentSchemeCardJCB        PaymentScheme = "jcb"
	PaymentSchemeCardUnionPay   PaymentScheme = "unionpay"

	PaymentSchemeSepaDebit  PaymentScheme = "sepa debit"
	PaymentSchemeSepaCredit PaymentScheme = "sepa credit"
	PaymentSchemeSepa       PaymentScheme = "sepa"

	PaymentSchemeApplePay  PaymentScheme = "apple pay"
	PaymentSchemeGooglePay PaymentScheme = "google pay"

	PaymentSchemeA2A      PaymentScheme = "a2a"
	PaymentSchemeACHDebit PaymentScheme = "ach debit"
	PaymentSchemeACH      PaymentScheme = "ach"
	PaymentSchemeRTP      PaymentScheme = "rtp"
)

func (t PaymentType) String() string {
	return string(t)
}

func (t PaymentStatus) String() string {
	return string(t)
}

func (t PaymentScheme) String() string {
	return string(t)
}

func (t PaymentAsset) String() string {
	return string(t)
}
