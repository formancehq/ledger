package messages

import (
	"time"

	"github.com/formancehq/payments/internal/app/models"
)

type paymentMessagePayload struct {
	ID        string               `json:"id"`
	Reference string               `json:"reference"`
	CreatedAt time.Time            `json:"createdAt"`
	Provider  string               `json:"provider"`
	Type      models.PaymentType   `json:"type"`
	Status    models.PaymentStatus `json:"status"`
	Scheme    models.PaymentScheme `json:"scheme"`
	Asset     models.PaymentAsset  `json:"asset"`

	// TODO: Remove 'initialAmount' once frontend has switched to 'amount
	InitialAmount int64 `json:"initialAmount"`
	Amount        int64 `json:"amount"`
}

func NewEventSavedPayments(payment *models.Payment, provider models.ConnectorProvider) EventMessage {
	payload := paymentMessagePayload{
		ID:            payment.ID.String(),
		Reference:     payment.Reference,
		Type:          payment.Type,
		Status:        payment.Status,
		InitialAmount: payment.Amount,
		Scheme:        payment.Scheme,
		Asset:         payment.Asset,
		CreatedAt:     payment.CreatedAt,
		Amount:        payment.Amount,
		Provider:      provider.String(),
	}

	return EventMessage{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeSavedPayments,
		Payload: payload,
	}
}
