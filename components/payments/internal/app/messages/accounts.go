package messages

import (
	"time"

	"github.com/formancehq/payments/internal/app/models"
)

type accountMessagePayload struct {
	ID        string             `json:"id"`
	CreatedAt time.Time          `json:"createdAt"`
	Reference string             `json:"reference"`
	Provider  string             `json:"provider"`
	Type      models.AccountType `json:"type"`
}

func NewEventSavedAccounts(accounts []models.Account) EventMessage {
	payload := make([]accountMessagePayload, len(accounts))

	for accountIdx, account := range accounts {
		payload[accountIdx] = accountMessagePayload{
			ID:        account.ID.String(),
			CreatedAt: account.CreatedAt,
			Reference: account.Reference,
			Provider:  account.Provider,
			Type:      account.Type,
		}
	}

	return EventMessage{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeSavedAccounts,
		Payload: payload,
	}
}
