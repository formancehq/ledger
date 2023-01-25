package messages

import (
	"time"

	"github.com/formancehq/payments/internal/app/models"
)

type connectorMessagePayload struct {
	CreatedAt time.Time                `json:"createdAt"`
	Connector models.ConnectorProvider `json:"connector"`
}

func NewEventResetConnector(connector models.ConnectorProvider) EventMessage {
	return EventMessage{
		Date:    time.Now().UTC(),
		App:     EventApp,
		Version: EventVersion,
		Type:    EventTypeConnectorReset,
		Payload: connectorMessagePayload{
			CreatedAt: time.Now().UTC(),
			Connector: connector,
		},
	}
}
