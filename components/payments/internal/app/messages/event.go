package messages

import (
	"time"
)

const (
	TopicPayments   = "payments"
	TopicConnectors = "connectors"

	EventVersion = "v1"
	EventApp     = "payments"

	EventTypeSavedPayments  = "SAVED_PAYMENT"
	EventTypeSavedAccounts  = "SAVED_ACCOUNT"
	EventTypeConnectorReset = "CONNECTOR_RESET"
)

type EventMessage struct {
	Date    time.Time `json:"date"`
	App     string    `json:"app"`
	Version string    `json:"version"`
	Type    string    `json:"type"`
	Payload any       `json:"payload"`
}
