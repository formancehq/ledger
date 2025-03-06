package system

import "encoding/json"

//go:generate mockgen -source controller.go -destination connectors_generated.go -package system . ConfigValidator
type ConfigValidator interface {
	ValidateConfig(connectorName string, rawConnectorConfig json.RawMessage) error
}
