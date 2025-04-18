package replication

import "encoding/json"

//go:generate mockgen -source connectors.go -destination connectors_generated.go -package replication . ConfigValidator
type ConfigValidator interface {
	ValidateConfig(connectorName string, rawConnectorConfig json.RawMessage) error
}
