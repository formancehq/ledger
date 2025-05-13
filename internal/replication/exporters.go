package replication

import "encoding/json"

//go:generate mockgen -source exporters.go -destination exporters_generated.go -package replication . ConfigValidator
type ConfigValidator interface {
	ValidateConfig(exporterName string, rawExporterConfig json.RawMessage) error
}
