package cmdutil

import (
	"fmt"
	"strings"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// ParseTargetType converts "account"/"transaction" to commonpb.TargetType.
func ParseTargetType(s string) (commonpb.TargetType, error) {
	return commonpb.ParseTargetType(s)
}

// ParseMetadataType converts "string"/"int64"/"bool"/etc to commonpb.MetadataType.
func ParseMetadataType(s string) (commonpb.MetadataType, error) {
	return commonpb.ParseMetadataType(s)
}

// MetadataTypeString returns user-friendly name for a MetadataType.
func MetadataTypeString(t commonpb.MetadataType) string {
	return commonpb.MetadataTypeToString(t)
}

// TargetTypeString returns user-friendly name for a TargetType.
func TargetTypeString(t commonpb.TargetType) string {
	return commonpb.TargetTypeToString(t)
}

// MetadataTypeOptions returns valid type names for interactive select.
func MetadataTypeOptions() []string {
	return commonpb.MetadataTypeOptions()
}

// TargetTypeOptions returns valid target names for interactive select.
func TargetTypeOptions() []string {
	return commonpb.TargetTypeOptions()
}

// ParseSchemaEntry parses a "target:key:type" string into its components.
func ParseSchemaEntry(s string) (commonpb.TargetType, string, commonpb.MetadataType, error) {
	parts := strings.SplitN(s, ":", 3)
	if len(parts) != 3 {
		return 0, "", 0, fmt.Errorf("invalid schema entry %q: expected target:key:type format", s)
	}

	target, err := ParseTargetType(parts[0])
	if err != nil {
		return 0, "", 0, err
	}

	key := parts[1]
	if key == "" {
		return 0, "", 0, fmt.Errorf("invalid schema entry %q: key cannot be empty", s)
	}

	mdType, err := ParseMetadataType(parts[2])
	if err != nil {
		return 0, "", 0, err
	}

	return target, key, mdType, nil
}
