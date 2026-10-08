package cmdutil

import (
	"fmt"
	"strings"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/protohelpers"
)

// ParseTargetType converts "account"/"transaction" to ledgerpb.TargetType.
func ParseTargetType(s string) (ledgerpb.TargetType, error) {
	return protohelpers.ParseTargetType(s)
}

// ParseMetadataType converts "string"/"int64"/"bool"/etc to ledgerpb.MetadataType.
func ParseMetadataType(s string) (ledgerpb.MetadataType, error) {
	return protohelpers.ParseMetadataType(s)
}

// MetadataTypeString returns user-friendly name for a MetadataType.
func MetadataTypeString(t ledgerpb.MetadataType) string {
	return protohelpers.MetadataTypeToString(t)
}

// TargetTypeString returns user-friendly name for a TargetType.
func TargetTypeString(t ledgerpb.TargetType) string {
	return protohelpers.TargetTypeToString(t)
}

// MetadataTypeOptions returns valid type names for interactive select.
func MetadataTypeOptions() []string {
	return protohelpers.MetadataTypeOptions()
}

// TargetTypeOptions returns valid target names for interactive select.
func TargetTypeOptions() []string {
	return protohelpers.TargetTypeOptions()
}

// ParseSchemaEntry parses a "target:key:type" string into its components.
func ParseSchemaEntry(s string) (ledgerpb.TargetType, string, ledgerpb.MetadataType, error) {
	targetName, rest, ok := strings.Cut(s, ":")
	separator := strings.LastIndex(rest, ":")
	if !ok || separator < 0 {
		return 0, "", 0, fmt.Errorf("invalid schema entry %q: expected target:key:type format", s)
	}

	target, err := ParseTargetType(targetName)
	if err != nil {
		return 0, "", 0, err
	}

	key := rest[:separator]
	if key == "" {
		return 0, "", 0, fmt.Errorf("invalid schema entry %q: key cannot be empty", s)
	}

	mdType, err := ParseMetadataType(rest[separator+1:])
	if err != nil {
		return 0, "", 0, err
	}

	return target, key, mdType, nil
}
