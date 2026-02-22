package cmdutil

import (
	"fmt"
	"strings"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

var (
	targetTypeMap = map[string]commonpb.TargetType{
		"account":     commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		"transaction": commonpb.TargetType_TARGET_TYPE_TRANSACTION,
	}

	targetTypeNames = map[commonpb.TargetType]string{
		commonpb.TargetType_TARGET_TYPE_ACCOUNT:     "account",
		commonpb.TargetType_TARGET_TYPE_TRANSACTION: "transaction",
	}

	metadataTypeMap = map[string]commonpb.MetadataType{
		"string": commonpb.MetadataType_METADATA_TYPE_STRING,
		"int64":  commonpb.MetadataType_METADATA_TYPE_INT64,
		"bool":   commonpb.MetadataType_METADATA_TYPE_BOOL,
		"uint64": commonpb.MetadataType_METADATA_TYPE_UINT64,
		"int8":   commonpb.MetadataType_METADATA_TYPE_INT8,
		"int16":  commonpb.MetadataType_METADATA_TYPE_INT16,
		"int32":  commonpb.MetadataType_METADATA_TYPE_INT32,
		"uint8":  commonpb.MetadataType_METADATA_TYPE_UINT8,
		"uint16": commonpb.MetadataType_METADATA_TYPE_UINT16,
		"uint32": commonpb.MetadataType_METADATA_TYPE_UINT32,
	}

	metadataTypeNames = map[commonpb.MetadataType]string{
		commonpb.MetadataType_METADATA_TYPE_STRING: "string",
		commonpb.MetadataType_METADATA_TYPE_INT64:  "int64",
		commonpb.MetadataType_METADATA_TYPE_BOOL:   "bool",
		commonpb.MetadataType_METADATA_TYPE_UINT64: "uint64",
		commonpb.MetadataType_METADATA_TYPE_INT8:   "int8",
		commonpb.MetadataType_METADATA_TYPE_INT16:  "int16",
		commonpb.MetadataType_METADATA_TYPE_INT32:  "int32",
		commonpb.MetadataType_METADATA_TYPE_UINT8:  "uint8",
		commonpb.MetadataType_METADATA_TYPE_UINT16: "uint16",
		commonpb.MetadataType_METADATA_TYPE_UINT32: "uint32",
	}
)

// ParseTargetType converts "account"/"transaction" to commonpb.TargetType.
func ParseTargetType(s string) (commonpb.TargetType, error) {
	t, ok := targetTypeMap[strings.ToLower(s)]
	if !ok {
		return 0, fmt.Errorf("invalid target type %q: must be one of %s", s, strings.Join(TargetTypeOptions(), ", "))
	}
	return t, nil
}

// ParseMetadataType converts "string"/"int64"/"bool"/etc to commonpb.MetadataType.
func ParseMetadataType(s string) (commonpb.MetadataType, error) {
	t, ok := metadataTypeMap[strings.ToLower(s)]
	if !ok {
		return 0, fmt.Errorf("invalid metadata type %q: must be one of %s", s, strings.Join(MetadataTypeOptions(), ", "))
	}
	return t, nil
}

// MetadataTypeString returns user-friendly name for a MetadataType.
func MetadataTypeString(t commonpb.MetadataType) string {
	if name, ok := metadataTypeNames[t]; ok {
		return name
	}
	return t.String()
}

// TargetTypeString returns user-friendly name for a TargetType.
func TargetTypeString(t commonpb.TargetType) string {
	if name, ok := targetTypeNames[t]; ok {
		return name
	}
	return t.String()
}

// MetadataTypeOptions returns valid type names for interactive select.
func MetadataTypeOptions() []string {
	return []string{"string", "int64", "bool", "uint64", "int8", "int16", "int32", "uint8", "uint16", "uint32"}
}

// TargetTypeOptions returns valid target names for interactive select.
func TargetTypeOptions() []string {
	return []string{"account", "transaction"}
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
