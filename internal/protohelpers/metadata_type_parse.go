package protohelpers

import (
	"fmt"
	"strings"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

var (
	targetTypeMap = map[string]commonpb.TargetType{
		"account":     commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		"transaction": commonpb.TargetType_TARGET_TYPE_TRANSACTION,
		"ledger":      commonpb.TargetType_TARGET_TYPE_LEDGER,
	}

	targetTypeNames = map[commonpb.TargetType]string{
		commonpb.TargetType_TARGET_TYPE_ACCOUNT:     "account",
		commonpb.TargetType_TARGET_TYPE_TRANSACTION: "transaction",
		commonpb.TargetType_TARGET_TYPE_LEDGER:      "ledger",
	}

	metadataTypeMap = map[string]commonpb.MetadataType{
		"string":   commonpb.MetadataType_METADATA_TYPE_STRING,
		"int64":    commonpb.MetadataType_METADATA_TYPE_INT64,
		"bool":     commonpb.MetadataType_METADATA_TYPE_BOOL,
		"uint64":   commonpb.MetadataType_METADATA_TYPE_UINT64,
		"int8":     commonpb.MetadataType_METADATA_TYPE_INT8,
		"int16":    commonpb.MetadataType_METADATA_TYPE_INT16,
		"int32":    commonpb.MetadataType_METADATA_TYPE_INT32,
		"uint8":    commonpb.MetadataType_METADATA_TYPE_UINT8,
		"uint16":   commonpb.MetadataType_METADATA_TYPE_UINT16,
		"uint32":   commonpb.MetadataType_METADATA_TYPE_UINT32,
		"datetime": commonpb.MetadataType_METADATA_TYPE_DATETIME,
	}

	metadataTypeNames = map[commonpb.MetadataType]string{
		commonpb.MetadataType_METADATA_TYPE_STRING:   "string",
		commonpb.MetadataType_METADATA_TYPE_INT64:    "int64",
		commonpb.MetadataType_METADATA_TYPE_BOOL:     "bool",
		commonpb.MetadataType_METADATA_TYPE_UINT64:   "uint64",
		commonpb.MetadataType_METADATA_TYPE_INT8:     "int8",
		commonpb.MetadataType_METADATA_TYPE_INT16:    "int16",
		commonpb.MetadataType_METADATA_TYPE_INT32:    "int32",
		commonpb.MetadataType_METADATA_TYPE_UINT8:    "uint8",
		commonpb.MetadataType_METADATA_TYPE_UINT16:   "uint16",
		commonpb.MetadataType_METADATA_TYPE_UINT32:   "uint32",
		commonpb.MetadataType_METADATA_TYPE_DATETIME: "datetime",
	}
)

// ParseTargetType converts "account"/"transaction"/"ledger" to commonpb.TargetType.
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

// MetadataTypeToString returns user-friendly name for a commonpb.MetadataType.
func MetadataTypeToString(t commonpb.MetadataType) string {
	if name, ok := metadataTypeNames[t]; ok {
		return name
	}

	return t.String()
}

// TargetTypeToString returns user-friendly name for a commonpb.TargetType.
func TargetTypeToString(t commonpb.TargetType) string {
	if name, ok := targetTypeNames[t]; ok {
		return name
	}

	return t.String()
}

// MetadataTypeOptions returns valid type names.
func MetadataTypeOptions() []string {
	return []string{"string", "int64", "bool", "uint64", "int8", "int16", "int32", "uint8", "uint16", "uint32", "datetime"}
}

// TargetTypeOptions returns valid target names.
func TargetTypeOptions() []string {
	return []string{"account", "transaction", "ledger"}
}
