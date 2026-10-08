package protohelpers

import (
	"fmt"
	"strings"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

var (
	targetTypeMap = map[string]ledgerpb.TargetType{
		"account":     ledgerpb.TargetType_TARGET_TYPE_ACCOUNT,
		"transaction": ledgerpb.TargetType_TARGET_TYPE_TRANSACTION,
		"ledger":      ledgerpb.TargetType_TARGET_TYPE_LEDGER,
	}

	targetTypeNames = map[ledgerpb.TargetType]string{
		ledgerpb.TargetType_TARGET_TYPE_ACCOUNT:     "account",
		ledgerpb.TargetType_TARGET_TYPE_TRANSACTION: "transaction",
		ledgerpb.TargetType_TARGET_TYPE_LEDGER:      "ledger",
	}

	metadataTypeMap = map[string]ledgerpb.MetadataType{
		"string":   ledgerpb.MetadataType_METADATA_TYPE_STRING,
		"int64":    ledgerpb.MetadataType_METADATA_TYPE_INT64,
		"bool":     ledgerpb.MetadataType_METADATA_TYPE_BOOL,
		"uint64":   ledgerpb.MetadataType_METADATA_TYPE_UINT64,
		"int8":     ledgerpb.MetadataType_METADATA_TYPE_INT8,
		"int16":    ledgerpb.MetadataType_METADATA_TYPE_INT16,
		"int32":    ledgerpb.MetadataType_METADATA_TYPE_INT32,
		"uint8":    ledgerpb.MetadataType_METADATA_TYPE_UINT8,
		"uint16":   ledgerpb.MetadataType_METADATA_TYPE_UINT16,
		"uint32":   ledgerpb.MetadataType_METADATA_TYPE_UINT32,
		"datetime": ledgerpb.MetadataType_METADATA_TYPE_DATETIME,
	}

	metadataTypeNames = map[ledgerpb.MetadataType]string{
		ledgerpb.MetadataType_METADATA_TYPE_STRING:   "string",
		ledgerpb.MetadataType_METADATA_TYPE_INT64:    "int64",
		ledgerpb.MetadataType_METADATA_TYPE_BOOL:     "bool",
		ledgerpb.MetadataType_METADATA_TYPE_UINT64:   "uint64",
		ledgerpb.MetadataType_METADATA_TYPE_INT8:     "int8",
		ledgerpb.MetadataType_METADATA_TYPE_INT16:    "int16",
		ledgerpb.MetadataType_METADATA_TYPE_INT32:    "int32",
		ledgerpb.MetadataType_METADATA_TYPE_UINT8:    "uint8",
		ledgerpb.MetadataType_METADATA_TYPE_UINT16:   "uint16",
		ledgerpb.MetadataType_METADATA_TYPE_UINT32:   "uint32",
		ledgerpb.MetadataType_METADATA_TYPE_DATETIME: "datetime",
	}
)

// ParseTargetType converts "account"/"transaction"/"ledger" to ledgerpb.TargetType.
func ParseTargetType(s string) (ledgerpb.TargetType, error) {
	t, ok := targetTypeMap[strings.ToLower(s)]
	if !ok {
		return 0, fmt.Errorf("invalid target type %q: must be one of %s", s, strings.Join(TargetTypeOptions(), ", "))
	}

	return t, nil
}

// ParseMetadataType converts "string"/"int64"/"bool"/etc to ledgerpb.MetadataType.
func ParseMetadataType(s string) (ledgerpb.MetadataType, error) {
	t, ok := metadataTypeMap[strings.ToLower(s)]
	if !ok {
		return 0, fmt.Errorf("invalid metadata type %q: must be one of %s", s, strings.Join(MetadataTypeOptions(), ", "))
	}

	return t, nil
}

// MetadataTypeToString returns user-friendly name for a ledgerpb.MetadataType.
func MetadataTypeToString(t ledgerpb.MetadataType) string {
	if name, ok := metadataTypeNames[t]; ok {
		return name
	}

	return t.String()
}

// TargetTypeToString returns user-friendly name for a ledgerpb.TargetType.
func TargetTypeToString(t ledgerpb.TargetType) string {
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
