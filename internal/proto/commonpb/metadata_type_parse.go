package commonpb

import (
	"fmt"
	"strings"
)

var (
	targetTypeMap = map[string]TargetType{
		"account":     TargetType_TARGET_TYPE_ACCOUNT,
		"transaction": TargetType_TARGET_TYPE_TRANSACTION,
	}

	targetTypeNames = map[TargetType]string{
		TargetType_TARGET_TYPE_ACCOUNT:     "account",
		TargetType_TARGET_TYPE_TRANSACTION: "transaction",
	}

	metadataTypeMap = map[string]MetadataType{
		"string": MetadataType_METADATA_TYPE_STRING,
		"int64":  MetadataType_METADATA_TYPE_INT64,
		"bool":   MetadataType_METADATA_TYPE_BOOL,
		"uint64": MetadataType_METADATA_TYPE_UINT64,
		"int8":   MetadataType_METADATA_TYPE_INT8,
		"int16":  MetadataType_METADATA_TYPE_INT16,
		"int32":  MetadataType_METADATA_TYPE_INT32,
		"uint8":  MetadataType_METADATA_TYPE_UINT8,
		"uint16": MetadataType_METADATA_TYPE_UINT16,
		"uint32": MetadataType_METADATA_TYPE_UINT32,
	}

	metadataTypeNames = map[MetadataType]string{
		MetadataType_METADATA_TYPE_STRING: "string",
		MetadataType_METADATA_TYPE_INT64:  "int64",
		MetadataType_METADATA_TYPE_BOOL:   "bool",
		MetadataType_METADATA_TYPE_UINT64: "uint64",
		MetadataType_METADATA_TYPE_INT8:   "int8",
		MetadataType_METADATA_TYPE_INT16:  "int16",
		MetadataType_METADATA_TYPE_INT32:  "int32",
		MetadataType_METADATA_TYPE_UINT8:  "uint8",
		MetadataType_METADATA_TYPE_UINT16: "uint16",
		MetadataType_METADATA_TYPE_UINT32: "uint32",
	}

	conversionStatusNames = map[MetadataConversionStatus]string{
		MetadataConversionStatus_METADATA_CONVERSION_COMPLETE:   "COMPLETE",
		MetadataConversionStatus_METADATA_CONVERSION_CONVERTING: "CONVERTING",
	}
)

// ParseTargetType converts "account"/"transaction" to TargetType.
func ParseTargetType(s string) (TargetType, error) {
	t, ok := targetTypeMap[strings.ToLower(s)]
	if !ok {
		return 0, fmt.Errorf("invalid target type %q: must be one of %s", s, strings.Join(TargetTypeOptions(), ", "))
	}
	return t, nil
}

// ParseMetadataType converts "string"/"int64"/"bool"/etc to MetadataType.
func ParseMetadataType(s string) (MetadataType, error) {
	t, ok := metadataTypeMap[strings.ToLower(s)]
	if !ok {
		return 0, fmt.Errorf("invalid metadata type %q: must be one of %s", s, strings.Join(MetadataTypeOptions(), ", "))
	}
	return t, nil
}

// MetadataTypeToString returns user-friendly name for a MetadataType.
func MetadataTypeToString(t MetadataType) string {
	if name, ok := metadataTypeNames[t]; ok {
		return name
	}
	return t.String()
}

// TargetTypeToString returns user-friendly name for a TargetType.
func TargetTypeToString(t TargetType) string {
	if name, ok := targetTypeNames[t]; ok {
		return name
	}
	return t.String()
}

// ConversionStatusToString returns user-friendly name for a MetadataConversionStatus.
func ConversionStatusToString(s MetadataConversionStatus) string {
	if name, ok := conversionStatusNames[s]; ok {
		return name
	}
	return s.String()
}

// MetadataTypeOptions returns valid type names.
func MetadataTypeOptions() []string {
	return []string{"string", "int64", "bool", "uint64", "int8", "int16", "int32", "uint8", "uint16", "uint32"}
}

// TargetTypeOptions returns valid target names.
func TargetTypeOptions() []string {
	return []string{"account", "transaction"}
}
