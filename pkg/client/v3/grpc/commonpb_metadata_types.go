package grpc

import (
	"fmt"
	"strings"
)

var targetTypeNames = map[TargetType]string{
	TargetType_TARGET_TYPE_ACCOUNT:     "account",
	TargetType_TARGET_TYPE_TRANSACTION: "transaction",
	TargetType_TARGET_TYPE_LEDGER:      "ledger",
}

var metadataTypeNames = map[MetadataType]string{
	MetadataType_METADATA_TYPE_STRING:   "string",
	MetadataType_METADATA_TYPE_INT64:    "int64",
	MetadataType_METADATA_TYPE_BOOL:     "bool",
	MetadataType_METADATA_TYPE_UINT64:   "uint64",
	MetadataType_METADATA_TYPE_INT8:     "int8",
	MetadataType_METADATA_TYPE_INT16:    "int16",
	MetadataType_METADATA_TYPE_INT32:    "int32",
	MetadataType_METADATA_TYPE_UINT8:    "uint8",
	MetadataType_METADATA_TYPE_UINT16:   "uint16",
	MetadataType_METADATA_TYPE_UINT32:   "uint32",
	MetadataType_METADATA_TYPE_DATETIME: "datetime",
}

func ParseTargetType(s string) (TargetType, error) {
	for value, name := range targetTypeNames {
		if strings.EqualFold(name, s) {
			return value, nil
		}
	}
	return 0, fmt.Errorf("invalid target type %q: must be one of %s", s, strings.Join(TargetTypeOptions(), ", "))
}

func ParseMetadataType(s string) (MetadataType, error) {
	for value, name := range metadataTypeNames {
		if strings.EqualFold(name, s) {
			return value, nil
		}
	}
	return 0, fmt.Errorf("invalid metadata type %q: must be one of %s", s, strings.Join(MetadataTypeOptions(), ", "))
}

func MetadataTypeToString(t MetadataType) string {
	if name, ok := metadataTypeNames[t]; ok {
		return name
	}
	return t.String()
}

func TargetTypeToString(t TargetType) string {
	if name, ok := targetTypeNames[t]; ok {
		return name
	}
	return t.String()
}

func MetadataTypeOptions() []string {
	return []string{"string", "int64", "bool", "uint64", "int8", "int16", "int32", "uint8", "uint16", "uint32", "datetime"}
}

func TargetTypeOptions() []string {
	return []string{"account", "transaction", "ledger"}
}
