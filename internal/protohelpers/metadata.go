package protohelpers

import (
	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// MetadataFromGoMap adapts the server's metadata representation to the public wire type.
func MetadataFromGoMap(m metadata.Metadata) map[string]*ledgerpb.MetadataValue {
	if m == nil {
		return nil
	}

	result := make(map[string]*ledgerpb.MetadataValue, len(m))
	for k, v := range m {
		result[k] = ledgerpb.NewStringValue(v)
	}

	return result
}

// MetadataToGoMap adapts public metadata values to the server's string map.
func MetadataToGoMap(m map[string]*ledgerpb.MetadataValue) metadata.Metadata {
	if m == nil {
		return nil
	}

	result := make(metadata.Metadata, len(m))
	for k, v := range m {
		if v != nil {
			result[k] = MetadataValueToString(v)
		}
	}

	return result
}

func MetadataMapToGoMap(mm *ledgerpb.MetadataMap) metadata.Metadata {
	if mm == nil {
		return nil
	}

	return MetadataToGoMap(mm.GetValues())
}

func MetadataMapFromGoMap(m metadata.Metadata) *ledgerpb.MetadataMap {
	if m == nil {
		return nil
	}

	return &ledgerpb.MetadataMap{Values: MetadataFromGoMap(m)}
}
