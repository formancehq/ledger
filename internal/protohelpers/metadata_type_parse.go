package protohelpers

import ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

// Compatibility aliases. Enum parsing and formatting are protobuf concerns and
// are implemented next to the generated public models.
var (
	ParseTargetType      = ledgerpb.ParseTargetType
	ParseMetadataType    = ledgerpb.ParseMetadataType
	MetadataTypeToString = ledgerpb.MetadataTypeToString
	TargetTypeToString   = ledgerpb.TargetTypeToString
	MetadataTypeOptions  = ledgerpb.MetadataTypeOptions
	TargetTypeOptions    = ledgerpb.TargetTypeOptions
)
