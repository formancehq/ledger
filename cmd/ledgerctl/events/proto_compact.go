package events

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// protoCompact marshals a proto message to a compact single-line JSON string.
// Use this in table cells and printf output instead of protojson.Format, which
// emits indented multi-line JSON that breaks pterm table layout.
func protoCompact(m proto.Message) string {
	if m == nil || !m.ProtoReflect().IsValid() {
		return ""
	}
	b, err := protojson.MarshalOptions{Multiline: false, EmitUnpopulated: false}.Marshal(m)
	if err != nil {
		return "[error]"
	}

	return string(b)
}
