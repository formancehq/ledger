package commonpb

import "google.golang.org/protobuf/encoding/protojson"

// MarshalJSON preserves protobuf oneofs and camelCase connection components
// when an enclosing log uses the application's ordinary JSON encoder.
func (x *MirrorSourceConfig) MarshalJSON() ([]byte, error) {
	return protojson.Marshal(x)
}
