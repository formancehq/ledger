//go:build fctl_component_guest

package servicepb

import "google.golang.org/protobuf/proto"

func unmarshalApplyBatch(payload []byte, batch *ApplyBatch) error {
	return proto.Unmarshal(payload, batch)
}
