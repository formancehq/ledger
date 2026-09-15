//go:build !fctl_component_guest

package servicepb

func unmarshalApplyBatch(payload []byte, batch *ApplyBatch) error {
	return batch.UnmarshalVT(payload)
}
