package signaturepb

import jsonv2 "encoding/json/v2"

// MarshalJSON emits the public camelCase projection used by the API. These
// envelopes are intentionally hand-written because signature.proto is shared
// with components that do not use the Ledger JSON generator.
func (x *SignedApplyBatch) MarshalJSON() ([]byte, error) {
	object := make(map[string]any)
	if x.KeyId != "" {
		object["keyId"] = x.KeyId
	}
	if len(x.Signature) != 0 {
		object["signature"] = x.Signature
	}
	if len(x.Payload) != 0 {
		object["payload"] = x.Payload
	}
	return jsonv2.Marshal(object)
}

func (x *SignedLog) MarshalJSON() ([]byte, error) {
	object := make(map[string]any)
	if x.KeyId != "" {
		object["keyId"] = x.KeyId
	}
	if len(x.Signature) != 0 {
		object["signature"] = x.Signature
	}
	if len(x.Payload) != 0 {
		object["payload"] = x.Payload
	}
	return jsonv2.Marshal(object)
}
