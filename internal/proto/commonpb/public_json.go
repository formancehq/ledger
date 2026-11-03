package commonpb

import "encoding/json"

// MarshalJSON implements the public REST shape for index identifiers.
func (x *IndexID) MarshalJSON() ([]byte, error) {
	if x == nil {
		return []byte("null"), nil
	}

	var value any
	switch kind := x.GetKind().(type) {
	case *IndexID_TxBuiltin:
		value = struct {
			TxBuiltin string `json:"txBuiltin"`
		}{TxBuiltin: kind.TxBuiltin.String()}
	case *IndexID_LogBuiltin:
		value = struct {
			LogBuiltin string `json:"logBuiltin"`
		}{LogBuiltin: kind.LogBuiltin.String()}
	case *IndexID_AccountBuiltin:
		value = struct {
			AccountBuiltin string `json:"accountBuiltin"`
		}{AccountBuiltin: kind.AccountBuiltin.String()}
	case *IndexID_Metadata:
		value = struct {
			Metadata *MetadataIndexID `json:"metadata"`
		}{Metadata: kind.Metadata}
	default:
		value = struct{}{}
	}

	return json.Marshal(value)
}

// MarshalJSON implements the public REST shape for a metadata index key.
func (x *MetadataIndexID) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Target string `json:"target"`
		Key    string `json:"key"`
	}{Target: x.GetTarget().String(), Key: x.GetKey()})
}

// MarshalJSON implements the public REST shape for an index registry entry.
func (x *Index) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ID                     *IndexID   `json:"id,omitempty"`
		CreatedAt              *Timestamp `json:"createdAt,omitempty"`
		Ledger                 string     `json:"ledger,omitempty"`
		ForwardEncodingVersion uint32     `json:"forwardEncodingVersion,omitempty"`
	}{
		ID: x.GetId(), CreatedAt: x.GetCreatedAt(), Ledger: x.GetLedger(),
		ForwardEncodingVersion: x.GetForwardEncodingVersion(),
	})
}

// MarshalJSON implements the public REST shape for signing keys.
func (x *SigningKey) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		KeyID       string `json:"keyId"`
		PublicKey   []byte `json:"publicKey"`
		ParentKeyID string `json:"parentKeyId,omitempty"`
	}{KeyID: x.GetKeyId(), PublicKey: x.GetPublicKey(), ParentKeyID: x.GetParentKeyId()})
}
