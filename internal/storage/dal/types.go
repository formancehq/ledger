package dal

import (
	"encoding/binary"
	"fmt"
)

type AccountKey struct {
	LedgerID uint32
	Account  string
}

type VolumeKey struct {
	AccountKey
	Asset string
}

// Bytes returns a canonical byte representation of the balance key.
// Format: [ledgerID (4 bytes)][account]\x00[asset]
// LedgerID is fixed-length so no separator needed after it.
func (bk VolumeKey) Bytes() []byte {
	ret := make([]byte, 4+len(bk.Account)+1+len(bk.Asset))
	binary.BigEndian.PutUint32(ret, bk.LedgerID)
	n := 4
	n += copy(ret[n:], bk.Account)
	ret[n] = 0x00
	n++
	copy(ret[n:], bk.Asset)
	return ret
}

// Unmarshal parses canonical bytes into the VolumeKey.
func (bk *VolumeKey) Unmarshal(d []byte) error {
	if len(d) < 4 {
		return fmt.Errorf("invalid balance key bytes: too short")
	}
	bk.LedgerID = binary.BigEndian.Uint32(d[:4])
	rest := d[4:]
	parts := splitNullBytes(rest, 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid balance key bytes: expected 2 parts after ledgerID, got %d", len(parts))
	}
	bk.Account = string(parts[0])
	bk.Asset = string(parts[1])
	return nil
}

var _ CanonicalBytes = (*VolumeKey)(nil)

type MetadataKey struct {
	AccountKey
	Key string
}

// Bytes returns a canonical byte representation of the metadata key.
// Format: [ledgerID (4 bytes)][account]\x01[key]
// Uses \x01 as separator before key to distinguish from balance keys.
func (mk MetadataKey) Bytes() []byte {
	ret := make([]byte, 4+len(mk.Account)+1+len(mk.Key))
	binary.BigEndian.PutUint32(ret, mk.LedgerID)
	n := 4
	n += copy(ret[n:], mk.Account)
	ret[n] = 0x01
	n++
	copy(ret[n:], mk.Key)
	return ret
}

// Unmarshal parses canonical bytes into the MetadataKey.
func (mk *MetadataKey) Unmarshal(d []byte) error {
	if len(d) < 4 {
		return fmt.Errorf("invalid metadata key bytes: too short")
	}
	mk.LedgerID = binary.BigEndian.Uint32(d[:4])

	// Rest is account + \x01 + key
	rest := d[4:]
	separator := -1
	for i, b := range rest {
		if b == 0x01 {
			separator = i
			break
		}
	}
	if separator == -1 {
		return fmt.Errorf("invalid metadata key bytes: missing account/key separator")
	}

	mk.Account = string(rest[:separator])
	mk.Key = string(rest[separator+1:])
	return nil
}

var _ CanonicalBytes = (*MetadataKey)(nil)

// LedgerMetadataKey represents a key for ledger metadata.
type LedgerMetadataKey struct {
	LedgerID uint32
	Key      string
}

// Bytes returns a canonical byte representation of the ledger metadata key.
// Format: [ledgerID (4 bytes)][key]
// No separator needed since ledgerID is fixed-length.
func (lmk LedgerMetadataKey) Bytes() []byte {
	ret := make([]byte, 4+len(lmk.Key))
	binary.BigEndian.PutUint32(ret, lmk.LedgerID)
	copy(ret[4:], lmk.Key)
	return ret
}

// Unmarshal parses canonical bytes into the LedgerMetadataKey.
func (lmk *LedgerMetadataKey) Unmarshal(d []byte) error {
	if len(d) < 4 {
		return fmt.Errorf("invalid ledger metadata key bytes: too short")
	}
	lmk.LedgerID = binary.BigEndian.Uint32(d[:4])
	lmk.Key = string(d[4:])
	return nil
}

var _ CanonicalBytes = (*LedgerMetadataKey)(nil)

type TransactionKey struct {
	LedgerID uint32
	ID       uint64
}

// Bytes returns a canonical byte representation of the transaction key.
// Format: [ledgerID (4 bytes)][txID (8 bytes)]
// Both fixed-length, no separator needed.
func (tk TransactionKey) Bytes() []byte {
	ret := make([]byte, 4+8)
	binary.BigEndian.PutUint32(ret, tk.LedgerID)
	binary.BigEndian.PutUint64(ret[4:], tk.ID)
	return ret
}

// Unmarshal parses canonical bytes into the TransactionKey.
func (tk *TransactionKey) Unmarshal(d []byte) error {
	if len(d) < 12 {
		return fmt.Errorf("invalid transaction key bytes: expected 12 bytes, got %d", len(d))
	}
	tk.LedgerID = binary.BigEndian.Uint32(d[:4])
	tk.ID = binary.BigEndian.Uint64(d[4:12])
	return nil
}

var _ CanonicalBytes = (*TransactionKey)(nil)

type IdempotencyKey struct {
	Key string
}

// Bytes returns a canonical byte representation of the idempotency key.
func (ik IdempotencyKey) Bytes() []byte {
	return []byte(ik.Key)
}

// Unmarshal parses canonical bytes into the IdempotencyKey.
func (ik *IdempotencyKey) Unmarshal(data []byte) error {
	ik.Key = string(data)
	return nil
}

var _ CanonicalBytes = (*IdempotencyKey)(nil)

// TransactionReferenceKey represents a unique reference scoped to a ledger.
type TransactionReferenceKey struct {
	LedgerID  uint32
	Reference string
}

// Bytes returns a canonical byte representation of the transaction reference key.
// Format: [ledgerID (4 bytes)][reference]
// No separator needed since ledgerID is fixed-length.
func (trk TransactionReferenceKey) Bytes() []byte {
	ret := make([]byte, 4+len(trk.Reference))
	binary.BigEndian.PutUint32(ret, trk.LedgerID)
	copy(ret[4:], trk.Reference)
	return ret
}

// Unmarshal parses canonical bytes into the TransactionReferenceKey.
func (trk *TransactionReferenceKey) Unmarshal(d []byte) error {
	if len(d) < 4 {
		return fmt.Errorf("invalid transaction reference key bytes: too short")
	}
	trk.LedgerID = binary.BigEndian.Uint32(d[:4])
	trk.Reference = string(d[4:])
	return nil
}

var _ CanonicalBytes = (*TransactionReferenceKey)(nil)

type LedgerKey struct {
	Name string
}

// Bytes returns a canonical byte representation of the ledger key.
func (lk LedgerKey) Bytes() []byte {
	return []byte(lk.Name)
}

// Unmarshal parses canonical bytes into the LedgerKey.
func (lk *LedgerKey) Unmarshal(data []byte) error {
	lk.Name = string(data)
	return nil
}

var _ CanonicalBytes = (*LedgerKey)(nil)

// SinkConfigKey uniquely identifies an event sink by name.
type SinkConfigKey struct {
	Name string
}

func (k SinkConfigKey) Bytes() []byte {
	return []byte(k.Name)
}

// splitNullBytes splits data by null bytes into at most n parts.
func splitNullBytes(data []byte, n int) [][]byte {
	var parts [][]byte
	start := 0
	for i, b := range data {
		if b == 0x00 && len(parts) < n-1 {
			parts = append(parts, data[start:i])
			start = i + 1
		}
	}
	parts = append(parts, data[start:])
	return parts
}
