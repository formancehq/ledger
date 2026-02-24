package dal

import (
	"encoding/binary"
	"fmt"
)

type AccountKey struct {
	Ledger  string
	Account string
}

type VolumeKey struct {
	AccountKey
	Asset string
}

// Bytes returns a canonical byte representation of the balance key.
// Format: [ledger]\x00[account]\x00[asset]
// Ledger names are restricted to printable ASCII, so \x00 is a safe separator.
func (bk VolumeKey) Bytes() []byte {
	ret := make([]byte, len(bk.Ledger)+1+len(bk.Account)+1+len(bk.Asset))
	n := copy(ret, bk.Ledger)
	ret[n] = 0x00
	n++
	n += copy(ret[n:], bk.Account)
	ret[n] = 0x00
	n++
	copy(ret[n:], bk.Asset)
	return ret
}

// Unmarshal parses canonical bytes into the VolumeKey.
func (bk *VolumeKey) Unmarshal(d []byte) error {
	parts := splitNullBytes(d, 3)
	if len(parts) != 3 {
		return fmt.Errorf("invalid balance key bytes: expected 3 parts, got %d", len(parts))
	}
	bk.Ledger = string(parts[0])
	bk.Account = string(parts[1])
	bk.Asset = string(parts[2])
	return nil
}

var _ CanonicalBytes = (*VolumeKey)(nil)

type MetadataKey struct {
	AccountKey
	Key string
}

// Bytes returns a canonical byte representation of the metadata key.
// Format: [ledger]\x00[account]\x01[key]
// Uses \x00 after ledger, \x01 before key to distinguish from balance keys.
func (mk MetadataKey) Bytes() []byte {
	ret := make([]byte, len(mk.Ledger)+1+len(mk.Account)+1+len(mk.Key))
	n := copy(ret, mk.Ledger)
	ret[n] = 0x00
	n++
	n += copy(ret[n:], mk.Account)
	ret[n] = 0x01
	n++
	copy(ret[n:], mk.Key)
	return ret
}

// Unmarshal parses canonical bytes into the MetadataKey.
func (mk *MetadataKey) Unmarshal(d []byte) error {
	// First split on \x00 to separate ledger from the rest
	parts := splitNullBytes(d, 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid metadata key bytes: expected ledger separator")
	}
	mk.Ledger = string(parts[0])

	// Rest is account + \x01 + key
	rest := parts[1]
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

type TransactionKey struct {
	Ledger string
	ID     uint64
}

// Bytes returns a canonical byte representation of the transaction key.
// Format: [ledger]\x00[txID (8 bytes)]
func (tk TransactionKey) Bytes() []byte {
	ret := make([]byte, len(tk.Ledger)+1+8)
	n := copy(ret, tk.Ledger)
	ret[n] = 0x00
	n++
	binary.BigEndian.PutUint64(ret[n:], tk.ID)
	return ret
}

// Unmarshal parses canonical bytes into the TransactionKey.
func (tk *TransactionKey) Unmarshal(d []byte) error {
	// Find the \x00 separator between ledger name and txID
	sep := -1
	for i, b := range d {
		if b == 0x00 {
			sep = i
			break
		}
	}
	if sep == -1 || len(d) < sep+1+8 {
		return fmt.Errorf("invalid transaction key bytes: expected [ledger]\\x00[txID(8)]")
	}
	tk.Ledger = string(d[:sep])
	tk.ID = binary.BigEndian.Uint64(d[sep+1 : sep+1+8])
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
	Ledger    string
	Reference string
}

// Bytes returns a canonical byte representation of the transaction reference key.
// Format: [ledger]\x00[reference]
func (trk TransactionReferenceKey) Bytes() []byte {
	ret := make([]byte, len(trk.Ledger)+1+len(trk.Reference))
	n := copy(ret, trk.Ledger)
	ret[n] = 0x00
	n++
	copy(ret[n:], trk.Reference)
	return ret
}

// Unmarshal parses canonical bytes into the TransactionReferenceKey.
func (trk *TransactionReferenceKey) Unmarshal(d []byte) error {
	parts := splitNullBytes(d, 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid transaction reference key bytes: expected 2 parts, got %d", len(parts))
	}
	trk.Ledger = string(parts[0])
	trk.Reference = string(parts[1])
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
