package data

import (
	"fmt"
)

type AccountKey struct {
	LedgerName string
	Account    string
}

func (ak AccountKey) Bytes(kb *KeyBuilder) {
	kb.PutString(ak.LedgerName)
	kb.PutString(ak.Account)
}

type VolumeKey struct {
	AccountKey
	Asset string
}

// Bytes returns a canonical byte representation of the balance key.
// Format: [ledger]\x00[account]\x00[asset]
// The null byte separators ensure unambiguous parsing.
func (bk VolumeKey) Bytes() []byte {
	// ledger + \x00 + account + \x00 + asset
	ret := make([]byte, len(bk.LedgerName)+1+len(bk.Account)+1+len(bk.Asset))
	n := copy(ret, bk.LedgerName)
	ret[n] = 0x00
	n++
	n += copy(ret[n:], bk.Account)
	ret[n] = 0x00
	n++
	copy(ret[n:], bk.Asset)
	return ret
}

// Unmarshal parses canonical bytes into the VolumeKey.
func (bk *VolumeKey) Unmarshal(data []byte) error {
	parts := splitNullBytes(data, 3)
	if len(parts) != 3 {
		return fmt.Errorf("invalid balance key bytes: expected 3 parts, got %d", len(parts))
	}
	bk.LedgerName = string(parts[0])
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
// Uses \x01 as separator before key to distinguish from balance keys.
func (mk MetadataKey) Bytes() []byte {
	// ledger + \x00 + account + \x01 + key
	ret := make([]byte, len(mk.LedgerName)+1+len(mk.Account)+1+len(mk.Key))
	n := copy(ret, mk.LedgerName)
	ret[n] = 0x00
	n++
	n += copy(ret[n:], mk.Account)
	ret[n] = 0x01
	n++
	copy(ret[n:], mk.Key)
	return ret
}

// Unmarshal parses canonical bytes into the MetadataKey.
func (mk *MetadataKey) Unmarshal(data []byte) error {
	// First find ledger (separated by \x00)
	firstNull := -1
	for i, b := range data {
		if b == 0x00 {
			firstNull = i
			break
		}
	}
	if firstNull == -1 {
		return fmt.Errorf("invalid metadata key bytes: missing ledger separator")
	}
	mk.LedgerName = string(data[:firstNull])

	// Rest is account + \x01 + key
	rest := data[firstNull+1:]
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
	LedgerName string
	Key        string
}

// Bytes returns a canonical byte representation of the ledger metadata key.
// Format: [ledger]\x02[key]
// Uses \x02 as separator to distinguish from other key types.
func (lmk LedgerMetadataKey) Bytes() []byte {
	ret := make([]byte, len(lmk.LedgerName)+1+len(lmk.Key))
	n := copy(ret, lmk.LedgerName)
	ret[n] = 0x02
	n++
	copy(ret[n:], lmk.Key)
	return ret
}

// Unmarshal parses canonical bytes into the LedgerMetadataKey.
func (lmk *LedgerMetadataKey) Unmarshal(data []byte) error {
	separator := -1
	for i, b := range data {
		if b == 0x02 {
			separator = i
			break
		}
	}
	if separator == -1 {
		return fmt.Errorf("invalid ledger metadata key bytes: missing separator")
	}
	lmk.LedgerName = string(data[:separator])
	lmk.Key = string(data[separator+1:])
	return nil
}

var _ CanonicalBytes = (*LedgerMetadataKey)(nil)

type TransactionKey struct {
	LedgerName string
	ID         uint64
}

// Bytes returns a canonical byte representation of the transaction key.
// Format: [ledger]\x00[8-byte big-endian ID]
func (tk TransactionKey) Bytes() []byte {
	ret := make([]byte, len(tk.LedgerName)+1+8)
	n := copy(ret, tk.LedgerName)
	ret[n] = 0x00
	n++
	// Big-endian encoding for proper lexicographic ordering
	ret[n] = byte(tk.ID >> 56)
	ret[n+1] = byte(tk.ID >> 48)
	ret[n+2] = byte(tk.ID >> 40)
	ret[n+3] = byte(tk.ID >> 32)
	ret[n+4] = byte(tk.ID >> 24)
	ret[n+5] = byte(tk.ID >> 16)
	ret[n+6] = byte(tk.ID >> 8)
	ret[n+7] = byte(tk.ID)
	return ret
}

// Unmarshal parses canonical bytes into the TransactionKey.
func (tk *TransactionKey) Unmarshal(data []byte) error {
	// Find the null separator
	separator := -1
	for i, b := range data {
		if b == 0x00 {
			separator = i
			break
		}
	}
	if separator == -1 {
		return fmt.Errorf("invalid transaction key bytes: missing separator")
	}
	if len(data) < separator+1+8 {
		return fmt.Errorf("invalid transaction key bytes: too short for ID")
	}
	tk.LedgerName = string(data[:separator])
	idBytes := data[separator+1:]
	tk.ID = uint64(idBytes[0])<<56 |
		uint64(idBytes[1])<<48 |
		uint64(idBytes[2])<<40 |
		uint64(idBytes[3])<<32 |
		uint64(idBytes[4])<<24 |
		uint64(idBytes[5])<<16 |
		uint64(idBytes[6])<<8 |
		uint64(idBytes[7])
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
