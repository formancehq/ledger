package readstore

import (
	"bytes"
	"encoding/binary"

	"github.com/formancehq/invariants"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// parseEventKey splits an event key into (group, seq, op) independently of
// scan direction. The fixed suffix locates the terminator from the right.
// Unknown operations and malformed keys must fail the caller's scan loudly.
func parseEventKey(key []byte, prefixLen int) (group []byte, seq uint64, op byte, ok bool) {
	if len(key) < prefixLen {
		return nil, 0, 0, false
	}
	rest := key[prefixLen:]
	tpos := len(rest) - metadataEventSuffixLen - 1
	if tpos < 0 || rest[tpos] != metadataEventTerminator {
		return nil, 0, 0, false
	}

	if op := rest[tpos+9]; !validEventOp(op) {
		return nil, 0, 0, false
	}

	return rest[:tpos], binary.BigEndian.Uint64(rest[tpos+1 : tpos+9]), rest[tpos+9], true
}

// parseAccountAssetKey accepts both the RC.5 first-touch row and the event
// form. The old row's eight-byte value is a synthetic ADD at that sequence.
// Its key sorts immediately before the event keys for the same account.
func parseAccountAssetKey(key, value []byte, prefixLen int) (group []byte, seq uint64, legacy bool, ok bool) {
	if len(key) <= prefixLen || key[0] != PrefixAccountByAsset {
		return nil, 0, false, false
	}
	account := key[prefixLen:]
	if bytes.IndexByte(account, metadataEventTerminator) >= 0 {
		group, seq, op, valid := parseEventKey(key, prefixLen)

		return group, seq, false, valid && op == MetadataEventAdd
	}
	if len(value) != 8 || invariants.ValidateLedgerAccountAddress(string(account)) != nil {
		return nil, 0, false, false
	}

	return account, binary.BigEndian.Uint64(value), true, true
}

// accountAssetPrefixLen locates the precision byte after the asset base. GC
// scans the entire zone, unlike a query whose prefix is already known.
func accountAssetPrefixLen(key []byte) (int, bool) {
	const assetStart = 1 + dal.LedgerNameFixedSize
	if len(key) <= assetStart || key[0] != PrefixAccountByAsset {
		return 0, false
	}
	end := bytes.IndexByte(key[assetStart:], 0)
	if end < 0 {
		return 0, false
	}
	prefixLen := assetStart + end + 2 // NUL plus precision

	return prefixLen, len(key) > prefixLen
}
