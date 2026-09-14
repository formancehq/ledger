package readstore

import "encoding/binary"

// parseEventKey splits an event key into (group, seq, op) independently of
// scan direction. The fixed suffix locates the terminator from the right.
// Unknown operations and malformed keys must fail the caller's scan loudly.
func parseEventKey(key []byte, prefixLen int) (group []byte, seq uint64, op byte, ok bool) {
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
