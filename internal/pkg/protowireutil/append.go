package protowireutil

import (
	"google.golang.org/protobuf/encoding/protowire"
)

// AppendFixed64 appends a fixed64 field (wire type 1) to buf.
func AppendFixed64(buf []byte, fieldNum protowire.Number, value uint64) []byte {
	buf = protowire.AppendTag(buf, fieldNum, protowire.Fixed64Type)
	buf = protowire.AppendFixed64(buf, value)

	return buf
}
