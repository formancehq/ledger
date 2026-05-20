package protowireutil

import (
	"google.golang.org/protobuf/encoding/protowire"
)

// VTMarshaler is implemented by vtprotobuf-generated messages.
type VTMarshaler interface {
	SizeVT() int
	MarshalToSizedBufferVT([]byte) (int, error)
}

// AppendFixed64 appends a fixed64 field (wire type 1) to buf.
func AppendFixed64(buf []byte, fieldNum protowire.Number, value uint64) []byte {
	buf = protowire.AppendTag(buf, fieldNum, protowire.Fixed64Type)
	buf = protowire.AppendFixed64(buf, value)

	return buf
}

// AppendVarint appends a varint field (wire type 0) to buf.
func AppendVarint(buf []byte, fieldNum protowire.Number, value uint64) []byte {
	buf = protowire.AppendTag(buf, fieldNum, protowire.VarintType)
	buf = protowire.AppendVarint(buf, value)

	return buf
}

// AppendBytes appends a length-delimited bytes field (wire type 2) to buf.
func AppendBytes(buf []byte, fieldNum protowire.Number, data []byte) []byte {
	buf = protowire.AppendTag(buf, fieldNum, protowire.BytesType)
	buf = protowire.AppendBytes(buf, data)

	return buf
}

// AppendString appends a length-delimited string field (wire type 2) to buf.
func AppendString(buf []byte, fieldNum protowire.Number, s string) []byte {
	buf = protowire.AppendTag(buf, fieldNum, protowire.BytesType)
	buf = protowire.AppendString(buf, s)

	return buf
}

// AppendMessage appends a length-delimited sub-message field (wire type 2) to buf.
// The message is marshaled in-place using vtprotobuf's MarshalToSizedBufferVT.
func AppendMessage(buf []byte, fieldNum protowire.Number, msg VTMarshaler) []byte {
	size := msg.SizeVT()
	if size == 0 {
		return buf
	}

	// Append tag + length prefix.
	buf = protowire.AppendTag(buf, fieldNum, protowire.BytesType)
	buf = protowire.AppendVarint(buf, uint64(size))

	// Grow buf to fit the message, then marshal backwards into the tail.
	start := len(buf)
	buf = append(buf, make([]byte, size)...)
	n, _ := msg.MarshalToSizedBufferVT(buf[start : start+size])

	// MarshalToSizedBufferVT writes backwards from the end; trim if n < size.
	if n < size {
		copy(buf[start:], buf[start+size-n:start+size])
		buf = buf[:start+n]
	}

	return buf
}
