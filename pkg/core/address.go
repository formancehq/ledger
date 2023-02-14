package core

import "encoding/binary"

// Address represents an address in the machine's resources, which include
// constants (literals) and variables passed to the program
type Address uint16

func NewAddress(x uint16) Address {
	return Address(x)
}

func (a Address) ToBytes() []byte {
	bytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(bytes, uint16(a))
	return bytes
}
