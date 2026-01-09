package raft

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/formancehq/go-libs/v3/time"
)

// GenerateRandomID generates a random uint64 ID
func GenerateRandomID() uint64 {
	var b [8]byte
	_, err := rand.Read(b[:])
	if err != nil {
		// Fallback to a simple random number if crypto/rand fails
		// This should never happen in practice
		return uint64(time.Now().UnixNano())
	}
	return binary.BigEndian.Uint64(b[:])
}