package commands

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// GenerateRandomID generates a random uint64 ID.
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

// NewCommand creates a new Proposal with the given orders.
func NewCommand(orders ...*raftcmdpb.Order) *raftcmdpb.Proposal {
	return &raftcmdpb.Proposal{
		Id:      GenerateRandomID(),
		Orders:  orders,
		Date:    commonpb.NewTimestamp(time.Now()),
		Preload: &raftcmdpb.PreloadSet{},
	}
}
