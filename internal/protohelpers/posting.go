package protohelpers

import (
	"math/big"

	"github.com/holiman/uint256"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// NewPosting constructs a server-side uncolored posting.
func NewPosting(source, destination, asset string, amount *big.Int) *ledgerpb.Posting {
	return NewColoredPosting(source, destination, asset, "", amount)
}

// NewColoredPosting constructs a server-side posting with an explicit color.
func NewColoredPosting(source, destination, asset, color string, amount *big.Int) *ledgerpb.Posting {
	var u uint256.Int
	if overflow := u.SetFromBig(amount); overflow {
		panic("NewColoredPosting: amount exceeds 256 bits")
	}

	return &ledgerpb.Posting{
		Source:      source,
		Destination: destination,
		Amount:      &ledgerpb.Uint256{V0: u[0], V1: u[1], V2: u[2], V3: u[3]},
		Asset:       asset,
		Color:       color,
	}
}
