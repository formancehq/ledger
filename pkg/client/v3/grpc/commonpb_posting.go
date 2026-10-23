package grpc

import (
	"fmt"
	"math/big"
)

// NewPosting constructs an uncolored posting.
func NewPosting(source, destination, asset string, amount *big.Int) *Posting {
	return NewColoredPosting(source, destination, asset, "", amount)
}

// NewColoredPosting constructs a posting with an explicit color.
func NewColoredPosting(source, destination, asset, color string, amount *big.Int) *Posting {
	return &Posting{
		Source:      source,
		Destination: destination,
		Amount:      NewUint256FromBig(amount),
		Asset:       asset,
		Color:       color,
	}
}

// NewUint256FromBig converts a non-negative integer into a Uint256.
func NewUint256FromBig(value *big.Int) *Uint256 {
	if value == nil || value.Sign() < 0 || value.BitLen() > 256 {
		panic(fmt.Sprintf("value does not fit in Uint256: %v", value))
	}
	copyValue := new(big.Int).Set(value)
	result := new(Uint256)
	result.V0 = copyValue.Uint64()
	copyValue.Rsh(copyValue, 64)
	result.V1 = copyValue.Uint64()
	copyValue.Rsh(copyValue, 64)
	result.V2 = copyValue.Uint64()
	copyValue.Rsh(copyValue, 64)
	result.V3 = copyValue.Uint64()
	return result
}
