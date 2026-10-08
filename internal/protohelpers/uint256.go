package protohelpers

import (
	"github.com/holiman/uint256"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

type uint256Limbs interface {
	GetV0() uint64
	GetV1() uint64
	GetV2() uint64
	GetV3() uint64
}

// IntoUint256 copies a public message or read-only view into a server uint256.
func IntoUint256(src uint256Limbs, dst *uint256.Int) {
	if src == nil {
		dst.Clear()

		return
	}
	dst[0] = src.GetV0()
	dst[1] = src.GetV1()
	dst[2] = src.GetV2()
	dst[3] = src.GetV3()
}

// SetFromUint256 copies server limbs into a public message.
func SetFromUint256(dst *ledgerpb.Uint256, src *uint256.Int) {
	dst.V0 = src[0]
	dst.V1 = src[1]
	dst.V2 = src[2]
	dst.V3 = src[3]
}

// NewUint256 constructs a public message from server uint256 limbs.
func NewUint256(src *uint256.Int) *ledgerpb.Uint256 {
	return &ledgerpb.Uint256{V0: src[0], V1: src[1], V2: src[2], V3: src[3]}
}
