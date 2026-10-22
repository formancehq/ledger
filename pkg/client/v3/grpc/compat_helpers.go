package grpc

import (
	"math/big"

	ledgerpb "github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
)

// These wrappers preserve the historical client import path while the
// implementation and protobuf descriptors live in ledgerpb.
var ErrTimestampBeforeEpoch = ledgerpb.ErrTimestampBeforeEpoch

type AssetColored = ledgerpb.AssetColored
type LogPayloadInterface = ledgerpb.LogPayloadInterface

func NewBigUint(value *big.Int) *BigUint { return ledgerpb.NewBigUint(value) }

func MustBigUintFromDecimal(decimal string) *BigUint {
	return ledgerpb.MustBigUintFromDecimal(decimal)
}

func NewSignedBigInt(value *big.Int) *SignedBigInt { return ledgerpb.NewSignedBigInt(value) }

func NewFailureDetails(code string, facts map[string]string) (*FailureDetails, error) {
	return ledgerpb.NewFailureDetails(code, facts)
}

func MustFailureDetails(code string, facts map[string]string) *FailureDetails {
	return ledgerpb.MustFailureDetails(code, facts)
}

func ValidateRequiredFailureFacts(details *FailureDetails) error {
	return ledgerpb.ValidateRequiredFailureFacts(details)
}

func FailureDetailsReason(details *FailureDetails) ErrorReason {
	return ledgerpb.FailureDetailsReason(details)
}

func FailureDetailsCode(details *FailureDetails) string {
	return ledgerpb.FailureDetailsCode(details)
}

func FailureDetailsFacts(details *FailureDetails) map[string]string {
	return ledgerpb.FailureDetailsFacts(details)
}

func NewTimestamp(t interface{ UnixMicro() int64 }) *Timestamp {
	return ledgerpb.NewTimestamp(t)
}

func NewUint256FromUint64(value uint64) *Uint256 {
	return ledgerpb.NewUint256FromUint64(value)
}

func LessByAssetColor[T AssetColored](a, b T) bool {
	return ledgerpb.LessByAssetColor(a, b)
}
