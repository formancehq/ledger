package domain

import (
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// TestLookupReasonCodeSeparatesTheSentinelFromAnUnknownName pins the
// distinction ReasonCode cannot express. Both a name absent from the enum and
// the enum's own UNSPECIFIED member map to the zero value, so a decoder
// reading a reason off the wire cannot tell "from a newer server" (trust the
// payload, classify from the code) from "no ledger server sends this" (reject
// the pair) without the second result.
func TestLookupReasonCodeSeparatesTheSentinelFromAnUnknownName(t *testing.T) {
	t.Parallel()

	const fromANewerServer = "SOME_REASON_FROM_A_NEWER_SERVER"

	sentinel := ReasonString(ledgerpb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	require.Equal(t, "UNSPECIFIED", sentinel)

	code, known := LookupReasonCode(sentinel)
	require.True(t, known, "the sentinel is a name the enum declares")
	require.Equal(t, ledgerpb.ErrorReason_ERROR_REASON_UNSPECIFIED, code)

	code, known = LookupReasonCode(fromANewerServer)
	require.False(t, known, "a name the enum does not declare")
	require.Equal(t, ledgerpb.ErrorReason_ERROR_REASON_UNSPECIFIED, code,
		"the zero value is still returned, which is why the second result is the discriminator")

	require.Equal(t, ReasonCode(sentinel), ReasonCode(fromANewerServer),
		"ReasonCode collapses both onto the zero value — the behaviour LookupReasonCode exists to refine")
}

// TestLookupReasonCodeResolvesEveryEnumName is the round-trip half: every
// reason the enum declares must resolve through the ReasonString/ReasonCode
// naming bijection, so a reason added to common.proto is known to the decoder
// without touching a lookup table.
func TestLookupReasonCodeResolvesEveryEnumName(t *testing.T) {
	t.Parallel()

	require.NotEmpty(t, ledgerpb.ErrorReason_name, "the enum scan found nothing — the scan is broken")

	for value := range ledgerpb.ErrorReason_name {
		expected := ledgerpb.ErrorReason(value)

		code, known := LookupReasonCode(ReasonString(expected))
		require.Truef(t, known, "reason %s does not round-trip through ReasonString", expected)
		require.Equal(t, expected, code)
	}
}
