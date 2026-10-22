package grpc_test

import (
	"testing"

	ledgergrpc "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/stretchr/testify/require"
)

func TestInternalCallerAttributionReasonIsAbsentFromPublicDescriptor(t *testing.T) {
	reasons := ledgergrpc.File_common_proto.Enums().ByName("ErrorReason")
	require.NotNil(t, reasons)
	require.Nil(t, reasons.Values().ByName("ERROR_REASON_INVALID_CALLER_ATTRIBUTION"))
	require.Nil(t, reasons.Values().ByNumber(72))
}
