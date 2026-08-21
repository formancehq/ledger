//go:build !antithesis

package grpc

import (
	"context"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func awaitAntithesisIdempotencyCommitProbe(
	_ context.Context,
	_ string,
	_ []*commonpb.Log,
) {
}
