// Package shared adapts the product command contract to Ledger's gRPC API.
// Connection, credentials, signing, prompts and rendering belong to ledgerctl.
package shared

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"

	"github.com/formancehq/fctl/pkg/pluginsdk"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// Executor is scoped to one CLI invocation. Result retains the native response
// for the host's existing renderers; the SDK response uses the product's JSON
// contract. Neither response carries credentials or terminal state.
type Executor struct {
	Client         servicepb.BucketServiceClient
	CheckpointID   uint64
	Consistency    string
	Result         any
	NextCursor     string
	PreviousCursor string
	Trailer        metadata.MD
	Apply          func(context.Context, string, ...*servicepb.Request) (*servicepb.ApplyResponse, error)
	// CreateLedgerRequests preserves ledgerctl's atomic initial indexes and its
	// extended mirror configuration, which are not exposed by the HTTP API.
	CreateLedgerRequests []*servicepb.Request
	LastApplyResponse    *servicepb.ApplyResponse
	InspectMetadataType  commonpb.MetadataType
}

// Execute dispatches one validated product operation. There are no retries at
// this layer; pagination belongs to the host and mutations run exactly once.
func (e *Executor) Execute(ctx context.Context, req pluginsdk.ExecuteRequest) (pluginsdk.ExecuteResponse, error) {
	e.Result, e.NextCursor, e.PreviousCursor = nil, "", ""
	e.Trailer, e.LastApplyResponse = nil, nil
	e.InspectMetadataType = commonpb.MetadataType_METADATA_TYPE_STRING
	if err := ctx.Err(); err != nil {
		return pluginsdk.ExecuteResponse{}, err
	}
	if response, handled, err := e.read(ctx, req); handled {
		return response, err
	}
	if response, handled, err := e.write(ctx, req); handled {
		return response, err
	}

	return pluginsdk.ExecuteResponse{}, fmt.Errorf("unsupported Ledger operation %s", strings.Join(req.CommandPath, " "))
}
