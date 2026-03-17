package scenario

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// Runner provides a simple interface for applying gRPC actions against a ledger cluster.
type Runner struct {
	ctx    context.Context
	client servicepb.BucketServiceClient
	log    func(step string)
}

// NewRunner creates a new Runner with the given context and client.
func NewRunner(ctx context.Context, client servicepb.BucketServiceClient) *Runner {
	return &Runner{
		ctx:    ctx,
		client: client,
		log:    func(string) {},
	}
}

// WithLogger sets a progress callback for Step() calls.
func (r *Runner) WithLogger(fn func(string)) *Runner {
	r.log = fn

	return r
}

// Apply sends a batch of actions to the server.
func (r *Runner) Apply(actions ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
	return r.client.Apply(r.ctx, &servicepb.ApplyRequest{Requests: actions})
}

// Step logs the step name, then applies the actions.
func (r *Runner) Step(name string, actions ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
	r.log(name)

	return r.Apply(actions...)
}

// Client returns the underlying gRPC client.
func (r *Runner) Client() servicepb.BucketServiceClient {
	return r.client
}

// Ctx returns the runner's context.
func (r *Runner) Ctx() context.Context {
	return r.ctx
}
