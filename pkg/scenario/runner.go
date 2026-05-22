package scenario

import (
	"context"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// Runner provides a simple interface for applying gRPC actions against a ledger cluster.
type Runner struct {
	ctx    context.Context
	client servicepb.BucketServiceClient
	log    func(step string)
	scale  float64
}

// NewRunner creates a new Runner with the given context and client.
func NewRunner(ctx context.Context, client servicepb.BucketServiceClient) *Runner {
	return &Runner{
		ctx:    ctx,
		client: client,
		log:    func(string) {},
		scale:  1.0,
	}
}

// WithLogger sets a progress callback for Step() calls.
func (r *Runner) WithLogger(fn func(string)) *Runner {
	r.log = fn

	return r
}

// WithScale sets a multiplier for iteration counts in scenarios.
// A scale of 2.0 doubles iteration counts; 0.5 halves them.
func (r *Runner) WithScale(scale float64) *Runner {
	r.scale = scale

	return r
}

// Iterations returns n scaled by the runner's scale factor, with a minimum of 1.
func (r *Runner) Iterations(n int) int {
	scaled := int(math.Round(float64(n) * r.scale))
	if scaled < 1 {
		return 1
	}

	return scaled
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

// Setup sends each action individually and ignores AlreadyExists errors,
// making provisioning idempotent (safe to re-run against an existing cluster).
func (r *Runner) Setup(actions ...*servicepb.Request) error {
	r.log("Setup")

	for _, action := range actions {
		if _, err := r.Apply(action); err != nil {
			if status.Code(err) == codes.AlreadyExists {
				continue
			}

			return err
		}
	}

	return nil
}
