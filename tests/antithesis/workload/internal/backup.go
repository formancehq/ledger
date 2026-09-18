package internal

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A healthy orphan cleanup normally gets a chance every 30s. This budget allows
// several scans without letting a faulted destination consume the whole 90min
// singleton deadline. It bounds scheduling further attempts, not the duration
// of an admitted backup: RPCs retain the caller's deadline. Expiration is an
// inconclusive observation, not proof of a deadlock or successful recovery.
const backupRetryTimeout = 2 * time.Minute

// IsBackupInProgress recognizes only the destination exclusion contract. The
// current server sends no structured reason and also uses FailedPrecondition
// for job-ID collisions and missing checkpoints, which must remain distinct.
func IsBackupInProgress(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.FailedPrecondition &&
		strings.HasSuffix(st.Message(), "backup: destination already has a running job")
}

// IsBackupCallerCancellation tolerates only cancellation explained by a finished
// caller context. A concurrent shutdown must not hide an unrelated server error,
// and a remote Canceled response with a live caller still needs investigation.
func IsBackupCallerCancellation(ctx context.Context, err error) bool {
	if ctx.Err() == nil || err == nil {
		return false
	}
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.Canceled
	}
	return errors.Is(err, ctx.Err())
}

// RetryBackup retries destination busy within one backup stage. Transport
// retries remain owned by NewGRPCConn; IsTransient is deliberately unchanged.
// A lost Start acknowledgment or terminal proposal can leave a durable RUNNING
// slot after the executor exits. Only committed completion/cleanup may free it.
func RetryBackup[T any](ctx context.Context, operation string, call func(context.Context) (T, error)) (T, error) {
	retryCtx := ctx
	cancelRetry := func() {}
	defer func() { cancelRetry() }()

	var zero T
	var lastBusy error
	for attempt := 0; ; attempt++ {
		if retryCtx.Err() != nil {
			if lastBusy != nil {
				return zero, fmt.Errorf("%s retry stopped (%v): %w", operation, retryCtx.Err(), lastBusy)
			}
			return zero, retryCtx.Err()
		}
		response, err := call(ctx)
		busy := IsBackupInProgress(err)
		if busy {
			if lastBusy == nil {
				retryCtx, cancelRetry = context.WithTimeout(ctx, backupRetryTimeout)
			}
			lastBusy = err
		}
		if lastBusy != nil {
			assert.Sometimes(err == nil, "backup recovery after destination busy succeeds", Details{
				"operation": operation, "attempts": attempt + 1, "error": err,
			})
		}
		if !busy {
			return response, err
		}
		log.Printf("%s destination busy on attempt %d; waiting for completion or orphan cleanup: %v", operation, attempt+1, err)

		timer := time.NewTimer(retryDelay(attempt))
		select {
		case <-retryCtx.Done():
			timer.Stop()
			return zero, fmt.Errorf("%s retry stopped (%v): %w", operation, retryCtx.Err(), lastBusy)
		case <-timer.C:
		}
	}
}
