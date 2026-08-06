package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// Upload retry bounds. The aws-sdk-go-v2 manager already retries transient
// individual part PUTs internally; this layer adds a bounded whole-object
// retry the SDK will NOT do for a non-retryable 400 such as the
// CompleteMultipartUpload InvalidPart we observed. Fixed (no config knob): a
// backup is worth a handful of retries, and the bound caps wasted time on a
// genuinely permanent error (auth/config).
const uploadMaxAttempts = 5

// Backoff bounds are vars (not consts) solely so tests can shrink them; they
// are never reconfigured at runtime.
var (
	uploadInitialBackoff = 500 * time.Millisecond
	uploadMaxBackoff     = 30 * time.Second
)

// bodyFactory produces a fresh reader for one upload attempt plus a cleanup to
// release it. A streamed body (an *os.File, an io.Pipe) is single-use, so a
// retried attempt MUST obtain a fresh one. cleanup runs after every attempt —
// success or failure — so no reader leaks across a retry.
type bodyFactory func() (io.Reader, func(), error)

// retryUpload runs attempt with bounded exponential backoff. It retries every
// error EXCEPT context cancellation, which is terminal (leadership loss,
// shutdown, or caller abort). Cancellation is detected via both ctx.Err() and
// the context sentinels because storage SDKs frequently wrap cancellation in
// an opaque error where errors.Is alone would miss it.
func retryUpload(ctx context.Context, key string, logger logging.Logger, attempt func() error) error {
	backoff := uploadInitialBackoff

	var lastErr error

	for i := 1; i <= uploadMaxAttempts; i++ {
		err := attempt()
		if err == nil {
			return nil
		}

		lastErr = err

		if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		if i == uploadMaxAttempts {
			break
		}

		logger.WithFields(map[string]any{
			"key":     key,
			"attempt": i,
			"backoff": backoff.String(),
			"error":   err,
		}).Infof("Backup upload attempt failed; retrying")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		backoff = min(backoff*2, uploadMaxBackoff)
	}

	return fmt.Errorf("upload %s failed after %d attempts: %w", key, uploadMaxAttempts, lastErr)
}

// putWithRetry uploads key through storage.PutFile with retryUpload's bounded
// backoff, obtaining a fresh body from newBody for each attempt and always
// releasing it via the returned cleanup.
func putWithRetry(ctx context.Context, storage Storage, key string, size int64, logger logging.Logger, newBody bodyFactory) error {
	return retryUpload(ctx, key, logger, func() error {
		body, cleanup, err := newBody()
		if err != nil {
			return fmt.Errorf("preparing upload body for %s: %w", key, err)
		}

		defer cleanup()

		return storage.PutFile(ctx, key, body, size)
	})
}

// writeManifestWithRetry marshals and writes the manifest with upload retry.
// A transient failure writing the tiny manifest AFTER a multi-hour data upload
// would otherwise discard the whole run, so the manifest write is retried just
// like the data objects. WriteManifest is left untouched for non-backup
// callers that manage their own error handling.
func writeManifestWithRetry(ctx context.Context, storage Storage, key string, manifest *Manifest, logger logging.Logger) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling manifest: %w", err)
	}

	return putWithRetry(ctx, storage, key, int64(len(data)), logger, func() (io.Reader, func(), error) {
		return bytes.NewReader(data), func() {}, nil
	})
}
