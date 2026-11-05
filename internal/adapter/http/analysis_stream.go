package http

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/formancehq/ledger/v3/internal/adapter/apitrace"
	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

const analysisWriteTimeout = 10 * time.Second

// wantsAnalysisStream requires an explicit, acceptable event-stream range.
// Wildcards alone retain JSON. Prefer JSON when its effective quality is higher.
func wantsAnalysisStream(r *http.Request) bool {
	streamQuality, jsonQuality, jsonSpecificity := float64(0), float64(0), -1
	for _, header := range r.Header.Values("Accept") {
		for part := range strings.SplitSeq(header, ",") {
			media, params, err := mime.ParseMediaType(strings.TrimSpace(part))
			if err != nil {
				continue
			}
			quality := float64(1)
			if q, ok := params["q"]; ok {
				quality, err = strconv.ParseFloat(q, 64)
				if err != nil || !(quality >= 0 && quality <= 1) {
					continue
				}
			}
			if media == "text/event-stream" && quality > streamQuality {
				streamQuality = quality
			}
			specificity := -1
			switch media {
			case "application/json":
				specificity = 2
			case "application/*":
				specificity = 1
			case "*/*":
				specificity = 0
			}
			if specificity >= 0 && (specificity > jsonSpecificity || (specificity == jsonSpecificity && quality > jsonQuality)) {
				jsonSpecificity, jsonQuality = specificity, quality
			}
		}
	}

	return streamQuality > 0 && streamQuality >= jsonQuality
}

type analysisProgressJSON struct {
	Processed uint64 `json:"processed"`
	Total     uint64 `json:"total"`
	Phase     string `json:"phase"`
}

type analysisErrorJSON struct {
	ErrorResponse

	Status     int `json:"status"`
	RetryAfter int `json:"retryAfter,omitempty"`
}

// streamAnalysis owns the computation in the request goroutine. Callback writes
// apply bounded backpressure without retaining an event queue or starting a job.
// A transport failure cancels the backend; no terminal success follows it.
func streamAnalysis(w http.ResponseWriter, r *http.Request, phase string, run func(context.Context, func(uint64, uint64)) (any, error)) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	r = r.WithContext(ctx)
	// Writes retain logging/telemetry accounting. Flush and deadline control
	// reach the underlying transport: chi/otel Flusher wrappers discard the
	// FlushError result, so stopping at their Flush method would hide failures.
	transport := w
	for {
		unwrapper, ok := transport.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			break
		}
		transport = unwrapper.Unwrap()
	}
	controller := http.NewResponseController(transport)
	// Interrupt a blocked network write when the request is canceled. Join this
	// callback before returning, so it cannot touch a writer reused by the server.
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(interrupted)
		// Unsupported in-memory writers cannot be interrupted; real HTTP servers
		// support deadlines. Cancellation still suppresses all subsequent events.
		_ = controller.SetWriteDeadline(time.Now())
	})
	defer func() {
		if !stop() {
			<-interrupted
		}
	}()

	var transportErr error
	send := func(event string, value any) error {
		if transportErr != nil {
			return transportErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// Marshal before writing the frame: encoding failure must not leave half a
		// result event that a consumer could mistake for a successful completion.
		payload, err := json.MarshalWithOptions(value, bigIntegerEncoding(r))
		if err != nil {
			return err
		}
		if err = controller.SetWriteDeadline(time.Now().Add(analysisWriteTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
			transportErr = err
			cancel()

			return err
		}
		// Clear each deadline after flushing. Keeping it armed while computation is
		// silent would incorrectly impose a ten-second total/idle analysis timeout.
		defer func() {
			if err := controller.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
				transportErr = err
				cancel()
			}
		}()
		if err = ctx.Err(); err != nil {
			return err
		}
		if _, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, payload); err == nil {
			err = controller.Flush()
		}
		if err != nil {
			transportErr = err
			cancel()
		}

		return err
	}
	fail := func(err error) {
		if transportErr != nil || ctx.Err() != nil {
			return
		}
		c := classifyError(err)
		body := analysisErrorJSON{ErrorResponse: ErrorResponse{ErrorCode: c.code, ErrorMessage: c.publicMessage(r, err)}, Status: c.status}
		if c.status == http.StatusServiceUnavailable {
			body.RetryAfter = 1
		}
		// Once delivery fails, cancellation and EOF are the only possible signal.
		_ = send("error", body)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			if recovered == http.ErrAbortHandler { //nolint:errorlint // recovered is any; preserve net/http sentinel identity, as jsonRecoverer does.
				panic(recovered)
			}
			stack := debug.Stack()
			apitrace.RecordPanic(r.Context(), recovered, stack)
			fail(fmt.Errorf("analysis handler panicked: %v\n%s", recovered, stack))
		}
	}()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	if err := send("started", struct{}{}); err != nil {
		return
	}
	result, err := run(ctx, func(processed, total uint64) {
		if err := send("progress", analysisProgressJSON{Processed: processed, Total: total, Phase: phase}); err != nil && transportErr == nil && ctx.Err() == nil {
			// Progress consists solely of integers and a constant phase. Encoding
			// failure is an invariant violation, not an event that may be skipped.
			transportErr = err
			cancel()
		}
	})
	if err != nil {
		fail(err)

		return
	}
	if transportErr != nil || ctx.Err() != nil {
		return
	}
	if err := send("result", BaseResponse[any]{Data: result}); err != nil {
		fail(err)
	}
}
