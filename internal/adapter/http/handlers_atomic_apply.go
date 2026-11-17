package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/pprof"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleAtomicApply submits one complete batch through the service completion
// boundary, including checkpoint readiness and response signing. HTTP performs
// authorization itself: calling the service does not run gRPC interceptors.
func (s *Server) handleAtomicApply(service servicepb.BucketServiceServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pprof.Do(r.Context(), pprof.Labels("component", "admission.http"), func(ctx context.Context) {
			s.serveAtomicApply(w, r.WithContext(ctx), service)
		})
	}
}

func (s *Server) serveAtomicApply(w http.ResponseWriter, r *http.Request, service servicepb.BucketServiceServer) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}
	batch, err := decodeAtomicApplyBatch(body)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}
	if len(batch.GetRequests()) == 0 {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("requests must contain at least one operation"))

		return
	}
	if s.bulkMaxSize > 0 && len(batch.GetRequests()) > s.bulkMaxSize {
		writeErrorResponse(w, http.StatusRequestEntityTooLarge, "BULK_SIZE_EXCEEDED", fmt.Errorf("batch size exceeded, max size is %d", s.bulkMaxSize))

		return
	}
	if len(r.Header.Values("Idempotency-Key")) > 1 {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("Idempotency-Key must appear at most once"))

		return
	}
	batch.IdempotencyKey = r.Header.Get("Idempotency-Key")
	if s.authCfg.Enabled {
		effective := internalauth.ExpandedScopesFromContext(r.Context())
		for i, request := range batch.GetRequests() {
			required := internalauth.RequiredScopeForRequest(request)
			if internalauth.HasScope(effective, required) {
				continue
			}
			if !internalauth.AuthPresentedFromContext(r.Context()) {
				writeErrorResponse(w, http.StatusUnauthorized, "UNAUTHENTICATED", fmt.Errorf("operation %d requires scope %s", i, required))
			} else {
				writeErrorResponse(w, http.StatusForbidden, "PERMISSION_DENIED", fmt.Errorf("operation %d requires scope %s", i, required))
			}

			return
		}
	}
	request := servicepb.UnsignedApplyRequest(batch.GetIdempotencyKey(), batch.GetRequests()...)
	if envelope := signedBatchFromContext(r.Context()); envelope != nil {
		request = servicepb.SignedApplyRequest(envelope)
		signedBatch, err := servicepb.PeekBatch(request)
		if err != nil || !proto.Equal(batch, signedBatch) {
			handleError(w, r, signing.ErrInvalidSignature)

			return
		}
	}
	trace.SpanFromContext(r.Context()).SetAttributes(attribute.Int("batch_size", len(batch.GetRequests())))
	response, err := service.Apply(r.Context(), request)
	if err != nil {
		handleError(w, r, err)

		return
	}
	writeOKChecked(w, r, atomicApplyResponse(response))
}
