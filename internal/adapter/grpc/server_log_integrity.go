package grpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/application/logintegrity"
	"github.com/formancehq/ledger/v3/internal/proto/rafttransportpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// LogIntegrityServiceServer reads one bounded range from a coherent local
// snapshot. The internal Raft listener supplies the cluster authentication.
type LogIntegrityServiceServer struct {
	rafttransportpb.UnimplementedLogIntegrityServiceServer

	store          *dal.Store
	waitForApplied func(context.Context, uint64) error
}

func NewLogIntegrityServiceServer(store *dal.Store, waitForApplied func(context.Context, uint64) error) rafttransportpb.LogIntegrityServiceServer {
	return &LogIntegrityServiceServer{store: store, waitForApplied: waitForApplied}
}

func (s *LogIntegrityServiceServer) GetRangeDigest(ctx context.Context, req *rafttransportpb.GetRangeDigestRequest) (*rafttransportpb.GetRangeDigestResponse, error) {
	first, last := req.GetFirstLogSequence(), req.GetLastLogSequence()
	if first == 0 || last < first || last-first >= logintegrity.MaxRangeEntries {
		return nil, status.Errorf(codes.InvalidArgument, "invalid log range [%d,%d]", first, last)
	}

	if minIndex := req.GetMinAppliedIndex(); minIndex > 0 {
		if s.waitForApplied == nil {
			return nil, status.Error(codes.Unavailable, "applied-index waiter unavailable")
		}
		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := s.waitForApplied(waitCtx, minIndex); err != nil {
			code := codes.Unavailable
			if errors.Is(err, context.DeadlineExceeded) {
				code = codes.DeadlineExceeded
			} else if errors.Is(err, context.Canceled) {
				code = codes.Canceled
			}

			return nil, status.Errorf(code, "waiting for applied index %d: %v", minIndex, err)
		}
	}

	handle, err := s.store.NewReadHandle()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "opening log snapshot: %v", err)
	}
	defer func() {
		// The read result is complete; Close releases the snapshot and lock.
		_ = handle.Close()
	}()

	applied, err := query.ReadLastAppliedIndex(handle)
	if err != nil {
		return nil, status.Errorf(codes.DataLoss, "reading applied index: %v", err)
	}
	if applied < req.GetMinAppliedIndex() {
		return nil, status.Errorf(codes.Unavailable, "snapshot applied index %d is below requested %d", applied, req.GetMinAppliedIndex())
	}

	head, err := logintegrity.LastLogSequence(handle)
	if err != nil {
		return nil, status.Errorf(codes.DataLoss, "reading log head: %v", err)
	}
	if head < last {
		if head == 0 {
			return nil, status.Error(codes.NotFound, "log is empty")
		}

		return nil, status.Errorf(codes.DataLoss, "log head %d is below requested sequence %d at applied index %d", head, last, applied)
	}

	digest, err := logintegrity.DigestRange(ctx, handle, first, last)
	if err != nil {
		if missing, ok := errors.AsType[*logintegrity.MissingLogError](err); ok {
			return nil, status.Errorf(codes.DataLoss, "log range [%d,%d] has a hole at %d", first, last, missing.Sequence)
		}
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}

		return nil, status.Errorf(codes.DataLoss, "digesting log range: %v", err)
	}
	if digest.First != first || digest.Last != last {
		return nil, status.Error(codes.Internal, fmt.Sprintf("unexpected digest range [%d,%d]", digest.First, digest.Last))
	}

	return &rafttransportpb.GetRangeDigestResponse{
		FirstLogSequence:      first,
		LastLogSequence:       last,
		Digest:                digest.Hash[:],
		DigestVersion:         digest.Version,
		AppliedIndex:          applied,
		LastLogSequenceOnNode: head,
	}, nil
}
