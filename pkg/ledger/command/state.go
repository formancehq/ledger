package command

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/pkg/errors"
)

type ReserveRequest struct {
	Timestamp core.Time
	Reference string
}

// State store in flight transactions
type State struct {
	mu                 sync.Mutex
	store              Store
	moreRecentInFlight *inFlight
	// inFlights container in flight transactions
	inFlights            map[*inFlight]struct{}
	inFlightsByReference map[string]*inFlight
	// lastTransactionDate store the more recent processed transactions
	// the matching log could be written or not
	lastTransactionDate core.Time
	// allowPastTimestamps allow to insert transactions in the past
	allowPastTimestamps bool
	lastTXID            *atomic.Int64
}

func (s *State) checkConstraints(ctx context.Context, r ReserveRequest) error {
	if !s.allowPastTimestamps {
		if s.moreRecentInFlight != nil && s.moreRecentInFlight.timestamp.After(r.Timestamp) {
			return errorsutil.NewError(ErrPastTransaction,
				errors.Errorf("%s (passed) is %s before %s (last)", r.Timestamp.Format(time.RFC3339Nano),
					s.moreRecentInFlight.timestamp.Sub(r.Timestamp),
					s.moreRecentInFlight.timestamp.Format(time.RFC3339Nano)))
		}
	}

	if r.Reference != "" {
		if _, ok := s.inFlightsByReference[r.Reference]; ok {
			return errorsutil.NewError(ErrConflictError, errors.New("reference already used, in flight occurring"))
		}
		_, err := s.store.ReadLogForCreatedTransactionWithReference(ctx, r.Reference)
		if err == nil {
			// Log found
			return errorsutil.NewError(ErrConflictError, errors.New("reference already used, log found in storage"))
		}

		if !storage.IsNotFoundError(err) {
			return errors.Wrap(err, "failed to read log with reference")
		}
	}

	return nil
}

func (s *State) Reserve(ctx context.Context, r ReserveRequest) (*Reserve, *core.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if r.Timestamp.IsZero() {
		r.Timestamp = core.Now()
	}

	if err := s.checkConstraints(ctx, r); err != nil {
		return nil, nil, err
	}

	ret := &inFlight{
		reference: r.Reference,
		timestamp: r.Timestamp,
	}
	s.inFlights[ret] = struct{}{}
	if r.Reference != "" {
		s.inFlightsByReference[r.Reference] = ret
	}

	// Link to previous in flight in the nominal case where transaction are properly handled in order
	if s.moreRecentInFlight != nil && !r.Timestamp.Before(s.moreRecentInFlight.timestamp) {
		ret.previous = s.moreRecentInFlight
		ret.previous.next = ret
	}
	if s.moreRecentInFlight == nil || !r.Timestamp.Before(s.moreRecentInFlight.timestamp) {
		s.moreRecentInFlight = ret
	}

	return &Reserve{
		inFlight: ret,
		state:    s,
	}, &r.Timestamp, nil
}

func (s *State) GetMoreRecentTransactionDate() core.Time {
	return s.lastTransactionDate
}

func (s *State) GetNextTXID() uint64 {
	return uint64(s.lastTXID.Add(1))
}

func Load(store Store, allowPastTimestamps bool) *State {
	log, err := store.ReadLastLogWithType(context.Background(), core.NewTransactionLogType, core.RevertedTransactionLogType)
	if err != nil && !storage.IsNotFoundError(err) {
		panic(err)
	}
	var (
		lastTxID            *uint64
		lastTransactionDate core.Time
	)
	if err == nil {
		switch payload := log.Data.(type) {
		case core.NewTransactionLogPayload:
			lastTxID = &payload.Transaction.ID
			lastTransactionDate = payload.Transaction.Timestamp
		case core.RevertedTransactionLogPayload:
			lastTxID = &payload.RevertTransaction.ID
			lastTransactionDate = payload.RevertTransaction.Timestamp
		default:
			panic(fmt.Sprintf("unhandled payload type: %T", payload))
		}
	}
	lastTXID := &atomic.Int64{}
	if lastTxID != nil {
		lastTXID.Add(int64(*lastTxID))
	} else {
		lastTXID.Add(-1)
	}
	return &State{
		store:                store,
		inFlights:            map[*inFlight]struct{}{},
		inFlightsByReference: map[string]*inFlight{},
		lastTransactionDate:  lastTransactionDate,
		allowPastTimestamps:  allowPastTimestamps,
		lastTXID:             lastTXID,
	}
}
