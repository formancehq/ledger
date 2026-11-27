package grpc

import (
	"context"
	"errors"
	"io"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
)

const (
	maxActiveLogSinkStreams = 8
	maxLogSinksPerPrincipal = 2
	maxUnacknowledgedLogs   = 16
	maxLogSinkPageBytes     = 8 << 20
	logSinkRescanInterval   = 250 * time.Millisecond
	logSinkAckTimeout       = 30 * time.Second
	logSinkSendTimeout      = 10 * time.Second
	logSinkOpenTimeout      = 10 * time.Second
)

type logSinkInput struct {
	ack *servicepb.AcknowledgeLedgerLog
	err error
}

// SubscribeLedgerLogs is an inbound, acknowledged sink. Its cursor is a
// ledger-local log ID and is never confused with the global events cursor.
func (impl *BucketServiceServerImpl) SubscribeLedgerLogs(stream servicepb.BucketService_SubscribeLedgerLogsServer) error {
	if impl.logSinkAcker == nil || impl.forwarder.node == nil {
		return status.Error(codes.Unavailable, "log sink service is not ready")
	}
	if !impl.forwarder.node.IsLeader() {
		return status.Error(codes.Unavailable, "connect to the current leader")
	}
	if err := impl.reserveLogSinkOpen(); err != nil {
		return err
	}
	reserved := true
	defer func() {
		if reserved {
			impl.releaseLogSinkOpen()
		}
	}()

	first, err := receiveLogSinkOpen(stream)
	if err != nil {
		return err
	}
	open := first.GetOpen()
	if open == nil || open.GetSinkName() == "" || open.GetClientId() == "" {
		return status.Error(codes.InvalidArgument, "first message must open a configured log sink")
	}

	ctx, cancel, expiry, err := logSinkAuthorizationContext(stream.Context())
	if err != nil {
		return err
	}
	defer cancel()
	authorizationError := func() error {
		if !time.Now().Before(expiry) {
			return status.Error(codes.Unauthenticated, "log sink token expired")
		}

		return ctx.Err()
	}
	send := func(response *servicepb.SubscribeLedgerLogsResponse) error {
		if err := authorizationError(); err != nil {
			return err
		}
		err := sendLogSinkResponse(ctx, stream, response)
		if authErr := authorizationError(); authErr != nil {
			return authErr
		}

		return err
	}
	cfg, acknowledged, err := impl.readLogSinkState(open.GetSinkName())
	if err != nil {
		return err
	}
	if err := authorizationError(); err != nil {
		return err
	}
	if cfg.GetGrpcLog() == nil || cfg.GetGrpcLog().GetClientId() != open.GetClientId() {
		return status.Error(codes.PermissionDenied, "log sink client does not match configuration")
	}
	if !logSinkOwnerMatches(ctx, cfg.GetGrpcLog().GetOwner()) {
		return status.Error(codes.PermissionDenied, "log sink principal does not match configuration")
	}
	incarnation := cfg.GetGrpcLog().GetIncarnation()
	if incarnation == 0 {
		return status.Error(codes.Internal, "log sink has no creation incarnation")
	}
	ledger := cfg.GetGrpcLog().GetLedger()
	err = impl.promoteLogSinkOpen(open.GetSinkName(), logSinkPrincipal(ctx))
	reserved = false
	if err != nil {
		return err
	}
	defer impl.releaseLogSink(open.GetSinkName())

	if err := send(&servicepb.SubscribeLedgerLogsResponse{
		Kind: &servicepb.SubscribeLedgerLogsResponse_Opened{Opened: &servicepb.OpenedLedgerLogSink{
			AcknowledgedLogId: acknowledged, ClusterId: impl.clusterID,
		}},
	}); err != nil {
		return err
	}

	input := make(chan logSinkInput, 1)
	go receiveLogSinkInput(stream, input)
	ticker := time.NewTicker(logSinkRescanInterval)
	defer ticker.Stop()
	sent := acknowledged
	inflight := make([]uint64, 0, maxUnacknowledgedLogs)
	lastAck := time.Now()
	for {
		if err := authorizationError(); err != nil {
			return err
		}
		if !impl.forwarder.node.IsLeader() {
			return status.Error(codes.Unavailable, "log sink leader changed")
		}
		if err := impl.checkLogSinkIncarnation(open.GetSinkName(), incarnation); err != nil {
			return err
		}
		if len(inflight) < maxUnacknowledgedLogs {
			logs, err := impl.readLogSinkPage(ctx, ledger, sent, maxUnacknowledgedLogs-len(inflight))
			if err != nil {
				if authErr := authorizationError(); authErr != nil {
					return authErr
				}

				return err
			}
			if err := authorizationError(); err != nil {
				return err
			}
			for _, log := range logs {
				if err := authorizationError(); err != nil {
					return err
				}
				if len(inflight) == 0 {
					lastAck = time.Now()
				}
				apply := log.GetPayload().GetApply()
				if apply == nil || apply.GetLog() == nil || apply.GetLog().GetId() <= sent {
					return status.Error(codes.Internal, "log sink read returned an unordered or non-ledger log")
				}
				id := apply.GetLog().GetId()
				if err := send(&servicepb.SubscribeLedgerLogsResponse{
					Kind: &servicepb.SubscribeLedgerLogsResponse_Log{Log: log},
				}); err != nil {
					return err
				}
				inflight = append(inflight, id)
				sent = id
			}
			if len(logs) > 0 {
				continue
			}
		}
		if len(inflight) > 0 && time.Since(lastAck) >= logSinkAckTimeout {
			return status.Error(codes.DeadlineExceeded, "log sink acknowledgement timed out")
		}
		select {
		case <-ctx.Done():
			return authorizationError()
		case message := <-input:
			if err := authorizationError(); err != nil {
				return err
			}
			if message.err != nil {
				if errors.Is(message.err, io.EOF) {
					return nil
				}

				return message.err
			}
			id := message.ack.GetLogId()
			if id != acknowledged && !slices.Contains(inflight, id) {
				return status.Error(codes.InvalidArgument, "acknowledgement must name a delivered ledger log")
			}
			if id != acknowledged {
				if err := impl.proposeLogSinkAck(ctx, open.GetSinkName(), incarnation, id); err != nil {
					if authErr := authorizationError(); authErr != nil {
						return authErr
					}

					return err
				}
				if err := authorizationError(); err != nil {
					return err
				}
				acknowledged = id
				for len(inflight) > 0 && inflight[0] <= id {
					inflight = inflight[1:]
				}
			}
			lastAck = time.Now()
			if err := send(&servicepb.SubscribeLedgerLogsResponse{
				Kind: &servicepb.SubscribeLedgerLogsResponse_Acked{Acked: &servicepb.AcknowledgedLedgerLog{LogId: id}},
			}); err != nil {
				return err
			}
		case <-ticker.C:
			// A rescan reads from the last sent ID. The index read is aligned to
			// a committed main-store horizon, so a commit cannot be lost between
			// an empty page and this wakeup.
		}
	}
}

func logSinkAuthorizationContext(ctx context.Context) (context.Context, context.CancelFunc, time.Time, error) {
	claims := internalauth.ClaimsFromContext(ctx)
	if claims == nil {
		return nil, nil, time.Time{}, status.Error(codes.Unauthenticated, "log sink requires an authenticated token")
	}
	expiry := claims.GetExpiration().Time
	if !time.Now().Before(expiry) {
		return nil, nil, time.Time{}, status.Error(codes.Unauthenticated, "log sink token expired")
	}
	authorized, cancel := context.WithDeadline(ctx, expiry)

	return authorized, cancel, expiry, nil
}

func receiveLogSinkOpen(stream servicepb.BucketService_SubscribeLedgerLogsServer) (*servicepb.SubscribeLedgerLogsRequest, error) {
	type result struct {
		request *servicepb.SubscribeLedgerLogsRequest
		err     error
	}
	done := make(chan result, 1)
	go func() {
		request, err := stream.Recv()
		done <- result{request: request, err: err}
	}()
	timer := time.NewTimer(logSinkOpenTimeout)
	defer timer.Stop()
	select {
	case received := <-done:
		return received.request, received.err
	case <-timer.C:
		return nil, status.Error(codes.DeadlineExceeded, "log sink open timed out")
	case <-stream.Context().Done():
		return nil, stream.Context().Err()
	}
}

func (impl *BucketServiceServerImpl) reserveLogSinkOpen() error {
	impl.logSinkMu.Lock()
	defer impl.logSinkMu.Unlock()
	if len(impl.activeLogSinks)+impl.pendingLogSinkOpens >= maxActiveLogSinkStreams {
		return status.Error(codes.ResourceExhausted, "too many active log sinks")
	}
	impl.pendingLogSinkOpens++

	return nil
}

func (impl *BucketServiceServerImpl) releaseLogSinkOpen() {
	impl.logSinkMu.Lock()
	impl.pendingLogSinkOpens--
	impl.logSinkMu.Unlock()
}

func (impl *BucketServiceServerImpl) promoteLogSinkOpen(name, principal string) error {
	impl.logSinkMu.Lock()
	defer impl.logSinkMu.Unlock()
	impl.pendingLogSinkOpens--

	return impl.claimLogSinkLocked(name, principal)
}

func logSinkPrincipal(ctx context.Context) string {
	claims := internalauth.ClaimsFromContext(ctx)
	if claims == nil {
		return "anonymous-or-auth-disabled"
	}
	if keyID := internalauth.KeyIDFromContext(ctx); keyID != "" {
		return "static:" + keyID + "\x00" + claims.Subject
	}

	return "oidc:" + claims.Issuer + "\x00" + claims.Subject
}

func logSinkOwnerMatches(ctx context.Context, owner *commonpb.CallerIdentity) bool {
	claims := internalauth.ClaimsFromContext(ctx)
	if claims == nil || owner == nil {
		return false
	}
	if keyID := internalauth.KeyIDFromContext(ctx); keyID != "" {
		return owner.GetKeyId() == keyID && owner.GetSubject() == claims.Subject
	}

	return owner.GetIssuer() != "" && owner.GetIssuer() == claims.Issuer && owner.GetSubject() == claims.Subject
}

func (impl *BucketServiceServerImpl) claimLogSink(name, principal string) error {
	impl.logSinkMu.Lock()
	defer impl.logSinkMu.Unlock()

	return impl.claimLogSinkLocked(name, principal)
}

func (impl *BucketServiceServerImpl) claimLogSinkLocked(name, principal string) error {
	if impl.activeLogSinks == nil {
		impl.activeLogSinks = make(map[string]string)
	}
	if impl.activeLogSinkPrincipals == nil {
		impl.activeLogSinkPrincipals = make(map[string]int)
	}
	if _, exists := impl.activeLogSinks[name]; exists {
		return status.Error(codes.AlreadyExists, "log sink already has an active stream")
	}
	if len(impl.activeLogSinks)+impl.pendingLogSinkOpens >= maxActiveLogSinkStreams || impl.activeLogSinkPrincipals[principal] >= maxLogSinksPerPrincipal {
		return status.Error(codes.ResourceExhausted, "too many active log sinks")
	}
	impl.activeLogSinks[name] = principal
	impl.activeLogSinkPrincipals[principal]++

	return nil
}

func (impl *BucketServiceServerImpl) releaseLogSink(name string) {
	impl.logSinkMu.Lock()
	if principal, exists := impl.activeLogSinks[name]; exists {
		impl.activeLogSinkPrincipals[principal]--
		if impl.activeLogSinkPrincipals[principal] == 0 {
			delete(impl.activeLogSinkPrincipals, principal)
		}
	}
	delete(impl.activeLogSinks, name)
	impl.logSinkMu.Unlock()
}

func (impl *BucketServiceServerImpl) readLogSinkState(name string) (*commonpb.SinkConfig, uint64, error) {
	handle, err := impl.store.NewDirectReadHandle()
	if err != nil {
		return nil, 0, status.Errorf(codes.Unavailable, "opening log sink state: %v", err)
	}
	defer func() { _ = handle.Close() }()
	cfg, err := impl.attrs.SinkConfig.Get(handle, domain.SinkConfigKey{Name: name}.Bytes())
	if err != nil {
		return nil, 0, status.Errorf(codes.Internal, "reading log sink configuration: %v", err)
	}
	if cfg == nil {
		return nil, 0, status.Error(codes.NotFound, "log sink is not configured")
	}
	if cfg.GetGrpcLog() == nil {
		return cfg, 0, nil
	}
	ledger, err := impl.attrs.Ledger.Get(handle, domain.LedgerKey{Name: cfg.GetGrpcLog().GetLedger()}.Bytes())
	if err != nil {
		return nil, 0, status.Errorf(codes.Internal, "reading log sink ledger identity: %v", err)
	}
	if ledger == nil || ledger.GetId() != cfg.GetGrpcLog().GetLedgerId() {
		return nil, 0, status.Error(codes.Unavailable, "log sink ledger was removed or recreated")
	}
	acknowledged, err := query.ReadGrpcLogSinkCursor(handle, cfg.GetGrpcLog().GetIncarnation())
	if err != nil {
		return nil, 0, status.Errorf(codes.Internal, "reading log sink cursor: %v", err)
	}

	return cfg, acknowledged, nil
}

func (impl *BucketServiceServerImpl) checkLogSinkIncarnation(name string, incarnation uint64) error {
	cfg, _, err := impl.readLogSinkState(name)
	if err != nil {
		return status.Error(codes.Unavailable, "log sink configuration changed")
	}
	if cfg.GetGrpcLog() == nil || cfg.GetGrpcLog().GetIncarnation() != incarnation {
		return status.Error(codes.Unavailable, "log sink configuration changed")
	}

	return nil
}

func (impl *BucketServiceServerImpl) readLogSinkPage(ctx context.Context, ledger string, after uint64, limit int) ([]*commonpb.Log, error) {
	cur, err := impl.ctrl.ListLogs(ctx, ledger, after, uint32(limit), nil, false)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "reading committed ledger logs: %v", err)
	}
	defer func() { _ = cur.Close() }()
	logs := make([]*commonpb.Log, 0, limit)
	pageBytes := 0
	for len(logs) < limit {
		log, err := cur.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "reading committed ledger log: %v", err)
		}
		logs = append(logs, log)
		pageBytes += log.SizeVT()
		if pageBytes >= maxLogSinkPageBytes {
			break
		}
	}

	return logs, nil
}

func receiveLogSinkInput(stream servicepb.BucketService_SubscribeLedgerLogsServer, output chan<- logSinkInput) {
	for {
		message, err := stream.Recv()
		if err != nil {
			select {
			case output <- logSinkInput{err: err}:
			case <-stream.Context().Done():
			}

			return
		}
		if message.GetAck() == nil {
			err = status.Error(codes.InvalidArgument, "subsequent messages must acknowledge a log")
		}
		select {
		case output <- logSinkInput{ack: message.GetAck(), err: err}:
		case <-stream.Context().Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func sendLogSinkResponse(ctx context.Context, stream servicepb.BucketService_SubscribeLedgerLogsServer, response *servicepb.SubscribeLedgerLogsResponse) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() { done <- stream.Send(response) }()
	timer := time.NewTimer(logSinkSendTimeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return status.Error(codes.DeadlineExceeded, "log sink receiver is too slow")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (impl *BucketServiceServerImpl) proposeLogSinkAck(ctx context.Context, sinkName string, incarnation, logID uint64) error {
	err := impl.logSinkAcker.Ack(ctx, sinkName, incarnation, logID)
	if errors.Is(err, state.ErrGrpcLogSinkStale) {
		return status.Error(codes.Unavailable, "log sink configuration changed")
	}
	if err != nil {
		return status.Errorf(codes.Unavailable, "acknowledging ledger log: %v", err)
	}

	return nil
}
