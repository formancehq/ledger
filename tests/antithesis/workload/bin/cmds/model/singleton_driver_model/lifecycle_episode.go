package main

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// lifecycleDriver owns the clients and oracle for drained lifecycle episodes.
// The client must surface maintenance errors without automatic retries.
type lifecycleDriver struct {
	client  servicepb.BucketServiceClient
	cluster clusterpb.ClusterServiceClient
	checker *Checker
}

// lifecycleCall retries availability failures, preserving a maintenance rejection
// as a definitive admission outcome. Callers pin an Apply idempotency key before
// entering this loop so a lost reply cannot duplicate a committed mutation.
func lifecycleCall[T any](ctx context.Context, call func(context.Context) (T, error)) (T, error) {
	for {
		attempt, cancel := context.WithTimeout(ctx, 5*time.Second)
		result, err := call(attempt)
		cancel()
		if err == nil || ctx.Err() != nil || internal.HasErrorReason(err, domain.ErrReasonMaintenanceMode) || (!internal.IsTransient(err) && !internal.IsCanceled(err)) {
			return result, err
		}
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// apply executes at the drained Checker seam. No other dispatch may
// race it: maintenance is checked at admission, not at the Raft commit position.
func (d *lifecycleDriver) apply(ctx context.Context, reqs ...*servicepb.Request) error {
	client, c := d.client, d.checker
	bulk := oracle.Bulk{Requests: reqs}
	c.mu.Lock()
	prediction := c.modelState.Apply(bulk)
	maintenance := c.modelState.MaintenanceMode()
	c.mu.Unlock()
	expected := prediction.Reason
	if maintenance {
		for _, req := range reqs {
			if req.GetSetMaintenanceMode() == nil {
				expected = domain.ErrReasonMaintenanceMode
				break
			}
		}
	}
	req := applyRequest(bulk)
	resp, err := lifecycleCall(ctx, func(ctx context.Context) (*servicepb.ApplyResponse, error) { return client.Apply(ctx, req) })
	dumpBatch(0, req, resp, err)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if expected != "" {
		if err == nil || internal.ErrorReason(err) != expected {
			return fmt.Errorf("%s: expected rejection %s, got %v", requestKinds(bulk), expected, err)
		}
		markModelOutcomeVerified()
		return nil
	}
	if err != nil {
		return fmt.Errorf("%s: %w", requestKinds(bulk), err)
	}
	if len(resp.GetLogs()) != len(reqs) {
		return fmt.Errorf("%s: expected %d logs, got %d", requestKinds(bulk), len(reqs), len(resp.GetLogs()))
	}
	for i, request := range reqs {
		if resp.GetLogs()[i].GetSequence() == 0 {
			return fmt.Errorf("%s: response lacks committed sequence", requestKinds(bulk))
		}
		if err := validateLifecycleLog(request, resp.GetLogs()[i].GetPayload()); err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.validateBulkSuccess(bulk, resp)
	c.mu.Unlock()
	markModelOutcomeVerified()
	return nil
}

func (d *lifecycleDriver) checkLedger(ctx context.Context, name string) error {
	client, c := d.client, d.checker
	info, err := lifecycleCall(ctx, func(ctx context.Context) (*commonpb.LedgerInfo, error) {
		return client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: name})
	})
	if err != nil {
		return fmt.Errorf("read ledger %s: %w", name, err)
	}
	c.mu.Lock()
	lifecycle, exists := c.modelState.Lifecycle(name)
	ls := c.modelState.Ledger(name)
	c.mu.Unlock()
	if !exists || lifecycle.Deleted || info.GetName() != name || info.GetMode() != lifecycle.Mode || !info.GetMirrorSource().EqualVT(lifecycle.MirrorSource) || !chartMatches(ls, info.GetAccountTypes()) || !ledgerMetaMatches(ls, info.GetMetadata()) {
		return fmt.Errorf("ledger %s does not match lifecycle oracle", name)
	}
	for _, tx := range ls.Txs().All() {
		resp, err := lifecycleCall(ctx, func(ctx context.Context) (*servicepb.GetTransactionResponse, error) {
			return client.GetTransaction(ctx, &servicepb.GetTransactionRequest{Ledger: name, TransactionId: tx.Id()})
		})
		if err != nil {
			return fmt.Errorf("read transaction %s/%d: %w", name, tx.Id(), err)
		}
		if !txRecordMatches(tx, resp.GetTransaction()) {
			return fmt.Errorf("transaction %s/%d does not match oracle", name, tx.Id())
		}
	}
	return nil
}

func (d *lifecycleDriver) checkDeletedLedger(ctx context.Context, name string) error {
	client := d.client
	probes := []struct {
		name string
		read func(context.Context) error
	}{
		{"ledger", func(ctx context.Context) error {
			_, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: name})
			return err
		}},
		{"transaction", func(ctx context.Context) error {
			_, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{Ledger: name, TransactionId: 1})
			return err
		}},
		{"accounts", func(ctx context.Context) error { _, err := actions.ListAllAccounts(ctx, client, name); return err }},
		{"transactions", func(ctx context.Context) error { _, err := actions.ListAllTransactions(ctx, client, name); return err }},
		{"logs", func(ctx context.Context) error { _, err := actions.ListAllLogs(ctx, client, name); return err }},
		{"schema", func(ctx context.Context) error {
			_, err := client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{Ledger: name})
			return err
		}},
		{"indexes", func(ctx context.Context) error {
			stream, err := client.ListIndexes(ctx, &servicepb.ListIndexesRequest{Scope: servicepb.ListIndexesRequest_SCOPE_LEDGER, Ledger: name})
			if err == nil {
				_, err = stream.Recv()
			}
			return err
		}},
	}
	for _, probe := range probes {
		_, err := lifecycleCall(ctx, func(ctx context.Context) (struct{}, error) { return struct{}{}, probe.read(ctx) })
		if status.Code(err) != codes.NotFound {
			return fmt.Errorf("deleted %s %s must return NotFound, got %v", name, probe.name, err)
		}
	}
	ledgers, err := lifecycleCall(ctx, func(ctx context.Context) (map[string]*commonpb.LedgerInfo, error) {
		return actions.ListLedgers(ctx, client)
	})
	if err != nil {
		return err
	}
	if _, exists := ledgers[name]; exists {
		return fmt.Errorf("deleted ledger %s survived in ListLedgers", name)
	}
	return nil
}

func lifecycleTransaction(name string) *servicepb.Request {
	req := actions.CreateTransactionAction(name, []*commonpb.Posting{commonpb.NewPosting("world", "cash:1", "USD/2", big.NewInt(17))}, map[string]string{"origin": "lifecycle"}, nil)
	req.GetApply().GetAction().GetCreateTransaction().Reference = "lifecycle-reference"
	return req
}

// runEpisode uses fresh, disposable ledgers so retirement cannot starve
// the concurrent business fleet. The caller owns the dispatch pause. The probe
// client must have automatic retries disabled, otherwise maintenance never returns.
func (d *lifecycleDriver) runEpisode(ctx context.Context, prefix string) (result error) {
	retired, promoted := prefix+"-retired", prefix+"-promoted"
	apply := func(reqs ...*servicepb.Request) error { return d.apply(ctx, reqs...) }
	if err := apply(actions.CreateLedgerAction(retired, nil), actions.AddAccountTypeAction(retired, "cash", "cash:{id}"), actions.SaveLedgerMetadataAction(retired, map[string]string{"owner": "model"})); err != nil {
		return err
	}
	if err := apply(lifecycleTransaction(retired)); err != nil {
		return err
	}
	if err := apply(actions.RevertTransactionAction(retired, 1, false, false, nil), actions.CreateBuiltinTxIndexAction(retired, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)); err != nil {
		return err
	}
	if err := d.checkLedger(ctx, retired); err != nil {
		return err
	}

	// An unconfigured source has no transport: createSource rejects its empty
	// oneof before starting a worker. It cannot import outside the model timeline,
	// and its non-empty ledger name makes source removal observable.
	mirror := &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: promoted, Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR, MirrorSource: &commonpb.MirrorSourceConfig{LedgerName: "unused"}}}}
	if err := apply(mirror, actions.AddAccountTypeAction(promoted, "cash", "cash:{id}")); err != nil {
		return err
	}
	if err := d.checkLedger(ctx, promoted); err != nil {
		return err
	}
	if err := apply(lifecycleTransaction(promoted)); err != nil {
		return err
	}
	promote := &servicepb.Request{Type: &servicepb.Request_PromoteLedger{PromoteLedger: &servicepb.PromoteLedgerRequest{Ledger: promoted}}}
	if err := apply(promote); err != nil {
		return err
	}
	if err := apply(promote); err != nil {
		return err
	}
	if err := apply(lifecycleTransaction(promoted)); err != nil {
		return err
	}
	if err := d.checkLedger(ctx, promoted); err != nil {
		return err
	}
	emitCoverage(true, coveragePromotionMessage, internal.Details{"ledger": promoted}, coverageHit)

	// Reuse the business generator to retire varied projections, not only the
	// fixed seed. Every outcome is predicted, including cross-ledger rollback.
	for range 12 {
		d.checker.mu.Lock()
		state := d.checker.modelState
		d.checker.mu.Unlock()
		bulk := generateBulk(state, []string{retired, promoted})
		if len(bulk.Requests) != 0 {
			if err := apply(bulk.Requests...); err != nil {
				return err
			}
		}
	}
	if err := d.checkLedger(ctx, retired); err != nil {
		return err
	}

	if err := apply(actions.DeleteLedgerAction(retired)); err != nil {
		return err
	}
	if err := d.checkDeletedLedger(ctx, retired); err != nil {
		return err
	}
	if err := apply(actions.SaveLedgerMetadataAction(promoted, map[string]string{"unexpected": "rollback"}), lifecycleTransaction(retired)); err != nil {
		return err
	}
	if err := apply(actions.CreateLedgerAction(retired, nil)); err != nil {
		return err
	}
	if err := d.checkLedger(ctx, promoted); err != nil {
		return err
	}
	emitCoverage(true, coverageDeletionMessage, internal.Details{"ledger": retired}, coverageHit)

	// Always attempt disable, even if the enable response was lost. A fresh
	// cleanup context is required when the episode's deadline has expired.
	restored := false
	defer func() {
		if restored {
			return
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := d.apply(cleanup, actions.SetMaintenanceModeAction(false)); err != nil {
			result = errors.Join(result, fmt.Errorf("disable maintenance: %w", err))
		}
	}()
	if err := apply(actions.SetMaintenanceModeAction(true)); err != nil {
		return err
	}
	if err := d.checkMaintenanceMode(ctx, true); err != nil {
		return err
	}
	if err := apply(actions.SaveLedgerMetadataAction(promoted, map[string]string{"maintenance": "rejected"})); err != nil {
		return err
	}
	if err := d.checkLedger(ctx, promoted); err != nil {
		return err
	}
	if err := apply(actions.SetMaintenanceModeAction(false)); err != nil {
		return err
	}
	if err := d.checkMaintenanceMode(ctx, false); err != nil {
		return err
	}
	restored = true
	if err := apply(actions.SaveLedgerMetadataAction(promoted, map[string]string{"maintenance": "recovered"})); err != nil {
		return err
	}
	if err := d.checkLedger(ctx, promoted); err != nil {
		return err
	}
	emitCoverage(true, coverageMaintenanceMessage, internal.Details{"ledger": promoted}, coverageHit)
	return nil
}

func (d *lifecycleDriver) checkMaintenanceMode(ctx context.Context, enabled bool) error {
	client := d.cluster
	state, err := lifecycleCall(ctx, func(ctx context.Context) (*clusterpb.ClusterState, error) {
		return client.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
	})
	if err != nil {
		return err
	}
	if state.GetMaintenanceMode() != enabled {
		return fmt.Errorf("maintenance mode round-trip: expected %t, got %t", enabled, state.GetMaintenanceMode())
	}
	return nil
}

// A bounded pool keeps tombstones from growing without limit during long chaos
// runs. The first episode runs before workers; later episodes exercise draining
// a busy driver and share pause ownership with backup/restore.
func (d *lifecycleDriver) runCycles(ctx context.Context, prefix string) error {
	c := d.checker
	for i := 1; i <= 3; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(30 * time.Second):
		}
		err := func() error {
			c.cycleMu.Lock()
			defer c.cycleMu.Unlock()
			defer c.resume()
			if !c.pauseAndDrain(ctx) {
				return ctx.Err()
			}
			return d.runEpisode(ctx, fmt.Sprintf("%s-lifecycle-%d", prefix, i))
		}()
		if err != nil {
			return err
		}
	}
	return nil
}
