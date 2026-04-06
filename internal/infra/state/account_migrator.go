package state

import (
	"context"
	"fmt"
	"sync"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/accounttype"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// AccountMigrateRequest contains the data needed to migrate an account type
// to a new pattern. Dispatched by the FSM to the AccountMigrator background worker.
type AccountMigrateRequest struct {
	LedgerName      string
	AccountTypeName string
	OldPattern      string
	TargetPattern   string
}

// accountMigrationGroup collects volumes and metadata keys for a single
// matched account during a migration scan.
type accountMigrationGroup struct {
	newAddress   string
	assets       map[string]struct{}
	metadataKeys map[string]struct{}
}

// AccountMigrator runs in the background to migrate accounts when an account
// type pattern is changed (StartAccountMigration).
//
// Incoming requests are drained from requestCh immediately (no back-pressure on
// the FSM) and processed sequentially by a single goroutine.
//
// Only the leader node performs the migration and proposes. Followers wait and
// re-check until the account type is no longer in MIGRATING state (completed by
// the leader through Raft).
type AccountMigrator struct {
	logger    logging.Logger
	dataStore *dal.Store
	attrs     *attributes.Attributes
	requestCh chan AccountMigrateRequest
	proposer  Proposer
	isLeader  func() bool
	batchSize int
	w         worker.Worker
	wg        sync.WaitGroup
}

// NewAccountMigrator creates a new background account migrator.
func NewAccountMigrator(
	logger logging.Logger,
	dataStore *dal.Store,
	attrs *attributes.Attributes,
	requestCh chan AccountMigrateRequest,
	proposer Proposer,
	isLeader func() bool,
	batchSize int,
) *AccountMigrator {
	return &AccountMigrator{
		logger:    logger.WithFields(map[string]any{"cmp": "account-migrator"}),
		dataStore: dataStore,
		attrs:     attrs,
		requestCh: requestCh,
		proposer:  proposer,
		isLeader:  isLeader,
		batchSize: batchSize,
		w:         worker.New(),
	}
}

// Start launches the background account migration goroutine.
func (am *AccountMigrator) Start() {
	am.w.Run(am.dispatchLoop)
}

// Stop signals the dispatcher goroutine to stop and waits for all in-flight
// migrations to finish.
func (am *AccountMigrator) Stop() {
	am.w.Stop()
	am.wg.Wait()
}

// dispatchLoop drains requestCh into an internal queue and processes requests
// sequentially (single goroutine, no pool).
func (am *AccountMigrator) dispatchLoop(stop <-chan struct{}) {
	ctx := worker.ContextFromStop(stop)

	var pending []AccountMigrateRequest

	for {
		if len(pending) > 0 {
			// Process the head of the queue, while still accepting new
			// requests and checking for stop.
			req := pending[0]
			pending = pending[1:]

			am.wg.Go(func() {
				am.migrateWithRetry(ctx, stop, req)
			})

			// Drain any requests that arrived while processing.
			for {
				select {
				case r := <-am.requestCh:
					pending = append(pending, r)
				default:
					goto donedraining
				}
			}
		donedraining:
			continue
		}

		// Nothing pending: wait for new work or stop.
		select {
		case <-stop:
			return
		case req := <-am.requestCh:
			pending = append(pending, req)
		}
	}
}

// isTypeStillMigrating checks whether an account type is still in MIGRATING
// state by reading the ledger's account types from the data store.
func (am *AccountMigrator) isTypeStillMigrating(ctx context.Context, ledgerName, accountTypeName, expectedOldPattern string) bool {
	ledgerInfo, err := query.GetLedgerByName(ctx, am.dataStore, ledgerName)
	if err != nil {
		return false
	}

	types := ledgerInfo.GetAccountTypes()
	if types == nil {
		return false
	}

	at, ok := types[accountTypeName]
	if !ok {
		return false
	}

	return at.GetStatus() == commonpb.AccountTypeStatus_ACCOUNT_TYPE_MIGRATING &&
		at.GetPattern() == expectedOldPattern
}

// migrateWithRetry retries migrate() with exponential backoff until it succeeds
// or the migrator is stopped.
// On follower nodes, the loop exits when the account type is no longer in
// MIGRATING state (completed by the leader through Raft).
func (am *AccountMigrator) migrateWithRetry(ctx context.Context, stop <-chan struct{}, req AccountMigrateRequest) {
	worker.RetryWithBackoff(stop, am.logger, func() error {
		if !am.isTypeStillMigrating(ctx, req.LedgerName, req.AccountTypeName, req.OldPattern) {
			am.logger.WithFields(map[string]any{
				"ledger":      req.LedgerName,
				"accountType": req.AccountTypeName,
			}).Infof("Account type no longer migrating (completed by leader), done")

			return nil
		}

		if !am.isLeader() {
			return worker.ErrNotLeader
		}

		return am.migrate(ctx, req)
	})
}

// migrate scans all account volumes and metadata for the specified ledger,
// finds accounts matching the old pattern, computes new addresses using the
// target pattern, and proposes batches of account renames through Raft.
//
// Uses two streaming Pebble passes (via StreamingIter):
//   - Pass 1: scan volumes to discover matching accounts and their assets
//   - Pass 2: scan metadata to collect metadata keys for matched accounts
//
// Both passes use the same point-in-time read snapshot for consistency.
func (am *AccountMigrator) migrate(ctx context.Context, req AccountMigrateRequest) error {
	logFields := map[string]any{
		"ledger":        req.LedgerName,
		"accountType":   req.AccountTypeName,
		"oldPattern":    req.OldPattern,
		"targetPattern": req.TargetPattern,
	}

	if !am.isTypeStillMigrating(ctx, req.LedgerName, req.AccountTypeName, req.OldPattern) {
		am.logger.WithFields(logFields).Infof("Account type no longer migrating (completed by leader), done")

		return nil
	}

	if !am.isLeader() {
		return worker.ErrNotLeader
	}

	// Validate the ledger exists (off the hot path).
	if _, err := query.GetLedgerByName(ctx, am.dataStore, req.LedgerName); err != nil {
		return fmt.Errorf("resolving ledger %q: %w", req.LedgerName, err)
	}

	// Parse old and target patterns.
	oldSegments, err := accounttype.ParsePattern(req.OldPattern)
	if err != nil {
		return fmt.Errorf("parsing old pattern %q: %w", req.OldPattern, err)
	}

	targetSegments, err := accounttype.ParsePattern(req.TargetPattern)
	if err != nil {
		return fmt.Errorf("parsing target pattern %q: %w", req.TargetPattern, err)
	}

	am.logger.WithFields(logFields).Infof("Starting account migration")

	// Build the canonical prefix for this ledger.
	ledgerPrefix := []byte(req.LedgerName + "\x00")

	// Open a Pebble read handle for a point-in-time snapshot used by both passes.
	reader := am.dataStore.NewReadHandle()

	defer func() { _ = reader.Close() }()

	// Pass 1: scan volumes to discover matching accounts and their assets.
	matched := make(map[string]*accountMigrationGroup)

	volIter, err := am.attrs.Volume.NewStreamingIter(reader, ledgerPrefix)
	if err != nil {
		return fmt.Errorf("creating volume iterator for ledger %s: %w", req.LedgerName, err)
	}

	for volIter.Next() {
		entry := volIter.Entry()

		var vk domain.VolumeKey

		if vkErr := vk.Unmarshal(entry.CanonicalKey); vkErr != nil {
			am.logger.Errorf("Failed to unmarshal volume key %x: %v", entry.CanonicalKey, vkErr)

			continue
		}

		bindings, ok := accounttype.MatchAddress(vk.Account, oldSegments)
		if !ok {
			continue
		}

		newAddr, rwErr := accounttype.RewriteAddress(bindings, targetSegments)
		if rwErr != nil {
			am.logger.Errorf("Failed to rewrite address %q: %v", vk.Account, rwErr)

			continue
		}

		group, exists := matched[vk.Account]
		if !exists {
			group = &accountMigrationGroup{
				newAddress:   newAddr,
				assets:       make(map[string]struct{}),
				metadataKeys: make(map[string]struct{}),
			}
			matched[vk.Account] = group
		}

		group.assets[vk.Asset] = struct{}{}
	}

	if err := volIter.Close(); err != nil {
		return fmt.Errorf("closing volume iterator for ledger %s: %w", req.LedgerName, err)
	}

	if err := volIter.Err(); err != nil {
		return fmt.Errorf("scanning volumes for ledger %s: %w", req.LedgerName, err)
	}

	am.logger.WithFields(map[string]any{
		"ledger":          req.LedgerName,
		"accountType":     req.AccountTypeName,
		"matchedAccounts": len(matched),
	}).Infof("Scanned volumes, starting metadata pass")

	// Pass 2: scan metadata to collect metadata keys for matched accounts.
	metaIter, err := am.attrs.Metadata.NewStreamingIter(reader, ledgerPrefix)
	if err != nil {
		return fmt.Errorf("creating metadata iterator for ledger %s: %w", req.LedgerName, err)
	}

	for metaIter.Next() {
		entry := metaIter.Entry()

		var mk domain.MetadataKey

		if mkErr := mk.Unmarshal(entry.CanonicalKey); mkErr != nil {
			continue
		}

		group, ok := matched[mk.Account]
		if !ok {
			continue
		}

		group.metadataKeys[mk.Key] = struct{}{}
	}

	if err := metaIter.Close(); err != nil {
		return fmt.Errorf("closing metadata iterator for ledger %s: %w", req.LedgerName, err)
	}

	if err := metaIter.Err(); err != nil {
		return fmt.Errorf("scanning metadata for ledger %s: %w", req.LedgerName, err)
	}

	// Batch matched accounts and propose through Raft.
	batch := make([]*raftcmdpb.AccountMigrationEntry, 0, am.batchSize)
	aborted := false

	for oldAddr, group := range matched {
		assets := make([]string, 0, len(group.assets))
		for a := range group.assets {
			assets = append(assets, a)
		}

		metadataKeys := make([]string, 0, len(group.metadataKeys))
		for k := range group.metadataKeys {
			metadataKeys = append(metadataKeys, k)
		}

		batch = append(batch, &raftcmdpb.AccountMigrationEntry{
			OldAddress:   oldAddr,
			NewAddress:   group.newAddress,
			Assets:       assets,
			MetadataKeys: metadataKeys,
		})

		if len(batch) >= am.batchSize {
			if !am.isTypeStillMigrating(ctx, req.LedgerName, req.AccountTypeName, req.OldPattern) {
				am.logger.WithFields(logFields).Infof("Account type no longer migrating mid-batch, aborting")

				aborted = true

				break
			}

			am.proposeBatch(req.LedgerName, req.AccountTypeName, req.OldPattern, batch)
			batch = make([]*raftcmdpb.AccountMigrationEntry, 0, am.batchSize)
		}
	}

	if aborted {
		return nil
	}

	// Propose any remaining partial batch.
	if len(batch) > 0 {
		if !am.isTypeStillMigrating(ctx, req.LedgerName, req.AccountTypeName, req.OldPattern) {
			am.logger.WithFields(logFields).Infof("Account type no longer migrating mid-batch, aborting")

			return nil
		}

		am.proposeBatch(req.LedgerName, req.AccountTypeName, req.OldPattern, batch)
	}

	// Propose migration completion.
	am.proposeComplete(req.LedgerName, req.AccountTypeName, req.OldPattern)

	am.logger.WithFields(logFields).Infof("Account migration complete, proposed completion")

	return nil
}

// proposeBatch proposes an AccountMigrationBatchOrder to Raft.
func (am *AccountMigrator) proposeBatch(
	ledgerName string,
	accountTypeName string,
	expectedOldPattern string,
	entries []*raftcmdpb.AccountMigrationEntry,
) {
	_ = am.proposer.ProposeOrders(&raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: ledgerName,
				Data: &raftcmdpb.LedgerApplyOrder_AccountMigrationBatch{
					AccountMigrationBatch: &raftcmdpb.AccountMigrationBatchOrder{
						AccountTypeName:    accountTypeName,
						ExpectedOldPattern: expectedOldPattern,
						Entries:            entries,
					},
				},
			},
		},
	})
}

// proposeComplete proposes a CompleteAccountMigrationOrder to Raft.
func (am *AccountMigrator) proposeComplete(
	ledgerName string,
	accountTypeName string,
	expectedOldPattern string,
) {
	_ = am.proposer.ProposeOrders(&raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: ledgerName,
				Data: &raftcmdpb.LedgerApplyOrder_CompleteAccountMigration{
					CompleteAccountMigration: &raftcmdpb.CompleteAccountMigrationOrder{
						AccountTypeName:    accountTypeName,
						ExpectedOldPattern: expectedOldPattern,
					},
				},
			},
		},
	})
}
