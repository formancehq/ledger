package state

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/accounttype"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// errMigrationCancelled is a sentinel used to break out of ForEachInPrefix
// when the migration is no longer active.
var errMigrationCancelled = errors.New("migration cancelled")

// AccountMigrationRequest contains the data needed to migrate accounts from
// one type pattern to another. Dispatched when a MigrateAccountType order is
// applied in the FSM.
type AccountMigrationRequest struct {
	LedgerName    string
	SourceType    string
	TargetType    string
	SourcePattern string
	TargetPattern string
}

// AccountMigrationWorker runs in the background to migrate account volumes
// from a source pattern to a target pattern.
// Follows the same leader-only, retry-with-backoff pattern as MetadataConverter.
type AccountMigrationWorker struct {
	logger    logging.Logger
	dataStore *dal.Store
	attrs     *attributes.Attributes
	requestCh chan AccountMigrationRequest
	proposer  Proposer
	isLeader  func() bool
	batchSize int
	w         worker.Worker
	wg        sync.WaitGroup
}

// NewAccountMigrationWorker creates a new background account migration worker.
func NewAccountMigrationWorker(
	logger logging.Logger,
	dataStore *dal.Store,
	attrs *attributes.Attributes,
	requestCh chan AccountMigrationRequest,
	proposer Proposer,
	isLeader func() bool,
	batchSize int,
) *AccountMigrationWorker {
	return &AccountMigrationWorker{
		logger:    logger.WithFields(map[string]any{"cmp": "account-migration"}),
		dataStore: dataStore,
		attrs:     attrs,
		requestCh: requestCh,
		proposer:  proposer,
		isLeader:  isLeader,
		batchSize: batchSize,
		w:         worker.New(),
	}
}

// Start launches the background migration goroutine.
func (amw *AccountMigrationWorker) Start() {
	amw.w.Run(amw.dispatchLoop)
}

// Stop signals the dispatcher to stop and waits for in-flight migrations.
func (amw *AccountMigrationWorker) Stop() {
	amw.w.Stop()
	amw.wg.Wait()
}

// dispatchLoop drains requestCh and processes one migration at a time.
func (amw *AccountMigrationWorker) dispatchLoop(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case req := <-amw.requestCh:
			amw.wg.Add(1)
			func() {
				defer amw.wg.Done()
				amw.migrateWithRetry(stop, req)
			}()
		}
	}
}

// isMigrationStillActive checks if the source type is still in MIGRATING status.
func (amw *AccountMigrationWorker) isMigrationStillActive(ledgerName, sourceType string) bool {
	ledgerInfo, err := query.GetLedgerByName(context.TODO(), amw.dataStore, ledgerName)
	if err != nil {
		return false
	}

	at, ok := ledgerInfo.AccountTypes[sourceType]
	if !ok {
		return false
	}

	return at.Status == commonpb.AccountTypeStatus_ACCOUNT_TYPE_MIGRATING
}

// migrateWithRetry retries migrate() with exponential backoff until done.
func (amw *AccountMigrationWorker) migrateWithRetry(stop <-chan struct{}, req AccountMigrationRequest) {
	worker.RetryWithBackoff(stop, amw.logger, func() error {
		if !amw.isMigrationStillActive(req.LedgerName, req.SourceType) {
			amw.logger.WithFields(map[string]any{
				"ledger":     req.LedgerName,
				"sourceType": req.SourceType,
			}).Infof("Migration no longer active (completed or cancelled), done")

			return nil
		}

		if !amw.isLeader() {
			return worker.ErrNotLeader
		}

		return amw.migrate(req)
	})
}

// migrate performs a single-pass streaming scan: discovers matching accounts
// and flushes rewrite batches as soon as batchSize is reached. This bounds
// memory usage to O(batchSize) instead of O(totalAccounts).
// Re-encountering an already-migrated account is harmless (idempotent rewrite).
func (amw *AccountMigrationWorker) migrate(req AccountMigrationRequest) error {
	logFields := map[string]any{
		"ledger":     req.LedgerName,
		"sourceType": req.SourceType,
		"targetType": req.TargetType,
	}

	sourceSegments, err := accounttype.ParsePattern(req.SourcePattern)
	if err != nil {
		return fmt.Errorf("parsing source pattern %q: %w", req.SourcePattern, err)
	}

	targetSegments, err := accounttype.ParsePattern(req.TargetPattern)
	if err != nil {
		return fmt.Errorf("parsing target pattern %q: %w", req.TargetPattern, err)
	}

	amw.logger.WithFields(logFields).Infof("Starting account migration")

	ledgerPrefix := []byte(req.LedgerName + "\x00")

	reader := amw.dataStore.NewReadHandle()
	defer func() { _ = reader.Close() }()

	// Single-pass scan: collect volume entries directly and flush when
	// we accumulate batchSize distinct accounts.
	var (
		volumeEntries []*raftcmdpb.MigrateAccountEntry
		accountCount  int
		migratedSoFar uint64
		// rewriteCache maps old account address → new address to avoid
		// re-parsing the pattern for every volume key of the same account.
		rewriteCache = make(map[string]string)
	)

	flush := func() error {
		if len(volumeEntries) == 0 {
			return nil
		}

		migratedSoFar += uint64(accountCount)

		_ = amw.proposer.ProposeOrders(&raftcmdpb.Order{
			Type: &raftcmdpb.Order_Apply{
				Apply: &raftcmdpb.LedgerApplyOrder{
					Ledger: req.LedgerName,
					Data: &raftcmdpb.LedgerApplyOrder_MigrateAccountBatch{
						MigrateAccountBatch: &raftcmdpb.MigrateAccountBatchOrder{
							SourceType:            req.SourceType,
							VolumeEntries:         volumeEntries,
							MigratedAccountsSoFar: migratedSoFar,
						},
					},
				},
			},
		})

		volumeEntries = volumeEntries[:0]
		accountCount = 0
		clear(rewriteCache)

		return nil
	}

	err = amw.attrs.Volume.ForEachInPrefix(reader, ledgerPrefix,
		func(entry attributes.ComputedEntry[*raftcmdpb.VolumePair]) error {
			var vk domain.VolumeKey
			if unmarshalErr := vk.Unmarshal(entry.CanonicalKey); unmarshalErr != nil {
				return nil
			}

			// Check cache first; compute rewrite on first encounter.
			newAddress, ok := rewriteCache[vk.Account]
			if !ok {
				bindings, matched := accounttype.MatchAddress(vk.Account, sourceSegments)
				if !matched {
					return nil
				}

				addr, rewriteErr := accounttype.RewriteAddress(bindings, targetSegments)
				if rewriteErr != nil || addr == vk.Account {
					return nil
				}

				newAddress = addr
				rewriteCache[vk.Account] = newAddress
				accountCount++

				// Check batch size on new account boundary.
				if accountCount >= amw.batchSize {
					if !amw.isMigrationStillActive(req.LedgerName, req.SourceType) {
						amw.logger.WithFields(logFields).Infof("Migration cancelled mid-batch, aborting")
						return errMigrationCancelled
					}
					if flushErr := flush(); flushErr != nil {
						return flushErr
					}
				}
			}

			newVK := domain.VolumeKey{
				AccountKey: domain.AccountKey{Ledger: vk.Ledger, Account: newAddress},
				Asset:      vk.Asset,
			}
			volumeEntries = append(volumeEntries, &raftcmdpb.MigrateAccountEntry{
				OldCanonicalKey: append([]byte(nil), entry.CanonicalKey...),
				NewCanonicalKey: newVK.Bytes(),
			})

			return nil
		},
	)
	if errors.Is(err, errMigrationCancelled) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("scanning volumes for migration: %w", err)
	}

	// Flush remaining entries.
	if err = flush(); err != nil {
		return fmt.Errorf("flushing final batch: %w", err)
	}

	// Propose completion.
	_ = amw.proposer.ProposeOrders(&raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: req.LedgerName,
				Data: &raftcmdpb.LedgerApplyOrder_CompleteMigration{
					CompleteMigration: &raftcmdpb.CompleteMigrationOrder{
						SourceType: req.SourceType,
					},
				},
			},
		},
	})

	amw.logger.WithFields(logFields).Infof("Migration complete, proposed %d accounts", migratedSoFar)

	return nil
}

