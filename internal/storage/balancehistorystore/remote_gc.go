package balancehistorystore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"go.opentelemetry.io/otel/metric"

	"github.com/formancehq/ledger/v3/internal/storage/balancehistoryarchive"
)

const (
	remoteGCFormatVersion      = 1
	DefaultRemoteGCGracePeriod = 24 * time.Hour
	DefaultRemoteGCScanLimit   = 1_000
	DefaultRemoteGCDeleteLimit = 100
)

// ErrRemoteGCRootsUnavailable means at least one active View makes the remote
// root set ambiguous. Manifest leases are version-only and Reset can reuse a
// version, so collection fails closed for the whole call and succeeds again
// after every View lease closes.
var ErrRemoteGCRootsUnavailable = errors.New("balance history remote GC roots are unavailable")

var errRemoteGCScanEpochChanged = errors.New("balance history remote GC scan epoch changed during listing")

// RemoteCollectorConfig controls the safety window between first observation
// and deletion. A zero GracePeriod selects DefaultRemoteGCGracePeriod.
type RemoteCollectorConfig struct {
	GracePeriod time.Duration
	Now         func() time.Time
}

// RemoteGCBudget bounds remote listing and deletion work in one call. Zero
// fields select conservative defaults.
type RemoteGCBudget struct {
	ScanLimit   int
	DeleteLimit int
}

// RemoteGCResult describes one bounded collection call.
type RemoteGCResult struct {
	ScannedObjects uint64
	ScannedBytes   uint64
	DeletedObjects uint64
	DeletedBytes   uint64
	RetiredRooted  uint64
	QueueObjects   uint64
	QueueBytes     uint64
	CycleCompleted bool
}

// RemoteCollector owns the process-local collection mutex and metrics. Its
// cursor and candidates live in the Store's Pebble DB so crashes and process
// restarts cannot bypass the observation grace period.
type RemoteCollector struct {
	store       *Store
	gracePeriod time.Duration
	now         func() time.Time

	mu      sync.Mutex
	metrics remoteGCMetrics
	hooks   remoteGCHooks
}

type remoteGCHooks struct {
	afterListBeforeSync   func() error
	afterDeleteBeforeAck  func([32]byte) error
	beforeInventoryGate   func()
	afterInventoryPage    func()
	beforeArchiveGate     func()
	afterArchiveGatePhase func()
}

type remoteGCState struct {
	FormatVersion           uint32 `json:"formatVersion"`
	Namespace               string `json:"namespace"`
	DestinationIdentity     string `json:"destinationIdentity"`
	ScanEpoch               uint64 `json:"scanEpoch"`
	CompletedInventoryEpoch uint64 `json:"completedInventoryEpoch,omitempty"`
	Cursor                  string `json:"cursor,omitempty"`
	Cycle                   uint64 `json:"cycle"`
	ScanObjects             uint64 `json:"scanObjects"`
	ScanBytes               uint64 `json:"scanBytes"`
	InventoryObjects        uint64 `json:"inventoryObjects"`
	InventoryBytes          uint64 `json:"inventoryBytes"`
	QueueObjects            uint64 `json:"queueObjects"`
	QueueBytes              uint64 `json:"queueBytes"`
	OldestObservedUnixNano  int64  `json:"oldestObservedUnixNano,omitempty"`
}

type remoteGCCandidate struct {
	FormatVersion         uint32 `json:"formatVersion"`
	Namespace             string `json:"namespace"`
	DestinationIdentity   string `json:"destinationIdentity"`
	Size                  uint64 `json:"size"`
	FirstObservedUnixNano int64  `json:"firstObservedUnixNano"`
	FirstObservedCycle    uint64 `json:"firstObservedCycle"`
	LastObservedCycle     uint64 `json:"lastObservedCycle"`
}

type remoteGCDeletion struct {
	digest    [32]byte
	candidate remoteGCCandidate
}

// NewRemoteCollector creates a collector for the Store's configured Archive.
// Destructive authority is available only when that exact Archive also
// implements the separate Reclaimer companion.
func NewRemoteCollector(store *Store, config RemoteCollectorConfig) (*RemoteCollector, error) {
	if store == nil {
		return nil, errors.New("balance history store is required for remote GC")
	}
	tiering := store.tiering.Load()
	if tiering == nil || tiering.reclaimer == nil || tiering.archiveIdentity == "" {
		return nil, balancehistoryarchive.ErrReclamationUnsupported
	}
	if tiering.reclaimer.Namespace() == "" {
		return nil, errors.New("balance history archive reclaimer namespace is required")
	}
	if config.GracePeriod < 0 {
		return nil, fmt.Errorf("balance history remote GC grace period must not be negative: %s", config.GracePeriod)
	}
	if config.GracePeriod == 0 {
		config.GracePeriod = DefaultRemoteGCGracePeriod
	}
	if config.Now == nil {
		config.Now = time.Now
	}

	return &RemoteCollector{
		store:       store,
		gracePeriod: config.GracePeriod,
		now:         config.Now,
	}, nil
}

// RegisterMetrics exposes this collector's label-free inventory, queue,
// safety, and remote-operation signals.
func (c *RemoteCollector) RegisterMetrics(meter metric.Meter) (metric.Registration, error) {
	if c == nil {
		return nil, errors.New("balance history remote collector is required")
	}

	return c.metrics.register(meter)
}

// Collect performs one durable list/synchronize/recheck/delete/ack cycle.
// No remote operation runs while Store.mutationMu is held.
func (c *RemoteCollector) Collect(ctx context.Context, budget RemoteGCBudget) (RemoteGCResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	budget, err := normalizeRemoteGCBudget(budget)
	if err != nil {
		return RemoteGCResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return RemoteGCResult{}, err
	}

	tiering := c.store.tiering.Load()
	if tiering == nil || tiering.reclaimer == nil || tiering.archiveIdentity == "" {
		return RemoteGCResult{}, balancehistoryarchive.ErrReclamationUnsupported
	}
	namespace := tiering.reclaimer.Namespace()
	if namespace == "" {
		return RemoteGCResult{}, errors.New("balance history archive reclaimer namespace is required")
	}

	state, err := c.prepareState(namespace, tiering.archiveIdentity)
	if err != nil {
		return RemoteGCResult{}, err
	}

	result, now, scanRestarted, err := c.inventoryPage(
		ctx,
		budget,
		tiering,
		namespace,
		state,
	)
	if err != nil {
		return result, err
	}
	if c.hooks.afterInventoryPage != nil {
		c.hooks.afterInventoryPage()
	}
	if scanRestarted {
		// A Store-authorized archive mutation raced the remote page. syncPage
		// durably discarded the stale cursor and pinned a fresh scan epoch, so
		// this bounded call completed safely and the next call restarts at the
		// beginning of the namespace.
		return result, nil
	}

	if budget.DeleteLimit == 0 {
		return result, nil
	}

	// The writer gate freezes upload-before-CAS and reconfiguration windows.
	// Capture every root once before any remote deletion so an unreadable active
	// manifest fails the entire call closed rather than permitting a prefix of
	// the queue to be deleted.
	if c.hooks.beforeArchiveGate != nil {
		c.hooks.beforeArchiveGate()
	}
	tiering.archiveGate.Lock()
	result, operationErr := c.collectCandidatesWithArchiveGateHeld(
		ctx,
		budget,
		tiering,
		namespace,
		now,
		result,
	)
	refreshErr := c.refreshResult(&result, namespace, tiering.archiveIdentity, now)
	tiering.archiveGate.Unlock()
	if c.hooks.afterArchiveGatePhase != nil {
		c.hooks.afterArchiveGatePhase()
	}
	if operationErr != nil {
		return result, operationErr
	}

	return result, refreshErr
}

// collectCandidatesWithArchiveGateHeld requires archiveGate for writing. The
// caller refreshes the durable result before releasing that gate, preserving
// the lock order archiveGate -> mutationMu without defer-order ambiguity.
func (c *RemoteCollector) collectCandidatesWithArchiveGateHeld(
	ctx context.Context,
	budget RemoteGCBudget,
	tiering *tieringState,
	namespace string,
	now time.Time,
	result RemoteGCResult,
) (RemoteGCResult, error) {
	if c.store.tiering.Load() != tiering {
		return result, errTieringReconfigured
	}
	var state remoteGCState

	c.store.mutationMu.Lock()
	c.store.leaseMu.Lock()
	roots, rootsErr := captureRemoteGCRoots(c.store, tiering.archiveIdentity)
	c.store.leaseMu.Unlock()
	if rootsErr != nil {
		c.store.mutationMu.Unlock()
		if errors.Is(rootsErr, ErrRemoteGCRootsUnavailable) {
			c.metrics.activeViewBlockedCycles.Add(1)
		}

		return result, rootsErr
	}

	eligible, err := readEligibleRemoteGCCandidates(
		c.store.db,
		namespace,
		tiering.archiveIdentity,
		now,
		c.gracePeriod,
		budget.DeleteLimit,
	)
	if err != nil {
		c.store.mutationMu.Unlock()

		return result, err
	}
	deletions := make([]remoteGCDeletion, 0, len(eligible))
	rooted := make([]remoteGCDeletion, 0)
	for _, candidate := range eligible {
		if _, ok := roots[candidate.digest]; ok {
			rooted = append(rooted, candidate)

			continue
		}
		deletions = append(deletions, candidate)
	}
	if len(rooted) > 0 {
		state, err = ackRemoteGCCandidates(c.store.db, namespace, tiering.archiveIdentity, rooted)
		if err != nil {
			c.store.mutationMu.Unlock()

			return result, err
		}
		result.RetiredRooted = uint64(len(rooted))
		result.QueueObjects = state.QueueObjects
		result.QueueBytes = state.QueueBytes
		c.observeState(state, now)
	}
	c.store.mutationMu.Unlock()

	for _, deletion := range deletions {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		// Delete is idempotent. A crash after this call and before the Sync ack
		// leaves the candidate queued, so the next process safely retries it.
		deleteStarted := time.Now()
		deleteErr := tiering.reclaimer.Delete(ctx, deletion.digest)
		c.metrics.recordDelete(ctx, time.Since(deleteStarted), deleteErr)
		if deleteErr != nil {
			return result, deleteErr
		}
		if c.hooks.afterDeleteBeforeAck != nil {
			if err := c.hooks.afterDeleteBeforeAck(deletion.digest); err != nil {
				return result, err
			}
		}

		c.store.mutationMu.Lock()
		state, err = ackRemoteGCCandidates(
			c.store.db,
			namespace,
			tiering.archiveIdentity,
			[]remoteGCDeletion{deletion},
		)
		c.store.mutationMu.Unlock()
		if err != nil {
			return result, err
		}
		result.DeletedObjects++
		result.DeletedBytes += deletion.candidate.Size
		result.QueueObjects = state.QueueObjects
		result.QueueBytes = state.QueueBytes
		c.metrics.deletedObjects.Add(1)
		c.metrics.deletedBytes.Add(metricInt64(deletion.candidate.Size))
		c.observeState(state, now)
	}

	return result, nil
}

func (c *RemoteCollector) inventoryPage(
	ctx context.Context,
	budget RemoteGCBudget,
	tiering *tieringState,
	namespace string,
	state remoteGCState,
) (RemoteGCResult, time.Time, bool, error) {
	// List and its durable page synchronization hold the archive writer gate.
	// Tier holds the same gate for reading across its pre-I/O epoch advance and
	// upload. Therefore an inventory page can never certify the interval after
	// an epoch advance but before its corresponding remote object creation.
	// Remote I/O remains outside mutationMu.
	if c.hooks.beforeInventoryGate != nil {
		c.hooks.beforeInventoryGate()
	}
	tiering.archiveGate.Lock()
	defer tiering.archiveGate.Unlock()
	if c.store.tiering.Load() != tiering {
		return RemoteGCResult{}, time.Time{}, false, errTieringReconfigured
	}

	listStarted := time.Now()
	page, err := tiering.reclaimer.List(ctx, state.Cursor, budget.ScanLimit)
	c.metrics.recordList(ctx, time.Since(listStarted), err)
	if err != nil {
		return RemoteGCResult{}, time.Time{}, false, err
	}
	if c.hooks.afterListBeforeSync != nil {
		if err := c.hooks.afterListBeforeSync(); err != nil {
			return RemoteGCResult{}, time.Time{}, false, err
		}
	}
	if c.store.tiering.Load() != tiering {
		return RemoteGCResult{}, time.Time{}, false, errTieringReconfigured
	}

	now := c.now().UTC()
	state, scannedObjects, scannedBytes, cycleCompleted, syncErr := c.syncPage(
		namespace,
		tiering.archiveIdentity,
		state.Cursor,
		page,
		now,
	)
	if syncErr != nil && !errors.Is(syncErr, errRemoteGCScanEpochChanged) {
		return RemoteGCResult{}, now, false, syncErr
	}
	if cycleCompleted {
		c.metrics.lastCompletedInventory.Store(now.Unix())
	}
	result := RemoteGCResult{
		ScannedObjects: scannedObjects,
		ScannedBytes:   scannedBytes,
		QueueObjects:   state.QueueObjects,
		QueueBytes:     state.QueueBytes,
		CycleCompleted: cycleCompleted,
	}
	c.observeState(state, now)
	refreshErr := c.refreshResult(&result, namespace, tiering.archiveIdentity, now)
	if refreshErr != nil {
		return result, now, errors.Is(syncErr, errRemoteGCScanEpochChanged), refreshErr
	}

	return result, now, errors.Is(syncErr, errRemoteGCScanEpochChanged), nil
}

func (c *RemoteCollector) refreshResult(
	result *RemoteGCResult,
	namespace string,
	destinationIdentity string,
	now time.Time,
) error {
	c.store.mutationMu.Lock()
	defer c.store.mutationMu.Unlock()

	state, found, err := readRemoteGCState(c.store.db)
	if err != nil {
		return err
	}
	if !found || state.Namespace != namespace || state.DestinationIdentity != destinationIdentity {
		return errors.New("balance history remote GC state changed before result refresh")
	}
	result.QueueObjects = state.QueueObjects
	result.QueueBytes = state.QueueBytes
	c.observeState(state, now)

	return nil
}

func normalizeRemoteGCBudget(budget RemoteGCBudget) (RemoteGCBudget, error) {
	if budget.ScanLimit < 0 || budget.DeleteLimit < 0 {
		return RemoteGCBudget{}, fmt.Errorf(
			"balance history remote GC budget must not be negative: scan=%d delete=%d",
			budget.ScanLimit,
			budget.DeleteLimit,
		)
	}
	if budget.ScanLimit == 0 {
		budget.ScanLimit = DefaultRemoteGCScanLimit
	}
	if budget.DeleteLimit == 0 {
		budget.DeleteLimit = DefaultRemoteGCDeleteLimit
	}

	return budget, nil
}

func (c *RemoteCollector) prepareState(namespace, destinationIdentity string) (remoteGCState, error) {
	c.store.mutationMu.Lock()
	defer c.store.mutationMu.Unlock()
	binding, bindingFound, err := readArchiveBindingRecord(c.store.db)
	if err != nil {
		return remoteGCState{}, err
	}
	if !bindingFound || binding.DestinationIdentity != destinationIdentity {
		return remoteGCState{}, errors.New("balance history remote GC has no matching archive binding")
	}

	state, found, err := readRemoteGCState(c.store.db)
	if err != nil {
		return remoteGCState{}, err
	}
	if found && state.Namespace == namespace && state.DestinationIdentity == destinationIdentity {
		if state.ScanEpoch == binding.MutationEpoch {
			return state, nil
		}
		state.ScanEpoch = binding.MutationEpoch
		state.Cursor = ""
		state.ScanObjects = 0
		state.ScanBytes = 0
		if err := persistRemoteGCState(c.store.db, state, pebble.Sync); err != nil {
			return remoteGCState{}, fmt.Errorf("restarting balance history remote GC scan after archive mutation: %w", err)
		}

		return state, nil
	}

	state = initialRemoteGCState(namespace, destinationIdentity, binding.MutationEpoch)
	encoded, err := json.Marshal(state)
	if err != nil {
		return remoteGCState{}, fmt.Errorf("marshaling balance history remote GC state: %w", err)
	}
	batch := c.store.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.DeleteRange(
		[]byte{prefixRemoteGCCandidate},
		[]byte{prefixRemoteGCCandidate + 1},
		nil,
	); err != nil {
		return remoteGCState{}, fmt.Errorf("staging stale balance history remote GC candidate reset: %w", err)
	}
	if err := batch.Set(remoteGCStateKey(), encoded, nil); err != nil {
		return remoteGCState{}, fmt.Errorf("staging balance history remote GC namespace: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return remoteGCState{}, fmt.Errorf("committing balance history remote GC namespace: %w", err)
	}

	return state, nil
}

func (c *RemoteCollector) syncPage(
	namespace string,
	destinationIdentity string,
	expectedCursor string,
	page balancehistoryarchive.RemoteObjectPage,
	now time.Time,
) (remoteGCState, uint64, uint64, bool, error) {
	c.store.mutationMu.Lock()
	defer c.store.mutationMu.Unlock()

	state, found, err := readRemoteGCState(c.store.db)
	if err != nil {
		return remoteGCState{}, 0, 0, false, err
	}
	if !found || state.Namespace != namespace || state.DestinationIdentity != destinationIdentity || state.Cursor != expectedCursor {
		return remoteGCState{}, 0, 0, false, errors.New("balance history remote GC cursor changed concurrently")
	}
	binding, bindingFound, err := readArchiveBindingRecord(c.store.db)
	if err != nil {
		return remoteGCState{}, 0, 0, false, err
	}
	if !bindingFound || binding.DestinationIdentity != destinationIdentity {
		return remoteGCState{}, 0, 0, false, errors.New("balance history remote GC archive binding changed during listing")
	}
	if state.ScanEpoch != binding.MutationEpoch {
		state.ScanEpoch = binding.MutationEpoch
		state.Cursor = ""
		state.ScanObjects = 0
		state.ScanBytes = 0
		if err := persistRemoteGCState(c.store.db, state, pebble.Sync); err != nil {
			return remoteGCState{}, 0, 0, false, err
		}

		return state, 0, 0, false, errRemoteGCScanEpochChanged
	}
	if page.NextCursor != "" && page.NextCursor == expectedCursor {
		return remoteGCState{}, 0, 0, false, errors.New("balance history remote GC listing did not advance its cursor")
	}

	batch := c.store.db.NewBatch()
	defer func() { _ = batch.Close() }()
	observed := make(map[[32]byte]remoteGCCandidate, len(page.Objects))
	var pageObjects, pageBytes uint64
	for _, object := range page.Objects {
		if object.SHA256 == ([32]byte{}) {
			return remoteGCState{}, 0, 0, false, errors.New("balance history remote GC listed an empty digest")
		}
		if object.Size < 0 {
			return remoteGCState{}, 0, 0, false, fmt.Errorf(
				"balance history remote GC listed negative object size for %x: %d",
				object.SHA256,
				object.Size,
			)
		}
		if _, duplicate := observed[object.SHA256]; duplicate {
			continue
		}
		size := uint64(object.Size)
		candidate, candidateFound, err := readRemoteGCCandidate(c.store.db, object.SHA256)
		if err != nil {
			return remoteGCState{}, 0, 0, false, err
		}
		if !candidateFound || candidate.Namespace != namespace || candidate.DestinationIdentity != destinationIdentity {
			candidate = remoteGCCandidate{
				FormatVersion:         remoteGCFormatVersion,
				Namespace:             namespace,
				DestinationIdentity:   destinationIdentity,
				Size:                  size,
				FirstObservedUnixNano: now.UnixNano(),
				FirstObservedCycle:    state.Cycle,
				LastObservedCycle:     state.Cycle,
			}
			state.QueueObjects++
			if err := addRemoteGCBytes(&state.QueueBytes, size); err != nil {
				return remoteGCState{}, 0, 0, false, err
			}
			if state.OldestObservedUnixNano == 0 || candidate.FirstObservedUnixNano < state.OldestObservedUnixNano {
				state.OldestObservedUnixNano = candidate.FirstObservedUnixNano
			}
		} else {
			if candidate.Size > state.QueueBytes {
				return remoteGCState{}, 0, 0, false, errors.New("balance history remote GC queue byte accounting underflow")
			}
			state.QueueBytes -= candidate.Size
			if err := addRemoteGCBytes(&state.QueueBytes, size); err != nil {
				return remoteGCState{}, 0, 0, false, err
			}
			candidate.Size = size
			candidate.LastObservedCycle = state.Cycle
		}
		observed[object.SHA256] = candidate
		encoded, err := json.Marshal(candidate)
		if err != nil {
			return remoteGCState{}, 0, 0, false, fmt.Errorf("marshaling balance history remote GC candidate: %w", err)
		}
		if err := batch.Set(remoteGCCandidateKey(object.SHA256), encoded, nil); err != nil {
			return remoteGCState{}, 0, 0, false, fmt.Errorf("staging balance history remote GC candidate: %w", err)
		}
		pageObjects++
		if err := addRemoteGCBytes(&pageBytes, size); err != nil {
			return remoteGCState{}, 0, 0, false, err
		}
	}

	if err := addRemoteGCBytes(&state.ScanObjects, pageObjects); err != nil {
		return remoteGCState{}, 0, 0, false, err
	}
	if err := addRemoteGCBytes(&state.ScanBytes, pageBytes); err != nil {
		return remoteGCState{}, 0, 0, false, err
	}
	state.Cursor = page.NextCursor
	cycleCompleted := page.NextCursor == ""
	if cycleCompleted {
		if err := pruneUnobservedRemoteGCCandidates(c.store.db, batch, &state, observed); err != nil {
			return remoteGCState{}, 0, 0, false, err
		}
		state.InventoryObjects = state.ScanObjects
		state.InventoryBytes = state.ScanBytes
		state.CompletedInventoryEpoch = state.ScanEpoch
		state.ScanObjects = 0
		state.ScanBytes = 0
		state.Cycle++
		if state.Cycle == 0 {
			return remoteGCState{}, 0, 0, false, errors.New("balance history remote GC cycle overflow")
		}
	}

	encodedState, err := json.Marshal(state)
	if err != nil {
		return remoteGCState{}, 0, 0, false, fmt.Errorf("marshaling balance history remote GC state: %w", err)
	}
	if err := batch.Set(remoteGCStateKey(), encodedState, nil); err != nil {
		return remoteGCState{}, 0, 0, false, fmt.Errorf("staging balance history remote GC cursor: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return remoteGCState{}, 0, 0, false, fmt.Errorf("committing balance history remote GC page: %w", err)
	}

	return state, pageObjects, pageBytes, cycleCompleted, nil
}

func pruneUnobservedRemoteGCCandidates(
	db *pebble.DB,
	batch *pebble.Batch,
	state *remoteGCState,
	observed map[[32]byte]remoteGCCandidate,
) error {
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{prefixRemoteGCCandidate},
		UpperBound: []byte{prefixRemoteGCCandidate + 1},
	})
	if err != nil {
		return fmt.Errorf("opening balance history remote GC prune iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var oldest int64
	for _, candidate := range observed {
		oldest = earlierUnixNano(oldest, candidate.FirstObservedUnixNano)
	}
	for valid := iter.First(); valid; valid = iter.Next() {
		digest, err := decodeRemoteGCCandidateKey(iter.Key())
		if err != nil {
			return err
		}
		if _, justObserved := observed[digest]; justObserved {
			continue
		}
		candidate, err := decodeRemoteGCCandidate(iter.Value())
		if err != nil {
			return err
		}
		if candidate.Namespace == state.Namespace &&
			(candidate.LastObservedCycle >= state.Cycle || candidate.LastObservedCycle > candidate.FirstObservedCycle) {
			oldest = earlierUnixNano(oldest, candidate.FirstObservedUnixNano)

			continue
		}
		if err := decrementRemoteGCQueue(state, candidate); err != nil {
			return err
		}
		if err := batch.Delete(remoteGCCandidateKey(digest), nil); err != nil {
			return fmt.Errorf("staging vanished balance history remote GC candidate deletion: %w", err)
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterating balance history remote GC candidates for pruning: %w", err)
	}
	state.OldestObservedUnixNano = oldest

	return nil
}

func readEligibleRemoteGCCandidates(
	db *pebble.DB,
	namespace string,
	destinationIdentity string,
	now time.Time,
	gracePeriod time.Duration,
	limit int,
) ([]remoteGCDeletion, error) {
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{prefixRemoteGCCandidate},
		UpperBound: []byte{prefixRemoteGCCandidate + 1},
	})
	if err != nil {
		return nil, fmt.Errorf("opening balance history remote GC candidate iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	eligible := make([]remoteGCDeletion, 0, limit)
	for valid := iter.First(); valid && len(eligible) < limit; valid = iter.Next() {
		digest, err := decodeRemoteGCCandidateKey(iter.Key())
		if err != nil {
			return nil, err
		}
		candidate, err := decodeRemoteGCCandidate(iter.Value())
		if err != nil {
			return nil, err
		}
		if candidate.Namespace != namespace || candidate.DestinationIdentity != destinationIdentity ||
			candidate.LastObservedCycle <= candidate.FirstObservedCycle {
			continue
		}
		firstObserved := time.Unix(0, candidate.FirstObservedUnixNano)
		if now.Before(firstObserved) || now.Sub(firstObserved) < gracePeriod {
			continue
		}
		eligible = append(eligible, remoteGCDeletion{digest: digest, candidate: candidate})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating balance history remote GC candidates: %w", err)
	}

	return eligible, nil
}

// captureRemoteGCRoots requires mutationMu and leaseMu. The caller also holds
// the archive gate for writing, establishing the global lock order:
// archiveGate -> mutationMu -> leaseMu.
func captureRemoteGCRoots(store *Store, destinationIdentity string) (map[[32]byte]struct{}, error) {
	latest, err := readManifest(store.db)
	if err != nil {
		return nil, fmt.Errorf("reading latest balance history remote GC root: %w", err)
	}
	roots := make(map[[32]byte]struct{})
	addManifestRemoteGCRoots(roots, latest)
	if len(roots) > 0 {
		binding, found, err := readArchiveBinding(store.db)
		if err != nil {
			return nil, err
		}
		if !found || binding != destinationIdentity {
			return nil, fmt.Errorf(
				"balance history remote GC archive binding is %q, configured %q",
				binding,
				destinationIdentity,
			)
		}
	}
	for version, leases := range store.manifestLeases {
		if leases == 0 {
			continue
		}

		// Manifest lease identity is version-only. Reset can reuse a version,
		// making it impossible to distinguish an old snapshot's vN from a new
		// live vN (ABA) without changing Store's CRITICAL lease structure. Block
		// all deletion while any View is active; once it closes, a later call
		// rechecks the latest manifest and resumes safely.
		return nil, fmt.Errorf("%w: active manifest %d has %d leases", ErrRemoteGCRootsUnavailable, version, leases)
	}

	return roots, nil
}

func addManifestRemoteGCRoots(roots map[[32]byte]struct{}, manifest Manifest) {
	for _, run := range manifest.Runs {
		for _, part := range run.ArchiveParts {
			roots[part.Ref.SHA256] = struct{}{}
		}
	}
}

func ackRemoteGCCandidates(
	db *pebble.DB,
	namespace string,
	destinationIdentity string,
	candidates []remoteGCDeletion,
) (remoteGCState, error) {
	state, found, err := readRemoteGCState(db)
	if err != nil {
		return remoteGCState{}, err
	}
	if !found || state.Namespace != namespace || state.DestinationIdentity != destinationIdentity {
		return remoteGCState{}, errors.New("balance history remote GC state changed before acknowledgement")
	}
	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()
	for _, deletion := range candidates {
		current, found, err := readRemoteGCCandidate(db, deletion.digest)
		if err != nil {
			return remoteGCState{}, err
		}
		if !found {
			continue
		}
		if current.Namespace != namespace || current.DestinationIdentity != destinationIdentity {
			return remoteGCState{}, errors.New("balance history remote GC candidate destination changed before acknowledgement")
		}
		if err := decrementRemoteGCQueue(&state, current); err != nil {
			return remoteGCState{}, err
		}
		if err := batch.Delete(remoteGCCandidateKey(deletion.digest), nil); err != nil {
			return remoteGCState{}, fmt.Errorf("staging balance history remote GC acknowledgement: %w", err)
		}
	}
	if state.QueueObjects == 0 {
		state.OldestObservedUnixNano = 0
	} else if candidateSetContainsFirstObserved(candidates, state.OldestObservedUnixNano) {
		oldest, err := oldestRemoteGCCandidate(db, candidates)
		if err != nil {
			return remoteGCState{}, err
		}
		state.OldestObservedUnixNano = oldest
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		return remoteGCState{}, fmt.Errorf("marshaling acknowledged balance history remote GC state: %w", err)
	}
	if err := batch.Set(remoteGCStateKey(), encoded, nil); err != nil {
		return remoteGCState{}, fmt.Errorf("staging acknowledged balance history remote GC state: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return remoteGCState{}, fmt.Errorf("committing balance history remote GC acknowledgement: %w", err)
	}

	return state, nil
}

func decrementRemoteGCQueue(state *remoteGCState, candidate remoteGCCandidate) error {
	if state.QueueObjects == 0 || candidate.Size > state.QueueBytes {
		return errors.New("balance history remote GC queue accounting underflow")
	}
	state.QueueObjects--
	state.QueueBytes -= candidate.Size

	return nil
}

func earlierUnixNano(current, candidate int64) int64 {
	if current == 0 || candidate < current {
		return candidate
	}

	return current
}

func candidateSetContainsFirstObserved(candidates []remoteGCDeletion, firstObserved int64) bool {
	for _, deletion := range candidates {
		if deletion.candidate.FirstObservedUnixNano == firstObserved {
			return true
		}
	}

	return false
}

func oldestRemoteGCCandidate(db *pebble.DB, excluded []remoteGCDeletion) (int64, error) {
	excludedDigests := make(map[[32]byte]struct{}, len(excluded))
	for _, deletion := range excluded {
		excludedDigests[deletion.digest] = struct{}{}
	}
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{prefixRemoteGCCandidate},
		UpperBound: []byte{prefixRemoteGCCandidate + 1},
	})
	if err != nil {
		return 0, fmt.Errorf("opening balance history remote GC oldest-candidate iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var oldest int64
	for valid := iter.First(); valid; valid = iter.Next() {
		digest, err := decodeRemoteGCCandidateKey(iter.Key())
		if err != nil {
			return 0, err
		}
		if _, skip := excludedDigests[digest]; skip {
			continue
		}
		candidate, err := decodeRemoteGCCandidate(iter.Value())
		if err != nil {
			return 0, err
		}
		oldest = earlierUnixNano(oldest, candidate.FirstObservedUnixNano)
	}
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("iterating balance history remote GC oldest candidate: %w", err)
	}
	if oldest == 0 {
		return 0, errors.New("balance history remote GC queue accounting has no oldest candidate")
	}

	return oldest, nil
}

func readRemoteGCState(reader pebbleValueGetter) (remoteGCState, bool, error) {
	encoded, closer, err := reader.Get(remoteGCStateKey())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return remoteGCState{}, false, nil
		}

		return remoteGCState{}, false, fmt.Errorf("reading balance history remote GC state: %w", err)
	}
	copyEncoded := append([]byte(nil), encoded...)
	if err := closer.Close(); err != nil {
		return remoteGCState{}, false, fmt.Errorf("closing balance history remote GC state: %w", err)
	}
	var state remoteGCState
	if err := json.Unmarshal(copyEncoded, &state); err != nil {
		return remoteGCState{}, false, fmt.Errorf("decoding balance history remote GC state: %w", err)
	}
	if state.FormatVersion != remoteGCFormatVersion || state.Namespace == "" ||
		state.DestinationIdentity == "" || state.ScanEpoch == 0 || state.Cycle == 0 {
		return remoteGCState{}, false, errors.New("invalid balance history remote GC state")
	}
	if (state.QueueObjects == 0 && (state.QueueBytes != 0 || state.OldestObservedUnixNano != 0)) ||
		(state.QueueObjects > 0 && state.OldestObservedUnixNano == 0) {
		return remoteGCState{}, false, errors.New("invalid balance history remote GC queue accounting")
	}

	return state, true, nil
}

func initialRemoteGCState(namespace, destinationIdentity string, scanEpoch uint64) remoteGCState {
	return remoteGCState{
		FormatVersion:       remoteGCFormatVersion,
		Namespace:           namespace,
		DestinationIdentity: destinationIdentity,
		ScanEpoch:           scanEpoch,
		Cycle:               1,
	}
}

func persistRemoteGCState(db *pebble.DB, state remoteGCState, writeOptions *pebble.WriteOptions) error {
	encoded, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshaling balance history remote GC state: %w", err)
	}
	if err := db.Set(remoteGCStateKey(), encoded, writeOptions); err != nil {
		return fmt.Errorf("persisting balance history remote GC state: %w", err)
	}

	return nil
}

func readRemoteGCCandidate(reader pebbleValueGetter, digest [32]byte) (remoteGCCandidate, bool, error) {
	encoded, closer, err := reader.Get(remoteGCCandidateKey(digest))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return remoteGCCandidate{}, false, nil
		}

		return remoteGCCandidate{}, false, fmt.Errorf("reading balance history remote GC candidate: %w", err)
	}
	copyEncoded := append([]byte(nil), encoded...)
	if err := closer.Close(); err != nil {
		return remoteGCCandidate{}, false, fmt.Errorf("closing balance history remote GC candidate: %w", err)
	}
	candidate, err := decodeRemoteGCCandidate(copyEncoded)
	if err != nil {
		return remoteGCCandidate{}, false, err
	}

	return candidate, true, nil
}

func decodeRemoteGCCandidate(encoded []byte) (remoteGCCandidate, error) {
	var candidate remoteGCCandidate
	if err := json.Unmarshal(encoded, &candidate); err != nil {
		return remoteGCCandidate{}, fmt.Errorf("decoding balance history remote GC candidate: %w", err)
	}
	if candidate.FormatVersion != remoteGCFormatVersion || candidate.Namespace == "" || candidate.DestinationIdentity == "" ||
		candidate.FirstObservedUnixNano == 0 || candidate.FirstObservedCycle == 0 ||
		candidate.LastObservedCycle < candidate.FirstObservedCycle {
		return remoteGCCandidate{}, errors.New("invalid balance history remote GC candidate")
	}

	return candidate, nil
}

func decodeRemoteGCCandidateKey(key []byte) ([32]byte, error) {
	if len(key) != 33 || key[0] != prefixRemoteGCCandidate {
		return [32]byte{}, errors.New("invalid balance history remote GC candidate key")
	}
	var digest [32]byte
	copy(digest[:], key[1:])

	return digest, nil
}

func addRemoteGCBytes(total *uint64, increment uint64) error {
	if increment > math.MaxUint64-*total {
		return errors.New("balance history remote GC byte accounting overflow")
	}
	*total += increment

	return nil
}

func (c *RemoteCollector) observeState(state remoteGCState, now time.Time) {
	c.metrics.queueObjects.Store(metricInt64(state.QueueObjects))
	c.metrics.queueBytes.Store(metricInt64(state.QueueBytes))
	oldestAge := int64(0)
	if state.OldestObservedUnixNano != 0 {
		oldest := time.Unix(0, state.OldestObservedUnixNano)
		if now.After(oldest) {
			oldestAge = int64(now.Sub(oldest) / time.Second)
		}
	}
	c.metrics.oldestQueuedAge.Store(oldestAge)
	c.metrics.inventoryObjects.Store(metricInt64(state.InventoryObjects))
	c.metrics.inventoryBytes.Store(metricInt64(state.InventoryBytes))
}

func metricInt64(value uint64) int64 {
	if value > math.MaxInt64 {
		return math.MaxInt64
	}

	return int64(value)
}
