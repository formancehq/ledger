package state

import (
	"errors"
	"fmt"
	"os"

	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// AuditReplayer executes chain-verified orders against an isolated empty FSM
// state. It is used by the store checker to derive logs and projections from
// the audit chain instead of treating the persisted Log projection as truth.
type AuditReplayer struct {
	machine *Machine
	store   *dal.Store
	dir     string
}

type auditReplayScopeFactory struct{ scope processing.Scope }

type auditReplayScope struct{ *WriteSet }

func (s *auditReplayScope) CheckCoverage(byte, processing.CoverageKey) error { return nil }
func (s *auditReplayScope) ForOrder(_, _ []byte) processing.Scope            { return s }

func (f auditReplayScopeFactory) NewScope(_ []byte) (processing.Scope, error) {
	return f.scope, nil
}

func (f auditReplayScopeFactory) NewProposalScope() (processing.Scope, error) {
	return f.scope, nil
}

type auditReplayNotifier struct{}

func (auditReplayNotifier) NotifyLogsCommitted(uint64) {}
func (auditReplayNotifier) NotifyConfigChanged()       {}

// NewAuditReplayer creates an isolated replay engine. Coverage bits are
// deliberately ignored: they are an admission/performance contract, while the
// checker already has the complete chain-bound orders and must reproduce their
// business effects without consulting the live projections.
func NewAuditReplayer(logger logging.Logger, clusterID string) (*AuditReplayer, error) {
	dir, err := os.MkdirTemp("", "checker-audit-replay-*")
	if err != nil {
		return nil, fmt.Errorf("creating audit replay directory: %w", err)
	}

	meterProvider := noop.NewMeterProvider()
	meter := meterProvider.Meter("checker.audit_replay")
	store, err := dal.NewStore(dir, logger, meter, dal.DefaultConfig())
	if err != nil {
		_ = os.RemoveAll(dir)

		return nil, fmt.Errorf("creating audit replay store: %w", err)
	}

	c, err := cache.New(1000, meter)
	if err != nil {
		_ = store.Close()
		_ = os.RemoveAll(dir)

		return nil, fmt.Errorf("creating audit replay cache: %w", err)
	}

	registry := NewStateRegistry(c, attributes.New())
	machine, err := NewMachine(
		logger,
		registry,
		NewCacheSnapshotter(logger, registry, nil),
		store,
		dal.NewSentinelFactory(store, false),
		meterProvider,
		keystore.NewKeyStore(),
		NewSharedState(),
		auditReplayNotifier{},
		nil,
		clusterID,
		0,
		func(_ *raftpb.Entry, _ *dal.WriteSession) error { return nil },
	)
	if err != nil {
		_ = store.Close()
		_ = os.RemoveAll(dir)

		return nil, fmt.Errorf("creating audit replay machine: %w", err)
	}
	if err := NewRecovery(machine, store).RecoverState(); err != nil {
		_ = store.Close()
		_ = os.RemoveAll(dir)

		return nil, fmt.Errorf("initializing audit replay state: %w", err)
	}

	return &AuditReplayer{machine: machine, store: store, dir: dir}, nil
}

func (r *AuditReplayer) Close() error {
	err := r.store.Close()
	_ = os.RemoveAll(r.dir)

	return err
}

// Replay applies one successful audited proposal and returns the exact logs
// produced by the canonical request processor. Call proposals in audit order.
func (r *AuditReplayer) Replay(at *commonpb.Timestamp, orders []*raftcmdpb.Order) ([]*commonpb.Log, error) {
	// Historical/unit-test stores may start their retained audit range before
	// the first replicated policy entry. Give revision 0 the legacy defaults
	// only when this proposal is not itself installing the real policy; a
	// revision-1 SetClusterPolicy must compare against the pristine revision 0.
	if r.machine.State.ClusterPolicy.GetMetadataMaxKeyBytes() == 0 {
		installsPolicy := false
		for _, order := range orders {
			if order.GetSystemScoped().GetSetClusterPolicy() != nil {
				installsPolicy = true

				break
			}
		}
		if !installsPolicy {
			r.machine.State.UpdateClusterPolicy(&commonpb.ClusterPolicy{
				Revision:                    0,
				QueryCheckpointLimit:        10,
				MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
				MetadataMaxKeyBytes:         domain.DefaultMetadataMaxKeyBytes,
				MetadataMaxValueBytes:       domain.DefaultMetadataMaxValueBytes,
				MetadataMaxEntityBytes:      domain.DefaultMetadataMaxEntityBytes,
				MetadataMaxCommandBytes:     domain.DefaultMetadataMaxCommandBytes,
			})
		}
	}

	buffer := r.machine.writeSet
	buffer.Reset(at)

	// Admission-only technical fields are outside the order's logical identity,
	// and older audit rows predate some of them. Reconstruct a missing revert
	// observation from the audit-derived replay state so the current processor
	// can execute the historical business order.
	for _, order := range orders {
		ls := order.GetLedgerScoped()
		revert := ls.GetApply().GetRevertTransaction()
		if revert == nil || len(order.GetTechnical().GetRevertTargetDigest()) != 0 {
			continue
		}
		stored, err := buffer.TransactionStates().Get(domain.TransactionKey{
			LedgerName: ls.GetLedger(),
			ID:         revert.GetTransactionId(),
		})
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return nil, fmt.Errorf("reading audit replay revert target: %w", err)
		}
		if order.GetTechnical() == nil {
			order.Technical = &raftcmdpb.OrderTechnical{}
		}
		var postings []*commonpb.Posting
		if stored != nil {
			postings = stored.Mutate().GetPostings()
		}
		order.Technical.RevertTargetDigest = domain.RevertTargetDigest(postings, stored != nil)
	}

	scope := &auditReplayScope{WriteSet: buffer}
	result, processErr := r.machine.processor.ProcessOrders(
		orders,
		auditReplayScopeFactory{scope: scope},
		buffer,
	)
	if processErr != nil {
		return nil, fmt.Errorf("processing audited orders: %w", processErr)
	}
	if validateErr := buffer.ValidateTransientVolumes(scope); validateErr != nil {
		return nil, fmt.Errorf("validating audited transient volumes: %w", validateErr)
	}

	batch := r.store.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()

	if err := buffer.Merge(batch, result.Logs); err != nil {
		return nil, fmt.Errorf("merging audited orders: %w", err)
	}
	if err := batch.Commit(); err != nil {
		return nil, fmt.Errorf("committing audited orders: %w", err)
	}

	logs := make([]*commonpb.Log, len(result.CreatedLogs))
	for i, log := range result.CreatedLogs {
		logs[i] = log.CloneVT()
	}

	return logs, nil
}
