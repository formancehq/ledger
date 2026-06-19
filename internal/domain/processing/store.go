package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source=store.go -destination=store_generated_test.go -typed -package=processing -mock_names=InMemoryStore=MockInMemoryStore

// InMemoryStore is the interface used by RequestProcessor to access data.
// It abstracts the underlying storage mechanism (e.g., WriteSet).
//
// Cache-backed Get* methods return a Reader view over the cache entry so
// processors cannot accidentally mutate cached state in place. Use
// Reader.Mutate() to obtain a writeable clone before modifying, then write
// the result back through the matching Put* method.
type InMemoryStore interface {
	// Ledger operations
	GetLedger(name string) (commonpb.LedgerInfoReader, bool)
	PutLedger(name string, info *commonpb.LedgerInfo)

	// Boundaries operations
	GetBoundaries(ledger string) (raftcmdpb.LedgerBoundariesReader, bool)
	PutBoundaries(ledger string, boundaries *raftcmdpb.LedgerBoundaries)

	// Volume operations (merged Input+Output)
	GetVolume(key domain.VolumeKey) (raftcmdpb.VolumePairReader, error)
	PutVolume(key domain.VolumeKey, value *raftcmdpb.VolumePair)

	// Account metadata operations
	GetAccountMetadata(key domain.MetadataKey) (commonpb.MetadataValueReader, error)
	PutAccountMetadata(key domain.MetadataKey, value *commonpb.MetadataValue)
	DeleteAccountMetadata(key domain.MetadataKey)

	// Ledger metadata operations
	GetLedgerMetadata(key domain.LedgerMetadataKey) (commonpb.MetadataValueReader, error)
	PutLedgerMetadata(key domain.LedgerMetadataKey, value *commonpb.MetadataValue)
	DeleteLedgerMetadata(key domain.LedgerMetadataKey)

	// Transaction reversion status operations
	GetReverted(key domain.TransactionKey) (bool, error)
	PutReverted(key domain.TransactionKey, reverted bool)

	// Idempotency key operations
	GetIdempotencyKey(key domain.IdempotencyKey) (commonpb.IdempotencyKeyValueReader, error)
	PutIdempotencyKey(key domain.IdempotencyKey, value *commonpb.IdempotencyKeyValue)

	// Transaction reference operations
	GetTransactionReference(key domain.TransactionReferenceKey) (commonpb.TransactionReferenceValueReader, error)
	PutTransactionReference(key domain.TransactionReferenceKey, value *commonpb.TransactionReferenceValue)

	// Transaction state operations
	GetTransactionState(key domain.TransactionKey) (commonpb.TransactionStateReader, error)
	PutTransactionState(key domain.TransactionKey, state *commonpb.TransactionState)

	// Signing key operations
	AddSigningKey(keyID string, publicKey []byte, parentKeyID string)
	RemoveSigningKey(keyID string)
	GetSigningKeyChildren(keyID string) []string
	SetRequireSignatures(require bool)

	// Maintenance mode operations
	SetMaintenanceMode(enabled bool)

	// Period schedule operations
	SetPeriodSchedule(cron string)
	DeletePeriodSchedule()

	// Events sink operations
	GetSinkConfig(name string) (commonpb.SinkConfigReader, error)
	AddSinkConfig(config *commonpb.SinkConfig)
	RemoveSinkConfig(name string)

	// Counters and timestamps
	GetNextSequenceID() uint64
	IncrementNextSequenceID() uint64
	GetNextAuditSequenceID() uint64
	GetNextLedgerID() uint32
	IncrementNextLedgerID() uint32
	GetDate() *commonpb.Timestamp

	// Period operations
	GetCurrentOpenPeriod() (*commonpb.Period, bool)
	GetClosingPeriods() []*commonpb.Period
	GetClosingPeriodByID(periodID uint64) (*commonpb.Period, bool)
	SetCurrentOpenPeriod(period *commonpb.Period)
	AddClosingPeriod(period *commonpb.Period)
	RemoveClosingPeriod(periodID uint64)
	GetNextPeriodID() uint64
	IncrementNextPeriodID() uint64

	// Archive period operations
	GetPeriodByID(periodID uint64) (*commonpb.Period, bool)
	UpdatePeriod(period *commonpb.Period)
	SetPurgeRange(periodID, startSequence, closeSequence, startAuditSequence, closeAuditSequence uint64)
	SetPendingArchive(periodID, startSequence, closeSequence, startAuditSequence, closeAuditSequence uint64)

	// Metadata conversion requests
	AddMetadataConvertRequest(ledgerName string, targetType commonpb.TargetType, key string, metadataType commonpb.MetadataType)

	// Prepared query operations
	GetPreparedQuery(ledgerName string, name string) (commonpb.PreparedQueryReader, error)
	PutPreparedQuery(ledgerName string, pq *commonpb.PreparedQuery)
	DeletePreparedQuery(ledgerName string, name string)

	// Numscript library operations
	GetNumscriptLatestVersion(ledgerName string, name string) (string, error)
	NumscriptVersionExists(ledgerName string, name, version string) (bool, error)
	PutNumscript(ledgerName string, info *commonpb.NumscriptInfo)
	DeleteNumscriptLatest(ledgerName string, name string)

	// Query checkpoint operations
	GetNextQueryCheckpointID() uint64
	IncrementNextQueryCheckpointID() uint64
	SaveQueryCheckpoint(cp *raftcmdpb.QueryCheckpointState)
	DeleteQueryCheckpoint(checkpointID uint64)

	// Query checkpoint schedule operations
	SetQueryCheckpointSchedule(cron string)
	DeleteQueryCheckpointSchedule()

	// Ledger cleanup
	MarkLedgerForCleanup(ledger string)

	// Numscript content resolution
	ResolveNumscriptContent(ledgerName string, name, version string) (commonpb.NumscriptInfoReader, error)
}
