package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

//go:generate mockgen -source=store.go -destination=store_mock_test.go -package=processing -mock_names=InMemoryStore=MockInMemoryStore

// InMemoryStore is the interface used by RequestProcessor to access data.
// It abstracts the underlying storage mechanism (e.g., Buffered).
type InMemoryStore interface {
	// Ledger operations
	GetLedger(name string) (*commonpb.LedgerInfo, bool)
	PutLedger(name string, info *commonpb.LedgerInfo)

	// Boundaries operations
	GetBoundaries(ledger string) (*raftcmdpb.LedgerBoundaries, bool)
	PutBoundaries(ledger string, boundaries *raftcmdpb.LedgerBoundaries)

	// Volume operations (merged Input+Output)
	GetVolume(key domain.VolumeKey) (*raftcmdpb.VolumePair, error)
	PutVolume(key domain.VolumeKey, value *raftcmdpb.VolumePair)

	// Account metadata operations
	GetAccountMetadata(key domain.MetadataKey) (*commonpb.MetadataValue, error)
	PutAccountMetadata(key domain.MetadataKey, value *commonpb.MetadataValue)
	DeleteAccountMetadata(key domain.MetadataKey)

	// Transaction reversion status operations
	GetReverted(key domain.TransactionKey) (bool, error)
	PutReverted(key domain.TransactionKey, reverted bool)

	// Idempotency key operations
	GetIdempotencyKey(key domain.IdempotencyKey) (*commonpb.IdempotencyKeyValue, error)
	PutIdempotencyKey(key domain.IdempotencyKey, value *commonpb.IdempotencyKeyValue)

	// Transaction reference operations
	GetTransactionReference(key domain.TransactionReferenceKey) (*commonpb.TransactionReferenceValue, error)
	PutTransactionReference(key domain.TransactionReferenceKey, value *commonpb.TransactionReferenceValue)

	// Transaction updates
	AddTransactionUpdate(key domain.TransactionKey, update *commonpb.TransactionUpdate)

	// Signing key operations
	AddSigningKey(keyID string, publicKey []byte, parentKeyID string)
	RemoveSigningKey(keyID string)
	GetSigningKeyChildren(keyID string) []string
	SetRequireSignatures(require bool)

	// Maintenance mode operations
	SetMaintenanceMode(enabled bool)

	// Audit config operations
	SetAuditEnabled(enabled bool)

	// Period schedule operations
	SetPeriodSchedule(cron string)
	DeletePeriodSchedule()

	// Events sink operations
	GetSinkConfig(name string) (*commonpb.SinkConfig, error)
	AddSinkConfig(config *commonpb.SinkConfig)
	RemoveSinkConfig(name string)

	// Log hash chaining
	GetLastLogHash() []byte
	SetLastLogHash(hash []byte)

	// Counters and timestamps
	GetNextSequenceID() uint64
	IncrementNextSequenceID() uint64
	GetDate() *commonpb.Timestamp

	// Period operations
	GetCurrentOpenPeriod() (*commonpb.Period, bool)
	GetClosingPeriod() (*commonpb.Period, bool)
	SetCurrentOpenPeriod(period *commonpb.Period)
	SetClosingPeriod(period *commonpb.Period)
	ClearClosingPeriod()
	GetNextPeriodID() uint64
	IncrementNextPeriodID() uint64

	// Archive period operations
	GetPeriodByID(periodID uint64) (*commonpb.Period, bool)
	UpdatePeriod(period *commonpb.Period)
	SetPurgeRange(periodID, startSequence, closeSequence uint64)
	SetPendingArchive(periodID, startSequence, closeSequence uint64)

	// Metadata conversion requests
	AddMetadataConvertRequest(ledgerName string, targetType commonpb.TargetType, key string, metadataType commonpb.MetadataType)

	// Prepared query operations
	GetPreparedQuery(ledger, name string) (*commonpb.PreparedQuery, error)
	PutPreparedQuery(pq *commonpb.PreparedQuery)
	DeletePreparedQuery(ledger, name string)

	// Numscript library operations
	GetNumscriptLatestVersion(name string) (string, error)
	NumscriptVersionExists(name, version string) (bool, error)
	PutNumscript(info *commonpb.NumscriptInfo)
	DeleteNumscriptLatest(name string)
}
