package replay

import (
	"math/big"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// Writer abstracts the state mutations performed during log replay.
// Implementations include the Checker's merge-operator-based replayStore
// and the backup restore's attribute writer.
type Writer interface {
	AddVolumeDelta(canonicalKey []byte, inputDelta, outputDelta *big.Int) error
	GetVolume(canonicalKey []byte) (*raftcmdpb.VolumePair, error)
	DeleteVolume(canonicalKey []byte) error
	MoveVolume(oldKey, newKey []byte) error
	SetMetadata(canonicalKey []byte, value string) error
	DeleteMetadata(canonicalKey []byte) error
	MoveMetadata(oldKey, newKey []byte) error
	CreateTransaction(canonicalKey []byte, seq uint64, timestamp *commonpb.Timestamp, metadata map[string]*commonpb.MetadataValue, postings []*commonpb.Posting, revertsTransaction uint64) error
	SetTransactionReference(ledgerName, reference string, txID uint64) error
	SetRevertedBy(canonicalKey []byte, revertTxID uint64, revertedAt *commonpb.Timestamp) error
	SaveTxMetadata(canonicalKey []byte, metadata map[string]*commonpb.MetadataValue) error
	DeleteTxMetadata(canonicalKey []byte, key string) error
	// Schema declarations are keyed by ledger (they live on LedgerInfo), not by
	// a canonical attribute key.
	SetMetadataFieldType(ledger string, target commonpb.TargetType, key string, fieldType commonpb.MetadataType) error
	RemoveMetadataFieldType(ledger string, target commonpb.TargetType, key string) error
	// Account types also live on LedgerInfo and are keyed by ledger.
	AddAccountType(ledger string, accountType *commonpb.AccountType) error
	RemoveAccountType(ledger string, name string) error
	// The default enforcement mode lives on LedgerInfo as well.
	SetDefaultEnforcementMode(ledger string, mode commonpb.ChartEnforcementMode) error
}
