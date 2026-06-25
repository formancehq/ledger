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
	CreateTransaction(canonicalKey []byte, seq uint64, timestamp *commonpb.Timestamp, metadata map[string]*commonpb.MetadataValue) error
	SetRevertedBy(canonicalKey []byte, revertTxID uint64) error
	SaveTxMetadata(canonicalKey []byte, metadata map[string]*commonpb.MetadataValue) error
	DeleteTxMetadata(canonicalKey []byte, key string) error

	// RecordAccount marks an account's per-account existence marker
	// (SubAttrAccount, EN-1276). Called during replay for every non-system
	// account a transaction touches while its ledger has default-bearing
	// account types — mirroring the FSM apply path so the checker can verify
	// the marker projection and the backup-rebuild can reconstruct it.
	// Idempotent: repeated calls for the same key are a no-op past the first.
	RecordAccount(canonicalKey []byte) error
}
