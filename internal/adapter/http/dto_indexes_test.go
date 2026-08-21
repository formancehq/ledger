package http

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestNewIndexDTO_MetadataOneof(t *testing.T) {
	t.Parallel()

	src := &commonpb.Index{
		Id: &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{
			Metadata: &commonpb.MetadataIndexID{
				Target: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:    "color",
			},
		}},
		BuildStatus:            commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY,
		Ledger:                 "main",
		ForwardEncodingVersion: 1,
	}

	got := newIndexDTO(src)

	require.NotNil(t, got.ID)
	require.NotNil(t, got.ID.Metadata)
	// Target 0 is TARGET_TYPE_ACCOUNT, a real value: it must be present.
	require.Equal(t, "TARGET_TYPE_ACCOUNT", got.ID.Metadata.Target)
	require.Equal(t, "color", got.ID.Metadata.Key)
	require.Equal(t, "metadata:TARGET_TYPE_ACCOUNT:color", got.CanonicalID)
	require.Equal(t, "INDEX_BUILD_STATUS_READY", got.BuildStatus)
	require.Equal(t, "main", got.Ledger)
	require.Equal(t, uint32(1), got.ForwardEncodingVersion)
	require.Nil(t, got.CreatedAt)
	require.Nil(t, got.ID.TxBuiltin)
}

// The oneof switch in newIndexIDDTO has no default, so a fifth variant would
// silently render `"id": {}` with nothing failing to compile. golangci-lint's
// `exhaustive` linter does not cover type switches over oneof wrappers. This
// table pins the rendered shape of all four variants — each case asserts the
// other three fields stay nil — but being hand-written it cannot notice a FIFTH
// variant appearing in the proto. TestNewIndexIDDTO_KindOneofExhaustive in
// dto_oneof_exhaustiveness_test.go is the descriptor-driven gate for that.
func TestNewIndexIDDTO_AllOneofVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		id                 *commonpb.IndexID
		wantTxBuiltin      *string
		wantLogBuiltin     *string
		wantAccountBuiltin *string
		wantMetadata       *metadataIndexIDDTO
		wantCanonicalID    string
	}{
		{
			name: "txBuiltin",
			id: &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{
				TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP,
			}},
			wantTxBuiltin:   new("TX_BUILTIN_INDEX_TIMESTAMP"),
			wantCanonicalID: "tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP",
		},
		{
			name: "logBuiltin",
			id: &commonpb.IndexID{Kind: &commonpb.IndexID_LogBuiltin{
				LogBuiltin: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE,
			}},
			wantLogBuiltin:  new("LOG_BUILTIN_INDEX_DATE"),
			wantCanonicalID: "log_builtin:LOG_BUILTIN_INDEX_DATE",
		},
		{
			name: "accountBuiltin",
			id: &commonpb.IndexID{Kind: &commonpb.IndexID_AccountBuiltin{
				AccountBuiltin: commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET,
			}},
			wantAccountBuiltin: new("ACCT_BUILTIN_INDEX_ASSET"),
			wantCanonicalID:    "account_builtin:ACCT_BUILTIN_INDEX_ASSET",
		},
		{
			name: "metadata",
			id: &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{
				Metadata: &commonpb.MetadataIndexID{
					Target: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
					Key:    "color",
				},
			}},
			wantMetadata: &metadataIndexIDDTO{
				Target: "TARGET_TYPE_TRANSACTION",
				Key:    "color",
			},
			wantCanonicalID: "metadata:TARGET_TYPE_TRANSACTION:color",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := newIndexDTO(&commonpb.Index{Id: tc.id})

			require.NotNil(t, got.ID)
			require.Equal(t, tc.wantTxBuiltin, got.ID.TxBuiltin)
			require.Equal(t, tc.wantLogBuiltin, got.ID.LogBuiltin)
			require.Equal(t, tc.wantAccountBuiltin, got.ID.AccountBuiltin)
			require.Equal(t, tc.wantMetadata, got.ID.Metadata)
			require.Equal(t, tc.wantCanonicalID, got.CanonicalID)
		})
	}
}

// TX_BUILTIN_INDEX_REFERENCE is enum value 0. protojson emits a set oneof
// member even at zero, and so must the DTO — otherwise the variant vanishes
// and the index becomes unidentifiable.
func TestNewIndexDTO_ZeroValuedOneofVariantIsEmitted(t *testing.T) {
	t.Parallel()

	src := &commonpb.Index{
		Id: &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{
			TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE,
		}},
	}

	got := newIndexDTO(src)

	require.NotNil(t, got.ID)
	require.NotNil(t, got.ID.TxBuiltin)
	require.Equal(t, "TX_BUILTIN_INDEX_REFERENCE", *got.ID.TxBuiltin)
	require.Equal(t, "tx_builtin:TX_BUILTIN_INDEX_REFERENCE", got.CanonicalID)
}

func TestNewIndexDTO_CreatedAtIsRFC3339(t *testing.T) {
	t.Parallel()

	src := &commonpb.Index{
		CreatedAt: &commonpb.Timestamp{Data: 1786540255458491},
	}

	got := newIndexDTO(src)

	require.NotNil(t, got.CreatedAt)
	// RFC3339Nano, never the protojson {"data":"<micros>"} wrapper. The exact
	// string pins the instant, the microsecond precision, and UTC normalisation.
	require.Equal(t, "2026-08-12T13:10:55.458491Z", *got.CreatedAt)
}

func TestNewIndexDTO_Nil(t *testing.T) {
	t.Parallel()

	require.Nil(t, newIndexDTO(nil))
}

func TestNewIndexDTOList_EmptyIsAllocatedAndNilsAreSkipped(t *testing.T) {
	t.Parallel()

	// A nil slice would marshal as null; the converter must allocate.
	require.NotNil(t, newIndexDTOList(nil))
	require.Empty(t, newIndexDTOList(nil))

	got := newIndexDTOList([]*commonpb.Index{
		nil,
		{Ledger: "main"},
	})

	require.Len(t, got, 1)
	require.Equal(t, "main", got[0].Ledger)
}

func TestNewIndexStatusDTO_MeaningfulZerosArePresent(t *testing.T) {
	t.Parallel()

	got := newIndexStatusDTO(&servicepb.GetIndexStatusResponse{})

	require.NotNil(t, got.Indexes, "must be an allocated empty slice so it marshals as [] not null")
	require.Empty(t, got.Indexes)
	require.Equal(t, uint64(0), got.Lag)
}

// CurrentVersion and PendingVersion are adjacent uint32 with near-identical
// names, so every field carries a distinct value here: a transposition in
// newIndexEntryDTO compiles, passes go vet, and would only show up as a wrong
// number on the wire.
func TestNewIndexStatusDTO_EntryFieldsAreMappedIndividually(t *testing.T) {
	t.Parallel()

	got := newIndexStatusDTO(&servicepb.GetIndexStatusResponse{
		LastIndexedSequence: 11, LastLogSequence: 13, Lag: 2, IndexFileSize: 4096,
		Indexes: []*servicepb.IndexEntry{{
			Ledger: "main", Cursor: 7, CurrentVersion: 2, PendingVersion: 3,
			Index: &commonpb.Index{Ledger: "main"},
		}},
	})

	require.Equal(t, uint64(11), got.LastIndexedSequence)
	require.Equal(t, uint64(13), got.LastLogSequence)
	require.Equal(t, uint64(2), got.Lag)
	require.Equal(t, uint64(4096), got.IndexFileSize)
	require.Len(t, got.Indexes, 1)
	require.Equal(t, "main", got.Indexes[0].Ledger)
	require.Equal(t, uint64(7), got.Indexes[0].Cursor)
	require.Equal(t, uint32(2), got.Indexes[0].CurrentVersion)
	require.Equal(t, uint32(3), got.Indexes[0].PendingVersion)
	require.NotNil(t, got.Indexes[0].Index)
}
