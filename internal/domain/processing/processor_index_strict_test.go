package processing

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestProcessCreateIndex_RejectsExistingWithoutMutation(t *testing.T) {
	t.Parallel()

	for _, id := range []*commonpb.IndexID{
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
		indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "score"),
	} {
		t.Run(indexes.Canonical(id), func(t *testing.T) {
			t.Parallel()

			scope := NewMockScope(gomock.NewController(t))
			// A divergent projection name must not redirect the existence probe.
			info := &commonpb.LedgerInfo{Name: "other-ledger", MetadataSchema: &commonpb.MetadataSchema{
				AccountFields: map[string]*commonpb.MetadataFieldSchema{"score": {Type: commonpb.MetadataType_METADATA_TYPE_INT64}},
			}}
			expectGetLedger(scope, domain.LedgerKey{Name: "ledger"}, info.AsReader(), nil)
			original := &commonpb.Index{Id: id, Ledger: "ledger", CreatedAt: &commonpb.Timestamp{Data: 42}, ForwardEncodingVersion: 3}
			before := original.CloneVT()
			stub := setupIndexesStub(scope)
			reads := 0
			stub.getHook = func(key domain.IndexKey) (commonpb.IndexReader, error) {
				reads++
				require.Equal(t, indexes.KeyFor("ledger", id), key)

				return original.AsReader(), nil
			}
			stub.putHook = func(domain.IndexKey, *commonpb.Index) { t.Fatal("duplicate creation must not write the registry") }

			payload, err := processCreateIndex("ledger", &raftcmdpb.CreateIndexOrder{Id: id}, &Context{Scope: scope})
			require.Nil(t, payload, "duplicate must not emit CreatedIndexLog")
			var duplicate *domain.ErrIndexAlreadyExists
			require.ErrorAs(t, err, &duplicate)
			require.Equal(t, indexes.Canonical(id), duplicate.Index)
			require.Equal(t, domain.ErrReasonIndexAlreadyExists, err.Reason())
			require.Equal(t, 1, reads)
			require.True(t, before.EqualVT(original), "duplicate must preserve timestamp and forward encoding version")
		})
	}
}

func TestProcessCreateIndex_PropagatesRegistryReadFailure(t *testing.T) {
	t.Parallel()

	fault := errors.New("index registry read failed")
	for _, tc := range []struct {
		name   string
		err    error
		reason string
	}{
		{name: "coverage", err: coverageMissDescribable{}, reason: domain.ErrReasonCoverageMiss},
		{name: "storage", err: fault, reason: domain.ErrReasonStorageOperation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			scope := NewMockScope(gomock.NewController(t))
			expectGetLedger(scope, domain.LedgerKey{Name: "ledger"}, (&commonpb.LedgerInfo{Name: "ledger"}).AsReader(), nil)
			id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
			stub := setupIndexesStub(scope)
			stub.expectGet(indexes.KeyFor("ledger", id), nil, tc.err)
			stub.putHook = func(domain.IndexKey, *commonpb.Index) { t.Fatal("failed read must not create an index") }

			payload, err := processCreateIndex("ledger", &raftcmdpb.CreateIndexOrder{Id: id}, &Context{Scope: scope})
			require.Nil(t, payload)
			require.NotNil(t, err)
			require.Equal(t, tc.reason, err.Reason())
			if tc.name == "coverage" {
				require.Equal(t, tc.err, err, "coverage identity must propagate verbatim")
			} else {
				require.ErrorIs(t, err, fault)
			}
		})
	}
}
