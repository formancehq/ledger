package ledger

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	ledger "github.com/formancehq/ledger/internal"
)

func newMetadataLimitTestController(t *testing.T) (*DefaultController, *MockStore, *MockNumscriptParser, *MockNumscriptRuntime) {
	t.Helper()

	mockController := gomock.NewController(t)
	store := NewMockStore(mockController)
	parser := NewMockNumscriptParser(mockController)
	runtime := NewMockNumscriptRuntime(mockController)
	controller := NewDefaultController(
		ledger.Ledger{},
		store,
		parser,
		NewMockNumscriptParser(mockController),
		NewMockNumscriptParser(mockController),
	)
	return controller, store, parser, runtime
}

func TestCreateTransactionRejectsNumscriptMetadataOverLimit(t *testing.T) {
	t.Parallel()

	controller, store, parser, runtime := newMetadataLimitTestController(t)
	parser.EXPECT().Parse("").Return(runtime, nil)
	runtime.EXPECT().Execute(gomock.Any(), store, nil).Return(&NumscriptExecutionResult{
		Postings: ledger.Postings{
			ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
		},
		Metadata: metadata.Metadata{
			"key": strings.Repeat("v", ledger.MaxMetadataValueSize+1),
		},
	}, nil)

	_, err := controller.createTransaction(context.Background(), store, nil, Parameters[CreateTransaction]{
		Input: CreateTransaction{},
	})
	require.ErrorIs(t, err, ledger.ErrMetadataLimitExceeded{})
}

func TestCreateTransactionRejectsMergedAccountMetadataOverLimit(t *testing.T) {
	t.Parallel()

	controller, store, parser, runtime := newMetadataLimitTestController(t)
	parser.EXPECT().Parse("").Return(runtime, nil)
	runtime.EXPECT().Execute(gomock.Any(), store, nil).Return(&NumscriptExecutionResult{
		Postings: ledger.Postings{
			ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
		},
	}, nil)
	store.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().UpsertAccounts(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, accounts ...ledger.AccountWithDefaultMetadata) error {
			accounts[0].Metadata = metadata.Metadata{
				"existing": strings.Repeat("v", ledger.MaxMetadataValueSize+1),
			}
			return nil
		},
	)

	_, err := controller.createTransaction(context.Background(), store, nil, Parameters[CreateTransaction]{
		Input: CreateTransaction{},
	})
	require.ErrorIs(t, err, ledger.ErrMetadataLimitExceeded{})
}

func TestRevertTransactionRejectsMetadataOverLimit(t *testing.T) {
	t.Parallel()

	controller, store, _, _ := newMetadataLimitTestController(t)
	tx := &ledger.Transaction{
		ID:         pointer.For(uint64(1)),
		RevertedAt: pointer.For(time.Now()),
	}
	store.EXPECT().RevertTransaction(gomock.Any(), uint64(1), time.Time{}).Return(tx, true, nil)
	store.EXPECT().GetBalances(gomock.Any(), gomock.Any()).Return(map[string]map[string]*big.Int{}, nil)

	_, err := controller.revertTransaction(context.Background(), store, nil, Parameters[RevertTransaction]{
		Input: RevertTransaction{
			TransactionID: 1,
			Metadata: metadata.Metadata{
				"key": strings.Repeat("v", ledger.MaxMetadataValueSize+1),
			},
		},
	})
	require.ErrorIs(t, err, ledger.ErrMetadataLimitExceeded{})
}

func TestSaveTransactionMetadataRejectsMergedMetadataOverLimit(t *testing.T) {
	t.Parallel()

	controller, store, _, _ := newMetadataLimitTestController(t)
	patch := metadata.Metadata{"key": "value"}
	store.EXPECT().UpdateTransactionMetadata(gomock.Any(), uint64(1), patch, time.Time{}).Return(&ledger.Transaction{
		TransactionData: ledger.TransactionData{
			Metadata: metadata.Metadata{
				"existing": strings.Repeat("v", ledger.MaxMetadataValueSize+1),
			},
		},
	}, true, nil)

	_, err := controller.saveTransactionMetadata(context.Background(), store, nil, Parameters[SaveTransactionMetadata]{
		Input: SaveTransactionMetadata{
			TransactionID: 1,
			Metadata:      patch,
		},
	})
	require.ErrorIs(t, err, ledger.ErrMetadataLimitExceeded{})
}

func TestSaveAccountMetadataRejectsMergedMetadataOverLimit(t *testing.T) {
	t.Parallel()

	controller, store, _, _ := newMetadataLimitTestController(t)
	patch := metadata.Metadata{"key": "value"}
	store.EXPECT().UpsertAccounts(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, accounts ...ledger.AccountWithDefaultMetadata) error {
			accounts[0].Metadata = metadata.Metadata{
				"existing": strings.Repeat("v", ledger.MaxMetadataValueSize+1),
			}
			return nil
		},
	)

	_, err := controller.saveAccountMetadata(context.Background(), store, nil, Parameters[SaveAccountMetadata]{
		Input: SaveAccountMetadata{
			Address:  "users:001",
			Metadata: patch,
		},
	})
	require.ErrorIs(t, err, ledger.ErrMetadataLimitExceeded{})
}

func TestImportAllowsHistoricalMetadataOverLimit(t *testing.T) {
	t.Parallel()

	controller, store, _, _ := newMetadataLimitTestController(t)
	tx := ledger.Transaction{
		ID: pointer.For(uint64(1)),
		TransactionData: ledger.TransactionData{
			Metadata: metadata.Metadata{
				"key": strings.Repeat("v", ledger.MaxMetadataValueSize+1),
			},
		},
	}
	log := ledger.Log{
		ID: pointer.For(uint64(1)),
		Data: ledger.CreatedTransaction{
			Transaction: tx,
		},
	}
	store.EXPECT().CommitTransaction(gomock.Any(), &tx).Return(nil)
	store.EXPECT().UpsertAccounts(gomock.Any()).Return(nil)
	store.EXPECT().InsertLog(gomock.Any(), &log).Return(nil)

	require.NoError(t, controller.importLog(context.Background(), store, log))
}
