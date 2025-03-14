package bulking

import (
	"encoding/json"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/uptrace/bun"
	"math/big"
	"testing"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/time"

	"errors"
	"github.com/formancehq/go-libs/v2/metadata"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestBulk(t *testing.T) {
	t.Parallel()

	now := time.Now()

	type bulkTestCase struct {
		name          string
		bulk          []BulkElement
		expectations  func(mockLedger *LedgerController)
		expectError   bool
		expectResults []BulkElementResult
		options       BulkingOptions
	}

	testCases := []bulkTestCase{
		{
			name: "create transaction",
			bulk: []BulkElement{{
				Action: ActionCreateTransaction,
				Data: TransactionRequest{
					Postings: []ledger.Posting{{
						Source:      "world",
						Destination: "bank",
						Amount:      big.NewInt(100),
						Asset:       "USD/2",
					}},
					Timestamp: now,
				},
			}},
			expectations: func(mockLedger *LedgerController) {
				postings := []ledger.Posting{{
					Source:      "world",
					Destination: "bank",
					Amount:      big.NewInt(100),
					Asset:       "USD/2",
				}}
				mockLedger.EXPECT().
					CreateTransaction(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.RunScript]{
						Input: ledgercontroller.TxToScriptData(ledger.TransactionData{
							Postings:  postings,
							Timestamp: now,
						}, false),
					}).
					Return(&ledger.Log{}, &ledger.CreatedTransaction{
						Transaction: ledger.Transaction{
							TransactionData: ledger.TransactionData{
								Postings:  postings,
								Metadata:  metadata.Metadata{},
								Timestamp: now,
							},
						},
					}, nil)
			},
			expectResults: []BulkElementResult{{
				Data: ledger.Transaction{
					TransactionData: ledger.TransactionData{
						Postings:  []ledger.Posting{{Source: "world", Destination: "bank", Amount: big.NewInt(100), Asset: "USD/2"}},
						Timestamp: now,
						Metadata:  metadata.Metadata{},
					},
				},
				LogID:     1,
				ElementID: 0,
			}},
		},
		{
			name: "add metadata on transaction",
			bulk: []BulkElement{{
				Action: ActionAddMetadata,
				Data: AddMetadataRequest{
					TargetID:   json.RawMessage(`1`),
					TargetType: "TRANSACTION",
					Metadata: metadata.Metadata{
						"foo": "bar",
					},
				},
			}},
			expectations: func(mockLedger *LedgerController) {
				mockLedger.EXPECT().
					SaveTransactionMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveTransactionMetadata]{
						Input: ledgercontroller.SaveTransactionMetadata{
							TransactionID: 1,
							Metadata: metadata.Metadata{
								"foo": "bar",
							},
						},
					}).
					Return(&ledger.Log{}, nil)
			},
			expectResults: []BulkElementResult{{}},
		},
		{
			name: "add metadata on account",
			bulk: []BulkElement{{
				Action: ActionAddMetadata,
				Data: AddMetadataRequest{
					TargetID:   json.RawMessage(`"world"`),
					TargetType: "ACCOUNT",
					Metadata: metadata.Metadata{
						"foo": "bar",
					},
				},
			}},
			expectations: func(mockLedger *LedgerController) {
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
						Input: ledgercontroller.SaveAccountMetadata{
							Address: "world",
							Metadata: metadata.Metadata{
								"foo": "bar",
							},
						},
					}).
					Return(&ledger.Log{}, nil)
			},
			expectResults: []BulkElementResult{{}},
		},
		{
			name: "revert transaction",
			bulk: []BulkElement{{
				Action: ActionRevertTransaction,
				Data: RevertTransactionRequest{
					ID: 1,
				},
			}},
			expectations: func(mockLedger *LedgerController) {
				mockLedger.EXPECT().
					RevertTransaction(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.RevertTransaction]{
						Input: ledgercontroller.RevertTransaction{
							TransactionID: 1,
						},
					}).
					Return(&ledger.Log{}, &ledger.RevertedTransaction{}, nil)
			},
			expectResults: []BulkElementResult{{
				Data: ledger.Transaction{},
			}},
		},
		{
			name: "delete metadata on transaction",
			bulk: []BulkElement{{
				Action: ActionDeleteMetadata,
				Data: DeleteMetadataRequest{
					TargetID:   json.RawMessage(`1`),
					TargetType: "TRANSACTION",
					Key:        "foo",
				},
			}},
			expectations: func(mockLedger *LedgerController) {
				mockLedger.EXPECT().
					DeleteTransactionMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.DeleteTransactionMetadata]{
						Input: ledgercontroller.DeleteTransactionMetadata{
							TransactionID: 1,
							Key:           "foo",
						},
					}).
					Return(&ledger.Log{}, nil)
			},
			expectResults: []BulkElementResult{{}},
		},
		{
			name: "delete metadata on account",
			bulk: []BulkElement{{
				Action: ActionDeleteMetadata,
				Data: DeleteMetadataRequest{
					TargetID:   json.RawMessage(`"world"`),
					TargetType: "ACCOUNT",
					Key:        "foo",
				},
			}},
			expectations: func(mockLedger *LedgerController) {
				mockLedger.EXPECT().
					DeleteAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.DeleteAccountMetadata]{
						Input: ledgercontroller.DeleteAccountMetadata{
							Address: "world",
							Key:     "foo",
						},
					}).
					Return(&ledger.Log{}, nil)
			},
			expectResults: []BulkElementResult{{}},
		},
		{
			name: "error in the middle",
			bulk: []BulkElement{{
				Action: ActionAddMetadata,
				Data: AddMetadataRequest{
					TargetID:   json.RawMessage(`"world"`),
					TargetType: "ACCOUNT",
					Metadata: metadata.Metadata{
						"foo": "bar",
					},
				},
			}, {
				Action: ActionAddMetadata,
				Data: AddMetadataRequest{
					TargetID:   json.RawMessage(`"world"`),
					TargetType: "ACCOUNT",
					Metadata: metadata.Metadata{
						"foo2": "bar2",
					},
				},
			}, {
				Action: ActionAddMetadata,
				Data: AddMetadataRequest{
					TargetID:   json.RawMessage(`"world"`),
					TargetType: "ACCOUNT",
					Metadata: metadata.Metadata{
						"foo3": "bar3",
					},
				},
			}},
			expectations: func(mockLedger *LedgerController) {
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
						Input: ledgercontroller.SaveAccountMetadata{
							Address: "world",
							Metadata: metadata.Metadata{
								"foo": "bar",
							},
						},
					}).
					Return(&ledger.Log{}, nil)
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
						Input: ledgercontroller.SaveAccountMetadata{
							Address: "world",
							Metadata: metadata.Metadata{
								"foo2": "bar2",
							},
						},
					}).
					Return(nil, errors.New("unexpected error"))
			},
			expectResults: []BulkElementResult{{}, {
				Error: errors.New("unexpected error"),
			}, {}},
			expectError: true,
		},
		{
			name: "error in the middle with continue on failure",
			bulk: []BulkElement{{
				Action: ActionAddMetadata,
				Data: AddMetadataRequest{
					TargetID:   json.RawMessage(`"world"`),
					TargetType: "ACCOUNT",
					Metadata: metadata.Metadata{
						"foo": "bar",
					},
				},
			}, {
				Action: ActionAddMetadata,
				Data: AddMetadataRequest{
					TargetID:   json.RawMessage(`"world"`),
					TargetType: "ACCOUNT",
					Metadata: metadata.Metadata{
						"foo2": "bar2",
					},
				},
			}, {
				Action: ActionAddMetadata,
				Data: AddMetadataRequest{
					TargetID:   json.RawMessage(`"world"`),
					TargetType: "ACCOUNT",
					Metadata: metadata.Metadata{
						"foo3": "bar3",
					},
				},
			}},
			options: BulkingOptions{
				ContinueOnFailure: true,
			},
			expectations: func(mockLedger *LedgerController) {
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
						Input: ledgercontroller.SaveAccountMetadata{
							Address: "world",
							Metadata: metadata.Metadata{
								"foo": "bar",
							},
						},
					}).
					Return(&ledger.Log{}, nil)
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
						Input: ledgercontroller.SaveAccountMetadata{
							Address: "world",
							Metadata: metadata.Metadata{
								"foo2": "bar2",
							},
						},
					}).
					Return(nil, errors.New("unexpected error"))
				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
						Input: ledgercontroller.SaveAccountMetadata{
							Address: "world",
							Metadata: metadata.Metadata{
								"foo3": "bar3",
							},
						},
					}).
					Return(&ledger.Log{}, nil)
			},
			expectResults: []BulkElementResult{{}, {
				Error: errors.New("unexpected error"),
			}, {}},
			expectError: true,
		},
		{
			name: "with atomic",
			bulk: []BulkElement{{
				Action: ActionAddMetadata,
				Data: AddMetadataRequest{
					TargetID:   json.RawMessage(`"world"`),
					TargetType: "ACCOUNT",
					Metadata: metadata.Metadata{
						"foo": "bar",
					},
				},
			}, {
				Action: ActionAddMetadata,
				Data: AddMetadataRequest{
					TargetID:   json.RawMessage(`"world"`),
					TargetType: "ACCOUNT",
					Metadata: metadata.Metadata{
						"foo2": "bar2",
					},
				},
			}},
			options: BulkingOptions{
				Atomic: true,
			},
			expectations: func(mockLedger *LedgerController) {
				mockLedger.EXPECT().
					BeginTX(gomock.Any(), nil).
					Return(mockLedger, &bun.Tx{}, nil)

				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
						Input: ledgercontroller.SaveAccountMetadata{
							Address: "world",
							Metadata: metadata.Metadata{
								"foo": "bar",
							},
						},
					}).
					Return(&ledger.Log{}, nil)

				mockLedger.EXPECT().
					SaveAccountMetadata(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]{
						Input: ledgercontroller.SaveAccountMetadata{
							Address: "world",
							Metadata: metadata.Metadata{
								"foo2": "bar2",
							},
						},
					}).
					Return(&ledger.Log{}, nil)

				mockLedger.EXPECT().
					Commit(gomock.Any()).
					Return(nil)
			},
			expectResults: []BulkElementResult{{}, {}},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			ctx := logging.TestingContext()

			ctrl := gomock.NewController(t)
			ledgerController := NewLedgerController(ctrl)

			testCase.expectations(ledgerController)

			bulker := NewBulker(ledgerController)
			bulk := make(Bulk, len(testCase.bulk))
			results := make(chan BulkElementResult, len(testCase.bulk))

			for _, element := range testCase.bulk {
				bulk <- element
			}
			close(bulk)

			require.NoError(t, bulker.Run(ctx, bulk, results, testCase.options))
		})
	}
}
