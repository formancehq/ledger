package processing

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func TestValidateChart(t *testing.T) {
	t.Parallel()

	t.Run("NilChart", func(t *testing.T) {
		t.Parallel()

		err := validateChart(nil)
		require.Error(t, err)
	})

	t.Run("EmptyRoots", func(t *testing.T) {
		t.Parallel()

		err := validateChart(&commonpb.ChartOfAccounts{Roots: map[string]*commonpb.ChartSegment{}})
		require.Error(t, err)
	})

	t.Run("InvalidSegmentName", func(t *testing.T) {
		t.Parallel()

		err := validateChart(&commonpb.ChartOfAccounts{
			Roots: map[string]*commonpb.ChartSegment{
				"invalid name!": {Account: true},
			},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid segment name")
	})

	t.Run("NoAccountNodes", func(t *testing.T) {
		t.Parallel()

		err := validateChart(&commonpb.ChartOfAccounts{
			Roots: map[string]*commonpb.ChartSegment{
				"users": {Account: false},
			},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "account: true")
	})

	t.Run("ValidSimpleChart", func(t *testing.T) {
		t.Parallel()

		err := validateChart(&commonpb.ChartOfAccounts{
			Roots: map[string]*commonpb.ChartSegment{
				"users": {Account: true},
			},
		})
		require.NoError(t, err)
	})

	t.Run("InvalidVariableEmptyName", func(t *testing.T) {
		t.Parallel()

		err := validateChart(&commonpb.ChartOfAccounts{
			Roots: map[string]*commonpb.ChartSegment{
				"users": {
					Variable: &commonpb.ChartVariable{
						Name:    "",
						Account: true,
					},
				},
			},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "variable name must be non-empty")
	})

	t.Run("InvalidVariablePattern", func(t *testing.T) {
		t.Parallel()

		err := validateChart(&commonpb.ChartOfAccounts{
			Roots: map[string]*commonpb.ChartSegment{
				"users": {
					Variable: &commonpb.ChartVariable{
						Name:    "id",
						Pattern: "[invalid",
						Account: true,
					},
				},
			},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid pattern")
	})

	t.Run("ValidChartWithVariable", func(t *testing.T) {
		t.Parallel()

		err := validateChart(&commonpb.ChartOfAccounts{
			Roots: map[string]*commonpb.ChartSegment{
				"users": {
					Variable: &commonpb.ChartVariable{
						Name:    "userId",
						Pattern: `^\d+$`,
						Account: true,
					},
				},
			},
		})
		require.NoError(t, err)
	})
}

func TestValidateAccountInChart(t *testing.T) {
	t.Parallel()

	chart := &commonpb.ChartOfAccounts{
		Roots: map[string]*commonpb.ChartSegment{
			"users": {
				Account: false,
				Variable: &commonpb.ChartVariable{
					Name:    "userId",
					Pattern: `^\d+$`,
					Account: true,
					Children: map[string]*commonpb.ChartSegment{
						"wallet": {Account: true},
					},
				},
			},
			"bank": {
				Account: true,
				Children: map[string]*commonpb.ChartSegment{
					"main": {Account: true},
				},
			},
		},
	}

	tests := []struct {
		name    string
		address string
		valid   bool
	}{
		{"WorldAlwaysValid", "world", true},
		{"FixedRootAccount", "bank", true},
		{"FixedChildAccount", "bank:main", true},
		{"VariableAccount", "users:123", true},
		{"VariableWithChild", "users:456:wallet", true},
		{"InvalidPatternMismatch", "users:abc", false},
		{"UnknownRoot", "unknown", false},
		{"IncompletePathNotAccount", "users", false},
		{"TooDeepPath", "bank:main:extra", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := validateAccountInChart(tt.address, chart)
			require.Equal(t, tt.valid, result, "address: %s", tt.address)
		})
	}
}

func TestValidatePostingsInChart_Strict(t *testing.T) {
	t.Parallel()

	chart := &commonpb.ChartOfAccounts{
		Roots: map[string]*commonpb.ChartSegment{
			"bank": {Account: true},
		},
	}

	t.Run("ValidPostings", func(t *testing.T) {
		t.Parallel()

		postings := []*commonpb.Posting{
			{Source: "world", Destination: "bank"},
		}
		warnings, err := validatePostingsInChart(postings, chart, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT)
		require.NoError(t, err)
		require.Empty(t, warnings)
	})

	t.Run("InvalidSource", func(t *testing.T) {
		t.Parallel()

		postings := []*commonpb.Posting{
			{Source: "unknown", Destination: "bank"},
		}
		_, err := validatePostingsInChart(postings, chart, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT)
		require.Error(t, err)

		var chartErr *domain.ErrAccountNotInChart
		require.ErrorAs(t, err, &chartErr)
		require.Equal(t, "unknown", chartErr.Address)
	})

	t.Run("InvalidDestination", func(t *testing.T) {
		t.Parallel()

		postings := []*commonpb.Posting{
			{Source: "world", Destination: "invalid"},
		}
		_, err := validatePostingsInChart(postings, chart, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT)
		require.Error(t, err)

		var chartErr *domain.ErrAccountNotInChart
		require.ErrorAs(t, err, &chartErr)
		require.Equal(t, "invalid", chartErr.Address)
	})
}

func TestValidatePostingsInChart_Audit(t *testing.T) {
	t.Parallel()

	chart := &commonpb.ChartOfAccounts{
		Roots: map[string]*commonpb.ChartSegment{
			"bank": {Account: true},
		},
	}

	postings := []*commonpb.Posting{
		{Source: "unknown", Destination: "invalid"},
	}
	warnings, err := validatePostingsInChart(postings, chart, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT)
	require.NoError(t, err)
	require.Len(t, warnings, 2)
	require.Equal(t, "unknown", warnings[0].GetAddress())
	require.Equal(t, "invalid", warnings[1].GetAddress())
}

func TestValidatePostingsInChart_Audit_Deduplication(t *testing.T) {
	t.Parallel()

	chart := &commonpb.ChartOfAccounts{
		Roots: map[string]*commonpb.ChartSegment{
			"bank": {Account: true},
		},
	}

	// Same invalid address appears as both source and destination
	postings := []*commonpb.Posting{
		{Source: "unknown", Destination: "unknown"},
	}
	warnings, err := validatePostingsInChart(postings, chart, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT)
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	require.Equal(t, "unknown", warnings[0].GetAddress())
}

func TestValidatePostingsInChart_NilChart(t *testing.T) {
	t.Parallel()

	postings := []*commonpb.Posting{
		{Source: "anything", Destination: "anywhere"},
	}
	warnings, err := validatePostingsInChart(postings, nil, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT)
	require.NoError(t, err)
	require.Empty(t, warnings)
}

func TestProcessSetChartOfAccounts(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	ledgerInfo := &commonpb.LedgerInfo{Name: "test-ledger"}

	chart := &commonpb.ChartOfAccounts{
		Roots: map[string]*commonpb.ChartSegment{
			"bank": {Account: true},
		},
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true).AnyTimes()
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any()).Do(
		func(_ string, info *commonpb.LedgerInfo) {
			require.NotNil(t, info.GetChartOfAccounts())
			require.Contains(t, info.GetChartOfAccounts().GetRoots(), "bank")
		},
	)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_SetChartOfAccounts{
					SetChartOfAccounts: &raftcmdpb.SetChartOfAccountsOrder{
						ChartOfAccounts: chart,
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)
	setLog := applyLog.GetLog().GetData().GetSetChartOfAccounts()
	require.NotNil(t, setLog)
	require.NotNil(t, setLog.GetChartOfAccounts())
}

func TestProcessSetChartOfAccounts_InvalidChart(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	ledgerInfo := &commonpb.LedgerInfo{Name: "test-ledger"}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true).AnyTimes()

	// Chart with no account nodes
	chart := &commonpb.ChartOfAccounts{
		Roots: map[string]*commonpb.ChartSegment{
			"bank": {Account: false},
		},
	}

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_SetChartOfAccounts{
					SetChartOfAccounts: &raftcmdpb.SetChartOfAccountsOrder{
						ChartOfAccounts: chart,
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var invalidChart *domain.ErrInvalidChart
	require.ErrorAs(t, err, &invalidChart)
}

func TestProcessSetChartOfAccounts_LedgerNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	mockStore.EXPECT().GetBoundaries("missing").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("missing").Return(nil, false).AnyTimes()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "missing",
				Data: &raftcmdpb.LedgerApplyOrder_SetChartOfAccounts{
					SetChartOfAccounts: &raftcmdpb.SetChartOfAccountsOrder{
						ChartOfAccounts: &commonpb.ChartOfAccounts{
							Roots: map[string]*commonpb.ChartSegment{
								"bank": {Account: true},
							},
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var ledgerNotFound *domain.ErrLedgerNotFound
	require.ErrorAs(t, err, &ledgerNotFound)
}

func TestProcessSetChartEnforcementMode(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	ledgerInfo := &commonpb.LedgerInfo{Name: "test-ledger"}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true).AnyTimes()
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any()).Do(
		func(_ string, info *commonpb.LedgerInfo) {
			require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, info.GetEnforcementMode())
		},
	)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_SetChartEnforcementMode{
					SetChartEnforcementMode: &raftcmdpb.SetChartEnforcementModeOrder{
						EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)
	modeLog := applyLog.GetLog().GetData().GetSetChartEnforcementMode()
	require.NotNil(t, modeLog)
	require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, modeLog.GetEnforcementMode())
}

func TestProcessSetChartEnforcementMode_LedgerNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	mockStore.EXPECT().GetBoundaries("missing").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("missing").Return(nil, false).AnyTimes()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "missing",
				Data: &raftcmdpb.LedgerApplyOrder_SetChartEnforcementMode{
					SetChartEnforcementMode: &raftcmdpb.SetChartEnforcementModeOrder{
						EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var ledgerNotFound *domain.ErrLedgerNotFound
	require.ErrorAs(t, err, &ledgerNotFound)
}

func TestProcessAddMetadata_ChartValidation_Strict(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	chart := &commonpb.ChartOfAccounts{
		Roots: map[string]*commonpb.ChartSegment{
			"bank": {Account: true},
		},
	}
	ledgerInfo := &commonpb.LedgerInfo{
		Name:            "test-ledger",
		ChartOfAccounts: chart,
		EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true).AnyTimes()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
					AddMetadata: &raftcmdpb.SaveMetadataOrder{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "invalid:account"},
							},
						},
						Metadata: &commonpb.MetadataSet{
							Metadata: []*commonpb.Metadata{
								{Key: "role", Value: commonpb.NewStringValue("admin")},
							},
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var chartErr *domain.ErrAccountNotInChart
	require.ErrorAs(t, err, &chartErr)
	require.Equal(t, "invalid:account", chartErr.Address)
}

func TestProcessAddMetadata_ChartValidation_ValidAccount(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	chart := &commonpb.ChartOfAccounts{
		Roots: map[string]*commonpb.ChartSegment{
			"bank": {Account: true},
		},
	}
	ledgerInfo := &commonpb.LedgerInfo{
		Name:            "test-ledger",
		ChartOfAccounts: chart,
		EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().PutAccountMetadata(gomock.Any(), gomock.Any())

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
					AddMetadata: &raftcmdpb.SaveMetadataOrder{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "bank"},
							},
						},
						Metadata: &commonpb.MetadataSet{
							Metadata: []*commonpb.Metadata{
								{Key: "role", Value: commonpb.NewStringValue("main")},
							},
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestProcessAddMetadata_ChartValidation_Audit(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	chart := &commonpb.ChartOfAccounts{
		Roots: map[string]*commonpb.ChartSegment{
			"bank": {Account: true},
		},
	}
	ledgerInfo := &commonpb.LedgerInfo{
		Name:            "test-ledger",
		ChartOfAccounts: chart,
		EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().PutAccountMetadata(gomock.Any(), gomock.Any())

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
					AddMetadata: &raftcmdpb.SaveMetadataOrder{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "invalid:account"},
							},
						},
						Metadata: &commonpb.MetadataSet{
							Metadata: []*commonpb.Metadata{
								{Key: "role", Value: commonpb.NewStringValue("admin")},
							},
						},
					},
				},
			},
		},
	}

	// In AUDIT mode, invalid account should still succeed with warnings
	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)
	savedMeta := applyLog.GetLog().GetData().GetSavedMetadata()
	require.NotNil(t, savedMeta)
	require.Len(t, savedMeta.GetWarnings(), 1)
	require.Equal(t, "invalid:account", savedMeta.GetWarnings()[0].GetAddress())
}

func TestProcessCreateTransaction_ChartValidation_Audit_Warnings(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	chart := &commonpb.ChartOfAccounts{
		Roots: map[string]*commonpb.ChartSegment{
			"bank": {Account: true},
		},
	}
	ledgerInfo := &commonpb.LedgerInfo{
		Name:            "test-ledger",
		ChartOfAccounts: chart,
		EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(gomock.Any(), gomock.Any())
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().GetVolume(gomock.Any()).Return(nil, domain.ErrNotFound).AnyTimes()
	mockStore.EXPECT().PutVolume(gomock.Any(), gomock.Any()).AnyTimes()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
					CreateTransaction: &raftcmdpb.CreateTransactionOrder{
						Postings: []*commonpb.Posting{
							commonpb.NewPosting("world", "invalid:account", "USD", big.NewInt(100)),
						},
						Force: true,
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)
	created := applyLog.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, created)
	require.Len(t, created.GetWarnings(), 1)
	require.Equal(t, "invalid:account", created.GetWarnings()[0].GetAddress())
}

func TestProcessCreateTransaction_ChartValidation_NoWarnings(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	chart := &commonpb.ChartOfAccounts{
		Roots: map[string]*commonpb.ChartSegment{
			"bank": {Account: true},
		},
	}
	ledgerInfo := &commonpb.LedgerInfo{
		Name:            "test-ledger",
		ChartOfAccounts: chart,
		EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(gomock.Any(), gomock.Any())
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().GetVolume(gomock.Any()).Return(nil, domain.ErrNotFound).AnyTimes()
	mockStore.EXPECT().PutVolume(gomock.Any(), gomock.Any()).AnyTimes()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "test-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
					CreateTransaction: &raftcmdpb.CreateTransactionOrder{
						Postings: []*commonpb.Posting{
							commonpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
						},
						Force: true,
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)
	created := applyLog.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, created)
	require.Empty(t, created.GetWarnings())
}

func TestProcessCreateLedger_WithChart(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	chart := &commonpb.ChartOfAccounts{
		Roots: map[string]*commonpb.ChartSegment{
			"bank": {Account: true},
		},
	}

	mockStore.EXPECT().GetLedger("chart-ledger").Return(nil, false)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutLedger("chart-ledger", gomock.Any()).Do(
		func(_ string, info *commonpb.LedgerInfo) {
			require.NotNil(t, info.GetChartOfAccounts())
			require.Contains(t, info.GetChartOfAccounts().GetRoots(), "bank")
			require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, info.GetEnforcementMode())
		},
	)
	mockStore.EXPECT().PutBoundaries("chart-ledger", gomock.Any())

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_CreateLedger{
			CreateLedger: &raftcmdpb.CreateLedgerOrder{
				Name:            "chart-ledger",
				ChartOfAccounts: chart,
				EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	createLog := result.GetCreateLedger()
	require.NotNil(t, createLog)
	require.NotNil(t, createLog.GetInfo().GetChartOfAccounts())
	require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, createLog.GetInfo().GetEnforcementMode())
}
