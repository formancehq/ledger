package processing

import (
	"math/big"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
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
	require.Equal(t, "unknown", warnings[0].Address)
	require.Equal(t, "invalid", warnings[1].Address)
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
	require.Equal(t, "unknown", warnings[0].Address)
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
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true)
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any()).Do(
		func(_ string, info *commonpb.LedgerInfo) {
			require.NotNil(t, info.ChartOfAccounts)
			require.Contains(t, info.ChartOfAccounts.Roots, "bank")
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
	setLog := applyLog.Log.Data.GetSetChartOfAccounts()
	require.NotNil(t, setLog)
	require.NotNil(t, setLog.ChartOfAccounts)
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
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true)

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
	mockStore.EXPECT().GetLedger("missing").Return(nil, false)

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
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true)
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any()).Do(
		func(_ string, info *commonpb.LedgerInfo) {
			require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, info.EnforcementMode)
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
	modeLog := applyLog.Log.Data.GetSetChartEnforcementMode()
	require.NotNil(t, modeLog)
	require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, modeLog.EnforcementMode)
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
	mockStore.EXPECT().GetLedger("missing").Return(nil, false)

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
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true)

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
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true)
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
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true)
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
	savedMeta := applyLog.Log.Data.GetSavedMetadata()
	require.NotNil(t, savedMeta)
	require.Len(t, savedMeta.Warnings, 1)
	require.Equal(t, "invalid:account", savedMeta.Warnings[0].Address)
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
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(gomock.Any(), gomock.Any())
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().GetVolume(gomock.Any()).Return(nil, dal.ErrNotFound).AnyTimes()
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
	created := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, created)
	require.Len(t, created.Warnings, 1)
	require.Equal(t, "invalid:account", created.Warnings[0].Address)
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
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, true)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(gomock.Any(), gomock.Any())
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().GetVolume(gomock.Any()).Return(nil, dal.ErrNotFound).AnyTimes()
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
	created := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, created)
	require.Empty(t, created.Warnings)
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
			require.NotNil(t, info.ChartOfAccounts)
			require.Contains(t, info.ChartOfAccounts.Roots, "bank")
			require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, info.EnforcementMode)
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
	require.NotNil(t, createLog.Info.ChartOfAccounts)
	require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, createLog.Info.EnforcementMode)
}
