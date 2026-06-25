package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// ledgerWithDefaults returns a LedgerInfo whose "users:{id}" account type
// carries default_metadata, so the EN-1276 apply-path gate is active.
// DefaultEnforcementMode is AUDIT so accounts that don't match the pattern
// (e.g. "vendors:acme") are not rejected.
func ledgerWithDefaults() *commonpb.LedgerInfo {
	return &commonpb.LedgerInfo{
		Name:                   "test-ledger",
		Id:                     1,
		DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
		AccountTypes: map[string]*commonpb.AccountType{
			"user": {
				Name:    "user",
				Pattern: "users:{id}",
				DefaultMetadata: map[string]*commonpb.MetadataValue{
					"tier": commonpb.NewStringValue("standard"),
				},
			},
		},
	}
}

// ledgerWithTwoDefaults returns a LedgerInfo whose account type declares two
// default metadata keys (tier and region), used for partial-override tests.
func ledgerWithTwoDefaults() *commonpb.LedgerInfo {
	return &commonpb.LedgerInfo{
		Name:                   "test-ledger",
		Id:                     1,
		DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
		AccountTypes: map[string]*commonpb.AccountType{
			"user": {
				Name:    "user",
				Pattern: "users:{id}",
				DefaultMetadata: map[string]*commonpb.MetadataValue{
					"tier":   commonpb.NewStringValue("standard"),
					"region": commonpb.NewStringValue("eu"),
				},
			},
		},
	}
}

// ledgerWithNoAccountTypeDefaults returns a LedgerInfo whose account type has
// no default_metadata, so the EN-1276 gate stays off.
func ledgerWithNoAccountTypeDefaults() *commonpb.LedgerInfo {
	return &commonpb.LedgerInfo{
		Name:                   "test-ledger",
		Id:                     1,
		DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
		AccountTypes: map[string]*commonpb.AccountType{
			"user": {
				Name:    "user",
				Pattern: "users:{id}",
				// No DefaultMetadata — gate must NOT fire.
			},
		},
	}
}

// expectWorldToAccountPostingMocks wires the volume reads/writes for a
// world -> account posting of `amount` USD. world is the source so the balance
// check is skipped.
func expectWorldToAccountPostingMocks(mockStore *MockScope, account string) {
	worldKey := domain.NewVolumeKey("test-ledger", "world", "USD")
	destKey := domain.NewVolumeKey("test-ledger", account, "USD")
	zero := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetVolume(worldKey).Return(zero.AsReader(), nil)
	mockStore.EXPECT().PutVolume(worldKey, gomock.Any())
	mockStore.EXPECT().GetVolume(destKey).Return(zero.AsReader(), nil)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any())
}

// commonTxMocks wires the calls every processCreateTransaction makes that are
// not specific to the default-metadata behaviour under test.
func commonTxMocks(mockStore *MockScope, info *commonpb.LedgerInfo) {
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(info, nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 1234567890}).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenChapter().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1)).AnyTimes()
	mockStore.EXPECT().PutTransactionState(gomock.Any(), gomock.Any())
}

func worldToAccountRequest(account string, metadata map[string]*commonpb.MetadataMap) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{
							{
								Source:      "world",
								Destination: account,
								Amount:      commonpb.NewUint256FromUint64(100),
								Asset:       "USD",
							},
						},
						AccountMetadata: metadata,
					},
				}},
			},
		},
	}
}

// twoPostingsToSameDestRequest builds a request with two postings that share
// the same non-world destination account.
func twoPostingsToSameDestRequest(account string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{
							{
								Source:      "world",
								Destination: account,
								Amount:      commonpb.NewUint256FromUint64(50),
								Asset:       "USD",
							},
							{
								Source:      "world",
								Destination: account,
								Amount:      commonpb.NewUint256FromUint64(50),
								Asset:       "EUR",
							},
						},
					},
				}},
			},
		},
	}
}

// twoNonWorldAccountsRequest builds a request where both source and destination
// are non-world accounts. Force=true skips the balance check on the source.
func twoNonWorldAccountsRequest(src, dst string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{
							{
								Source:      src,
								Destination: dst,
								Amount:      commonpb.NewUint256FromUint64(100),
								Asset:       "USD",
							},
						},
						Force: true,
					},
				}},
			},
		},
	}
}

func TestProcessCreateTransaction_AppliesDefaultMetadataToNewAccount(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	acctKey := domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"}

	commonTxMocks(mockStore, ledgerWithDefaults())
	expectWorldToAccountPostingMocks(mockStore, "users:alice")

	// New account: marker absent.
	mockStore.EXPECT().GetAccount(acctKey).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().PutAccount(acctKey, gomock.Any())

	// The default key is written.
	mockStore.EXPECT().PutAccountMetadata(
		domain.MetadataKey{AccountKey: acctKey, Key: "tier"},
		commonpb.NewStringValue("standard"),
	)

	result, err := processor.ProcessOrder(requestToOrder(worldToAccountRequest("users:alice", nil)), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Defaults also ride into the log payload so they are audited + indexed.
	createdTx := result.GetApply().GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Equal(t, commonpb.NewStringValue("standard"),
		createdTx.GetAccountMetadata()["users:alice"].GetValues()["tier"])
}

func TestProcessCreateTransaction_ExplicitMetadataWinsOverDefault(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	acctKey := domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"}

	commonTxMocks(mockStore, ledgerWithDefaults())
	expectWorldToAccountPostingMocks(mockStore, "users:alice")

	mockStore.EXPECT().GetAccount(acctKey).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().PutAccount(acctKey, gomock.Any())

	// Explicit "tier" must win: the default value "standard" must never be written.
	mockStore.EXPECT().PutAccountMetadata(
		domain.MetadataKey{AccountKey: acctKey, Key: "tier"},
		commonpb.NewStringValue("premium"),
	)

	explicit := map[string]*commonpb.MetadataMap{
		"users:alice": {Values: map[string]*commonpb.MetadataValue{"tier": commonpb.NewStringValue("premium")}},
	}

	result, err := processor.ProcessOrder(requestToOrder(worldToAccountRequest("users:alice", explicit)), mockStore)
	require.NoError(t, err)

	createdTx := result.GetApply().GetLog().GetData().GetCreatedTransaction()
	require.Equal(t, commonpb.NewStringValue("premium"),
		createdTx.GetAccountMetadata()["users:alice"].GetValues()["tier"])
}

func TestProcessCreateTransaction_ExistingAccountNotDefaulted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	acctKey := domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"}

	commonTxMocks(mockStore, ledgerWithDefaults())
	expectWorldToAccountPostingMocks(mockStore, "users:alice")

	// Account already exists: marker present. No PutAccount, no default metadata
	// write must happen (gomock fails the test if either is called).
	mockStore.EXPECT().GetAccount(acctKey).Return(&commonpb.AccountState{}, nil)

	result, err := processor.ProcessOrder(requestToOrder(worldToAccountRequest("users:alice", nil)), mockStore)
	require.NoError(t, err)

	createdTx := result.GetApply().GetLog().GetData().GetCreatedTransaction()
	require.Nil(t, createdTx.GetAccountMetadata()["users:alice"])
}

// TestProcessCreateTransaction_SameDestTwice_DedupSeen verifies that two
// postings within a single transaction that share the same non-world
// destination account only trigger GetAccount + PutAccount exactly once
// (the `seen` set deduplication).
func TestProcessCreateTransaction_SameDestTwice_DedupSeen(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	acctKey := domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"}

	// Two postings: world->users:alice USD and world->users:alice EUR.
	// Volume mocks for both postings.
	worldUSDKey := domain.NewVolumeKey("test-ledger", "world", "USD")
	worldEURKey := domain.NewVolumeKey("test-ledger", "world", "EUR")
	destUSDKey := domain.NewVolumeKey("test-ledger", "users:alice", "USD")
	destEURKey := domain.NewVolumeKey("test-ledger", "users:alice", "EUR")
	zero := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}
	mockStore.EXPECT().GetVolume(worldUSDKey).Return(zero.AsReader(), nil)
	mockStore.EXPECT().PutVolume(worldUSDKey, gomock.Any())
	mockStore.EXPECT().GetVolume(destUSDKey).Return(zero.AsReader(), nil)
	mockStore.EXPECT().PutVolume(destUSDKey, gomock.Any())
	mockStore.EXPECT().GetVolume(worldEURKey).Return(zero.AsReader(), nil)
	mockStore.EXPECT().PutVolume(worldEURKey, gomock.Any())
	mockStore.EXPECT().GetVolume(destEURKey).Return(zero.AsReader(), nil)
	mockStore.EXPECT().PutVolume(destEURKey, gomock.Any())

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerWithDefaults(), nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 1234567890}).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenChapter().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1)).AnyTimes()
	mockStore.EXPECT().PutTransactionState(gomock.Any(), gomock.Any())

	// Key assertion: GetAccount and PutAccount must each be called exactly ONCE
	// despite two postings to the same destination.
	mockStore.EXPECT().GetAccount(acctKey).Return(nil, domain.ErrNotFound).Times(1)
	mockStore.EXPECT().PutAccount(acctKey, gomock.Any()).Times(1)

	mockStore.EXPECT().PutAccountMetadata(
		domain.MetadataKey{AccountKey: acctKey, Key: "tier"},
		commonpb.NewStringValue("standard"),
	)

	result, err := processor.ProcessOrder(requestToOrder(twoPostingsToSameDestRequest("users:alice")), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)
}

// TestProcessCreateTransaction_NonWorldSource_MarkerWritten verifies that a
// non-world source account (seen for the first time) also gets its marker
// written and default metadata applied, since it is a non-world account
// touched by the transaction.
func TestProcessCreateTransaction_NonWorldSource_MarkerWritten(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	srcKey := domain.AccountKey{LedgerName: "test-ledger", Account: "users:src"}
	dstKey := domain.AccountKey{LedgerName: "test-ledger", Account: "users:dst"}

	// Force=true so the source balance check is bypassed.
	srcVolumeKey := domain.NewVolumeKey("test-ledger", "users:src", "USD")
	dstVolumeKey := domain.NewVolumeKey("test-ledger", "users:dst", "USD")
	zero := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}
	mockStore.EXPECT().GetVolume(srcVolumeKey).Return(zero.AsReader(), nil)
	mockStore.EXPECT().PutVolume(srcVolumeKey, gomock.Any())
	mockStore.EXPECT().GetVolume(dstVolumeKey).Return(zero.AsReader(), nil)
	mockStore.EXPECT().PutVolume(dstVolumeKey, gomock.Any())

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerWithDefaults(), nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 1234567890}).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenChapter().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1)).AnyTimes()
	mockStore.EXPECT().PutTransactionState(gomock.Any(), gomock.Any())

	// Both source and destination are new: each gets a marker and tier default.
	mockStore.EXPECT().GetAccount(srcKey).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().PutAccount(srcKey, gomock.Any())
	mockStore.EXPECT().PutAccountMetadata(
		domain.MetadataKey{AccountKey: srcKey, Key: "tier"},
		commonpb.NewStringValue("standard"),
	)

	mockStore.EXPECT().GetAccount(dstKey).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().PutAccount(dstKey, gomock.Any())
	mockStore.EXPECT().PutAccountMetadata(
		domain.MetadataKey{AccountKey: dstKey, Key: "tier"},
		commonpb.NewStringValue("standard"),
	)

	result, err := processor.ProcessOrder(requestToOrder(twoNonWorldAccountsRequest("users:src", "users:dst")), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)
}

// TestProcessCreateTransaction_NoMatchingType_MarkerOnlyNoDefaults verifies
// that when a new account does not match any account type, a marker IS still
// written (presence-only), but NO default metadata is added.
func TestProcessCreateTransaction_NoMatchingType_MarkerOnlyNoDefaults(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	// The ledger has a type "users:{id}" with defaults, but the account
	// "vendors:acme" does not match that pattern.
	acctKey := domain.AccountKey{LedgerName: "test-ledger", Account: "vendors:acme"}

	commonTxMocks(mockStore, ledgerWithDefaults())
	expectWorldToAccountPostingMocks(mockStore, "vendors:acme")

	// New account: marker absent.
	mockStore.EXPECT().GetAccount(acctKey).Return(nil, domain.ErrNotFound)
	// Marker IS written even though no type matches.
	mockStore.EXPECT().PutAccount(acctKey, gomock.Any())
	// No PutAccountMetadata expected — gomock strict mode will fail if called.

	result, err := processor.ProcessOrder(requestToOrder(worldToAccountRequest("vendors:acme", nil)), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	createdTx := result.GetApply().GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	// No account metadata for the unmatched account.
	require.Nil(t, createdTx.GetAccountMetadata()["vendors:acme"])
}

// TestProcessCreateTransaction_GateOff_NoAccountCalls verifies that when the
// ledger has no account type with default_metadata, the EN-1276 gate is off
// and neither GetAccount nor PutAccount is ever called.
func TestProcessCreateTransaction_GateOff_NoAccountCalls(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	// Use a ledger whose account type has NO default_metadata — gate stays off.
	commonTxMocks(mockStore, ledgerWithNoAccountTypeDefaults())
	expectWorldToAccountPostingMocks(mockStore, "users:alice")

	// Strict gomock: GetAccount and PutAccount must NOT be called at all.
	// (No EXPECT() registered → any call panics the test.)

	result, err := processor.ProcessOrder(requestToOrder(worldToAccountRequest("users:alice", nil)), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)
}

// TestProcessCreateTransaction_PartialExplicitMetadata verifies that when the
// order sets only one of the two default keys, the explicit key is preserved
// and the unset default key is still added.
func TestProcessCreateTransaction_PartialExplicitMetadata(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	acctKey := domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"}

	// Ledger type declares two defaults: tier=standard, region=eu.
	commonTxMocks(mockStore, ledgerWithTwoDefaults())
	expectWorldToAccountPostingMocks(mockStore, "users:alice")

	mockStore.EXPECT().GetAccount(acctKey).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().PutAccount(acctKey, gomock.Any())

	// Explicit order metadata sets "tier"=premium only.
	// gomock: tier=premium (explicit wins), region=eu (default applied).
	mockStore.EXPECT().PutAccountMetadata(
		domain.MetadataKey{AccountKey: acctKey, Key: "tier"},
		commonpb.NewStringValue("premium"),
	)
	mockStore.EXPECT().PutAccountMetadata(
		domain.MetadataKey{AccountKey: acctKey, Key: "region"},
		commonpb.NewStringValue("eu"),
	)

	explicit := map[string]*commonpb.MetadataMap{
		"users:alice": {Values: map[string]*commonpb.MetadataValue{
			"tier": commonpb.NewStringValue("premium"),
		}},
	}

	result, err := processor.ProcessOrder(requestToOrder(worldToAccountRequest("users:alice", explicit)), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	createdTx := result.GetApply().GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)

	accountMeta := createdTx.GetAccountMetadata()["users:alice"]
	require.NotNil(t, accountMeta)
	// Explicit key must survive.
	require.Equal(t, commonpb.NewStringValue("premium"), accountMeta.GetValues()["tier"])
	// Default key must be added.
	require.Equal(t, commonpb.NewStringValue("eu"), accountMeta.GetValues()["region"])
}
