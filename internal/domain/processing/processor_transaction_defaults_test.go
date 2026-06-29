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
// no default_metadata. The universal existence marker is still written for new
// accounts; only the default-metadata merge is skipped (FindMatchingType for a
// type with no defaults yields nothing to merge).
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
func expectWorldToAccountPostingMocks(t *testing.T, mockStore *MockScope, account string) {
	t.Helper()
	worldKey := domain.NewVolumeKey("test-ledger", "world", "USD")
	destKey := domain.NewVolumeKey("test-ledger", account, "USD")
	zero := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	expectGetVolume(mockStore, worldKey, zero.AsReader(), nil)
	expectPutVolume(t, mockStore, worldKey, nil)
	expectGetVolume(mockStore, destKey, zero.AsReader(), nil)
	expectPutVolume(t, mockStore, destKey, nil)
}

// commonTxMocks wires the calls every processCreateTransaction makes that are
// not specific to the default-metadata behaviour under test.
func commonTxMocks(t *testing.T, mockStore *MockScope, info *commonpb.LedgerInfo) {
	t.Helper()
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, info.AsReader(), nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 1234567890}).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenChapter().Return(nil, false)
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1)).AnyTimes()
	expectPutTransactionState(t, mockStore, domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, nil)
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

	commonTxMocks(t, mockStore, ledgerWithDefaults())
	expectWorldToAccountPostingMocks(t, mockStore, "users:alice")

	// New account: marker absent.
	mockStore.EXPECT().GetAccount(acctKey).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().PutAccount(acctKey, gomock.Any())

	// The default key is written.
	expectPutAccountMetadata(t, mockStore, domain.MetadataKey{AccountKey: acctKey, Key: "tier"}, nil)

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

	commonTxMocks(t, mockStore, ledgerWithDefaults())
	expectWorldToAccountPostingMocks(t, mockStore, "users:alice")

	mockStore.EXPECT().GetAccount(acctKey).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().PutAccount(acctKey, gomock.Any())

	// Explicit "tier" must win: the default value "standard" must never be written.
	expectPutAccountMetadata(t, mockStore, domain.MetadataKey{AccountKey: acctKey, Key: "tier"}, nil)

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

	commonTxMocks(t, mockStore, ledgerWithDefaults())
	expectWorldToAccountPostingMocks(t, mockStore, "users:alice")

	// Account already exists: marker present. No PutAccount, no default metadata
	// write must happen (gomock fails the test if either is called).
	mockStore.EXPECT().GetAccount(acctKey).Return((&commonpb.AccountState{}).AsReader(), nil)

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
	expectGetVolume(mockStore, worldUSDKey, zero.AsReader(), nil)
	expectPutVolume(t, mockStore, worldUSDKey, nil)
	expectGetVolume(mockStore, destUSDKey, zero.AsReader(), nil)
	expectPutVolume(t, mockStore, destUSDKey, nil)
	expectGetVolume(mockStore, worldEURKey, zero.AsReader(), nil)
	expectPutVolume(t, mockStore, worldEURKey, nil)
	expectGetVolume(mockStore, destEURKey, zero.AsReader(), nil)
	expectPutVolume(t, mockStore, destEURKey, nil)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerWithDefaults().AsReader(), nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 1234567890}).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenChapter().Return(nil, false)
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1)).AnyTimes()
	expectPutTransactionState(t, mockStore, domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, nil)

	// Key assertion: GetAccount and PutAccount must each be called exactly ONCE
	// despite two postings to the same destination.
	mockStore.EXPECT().GetAccount(acctKey).Return(nil, domain.ErrNotFound).Times(1)
	mockStore.EXPECT().PutAccount(acctKey, gomock.Any()).Times(1)

	expectPutAccountMetadata(t, mockStore, domain.MetadataKey{AccountKey: acctKey, Key: "tier"}, nil)

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
	expectGetVolume(mockStore, srcVolumeKey, zero.AsReader(), nil)
	expectPutVolume(t, mockStore, srcVolumeKey, nil)
	expectGetVolume(mockStore, dstVolumeKey, zero.AsReader(), nil)
	expectPutVolume(t, mockStore, dstVolumeKey, nil)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerWithDefaults().AsReader(), nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 1234567890}).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenChapter().Return(nil, false)
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1)).AnyTimes()
	expectPutTransactionState(t, mockStore, domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, nil)

	// Both source and destination are new: each gets a marker and tier default.
	mockStore.EXPECT().GetAccount(srcKey).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().PutAccount(srcKey, gomock.Any())
	expectPutAccountMetadata(t, mockStore, domain.MetadataKey{AccountKey: srcKey, Key: "tier"}, nil)

	mockStore.EXPECT().GetAccount(dstKey).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().PutAccount(dstKey, gomock.Any())
	expectPutAccountMetadata(t, mockStore, domain.MetadataKey{AccountKey: dstKey, Key: "tier"}, nil)

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

	commonTxMocks(t, mockStore, ledgerWithDefaults())
	expectWorldToAccountPostingMocks(t, mockStore, "vendors:acme")

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

// TestProcessCreateTransaction_NoDefaults_MarkerWrittenNoMetadata verifies that
// when the ledger has no account type with default_metadata, the universal
// existence marker is STILL written for a new account (markers are not gated on
// defaults), but NO default metadata is applied (nothing to merge).
func TestProcessCreateTransaction_NoDefaults_MarkerWrittenNoMetadata(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	acctKey := domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"}

	// Use a ledger whose account type has NO default_metadata.
	commonTxMocks(t, mockStore, ledgerWithNoAccountTypeDefaults())
	expectWorldToAccountPostingMocks(t, mockStore, "users:alice")

	// New account: marker absent, then written. No PutAccountMetadata expected —
	// strict gomock fails the test if a default metadata write is attempted.
	mockStore.EXPECT().GetAccount(acctKey).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().PutAccount(acctKey, gomock.Any())

	result, err := processor.ProcessOrder(requestToOrder(worldToAccountRequest("users:alice", nil)), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	createdTx := result.GetApply().GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Nil(t, createdTx.GetAccountMetadata()["users:alice"])
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
	commonTxMocks(t, mockStore, ledgerWithTwoDefaults())
	expectWorldToAccountPostingMocks(t, mockStore, "users:alice")

	mockStore.EXPECT().GetAccount(acctKey).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().PutAccount(acctKey, gomock.Any())

	// Explicit order metadata sets "tier"=premium only.
	// gomock: tier=premium (explicit wins), region=eu (default applied).
	expectPutAccountMetadata(t, mockStore, domain.MetadataKey{AccountKey: acctKey, Key: "tier"}, nil)
	expectPutAccountMetadata(t, mockStore, domain.MetadataKey{AccountKey: acctKey, Key: "region"}, nil)

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
