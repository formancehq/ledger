package testutil

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"

	. "github.com/onsi/gomega" //nolint:staticcheck // dot import is idiomatic for Gomega test helpers

	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
)

// Re-exports from pkg/scenario/actions for backward compatibility.
var (
	ExtractGRPCErrorInfo = actions.ExtractGRPCErrorInfo

	// Action builders.
	CreateLedgerAction                 = actions.CreateLedgerAction
	DeleteLedgerAction                 = actions.DeleteLedgerAction
	CreateTransactionAction            = actions.CreateTransactionAction
	CreateForceTransactionAction       = actions.CreateForceTransactionAction
	CreateForceScriptTransactionAction = actions.CreateForceScriptTransactionAction
	CreateScriptTransactionAction      = actions.CreateScriptTransactionAction
	AddAccountTypeAction               = actions.AddAccountTypeAction
	RemoveAccountTypeAction            = actions.RemoveAccountTypeAction
	MigrateAccountTypeAction           = actions.MigrateAccountTypeAction
	SaveAccountMetadataAction          = actions.SaveAccountMetadataAction
	DeleteAccountMetadataAction        = actions.DeleteAccountMetadataAction
	SaveTransactionMetadataAction      = actions.SaveTransactionMetadataAction
	DeleteTransactionMetadataAction    = actions.DeleteTransactionMetadataAction
	RevertTransactionAction            = actions.RevertTransactionAction
	WithTimestamp                      = actions.WithTimestamp
	WithExpandVolumes                  = actions.WithExpandVolumes
	NewPosting                         = actions.NewPosting
	RegisterSigningKeyAction           = actions.RegisterSigningKeyAction
	RevokeSigningKeyAction             = actions.RevokeSigningKeyAction
	SetSigningConfigAction             = actions.SetSigningConfigAction
	FindSigningKey                     = actions.FindSigningKey
	FindMetadataValue                  = actions.FindMetadataValue
	ClosePeriodAction                  = actions.ClosePeriodAction
	SetMaintenanceModeAction           = actions.SetMaintenanceModeAction
	SetAuditConfigAction               = actions.SetAuditConfigAction
	SetPeriodScheduleAction            = actions.SetPeriodScheduleAction
	DeletePeriodScheduleAction         = actions.DeletePeriodScheduleAction
	SetMetadataFieldTypeAction         = actions.SetMetadataFieldTypeAction
	RemoveMetadataFieldTypeAction      = actions.RemoveMetadataFieldTypeAction
	CreateLedgerWithSchemaAction       = actions.CreateLedgerWithSchemaAction
	SaveTypedAccountMetadataAction     = actions.SaveTypedAccountMetadataAction
	SaveTypedTransactionMetadataAction = actions.SaveTypedTransactionMetadataAction
	SaveNumscriptAction                = actions.SaveNumscriptAction
	SaveNumscriptWithVersionAction     = actions.SaveNumscriptWithVersionAction
	DeleteNumscriptAction              = actions.DeleteNumscriptAction
	CreateScriptRefTransactionAction   = actions.CreateScriptRefTransactionAction
	CreateBuiltinTxIndexAction         = actions.CreateBuiltinTxIndexAction
	DropBuiltinTxIndexAction           = actions.DropBuiltinTxIndexAction
	CreateAccountMetadataIndexAction   = actions.CreateAccountMetadataIndexAction
	DropAccountMetadataIndexAction     = actions.DropAccountMetadataIndexAction
	ArchivePeriodAction                = actions.ArchivePeriodAction
	GetCreatedTransactionID            = actions.GetCreatedTransactionID

	// Filter builders.
	StringMetadataFilter          = actions.StringMetadataFilter
	AddressPrefixFilter           = actions.AddressPrefixFilter
	AddressExactFilter            = actions.AddressExactFilter
	ReferenceFilter               = actions.ReferenceFilter
	AndFilter                     = actions.AndFilter
	OrFilter                      = actions.OrFilter
	NotFilter                     = actions.NotFilter
	LedgerFilter                  = actions.LedgerFilter
	ParamAddressPrefixFilter      = actions.ParamAddressPrefixFilter
	ParamAddressExactFilter       = actions.ParamAddressExactFilter
	ParamStringMetadataFilter     = actions.ParamStringMetadataFilter
	ParamBoolMetadataFilter       = actions.ParamBoolMetadataFilter
	ParamInt64RangeMetadataFilter = actions.ParamInt64RangeMetadataFilter
	Int64RangeMetadataFilter      = actions.Int64RangeMetadataFilter
	BoolMetadataFilter            = actions.BoolMetadataFilter
	StringParam                   = actions.StringParam
	Int64Param                    = actions.Int64Param
	Uint64Param                   = actions.Uint64Param
	BoolParam                     = actions.BoolParam

	// Read helpers.
	ListLedgers                    = actions.ListLedgers
	ListNumscripts                 = actions.ListNumscripts
	ListAllAccounts                = actions.ListAllAccounts
	ListAllTransactions            = actions.ListAllTransactions
	ListAllLogs                    = actions.ListAllLogs
	ListAllPeriods                 = actions.ListAllPeriods
	GetAccount                     = actions.GetAccount
	GetTransaction                 = actions.GetTransaction
	GetLedger                      = actions.GetLedger
	GetLedgerStats                 = actions.GetLedgerStats
	GetNumscript                   = actions.GetNumscript
	AggregateVolumes               = actions.AggregateVolumes
	ListAuditEntries               = actions.ListAuditEntries
	GetMetadataSchemaStatus        = actions.GetMetadataSchemaStatus
	GetPeriodSchedule              = actions.GetPeriodSchedule
	AnalyzeAccounts                = actions.AnalyzeAccounts
	AnalyzeTransactions            = actions.AnalyzeTransactions
	GetLog                         = actions.GetLog
	GetAuditEntry                  = actions.GetAuditEntry
	Discovery                      = actions.Discovery
	GetStoreMetrics                = actions.GetStoreMetrics
	GetReadIndexMetrics            = actions.GetReadIndexMetrics
	GetIndexStatus                 = actions.GetIndexStatus
	ListAccountsFiltered           = actions.ListAccountsFiltered
	ListTransactionsFiltered       = actions.ListTransactionsFiltered
	CreatePreparedQuery            = actions.CreatePreparedQuery
	UpdatePreparedQuery            = actions.UpdatePreparedQuery
	DeletePreparedQuery            = actions.DeletePreparedQuery
	ListPreparedQueries            = actions.ListPreparedQueries
	ExecutePreparedQuery           = actions.ExecutePreparedQuery
	ExecutePreparedQueryWithParams = actions.ExecutePreparedQueryWithParams

	// Backup helpers.
	CollectCheckStoreEvents  = actions.CollectCheckStoreEvents
	UploadAndFinalizeRestore = actions.UploadAndFinalizeRestore
)

// Type re-exports for backward compatibility.
type (
	CheckStoreResult = actions.CheckStoreResult
	BackupData       = actions.BackupData
)

// GenerateTestKeypair generates an Ed25519 keypair and returns (publicKey, privateKey).
func GenerateTestKeypair() (ed25519.PublicKey, ed25519.PrivateKey) {
	pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
	Expect(err).To(Succeed())

	return pubKey, privKey
}

// SignRequest signs a request with the given key and returns the same request (modified in place).
func SignRequest(req *servicepb.Request, keyID string, privKey ed25519.PrivateKey) *servicepb.Request {
	Expect(signing.Sign(req, keyID, privKey)).To(Succeed())

	return req
}

// ListAllSigningKeys collects all signing keys from the ListSigningKeys stream into a slice.
func ListAllSigningKeys(ctx context.Context, client servicepb.BucketServiceClient) []*commonpb.SigningKey {
	stream, err := client.ListSigningKeys(ctx, &servicepb.ListSigningKeysRequest{})
	Expect(err).To(Succeed())

	var keys []*commonpb.SigningKey
	for {
		key, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		Expect(err).To(Succeed())
		keys = append(keys, key)
	}

	return keys
}
