package testutil

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"time"

	. "github.com/onsi/gomega" //nolint:staticcheck // dot import is idiomatic for Gomega test helpers
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/restorepb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// ExtractGRPCErrorInfo extracts the ErrorInfo detail from a gRPC error.
func ExtractGRPCErrorInfo(err error) *errdetails.ErrorInfo {
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info
		}
	}

	return nil
}

// Helper functions for creating gRPC requests

// CreateLedgerAction creates an action for creating a new ledger.
func CreateLedgerAction(name string, _ map[string]string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{
				Name: name,
			},
		},
	}
}

// DeleteLedgerAction creates an action for deleting a ledger.
func DeleteLedgerAction(ledgerName string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_DeleteLedger{
			DeleteLedger: &servicepb.DeleteLedgerRequest{
				Name: ledgerName,
			},
		},
	}
}

// CreateTransactionAction creates an action for creating a transaction.
func CreateTransactionAction(ledgerName string, postings []*commonpb.Posting, metadata map[string]string, accountMetadata map[string]*commonpb.MetadataSet) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings:        postings,
						Metadata:        commonpb.MetadataSetFromMap(metadata),
						AccountMetadata: accountMetadata,
					},
				},
			},
		},
	}
}

// CreateForceTransactionAction creates an action for creating a transaction with force=true (bypasses balance checks).
func CreateForceTransactionAction(ledgerName string, postings []*commonpb.Posting, metadata map[string]string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: postings,
						Metadata: commonpb.MetadataSetFromMap(metadata),
						Force:    true,
					},
				},
			},
		},
	}
}

// CreateForceScriptTransactionAction creates an action for creating a transaction using Numscript with force=true.
func CreateForceScriptTransactionAction(ledgerName string, script string, vars map[string]string, metadata map[string]string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: script,
							Vars:  vars,
						},
						Metadata: commonpb.MetadataSetFromMap(metadata),
						Force:    true,
					},
				},
			},
		},
	}
}

// CreateScriptTransactionAction creates an action for creating a transaction using Numscript.
func CreateScriptTransactionAction(ledgerName string, script string, vars map[string]string, metadata map[string]string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: script,
							Vars:  vars,
						},
						Metadata: commonpb.MetadataSetFromMap(metadata),
					},
				},
			},
		},
	}
}

// AddAccountTypeAction creates an action for adding an account type to a ledger.
func AddAccountTypeAction(ledgerName, name, pattern string, enforcement commonpb.ChartEnforcementMode) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_AddAccountType{
			AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
				Ledger: ledgerName,
				AccountType: &commonpb.AccountType{
					Name:            name,
					Pattern:         pattern,
					Status:          commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE,
					EnforcementMode: enforcement,
				},
			},
		},
	}
}

// UpdateAccountTypeAction creates an action for updating an account type's enforcement mode.
func UpdateAccountTypeAction(ledgerName, name string, enforcement commonpb.ChartEnforcementMode) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_UpdateAccountType{
			UpdateAccountType: &servicepb.UpdateAccountTypeLedgerRequest{
				Ledger:          ledgerName,
				Name:            name,
				EnforcementMode: enforcement,
			},
		},
	}
}

// RemoveAccountTypeAction creates an action for removing an account type.
func RemoveAccountTypeAction(ledgerName, name string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_RemoveAccountType{
			RemoveAccountType: &servicepb.RemoveAccountTypeLedgerRequest{
				Ledger: ledgerName,
				Name:   name,
			},
		},
	}
}

// SaveAccountMetadataAction creates an action for saving account metadata.
func SaveAccountMetadataAction(ledgerName, address string, metadata map[string]string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: address},
							},
						},
						Metadata: commonpb.MetadataSetFromMap(metadata),
					},
				},
			},
		},
	}
}

// DeleteAccountMetadataAction creates an action for deleting account metadata.
func DeleteAccountMetadataAction(ledgerName, address, key string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: address},
							},
						},
						Key: key,
					},
				},
			},
		},
	}
}

// SaveTransactionMetadataAction creates an action for saving transaction metadata.
func SaveTransactionMetadataAction(ledgerName string, transactionID uint64, metadata map[string]string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{Id: transactionID},
							},
						},
						Metadata: commonpb.MetadataSetFromMap(metadata),
					},
				},
			},
		},
	}
}

// DeleteTransactionMetadataAction creates an action for deleting transaction metadata.
func DeleteTransactionMetadataAction(ledgerName string, transactionID uint64, key string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{Id: transactionID},
							},
						},
						Key: key,
					},
				},
			},
		},
	}
}

// RevertTransactionAction creates an action for reverting a transaction.
func RevertTransactionAction(ledgerName string, transactionID uint64, force, atEffectiveDate bool, metadata map[string]string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_RevertTransaction{
					RevertTransaction: &servicepb.RevertTransactionPayload{
						TransactionId:   transactionID,
						Force:           force,
						AtEffectiveDate: atEffectiveDate,
						Metadata:        commonpb.MetadataSetFromMap(metadata),
					},
				},
			},
		},
	}
}

// WithTimestamp sets the timestamp on a create transaction request.
func WithTimestamp(req *servicepb.Request, t time.Time) *servicepb.Request {
	if reqType, ok := req.GetType().(*servicepb.Request_Apply); ok {
		if d, ok := reqType.Apply.GetData().(*servicepb.LedgerApplyRequest_CreateTransaction); ok {
			d.CreateTransaction.Timestamp = &commonpb.Timestamp{Data: uint64(t.UnixMicro())}
		}
	}

	return req
}

// WithExpandVolumes sets the ExpandVolumes flag on a create or revert transaction request.
func WithExpandVolumes(req *servicepb.Request) *servicepb.Request {
	if reqType, ok := req.GetType().(*servicepb.Request_Apply); ok {
		switch d := reqType.Apply.GetData().(type) {
		case *servicepb.LedgerApplyRequest_CreateTransaction:
			d.CreateTransaction.ExpandVolumes = true
		case *servicepb.LedgerApplyRequest_RevertTransaction:
			d.RevertTransaction.ExpandVolumes = true
		}
	}

	return req
}

// NewPosting creates a new posting protobuf message.
func NewPosting(source, destination string, amount *big.Int, asset string) *commonpb.Posting {
	return commonpb.NewPosting(source, destination, asset, amount)
}

// ListLedgers collects all ledgers from the streaming RPC into a map.
func ListLedgers(ctx context.Context, client servicepb.BucketServiceClient) (map[string]*commonpb.LedgerInfo, error) {
	stream, err := client.ListLedgers(ctx, &servicepb.ListLedgersRequest{})
	if err != nil {
		return nil, err
	}

	ledgers := make(map[string]*commonpb.LedgerInfo)
	for {
		ledger, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		ledgers[ledger.GetName()] = ledger
	}

	return ledgers, nil
}

// SetMetadataFieldTypeAction creates a request to declare a metadata field type.
func SetMetadataFieldTypeAction(ledger string, targetType commonpb.TargetType, key string, metadataType commonpb.MetadataType) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SetMetadataFieldType{
			SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
				Ledger:     ledger,
				TargetType: targetType,
				Key:        key,
				Type:       metadataType,
			},
		},
	}
}

// RemoveMetadataFieldTypeAction creates a request to remove a metadata field type declaration.
func RemoveMetadataFieldTypeAction(ledger string, targetType commonpb.TargetType, key string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_RemoveMetadataFieldType{
			RemoveMetadataFieldType: &servicepb.RemoveMetadataFieldTypeRequest{
				Ledger:     ledger,
				TargetType: targetType,
				Key:        key,
			},
		},
	}
}

// CreateLedgerWithSchemaAction creates a ledger with an initial metadata schema.
func CreateLedgerWithSchemaAction(name string, _ map[string]string, schema []*commonpb.SetMetadataFieldTypeCommand) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{
				Name:          name,
				InitialSchema: schema,
			},
		},
	}
}

// SaveTypedAccountMetadataAction creates a request with a typed MetadataSet (not map[string]string).
func SaveTypedAccountMetadataAction(ledgerName, address string, metadata *commonpb.MetadataSet) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: address},
							},
						},
						Metadata: metadata,
					},
				},
			},
		},
	}
}

// SaveTypedTransactionMetadataAction creates a request with a typed MetadataSet (not map[string]string).
func SaveTypedTransactionMetadataAction(ledgerName string, txID uint64, metadata *commonpb.MetadataSet) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{Id: txID},
							},
						},
						Metadata: metadata,
					},
				},
			},
		},
	}
}

// SaveNumscriptAction creates an action for saving a numscript to the library.
func SaveNumscriptAction(name, content string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SaveNumscript{
			SaveNumscript: &servicepb.SaveNumscriptRequest{
				Name:    name,
				Content: content,
			},
		},
	}
}

// SaveNumscriptWithVersionAction creates an action for saving a numscript with a specific version.
func SaveNumscriptWithVersionAction(name, content, version string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SaveNumscript{
			SaveNumscript: &servicepb.SaveNumscriptRequest{
				Name:    name,
				Content: content,
				Version: version,
			},
		},
	}
}

// DeleteNumscriptAction creates an action for deleting a numscript from the library.
func DeleteNumscriptAction(name string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_DeleteNumscript{
			DeleteNumscript: &servicepb.DeleteNumscriptRequest{
				Name: name,
			},
		},
	}
}

// CreateScriptRefTransactionAction creates a transaction using a script reference from the library.
func CreateScriptRefTransactionAction(ledgerName, scriptName, version string, vars map[string]string, metadata map[string]string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						ScriptReference: &servicepb.ScriptReference{
							Name:    scriptName,
							Version: version,
							Vars:    vars,
						},
						Metadata: commonpb.MetadataSetFromMap(metadata),
					},
				},
			},
		},
	}
}

// ListNumscripts collects all numscripts from the streaming RPC.
func ListNumscripts(ctx context.Context, client servicepb.BucketServiceClient) ([]*commonpb.NumscriptInfo, error) {
	stream, err := client.ListNumscripts(ctx, &servicepb.ListNumscriptsRequest{})
	if err != nil {
		return nil, err
	}

	var scripts []*commonpb.NumscriptInfo
	for {
		info, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		scripts = append(scripts, info)
	}

	return scripts, nil
}

// RegisterSigningKeyAction creates a RegisterSigningKey request.
func RegisterSigningKeyAction(keyID string, pubKey ed25519.PublicKey) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_RegisterSigningKey{
			RegisterSigningKey: &servicepb.RegisterSigningKeyRequest{
				KeyId:     keyID,
				PublicKey: []byte(pubKey),
			},
		},
	}
}

// RevokeSigningKeyAction creates a RevokeSigningKey request.
func RevokeSigningKeyAction(keyID string, cascade bool) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_RevokeSigningKey{
			RevokeSigningKey: &servicepb.RevokeSigningKeyRequest{
				KeyId:   keyID,
				Cascade: cascade,
			},
		},
	}
}

// SetSigningConfigAction creates a SetSigningConfig request.
func SetSigningConfigAction(requireSignatures bool) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SetSigningConfig{
			SetSigningConfig: &servicepb.SetSigningConfigRequest{
				RequireSignatures: requireSignatures,
			},
		},
	}
}

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

// FindSigningKey finds a key by ID in a slice of signing keys. Returns nil if not found.
func FindSigningKey(keys []*commonpb.SigningKey, keyID string) *commonpb.SigningKey {
	for _, k := range keys {
		if k.GetKeyId() == keyID {
			return k
		}
	}

	return nil
}

// FindMetadataValue looks up a key in a MetadataSet and returns the *MetadataValue (nil if not found).
func FindMetadataValue(ms *commonpb.MetadataSet, key string) *commonpb.MetadataValue {
	if ms == nil {
		return nil
	}
	for _, md := range ms.GetMetadata() {
		if md.GetKey() == key {
			return md.GetValue()
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// Period actions
// ---------------------------------------------------------------------------

// ClosePeriodAction creates a request to close the current accounting period.
func ClosePeriodAction() *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_ClosePeriod{
			ClosePeriod: &servicepb.ClosePeriodRequest{},
		},
	}
}

// SetMaintenanceModeAction creates a request to enable or disable maintenance mode.
func SetMaintenanceModeAction(enabled bool) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SetMaintenanceMode{
			SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{
				Enabled: enabled,
			},
		},
	}
}

// SetAuditConfigAction creates a request to enable or disable audit logging.
func SetAuditConfigAction(enabled bool) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SetAuditConfig{
			SetAuditConfig: &servicepb.SetAuditConfigRequest{
				Enabled: enabled,
			},
		},
	}
}

// SetPeriodScheduleAction creates a request to set the period schedule cron expression.
func SetPeriodScheduleAction(cron string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SetPeriodSchedule{
			SetPeriodSchedule: &servicepb.SetPeriodScheduleRequest{
				Cron: cron,
			},
		},
	}
}

// DeletePeriodScheduleAction creates a request to remove the period schedule.
func DeletePeriodScheduleAction() *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_DeletePeriodSchedule{
			DeletePeriodSchedule: &servicepb.DeletePeriodScheduleRequest{},
		},
	}
}

// ---------------------------------------------------------------------------
// Read helpers (streaming RPCs)
// ---------------------------------------------------------------------------

// ListAllAccounts collects all accounts for a ledger from the streaming RPC.
func ListAllAccounts(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string) ([]*commonpb.Account, error) {
	stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{
		Ledger: ledgerName,
	})
	if err != nil {
		return nil, err
	}

	var accounts []*commonpb.Account
	for {
		account, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, account)
	}

	return accounts, nil
}

// ListAllTransactions collects all transactions for a ledger from the streaming RPC.
func ListAllTransactions(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string) ([]*commonpb.Transaction, error) {
	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger: ledgerName,
	})
	if err != nil {
		return nil, err
	}

	var transactions []*commonpb.Transaction
	for {
		tx, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// ListAllLogs collects all system logs from the streaming RPC.
func ListAllLogs(ctx context.Context, client servicepb.BucketServiceClient) ([]*commonpb.Log, error) {
	stream, err := client.ListLogs(ctx, &servicepb.ListLogsRequest{})
	if err != nil {
		return nil, err
	}

	var logs []*commonpb.Log
	for {
		log, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		logs = append(logs, log)
	}

	return logs, nil
}

// ListAllPeriods collects all periods from the streaming RPC.
func ListAllPeriods(ctx context.Context, client servicepb.BucketServiceClient) ([]*commonpb.Period, error) {
	stream, err := client.ListPeriods(ctx, &servicepb.ListPeriodsRequest{})
	if err != nil {
		return nil, err
	}

	var periods []*commonpb.Period
	for {
		period, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		periods = append(periods, period)
	}

	return periods, nil
}

// GetAccount retrieves a single account by address.
func GetAccount(ctx context.Context, client servicepb.BucketServiceClient, ledgerName, address string) (*commonpb.Account, error) {
	return client.GetAccount(ctx, &servicepb.GetAccountRequest{
		Ledger:  ledgerName,
		Address: address,
	})
}

// GetTransaction retrieves a single transaction by ID.
func GetTransaction(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string, txID uint64) (*servicepb.GetTransactionResponse, error) {
	return client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		Ledger:        ledgerName,
		TransactionId: txID,
	})
}

// GetLedger retrieves ledger info by name.
func GetLedger(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string) (*commonpb.LedgerInfo, error) {
	return client.GetLedger(ctx, &servicepb.GetLedgerRequest{
		Ledger: ledgerName,
	})
}

// GetLedgerStats retrieves transaction and account counts for a ledger.
func GetLedgerStats(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string) (*commonpb.LedgerStats, error) {
	return client.GetLedgerStats(ctx, &servicepb.GetLedgerStatsRequest{
		Ledger: ledgerName,
	})
}

// GetNumscript retrieves a numscript by name and optional version ("" = latest).
func GetNumscript(ctx context.Context, client servicepb.BucketServiceClient, name, version string) (*commonpb.NumscriptInfo, error) {
	return client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
		Name:    name,
		Version: version,
	})
}

// AggregateVolumes returns aggregated volumes for a ledger.
func AggregateVolumes(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string) (*commonpb.AggregateResult, error) {
	return client.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{
		Ledger: ledgerName,
	})
}

// ListAuditEntries collects all audit entries from the streaming RPC.
func ListAuditEntries(ctx context.Context, client servicepb.BucketServiceClient, failuresOnly bool) ([]*auditpb.AuditEntry, error) {
	stream, err := client.ListAuditEntries(ctx, &servicepb.ListAuditEntriesRequest{
		FailuresOnly: failuresOnly,
	})
	if err != nil {
		return nil, err
	}

	var entries []*auditpb.AuditEntry
	for {
		entry, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// GetMetadataSchemaStatus retrieves the metadata schema conversion status for a ledger.
func GetMetadataSchemaStatus(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string) (*servicepb.GetMetadataSchemaStatusResponse, error) {
	return client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
		Ledger: ledgerName,
	})
}

// GetPeriodSchedule retrieves the current period schedule cron expression.
func GetPeriodSchedule(ctx context.Context, client servicepb.BucketServiceClient) (string, error) {
	resp, err := client.GetPeriodSchedule(ctx, &servicepb.GetPeriodScheduleRequest{})
	if err != nil {
		return "", err
	}

	return resp.GetCron(), nil
}

// ---------------------------------------------------------------------------
// Store integrity & backup helpers
// ---------------------------------------------------------------------------

// CheckStoreResult holds the errors and progress events from a CheckStore RPC call.
type CheckStoreResult struct {
	Errors   []*servicepb.CheckStoreError
	Progress []*servicepb.CheckStoreProgress
}

// CollectCheckStoreEvents runs the CheckStore RPC and returns all errors and progress events.
func CollectCheckStoreEvents(ctx context.Context, client servicepb.BucketServiceClient) (*CheckStoreResult, error) {
	stream, err := client.CheckStore(ctx, &servicepb.CheckStoreRequest{})
	if err != nil {
		return nil, err
	}

	result := &CheckStoreResult{}
	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		switch t := event.GetType().(type) {
		case *servicepb.CheckStoreEvent_Error:
			result.Errors = append(result.Errors, t.Error)
		case *servicepb.CheckStoreEvent_Progress:
			result.Progress = append(result.Progress, t.Progress)
		}
	}

	return result, nil
}

// StreamBackup runs the Backup RPC and writes the tar archive to the provided writer.
// Returns the total bytes written and the content SHA-256 reported by the server.
func StreamBackup(ctx context.Context, client clusterpb.ClusterServiceClient, w io.Writer) (uint64, string, error) {
	stream, err := client.Backup(ctx, &clusterpb.BackupRequest{})
	if err != nil {
		return 0, "", err
	}

	var (
		totalBytes    uint64
		contentSha256 string
	)
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, "", err
		}

		if len(resp.GetData()) > 0 {
			if _, err := w.Write(resp.GetData()); err != nil {
				return 0, "", err
			}
			totalBytes += uint64(len(resp.GetData()))
		}

		if resp.GetEof() {
			contentSha256 = resp.GetContentSha256()

			break
		}
	}

	return totalBytes, contentSha256, nil
}

// BackupData holds the raw backup archive and its SHA-256 hash.
type BackupData struct {
	Data []byte
	Hash string
}

// BackupToBuffer runs the Backup RPC and captures the full tar archive in memory.
// It verifies the SHA-256 hash matches the server-reported value.
func BackupToBuffer(ctx context.Context, client clusterpb.ClusterServiceClient) (*BackupData, error) {
	stream, err := client.Backup(ctx, &clusterpb.BackupRequest{})
	if err != nil {
		return nil, fmt.Errorf("backup RPC: %w", err)
	}

	var buf bytes.Buffer
	hash := sha256.New()

	var contentSha256 string
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("backup recv: %w", err)
		}

		if len(resp.GetData()) > 0 {
			buf.Write(resp.GetData())
			_, _ = hash.Write(resp.GetData())
		}

		if resp.GetEof() {
			contentSha256 = resp.GetContentSha256()

			break
		}
	}

	actualHash := hex.EncodeToString(hash.Sum(nil))
	if actualHash != contentSha256 {
		return nil, fmt.Errorf("backup hash mismatch: got %s, server reported %s", actualHash, contentSha256)
	}

	return &BackupData{Data: buf.Bytes(), Hash: contentSha256}, nil
}

// ---------------------------------------------------------------------------
// Prepared Query actions (via gRPC direct)
// ---------------------------------------------------------------------------

// CreatePreparedQuery creates a prepared query via the gRPC API.
func CreatePreparedQuery(ctx context.Context, client servicepb.BucketServiceClient, name, ledger string, target commonpb.QueryTarget, filter *commonpb.QueryFilter) error {
	_, err := client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
		Query: &commonpb.PreparedQuery{
			Name:   name,
			Ledger: ledger,
			Target: target,
			Filter: filter,
		},
	})

	return err
}

// UpdatePreparedQuery updates the filter of an existing prepared query.
func UpdatePreparedQuery(ctx context.Context, client servicepb.BucketServiceClient, ledger, name string, filter *commonpb.QueryFilter) error {
	_, err := client.UpdatePreparedQuery(ctx, &servicepb.UpdatePreparedQueryRequest{
		Ledger: ledger,
		Name:   name,
		Filter: filter,
	})

	return err
}

// DeletePreparedQuery deletes a prepared query.
func DeletePreparedQuery(ctx context.Context, client servicepb.BucketServiceClient, ledger, name string) error {
	_, err := client.DeletePreparedQuery(ctx, &servicepb.DeletePreparedQueryRequest{
		Ledger: ledger,
		Name:   name,
	})

	return err
}

// ListPreparedQueries lists all prepared queries for a ledger.
func ListPreparedQueries(ctx context.Context, client servicepb.BucketServiceClient, ledger string) ([]*commonpb.PreparedQuery, error) {
	resp, err := client.ListPreparedQueries(ctx, &servicepb.ListPreparedQueriesRequest{
		Ledger: ledger,
	})
	if err != nil {
		return nil, err
	}

	return resp.GetQueries(), nil
}

// ExecutePreparedQuery executes a prepared query and returns the response.
func ExecutePreparedQuery(ctx context.Context, client servicepb.BucketServiceClient, ledger, queryName string, mode commonpb.QueryMode, pageSize uint32) (*servicepb.ExecutePreparedQueryResponse, error) {
	return client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
		Ledger:    ledger,
		QueryName: queryName,
		Mode:      mode,
		PageSize:  pageSize,
	})
}

// ---------------------------------------------------------------------------
// Index actions (via Apply)
// ---------------------------------------------------------------------------

// CreateBuiltinTxIndexAction creates an action for creating a builtin transaction index.
func CreateBuiltinTxIndexAction(ledger string, idx commonpb.TransactionBuiltinIndex) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_CreateIndex{
			CreateIndex: &servicepb.CreateIndexRequest{
				Ledger: ledger,
				Index: &servicepb.CreateIndexRequest_Transaction{
					Transaction: &commonpb.TransactionIndex{
						Kind: &commonpb.TransactionIndex_Builtin{
							Builtin: idx,
						},
					},
				},
			},
		},
	}
}

// DropBuiltinTxIndexAction creates an action for dropping a builtin transaction index.
func DropBuiltinTxIndexAction(ledger string, idx commonpb.TransactionBuiltinIndex) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_DropIndex{
			DropIndex: &servicepb.DropIndexRequest{
				Ledger: ledger,
				Index: &servicepb.DropIndexRequest_Transaction{
					Transaction: &commonpb.TransactionIndex{
						Kind: &commonpb.TransactionIndex_Builtin{
							Builtin: idx,
						},
					},
				},
			},
		},
	}
}

// CreateAccountMetadataIndexAction creates an action for creating an account metadata index.
func CreateAccountMetadataIndexAction(ledger, metadataKey string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_CreateIndex{
			CreateIndex: &servicepb.CreateIndexRequest{
				Ledger: ledger,
				Index: &servicepb.CreateIndexRequest_Account{
					Account: &commonpb.AccountIndex{
						Kind: &commonpb.AccountIndex_MetadataKey{
							MetadataKey: metadataKey,
						},
					},
				},
			},
		},
	}
}

// DropAccountMetadataIndexAction creates an action for dropping an account metadata index.
func DropAccountMetadataIndexAction(ledger, metadataKey string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_DropIndex{
			DropIndex: &servicepb.DropIndexRequest{
				Ledger: ledger,
				Index: &servicepb.DropIndexRequest_Account{
					Account: &commonpb.AccountIndex{
						Kind: &commonpb.AccountIndex_MetadataKey{
							MetadataKey: metadataKey,
						},
					},
				},
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Archive action (via Apply)
// ---------------------------------------------------------------------------

// ArchivePeriodAction creates an action for archiving a closed period.
func ArchivePeriodAction(periodID uint64) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_ArchivePeriod{
			ArchivePeriod: &servicepb.ArchivePeriodRequest{
				PeriodId: periodID,
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Analytics (streaming RPCs)
// ---------------------------------------------------------------------------

// AnalyzeAccounts runs the AnalyzeAccounts streaming RPC and returns the final result.
func AnalyzeAccounts(ctx context.Context, client servicepb.BucketServiceClient, ledger string, variableThreshold uint32) (*servicepb.AnalyzeAccountsResponse, error) {
	stream, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
		Ledger:            ledger,
		VariableThreshold: variableThreshold,
	})
	if err != nil {
		return nil, err
	}

	var result *servicepb.AnalyzeAccountsResponse
	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if r := event.GetResult(); r != nil {
			result = r
		}
	}

	return result, nil
}

// AnalyzeTransactions runs the AnalyzeTransactions streaming RPC and returns the final result.
func AnalyzeTransactions(ctx context.Context, client servicepb.BucketServiceClient, ledger string) (*servicepb.AnalyzeTransactionsResponse, error) {
	stream, err := client.AnalyzeTransactions(ctx, &servicepb.AnalyzeTransactionsRequest{
		Ledger: ledger,
	})
	if err != nil {
		return nil, err
	}

	var result *servicepb.AnalyzeTransactionsResponse
	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if r := event.GetResult(); r != nil {
			result = r
		}
	}

	return result, nil
}

// ---------------------------------------------------------------------------
// Single entity reads
// ---------------------------------------------------------------------------

// GetLog retrieves a single log entry by sequence number.
func GetLog(ctx context.Context, client servicepb.BucketServiceClient, sequence uint64) (*commonpb.Log, error) {
	return client.GetLog(ctx, &servicepb.GetLogRequest{
		Sequence: sequence,
	})
}

// GetAuditEntry retrieves a single audit entry by sequence number.
func GetAuditEntry(ctx context.Context, client servicepb.BucketServiceClient, sequence uint64) (*auditpb.AuditEntry, error) {
	return client.GetAuditEntry(ctx, &servicepb.GetAuditEntryRequest{
		Sequence: sequence,
	})
}

// ---------------------------------------------------------------------------
// Monitoring / Discovery
// ---------------------------------------------------------------------------

// Discovery calls the Discovery RPC.
func Discovery(ctx context.Context, client servicepb.BucketServiceClient) (*servicepb.DiscoveryResponse, error) {
	return client.Discovery(ctx, &servicepb.DiscoveryRequest{})
}

// GetStoreMetrics calls the GetStoreMetrics RPC.
func GetStoreMetrics(ctx context.Context, client servicepb.BucketServiceClient) (*servicepb.GetStoreMetricsResponse, error) {
	return client.GetStoreMetrics(ctx, &servicepb.GetStoreMetricsRequest{})
}

// GetReadIndexMetrics calls the GetReadIndexMetrics RPC.
func GetReadIndexMetrics(ctx context.Context, client servicepb.BucketServiceClient) (*servicepb.GetReadIndexMetricsResponse, error) {
	return client.GetReadIndexMetrics(ctx, &servicepb.GetReadIndexMetricsRequest{})
}

// GetIndexStatus calls the GetIndexStatus RPC.
func GetIndexStatus(ctx context.Context, client servicepb.BucketServiceClient) (*servicepb.GetIndexStatusResponse, error) {
	return client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{})
}

// ---------------------------------------------------------------------------
// List with filters (uses streaming RPCs with params)
// ---------------------------------------------------------------------------

// ListAccountsFiltered collects accounts with pagination and filter params.
func ListAccountsFiltered(ctx context.Context, client servicepb.BucketServiceClient, ledger string, pageSize uint32, afterAddress string, filter *commonpb.QueryFilter) ([]*commonpb.Account, error) {
	stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{
		Ledger:       ledger,
		PageSize:     pageSize,
		AfterAddress: afterAddress,
		Filter:       filter,
	})
	if err != nil {
		return nil, err
	}

	var accounts []*commonpb.Account
	for {
		account, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, account)
	}

	return accounts, nil
}

// ListTransactionsFiltered collects transactions with pagination and filter params.
func ListTransactionsFiltered(ctx context.Context, client servicepb.BucketServiceClient, ledger string, pageSize uint32, afterTxID uint64, filter *commonpb.QueryFilter) ([]*commonpb.Transaction, error) {
	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger:    ledger,
		PageSize:  pageSize,
		AfterTxId: afterTxID,
		Filter:    filter,
	})
	if err != nil {
		return nil, err
	}

	var transactions []*commonpb.Transaction
	for {
		tx, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// ---------------------------------------------------------------------------
// Query filter builders
// ---------------------------------------------------------------------------

// StringMetadataFilter creates a filter matching a metadata string field with an exact value.
func StringMetadataFilter(key, value string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: key},
				Condition: &commonpb.FieldCondition_StringCond{
					StringCond: &commonpb.StringCondition{
						Value: &commonpb.StringCondition_Hardcoded{
							Hardcoded: value,
						},
					},
				},
			},
		},
	}
}

// AddressPrefixFilter creates a filter matching accounts by address prefix.
func AddressPrefixFilter(prefix string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Address{
			Address: &commonpb.AddressMatch{
				Match: &commonpb.AddressMatch_HardcodedPrefix{
					HardcodedPrefix: prefix,
				},
			},
		},
	}
}

// AddressExactFilter creates a filter matching accounts by exact address.
func AddressExactFilter(addr string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Address{
			Address: &commonpb.AddressMatch{
				Match: &commonpb.AddressMatch_HardcodedExact{
					HardcodedExact: addr,
				},
			},
		},
	}
}

// ReferenceFilter creates a filter matching transactions by reference (exact match).
func ReferenceFilter(ref string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Reference{
			Reference: &commonpb.ReferenceCondition{
				Cond: &commonpb.StringCondition{
					Value: &commonpb.StringCondition_Hardcoded{
						Hardcoded: ref,
					},
				},
			},
		},
	}
}

// AndFilter creates a logical AND filter combining multiple filters.
func AndFilter(filters ...*commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_And{
			And: &commonpb.AndFilter{Filters: filters},
		},
	}
}

// OrFilter creates a logical OR filter combining multiple filters.
func OrFilter(filters ...*commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Or{
			Or: &commonpb.OrFilter{Filters: filters},
		},
	}
}

// NotFilter creates a logical NOT filter.
func NotFilter(f *commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Not{
			Not: &commonpb.NotFilter{Filter: f},
		},
	}
}

// LedgerFilter creates a filter matching entries by ledger name.
func LedgerFilter(ledger string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Ledger{
			Ledger: &commonpb.LedgerCondition{
				Cond: &commonpb.StringCondition{
					Value: &commonpb.StringCondition_Hardcoded{
						Hardcoded: ledger,
					},
				},
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Parameterized filter builders (for prepared queries with runtime parameters)
// ---------------------------------------------------------------------------

// ParamAddressPrefixFilter creates a filter matching accounts by a parameterized address prefix.
// The actual prefix value is supplied at execution time via parameters map.
func ParamAddressPrefixFilter(paramName string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Address{
			Address: &commonpb.AddressMatch{
				Match: &commonpb.AddressMatch_ParamPrefix{
					ParamPrefix: paramName,
				},
			},
		},
	}
}

// ParamAddressExactFilter creates a filter matching accounts by a parameterized exact address.
func ParamAddressExactFilter(paramName string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Address{
			Address: &commonpb.AddressMatch{
				Match: &commonpb.AddressMatch_ParamExact{
					ParamExact: paramName,
				},
			},
		},
	}
}

// ParamStringMetadataFilter creates a filter matching a metadata string field with a parameterized value.
func ParamStringMetadataFilter(key, paramName string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: key},
				Condition: &commonpb.FieldCondition_StringCond{
					StringCond: &commonpb.StringCondition{
						Value: &commonpb.StringCondition_Param{
							Param: paramName,
						},
					},
				},
			},
		},
	}
}

// ParamBoolMetadataFilter creates a filter matching a metadata bool field with a parameterized value.
func ParamBoolMetadataFilter(key, paramName string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: key},
				Condition: &commonpb.FieldCondition_BoolCond{
					BoolCond: &commonpb.BoolCondition{
						Value: &commonpb.BoolCondition_Param{
							Param: paramName,
						},
					},
				},
			},
		},
	}
}

// ParamInt64RangeMetadataFilter creates a filter matching a metadata int64 field
// with parameterized min/max bounds.
func ParamInt64RangeMetadataFilter(key, paramMin, paramMax string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: key},
				Condition: &commonpb.FieldCondition_IntCond{
					IntCond: &commonpb.IntCondition{
						ParamMin: paramMin,
						ParamMax: paramMax,
					},
				},
			},
		},
	}
}

// Int64RangeMetadataFilter creates a filter matching a metadata int64 field
// with hardcoded min/max bounds.
func Int64RangeMetadataFilter(key string, minVal, maxVal *int64) *commonpb.QueryFilter {
	cond := &commonpb.IntCondition{}
	if minVal != nil {
		cond.Min = minVal
	}
	if maxVal != nil {
		cond.Max = maxVal
	}

	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: key},
				Condition: &commonpb.FieldCondition_IntCond{
					IntCond: cond,
				},
			},
		},
	}
}

// BoolMetadataFilter creates a filter matching a metadata bool field with a hardcoded value.
func BoolMetadataFilter(key string, val bool) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: key},
				Condition: &commonpb.FieldCondition_BoolCond{
					BoolCond: &commonpb.BoolCondition{
						Value: &commonpb.BoolCondition_Hardcoded{
							Hardcoded: val,
						},
					},
				},
			},
		},
	}
}

// ExecutePreparedQueryWithParams executes a prepared query with runtime parameters.
func ExecutePreparedQueryWithParams(ctx context.Context, client servicepb.BucketServiceClient, ledger, queryName string, mode commonpb.QueryMode, pageSize uint32, params map[string]*commonpb.ParameterValue) (*servicepb.ExecutePreparedQueryResponse, error) {
	return client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
		Ledger:     ledger,
		QueryName:  queryName,
		Mode:       mode,
		PageSize:   pageSize,
		Parameters: params,
	})
}

// StringParam creates a ParameterValue with a string value.
func StringParam(s string) *commonpb.ParameterValue {
	return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_StringValue{StringValue: s}}
}

// Int64Param creates a ParameterValue with an int64 value.
func Int64Param(v int64) *commonpb.ParameterValue {
	return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Int64Value{Int64Value: v}}
}

// Uint64Param creates a ParameterValue with a uint64 value.
func Uint64Param(v uint64) *commonpb.ParameterValue {
	return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: v}}
}

// BoolParam creates a ParameterValue with a bool value.
func BoolParam(v bool) *commonpb.ParameterValue {
	return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_BoolValue{BoolValue: v}}
}

// UploadAndFinalizeRestore uploads a backup to a restore-mode server, validates it,
// and finalizes the restore. The caller must start the server with --restore before
// calling this, and restart it normally after.
func UploadAndFinalizeRestore(ctx context.Context, restoreClient restorepb.RestoreServiceClient, backup *BackupData) error {
	// Upload in 64KB chunks.
	stream, err := restoreClient.UploadBackup(ctx)
	if err != nil {
		return fmt.Errorf("upload backup: %w", err)
	}

	const chunkSize = 64 * 1024
	for offset := 0; offset < len(backup.Data); offset += chunkSize {
		end := min(offset+chunkSize, len(backup.Data))
		if err := stream.Send(&restorepb.UploadBackupRequest{
			Data: backup.Data[offset:end],
		}); err != nil {
			return fmt.Errorf("upload send chunk: %w", err)
		}
	}

	if err := stream.Send(&restorepb.UploadBackupRequest{
		Eof:           true,
		ContentSha256: backup.Hash,
	}); err != nil {
		return fmt.Errorf("upload send EOF: %w", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("upload close: %w", err)
	}
	if resp.GetSha256() != backup.Hash {
		return fmt.Errorf("upload hash mismatch: got %s, expected %s", resp.GetSha256(), backup.Hash)
	}

	// Validate.
	valStream, err := restoreClient.ValidateRestore(ctx, &restorepb.ValidateRestoreRequest{})
	if err != nil {
		return fmt.Errorf("validate restore: %w", err)
	}

	var validationErrors []string
	for {
		event, err := valStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("validate recv: %w", err)
		}
		if e := event.GetError(); e != nil {
			validationErrors = append(validationErrors, e.GetMessage())
		}
	}
	if len(validationErrors) > 0 {
		return fmt.Errorf("validation errors: %v", validationErrors)
	}

	// Finalize.
	if _, err := restoreClient.FinalizeRestore(ctx, &restorepb.FinalizeRestoreRequest{}); err != nil {
		return fmt.Errorf("finalize restore: %w", err)
	}

	return nil
}
