package actions

import (
	"crypto/ed25519"
	"math/big"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
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
func SaveNumscriptAction(ledger, name, content string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SaveNumscript{
			SaveNumscript: &servicepb.SaveNumscriptRequest{
				Ledger:  ledger,
				Name:    name,
				Content: content,
			},
		},
	}
}

// SaveNumscriptWithVersionAction creates an action for saving a numscript with a specific version.
func SaveNumscriptWithVersionAction(ledger, name, content, version string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SaveNumscript{
			SaveNumscript: &servicepb.SaveNumscriptRequest{
				Ledger:  ledger,
				Name:    name,
				Content: content,
				Version: version,
			},
		},
	}
}

// DeleteNumscriptAction creates an action for deleting a numscript from the library.
func DeleteNumscriptAction(ledger, name string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_DeleteNumscript{
			DeleteNumscript: &servicepb.DeleteNumscriptRequest{
				Ledger: ledger,
				Name:   name,
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

// CreatePreparedQueryAction creates an action for creating a prepared query.
func CreatePreparedQueryAction(name, ledger string, target commonpb.QueryTarget, filter *commonpb.QueryFilter) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_CreatePreparedQuery{
			CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   name,
					Ledger: ledger,
					Target: target,
					Filter: filter,
				},
			},
		},
	}
}

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

// GetCreatedTransactionID extracts the first created transaction ID from an ApplyResponse.
// Returns (id, true) on success or (0, false) if no transaction was found.
func GetCreatedTransactionID(resp *servicepb.ApplyResponse) (uint64, bool) {
	if len(resp.GetLogs()) == 0 {
		return 0, false
	}
	applyLog := resp.GetLogs()[0].GetPayload().GetApply()
	if applyLog == nil {
		return 0, false
	}
	tx := applyLog.GetLog().GetData().GetCreatedTransaction()
	if tx == nil {
		return 0, false
	}

	return tx.GetTransaction().GetId(), true
}
