package actions

import (
	"crypto/ed25519"
	"math/big"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/protohelpers"
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
func CreateLedgerAction(name string, _ map[string]string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_CreateLedger{
			CreateLedger: &commonpb.CreateLedgerRequest{
				Name: name,
			},
		},
	}
}

// DeleteLedgerAction creates an action for deleting a ledger.
func DeleteLedgerAction(ledgerName string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_DeleteLedger{
			DeleteLedger: &commonpb.DeleteLedgerRequest{
				Name: ledgerName,
			},
		},
	}
}

// CreateTransactionAction creates an action for creating a transaction.
func CreateTransactionAction(ledgerName string, postings []*commonpb.Posting, metadata map[string]string, accountMetadata map[string]*commonpb.MetadataMap) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_CreateTransaction{
					CreateTransaction: &commonpb.CreateTransactionPayload{
						Postings:        postings,
						Metadata:        protohelpers.MetadataFromGoMap(metadata),
						AccountMetadata: accountMetadata,
					},
				}},
			},
		},
	}
}

// CreateForceTransactionAction creates an action for creating a transaction with force=true (bypasses balance checks).
func CreateForceTransactionAction(ledgerName string, postings []*commonpb.Posting, metadata map[string]string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_CreateTransaction{
					CreateTransaction: &commonpb.CreateTransactionPayload{
						Postings: postings,
						Metadata: protohelpers.MetadataFromGoMap(metadata),
						Force:    true,
					},
				}},
			},
		},
	}
}

// CreateForceScriptTransactionAction creates an action for creating a transaction using Numscript with force=true.
func CreateForceScriptTransactionAction(ledgerName string, script string, vars map[string]string, metadata map[string]string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_CreateTransaction{
					CreateTransaction: &commonpb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: script,
							Vars:  vars,
						},
						Metadata: protohelpers.MetadataFromGoMap(metadata),
						Force:    true,
					},
				}},
			},
		},
	}
}

// CreateScriptTransactionAction creates an action for creating a transaction using Numscript.
func CreateScriptTransactionAction(ledgerName string, script string, vars map[string]string, metadata map[string]string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_CreateTransaction{
					CreateTransaction: &commonpb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: script,
							Vars:  vars,
						},
						Metadata: protohelpers.MetadataFromGoMap(metadata),
					},
				}},
			},
		},
	}
}

// AddAccountTypeAction creates an action for adding an account type to a ledger.
func AddAccountTypeAction(ledgerName, name, pattern string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_AddAccountType{
			AddAccountType: &commonpb.AddAccountTypeLedgerRequest{
				Ledger: ledgerName,
				AccountType: &commonpb.AccountType{
					Name:    name,
					Pattern: pattern,
				},
			},
		},
	}
}

// AddEphemeralAccountTypeAction creates an action for adding an ephemeral account type to a ledger.
// Ephemeral accounts have their volumes purged when input == output (zero balance).
func AddEphemeralAccountTypeAction(ledgerName, name, pattern string) *commonpb.Request {
	return AddAccountTypeWithPersistenceAction(ledgerName, name, pattern, commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL)
}

// AddAccountTypeWithPersistenceAction creates an action for adding an account type with a specific persistence mode.
func AddAccountTypeWithPersistenceAction(ledgerName, name, pattern string, persistence commonpb.AccountTypePersistence) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_AddAccountType{
			AddAccountType: &commonpb.AddAccountTypeLedgerRequest{
				Ledger: ledgerName,
				AccountType: &commonpb.AccountType{
					Name:        name,
					Pattern:     pattern,
					Persistence: persistence,
				},
			},
		},
	}
}

// RemoveAccountTypeAction creates an action for removing an account type.
func RemoveAccountTypeAction(ledgerName, name string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_RemoveAccountType{
			RemoveAccountType: &commonpb.RemoveAccountTypeLedgerRequest{
				Ledger: ledgerName,
				Name:   name,
			},
		},
	}
}

// SaveAccountMetadataAction creates an action for saving account metadata.
func SaveAccountMetadataAction(ledgerName, address string, metadata map[string]string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: address},
							},
						},
						Metadata: protohelpers.MetadataFromGoMap(metadata),
					},
				}},
			},
		},
	}
}

// DeleteAccountMetadataAction creates an action for deleting account metadata.
func DeleteAccountMetadataAction(ledgerName, address, key string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: address},
							},
						},
						Key: key,
					},
				}},
			},
		},
	}
}

// SaveTransactionMetadataAction creates an action for saving transaction metadata.
func SaveTransactionMetadataAction(ledgerName string, transactionID uint64, metadata map[string]string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_TransactionId{TransactionId: transactionID},
						},
						Metadata: protohelpers.MetadataFromGoMap(metadata),
					},
				}},
			},
		},
	}
}

// DeleteTransactionMetadataAction creates an action for deleting transaction metadata.
func DeleteTransactionMetadataAction(ledgerName string, transactionID uint64, key string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_TransactionId{TransactionId: transactionID},
						},
						Key: key,
					},
				}},
			},
		},
	}
}

// SaveLedgerMetadataAction creates an action for saving ledger metadata.
func SaveLedgerMetadataAction(ledgerName string, metadata map[string]string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_SaveLedgerMetadata{
			SaveLedgerMetadata: &commonpb.SaveLedgerMetadataRequest{
				Ledger:   ledgerName,
				Metadata: protohelpers.MetadataFromGoMap(metadata),
			},
		},
	}
}

// DeleteLedgerMetadataAction creates an action for deleting a ledger metadata key.
func DeleteLedgerMetadataAction(ledgerName, key string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_DeleteLedgerMetadata{
			DeleteLedgerMetadata: &commonpb.DeleteLedgerMetadataRequest{
				Ledger: ledgerName,
				Key:    key,
			},
		},
	}
}

// RevertTransactionAction creates an action for reverting a transaction.
func RevertTransactionAction(ledgerName string, transactionID uint64, force, atEffectiveDate bool, metadata map[string]string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_RevertTransaction{
					RevertTransaction: &commonpb.RevertTransactionPayload{
						TransactionId:   transactionID,
						Force:           force,
						AtEffectiveDate: atEffectiveDate,
						Metadata:        protohelpers.MetadataFromGoMap(metadata),
					},
				}},
			},
		},
	}
}

// WithTimestamp sets the timestamp on a create transaction request.
func WithTimestamp(req *commonpb.Request, t time.Time) *commonpb.Request {
	if reqType, ok := req.GetType().(*commonpb.Request_Apply); ok {
		if d, ok := reqType.Apply.GetAction().GetData().(*commonpb.LedgerAction_CreateTransaction); ok {
			d.CreateTransaction.Timestamp = &commonpb.Timestamp{Data: uint64(t.UnixMicro())}
		}
	}

	return req
}

// NewPosting creates a new uncolored posting protobuf message.
func NewPosting(source, destination string, amount *big.Int, asset string) *commonpb.Posting {
	return protohelpers.NewPosting(source, destination, asset, amount)
}

// NewColoredPosting creates a new posting with an explicit color.
func NewColoredPosting(source, destination string, amount *big.Int, asset, color string) *commonpb.Posting {
	return protohelpers.NewColoredPosting(source, destination, asset, color, amount)
}

// RegisterSigningKeyAction creates a RegisterSigningKey request.
func RegisterSigningKeyAction(keyID string, pubKey ed25519.PublicKey) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_RegisterSigningKey{
			RegisterSigningKey: &commonpb.RegisterSigningKeyRequest{
				KeyId:     keyID,
				PublicKey: []byte(pubKey),
			},
		},
	}
}

// RevokeSigningKeyAction creates a RevokeSigningKey request.
func RevokeSigningKeyAction(keyID string, cascade bool) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_RevokeSigningKey{
			RevokeSigningKey: &commonpb.RevokeSigningKeyRequest{
				KeyId:   keyID,
				Cascade: cascade,
			},
		},
	}
}

// SetSigningConfigAction creates a SetSigningConfig request.
func SetSigningConfigAction(requireSignatures bool) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_SetSigningConfig{
			SetSigningConfig: &commonpb.SetSigningConfigRequest{
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

// FindMetadataValue looks up a key in a metadata map and returns the *MetadataValue (nil if not found).
func FindMetadataValue(m map[string]*commonpb.MetadataValue, key string) *commonpb.MetadataValue {
	if m == nil {
		return nil
	}

	return m[key]
}

// SetMaintenanceModeAction creates a request to enable or disable maintenance mode.
func SetMaintenanceModeAction(enabled bool) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_SetMaintenanceMode{
			SetMaintenanceMode: &commonpb.SetMaintenanceModeRequest{
				Enabled: enabled,
			},
		},
	}
}

// SetMetadataFieldTypeAction creates a request to declare a metadata field type.
func SetMetadataFieldTypeAction(ledger string, targetType commonpb.TargetType, key string, metadataType commonpb.MetadataType) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_SetMetadataFieldType{
			SetMetadataFieldType: &commonpb.SetMetadataFieldTypeRequest{
				Ledger:     ledger,
				TargetType: targetType,
				Key:        key,
				Type:       metadataType,
			},
		},
	}
}

// RemoveMetadataFieldTypeAction creates a request to remove a metadata field type declaration.
func RemoveMetadataFieldTypeAction(ledger string, targetType commonpb.TargetType, key string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_RemoveMetadataFieldType{
			RemoveMetadataFieldType: &commonpb.RemoveMetadataFieldTypeRequest{
				Ledger:     ledger,
				TargetType: targetType,
				Key:        key,
			},
		},
	}
}

// CreateLedgerWithSchemaAction creates a ledger with an initial metadata schema.
func CreateLedgerWithSchemaAction(name string, _ map[string]string, schema []*commonpb.SetMetadataFieldTypeCommand) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_CreateLedger{
			CreateLedger: &commonpb.CreateLedgerRequest{
				Name:          name,
				InitialSchema: schema,
			},
		},
	}
}

// CreateLedgerWithAccountTypesAction creates a ledger with account types declared
// at creation time (CreateLedgerRequest.account_types).
func CreateLedgerWithAccountTypesAction(name string, accountTypes map[string]*commonpb.AccountType) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_CreateLedger{
			CreateLedger: &commonpb.CreateLedgerRequest{
				Name:         name,
				AccountTypes: accountTypes,
			},
		},
	}
}

// SaveTypedAccountMetadataAction creates a request with a typed metadata map (not map[string]string).
func SaveTypedAccountMetadataAction(ledgerName, address string, metadata map[string]*commonpb.MetadataValue) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: address},
							},
						},
						Metadata: metadata,
					},
				}},
			},
		},
	}
}

// SaveTypedTransactionMetadataAction creates a request with a typed metadata map (not map[string]string).
func SaveTypedTransactionMetadataAction(ledgerName string, txID uint64, metadata map[string]*commonpb.MetadataValue) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_TransactionId{TransactionId: txID},
						},
						Metadata: metadata,
					},
				}},
			},
		},
	}
}

// SaveNumscriptWithVersionAction creates an action for saving a numscript with a specific version.
func SaveNumscriptWithVersionAction(ledger, name, content, version string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_SaveNumscript{
			SaveNumscript: &commonpb.SaveNumscriptRequest{
				Ledger:  ledger,
				Name:    name,
				Content: content,
				Version: version,
			},
		},
	}
}

// CreateScriptRefTransactionAction creates a transaction using a script reference
// from the library. version is required: the literal "latest" or an exact full
// semver.
func CreateScriptRefTransactionAction(ledgerName, scriptName, version string, vars map[string]string, metadata map[string]string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_CreateTransaction{
					CreateTransaction: &commonpb.CreateTransactionPayload{
						ScriptReference: &commonpb.ScriptReference{
							Name:    scriptName,
							Version: version,
							Vars:    vars,
						},
						Metadata: protohelpers.MetadataFromGoMap(metadata),
					},
				}},
			},
		},
	}
}

// CreateBuiltinTxIndexAction creates an action for creating a builtin transaction index.
func CreateBuiltinTxIndexAction(ledger string, idx commonpb.TransactionBuiltinIndex) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_CreateIndex{
			CreateIndex: &commonpb.CreateIndexRequest{
				Ledger: ledger,
				Id:     &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: idx}},
			},
		},
	}
}

// DropBuiltinTxIndexAction creates an action for dropping a builtin transaction index.
func DropBuiltinTxIndexAction(ledger string, idx commonpb.TransactionBuiltinIndex) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_DropIndex{
			DropIndex: &commonpb.DropIndexRequest{
				Ledger: ledger,
				Id:     &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: idx}},
			},
		},
	}
}

// CreateAccountMetadataIndexAction creates an action for creating an account metadata index.
func CreateAccountMetadataIndexAction(ledger, metadataKey string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_CreateIndex{
			CreateIndex: &commonpb.CreateIndexRequest{
				Ledger: ledger,
				Id:     metadataIndexID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, metadataKey),
			},
		},
	}
}

// DropAccountMetadataIndexAction creates an action for dropping an account metadata index.
func DropAccountMetadataIndexAction(ledger, metadataKey string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_DropIndex{
			DropIndex: &commonpb.DropIndexRequest{
				Ledger: ledger,
				Id:     metadataIndexID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, metadataKey),
			},
		},
	}
}

// CreateTransactionMetadataIndexAction creates an action for creating a transaction metadata index.
func CreateTransactionMetadataIndexAction(ledger, metadataKey string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_CreateIndex{
			CreateIndex: &commonpb.CreateIndexRequest{
				Ledger: ledger,
				Id:     metadataIndexID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, metadataKey),
			},
		},
	}
}

// DropTransactionMetadataIndexAction creates an action for dropping a transaction metadata index.
func DropTransactionMetadataIndexAction(ledger, metadataKey string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_DropIndex{
			DropIndex: &commonpb.DropIndexRequest{
				Ledger: ledger,
				Id:     metadataIndexID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, metadataKey),
			},
		},
	}
}

func metadataIndexID(target commonpb.TargetType, key string) *commonpb.IndexID {
	return &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{
		Target: target,
		Key:    key,
	}}}
}

// CreatePreparedQueryAction creates an action for creating a prepared query.
func CreatePreparedQueryAction(name, ledger string, target commonpb.QueryTarget, filter *commonpb.QueryFilter) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_CreatePreparedQuery{
			CreatePreparedQuery: &commonpb.CreatePreparedQueryRequest{
				Ledger: ledger,
				Query: &commonpb.PreparedQuery{
					Name:   name,
					Target: target,
					Filter: filter,
				},
			},
		},
	}
}

// UpdatePreparedQueryAction creates an action for updating a prepared query's filter.
func UpdatePreparedQueryAction(ledger, name string, filter *commonpb.QueryFilter) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_UpdatePreparedQuery{
			UpdatePreparedQuery: &commonpb.UpdatePreparedQueryRequest{
				Ledger: ledger,
				Name:   name,
				Filter: filter,
			},
		},
	}
}

// DeletePreparedQueryAction creates an action for removing a prepared query.
func DeletePreparedQueryAction(ledger, name string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_DeletePreparedQuery{
			DeletePreparedQuery: &commonpb.DeletePreparedQueryRequest{
				Ledger: ledger,
				Name:   name,
			},
		},
	}
}

// CreateQueryCheckpointAction creates an action for taking a query checkpoint.
// The checkpoint is a batch trigger: admission accepts it only as the last
// action of a batch. With response payloads enabled, Apply waits until the read
// index checkpoint is materialized on the serving node, unless deleted meanwhile.
// An idempotent replay returns the historical result without waiting or recreating
// the checkpoint; it makes no current readiness/existence guarantee. With skip_response,
// only the leader is guaranteed ready; a forwarding follower skips its local
// wait because the leader has already stripped the checkpoint ID.
func CreateQueryCheckpointAction() *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_CreateQueryCheckpoint{
			CreateQueryCheckpoint: &commonpb.CreateQueryCheckpointRequest{},
		},
	}
}

// DeleteQueryCheckpointAction creates an action for removing a query checkpoint.
func DeleteQueryCheckpointAction(checkpointID uint64) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_DeleteQueryCheckpoint{
			DeleteQueryCheckpoint: &commonpb.DeleteQueryCheckpointRequest{
				CheckpointId: checkpointID,
			},
		},
	}
}

// WithReference sets the reference on a create transaction request.
func WithReference(req *commonpb.Request, reference string) *commonpb.Request {
	if reqType, ok := req.GetType().(*commonpb.Request_Apply); ok {
		if d, ok := reqType.Apply.GetAction().GetData().(*commonpb.LedgerAction_CreateTransaction); ok {
			d.CreateTransaction.Reference = reference
		}
	}

	return req
}

// WithSkippableReasons sets the per-apply skippable_reasons whitelist on an
// Apply request. Each reason is a business-level error the caller accepts to
// see converted into an OrderSkippedLog instead of failing the proposal.
// Validated at admission against a per-action whitelist.
func WithSkippableReasons(req *commonpb.Request, reasons ...commonpb.ErrorReason) *commonpb.Request {
	if reqType, ok := req.GetType().(*commonpb.Request_Apply); ok {
		reqType.Apply.SkippableReasons = reasons
	}

	return req
}

// WithIdempotencyKey wraps requests into an unsigned ApplyRequest under the
// given idempotency key — idempotency is keyed per atomic batch.
func WithIdempotencyKey(key string, reqs ...*commonpb.Request) *commonpb.ApplyRequest {
	return commonpb.UnsignedApplyRequest(key, reqs...)
}

// GetCreatedTransactionID extracts the first created transaction ID from an ApplyResponse.
// Returns (id, true) on success or (0, false) if no transaction was found.
func GetCreatedTransactionID(resp *commonpb.ApplyResponse) (uint64, bool) {
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

// GetCreatedQueryCheckpoint extracts the checkpoint ID and the max global log
// sequence of the checkpoint created by this batch. The checkpoint trigger is
// always the last action of a batch, but the log carrying it is located by
// payload type rather than by position. Returns (0, 0, false) when the batch
// created no checkpoint, including when the caller set skip_response.
func GetCreatedQueryCheckpoint(resp *commonpb.ApplyResponse) (checkpointID, maxSequence uint64, ok bool) {
	for _, entry := range resp.GetLogs() {
		cp := entry.GetPayload().GetCreatedQueryCheckpoint()
		if cp == nil {
			continue
		}

		return cp.GetCheckpointId(), cp.GetMaxSequence(), true
	}

	return 0, 0, false
}

// GetAllCreatedTransactionIDs extracts all created transaction IDs from a batched ApplyResponse.
func GetAllCreatedTransactionIDs(resp *commonpb.ApplyResponse) []uint64 {
	var ids []uint64
	for _, entry := range resp.GetLogs() {
		applyLog := entry.GetPayload().GetApply()
		if applyLog == nil {
			continue
		}
		tx := applyLog.GetLog().GetData().GetCreatedTransaction()
		if tx == nil {
			continue
		}
		ids = append(ids, tx.GetTransaction().GetId())
	}

	return ids
}
