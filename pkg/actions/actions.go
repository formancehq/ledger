package actions

import (
	"crypto/ed25519"
	"math/big"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

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
func CreateLedgerAction(name string, _ map[string]string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_CreateLedger{
			CreateLedger: &ledgerpb.CreateLedgerRequest{
				Name: name,
			},
		},
	}
}

// DeleteLedgerAction creates an action for deleting a ledger.
func DeleteLedgerAction(ledgerName string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_DeleteLedger{
			DeleteLedger: &ledgerpb.DeleteLedgerRequest{
				Name: ledgerName,
			},
		},
	}
}

// CreateTransactionAction creates an action for creating a transaction.
func CreateTransactionAction(ledgerName string, postings []*ledgerpb.Posting, metadata map[string]string, accountMetadata map[string]*ledgerpb.MetadataMap) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
					CreateTransaction: &ledgerpb.CreateTransactionPayload{
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
func CreateForceTransactionAction(ledgerName string, postings []*ledgerpb.Posting, metadata map[string]string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
					CreateTransaction: &ledgerpb.CreateTransactionPayload{
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
func CreateForceScriptTransactionAction(ledgerName string, script string, vars map[string]string, metadata map[string]string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
					CreateTransaction: &ledgerpb.CreateTransactionPayload{
						Script: &ledgerpb.Script{
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
func CreateScriptTransactionAction(ledgerName string, script string, vars map[string]string, metadata map[string]string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
					CreateTransaction: &ledgerpb.CreateTransactionPayload{
						Script: &ledgerpb.Script{
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
func AddAccountTypeAction(ledgerName, name, pattern string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_AddAccountType{
			AddAccountType: &ledgerpb.AddAccountTypeLedgerRequest{
				Ledger: ledgerName,
				AccountType: &ledgerpb.AccountType{
					Name:    name,
					Pattern: pattern,
				},
			},
		},
	}
}

// AddEphemeralAccountTypeAction creates an action for adding an ephemeral account type to a ledger.
// Ephemeral accounts have their volumes purged when input == output (zero balance).
func AddEphemeralAccountTypeAction(ledgerName, name, pattern string) *ledgerpb.Request {
	return AddAccountTypeWithPersistenceAction(ledgerName, name, pattern, ledgerpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL)
}

// AddAccountTypeWithPersistenceAction creates an action for adding an account type with a specific persistence mode.
func AddAccountTypeWithPersistenceAction(ledgerName, name, pattern string, persistence ledgerpb.AccountTypePersistence) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_AddAccountType{
			AddAccountType: &ledgerpb.AddAccountTypeLedgerRequest{
				Ledger: ledgerName,
				AccountType: &ledgerpb.AccountType{
					Name:        name,
					Pattern:     pattern,
					Persistence: persistence,
				},
			},
		},
	}
}

// RemoveAccountTypeAction creates an action for removing an account type.
func RemoveAccountTypeAction(ledgerName, name string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_RemoveAccountType{
			RemoveAccountType: &ledgerpb.RemoveAccountTypeLedgerRequest{
				Ledger: ledgerName,
				Name:   name,
			},
		},
	}
}

// SaveAccountMetadataAction creates an action for saving account metadata.
func SaveAccountMetadataAction(ledgerName, address string, metadata map[string]string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_AddMetadata{
					AddMetadata: &ledgerpb.SaveMetadataCommand{
						Target: &ledgerpb.Target{
							Target: &ledgerpb.Target_Account{
								Account: &ledgerpb.TargetAccount{Addr: address},
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
func DeleteAccountMetadataAction(ledgerName, address, key string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_DeleteMetadata{
					DeleteMetadata: &ledgerpb.DeleteMetadataCommand{
						Target: &ledgerpb.Target{
							Target: &ledgerpb.Target_Account{
								Account: &ledgerpb.TargetAccount{Addr: address},
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
func SaveTransactionMetadataAction(ledgerName string, transactionID uint64, metadata map[string]string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_AddMetadata{
					AddMetadata: &ledgerpb.SaveMetadataCommand{
						Target: &ledgerpb.Target{
							Target: &ledgerpb.Target_TransactionId{TransactionId: transactionID},
						},
						Metadata: protohelpers.MetadataFromGoMap(metadata),
					},
				}},
			},
		},
	}
}

// DeleteTransactionMetadataAction creates an action for deleting transaction metadata.
func DeleteTransactionMetadataAction(ledgerName string, transactionID uint64, key string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_DeleteMetadata{
					DeleteMetadata: &ledgerpb.DeleteMetadataCommand{
						Target: &ledgerpb.Target{
							Target: &ledgerpb.Target_TransactionId{TransactionId: transactionID},
						},
						Key: key,
					},
				}},
			},
		},
	}
}

// SaveLedgerMetadataAction creates an action for saving ledger metadata.
func SaveLedgerMetadataAction(ledgerName string, metadata map[string]string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_SaveLedgerMetadata{
			SaveLedgerMetadata: &ledgerpb.SaveLedgerMetadataRequest{
				Ledger:   ledgerName,
				Metadata: protohelpers.MetadataFromGoMap(metadata),
			},
		},
	}
}

// DeleteLedgerMetadataAction creates an action for deleting a ledger metadata key.
func DeleteLedgerMetadataAction(ledgerName, key string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_DeleteLedgerMetadata{
			DeleteLedgerMetadata: &ledgerpb.DeleteLedgerMetadataRequest{
				Ledger: ledgerName,
				Key:    key,
			},
		},
	}
}

// RevertTransactionAction creates an action for reverting a transaction.
func RevertTransactionAction(ledgerName string, transactionID uint64, force, atEffectiveDate bool, metadata map[string]string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_RevertTransaction{
					RevertTransaction: &ledgerpb.RevertTransactionPayload{
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
func WithTimestamp(req *ledgerpb.Request, t time.Time) *ledgerpb.Request {
	if reqType, ok := req.GetType().(*ledgerpb.Request_Apply); ok {
		if d, ok := reqType.Apply.GetAction().GetData().(*ledgerpb.LedgerAction_CreateTransaction); ok {
			d.CreateTransaction.Timestamp = &ledgerpb.Timestamp{Data: uint64(t.UnixMicro())}
		}
	}

	return req
}

// NewPosting creates a new uncolored posting protobuf message.
func NewPosting(source, destination string, amount *big.Int, asset string) *ledgerpb.Posting {
	return protohelpers.NewPosting(source, destination, asset, amount)
}

// NewColoredPosting creates a new posting with an explicit color.
func NewColoredPosting(source, destination string, amount *big.Int, asset, color string) *ledgerpb.Posting {
	return protohelpers.NewColoredPosting(source, destination, asset, color, amount)
}

// RegisterSigningKeyAction creates a RegisterSigningKey request.
func RegisterSigningKeyAction(keyID string, pubKey ed25519.PublicKey) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_RegisterSigningKey{
			RegisterSigningKey: &ledgerpb.RegisterSigningKeyRequest{
				KeyId:     keyID,
				PublicKey: []byte(pubKey),
			},
		},
	}
}

// RevokeSigningKeyAction creates a RevokeSigningKey request.
func RevokeSigningKeyAction(keyID string, cascade bool) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_RevokeSigningKey{
			RevokeSigningKey: &ledgerpb.RevokeSigningKeyRequest{
				KeyId:   keyID,
				Cascade: cascade,
			},
		},
	}
}

// SetSigningConfigAction creates a SetSigningConfig request.
func SetSigningConfigAction(requireSignatures bool) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_SetSigningConfig{
			SetSigningConfig: &ledgerpb.SetSigningConfigRequest{
				RequireSignatures: requireSignatures,
			},
		},
	}
}

// FindSigningKey finds a key by ID in a slice of signing keys. Returns nil if not found.
func FindSigningKey(keys []*ledgerpb.SigningKey, keyID string) *ledgerpb.SigningKey {
	for _, k := range keys {
		if k.GetKeyId() == keyID {
			return k
		}
	}

	return nil
}

// FindMetadataValue looks up a key in a metadata map and returns the *MetadataValue (nil if not found).
func FindMetadataValue(m map[string]*ledgerpb.MetadataValue, key string) *ledgerpb.MetadataValue {
	if m == nil {
		return nil
	}

	return m[key]
}

// SetMaintenanceModeAction creates a request to enable or disable maintenance mode.
func SetMaintenanceModeAction(enabled bool) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_SetMaintenanceMode{
			SetMaintenanceMode: &ledgerpb.SetMaintenanceModeRequest{
				Enabled: enabled,
			},
		},
	}
}

// SetMetadataFieldTypeAction creates a request to declare a metadata field type.
func SetMetadataFieldTypeAction(ledger string, targetType ledgerpb.TargetType, key string, metadataType ledgerpb.MetadataType) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_SetMetadataFieldType{
			SetMetadataFieldType: &ledgerpb.SetMetadataFieldTypeRequest{
				Ledger:     ledger,
				TargetType: targetType,
				Key:        key,
				Type:       metadataType,
			},
		},
	}
}

// RemoveMetadataFieldTypeAction creates a request to remove a metadata field type declaration.
func RemoveMetadataFieldTypeAction(ledger string, targetType ledgerpb.TargetType, key string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_RemoveMetadataFieldType{
			RemoveMetadataFieldType: &ledgerpb.RemoveMetadataFieldTypeRequest{
				Ledger:     ledger,
				TargetType: targetType,
				Key:        key,
			},
		},
	}
}

// CreateLedgerWithSchemaAction creates a ledger with an initial metadata schema.
func CreateLedgerWithSchemaAction(name string, _ map[string]string, schema []*ledgerpb.SetMetadataFieldTypeCommand) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_CreateLedger{
			CreateLedger: &ledgerpb.CreateLedgerRequest{
				Name:          name,
				InitialSchema: schema,
			},
		},
	}
}

// CreateLedgerWithAccountTypesAction creates a ledger with account types declared
// at creation time (CreateLedgerRequest.account_types).
func CreateLedgerWithAccountTypesAction(name string, accountTypes map[string]*ledgerpb.AccountType) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_CreateLedger{
			CreateLedger: &ledgerpb.CreateLedgerRequest{
				Name:         name,
				AccountTypes: accountTypes,
			},
		},
	}
}

// SaveTypedAccountMetadataAction creates a request with a typed metadata map (not map[string]string).
func SaveTypedAccountMetadataAction(ledgerName, address string, metadata map[string]*ledgerpb.MetadataValue) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_AddMetadata{
					AddMetadata: &ledgerpb.SaveMetadataCommand{
						Target: &ledgerpb.Target{
							Target: &ledgerpb.Target_Account{
								Account: &ledgerpb.TargetAccount{Addr: address},
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
func SaveTypedTransactionMetadataAction(ledgerName string, txID uint64, metadata map[string]*ledgerpb.MetadataValue) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_AddMetadata{
					AddMetadata: &ledgerpb.SaveMetadataCommand{
						Target: &ledgerpb.Target{
							Target: &ledgerpb.Target_TransactionId{TransactionId: txID},
						},
						Metadata: metadata,
					},
				}},
			},
		},
	}
}

// SaveNumscriptWithVersionAction creates an action for saving a numscript with a specific version.
func SaveNumscriptWithVersionAction(ledger, name, content, version string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_SaveNumscript{
			SaveNumscript: &ledgerpb.SaveNumscriptRequest{
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
func CreateScriptRefTransactionAction(ledgerName, scriptName, version string, vars map[string]string, metadata map[string]string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: ledgerName,
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
					CreateTransaction: &ledgerpb.CreateTransactionPayload{
						ScriptReference: &ledgerpb.ScriptReference{
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
func CreateBuiltinTxIndexAction(ledger string, idx ledgerpb.TransactionBuiltinIndex) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_CreateIndex{
			CreateIndex: &ledgerpb.CreateIndexRequest{
				Ledger: ledger,
				Id:     &ledgerpb.IndexID{Kind: &ledgerpb.IndexID_TxBuiltin{TxBuiltin: idx}},
			},
		},
	}
}

// DropBuiltinTxIndexAction creates an action for dropping a builtin transaction index.
func DropBuiltinTxIndexAction(ledger string, idx ledgerpb.TransactionBuiltinIndex) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_DropIndex{
			DropIndex: &ledgerpb.DropIndexRequest{
				Ledger: ledger,
				Id:     &ledgerpb.IndexID{Kind: &ledgerpb.IndexID_TxBuiltin{TxBuiltin: idx}},
			},
		},
	}
}

// CreateAccountMetadataIndexAction creates an action for creating an account metadata index.
func CreateAccountMetadataIndexAction(ledger, metadataKey string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_CreateIndex{
			CreateIndex: &ledgerpb.CreateIndexRequest{
				Ledger: ledger,
				Id:     metadataIndexID(ledgerpb.TargetType_TARGET_TYPE_ACCOUNT, metadataKey),
			},
		},
	}
}

// DropAccountMetadataIndexAction creates an action for dropping an account metadata index.
func DropAccountMetadataIndexAction(ledger, metadataKey string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_DropIndex{
			DropIndex: &ledgerpb.DropIndexRequest{
				Ledger: ledger,
				Id:     metadataIndexID(ledgerpb.TargetType_TARGET_TYPE_ACCOUNT, metadataKey),
			},
		},
	}
}

// CreateTransactionMetadataIndexAction creates an action for creating a transaction metadata index.
func CreateTransactionMetadataIndexAction(ledger, metadataKey string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_CreateIndex{
			CreateIndex: &ledgerpb.CreateIndexRequest{
				Ledger: ledger,
				Id:     metadataIndexID(ledgerpb.TargetType_TARGET_TYPE_TRANSACTION, metadataKey),
			},
		},
	}
}

// DropTransactionMetadataIndexAction creates an action for dropping a transaction metadata index.
func DropTransactionMetadataIndexAction(ledger, metadataKey string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_DropIndex{
			DropIndex: &ledgerpb.DropIndexRequest{
				Ledger: ledger,
				Id:     metadataIndexID(ledgerpb.TargetType_TARGET_TYPE_TRANSACTION, metadataKey),
			},
		},
	}
}

func metadataIndexID(target ledgerpb.TargetType, key string) *ledgerpb.IndexID {
	return &ledgerpb.IndexID{Kind: &ledgerpb.IndexID_Metadata{Metadata: &ledgerpb.MetadataIndexID{
		Target: target,
		Key:    key,
	}}}
}

// CreatePreparedQueryAction creates an action for creating a prepared query.
func CreatePreparedQueryAction(name, ledger string, target ledgerpb.QueryTarget, filter *ledgerpb.QueryFilter) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_CreatePreparedQuery{
			CreatePreparedQuery: &ledgerpb.CreatePreparedQueryRequest{
				Ledger: ledger,
				Query: &ledgerpb.PreparedQuery{
					Name:   name,
					Target: target,
					Filter: filter,
				},
			},
		},
	}
}

// UpdatePreparedQueryAction creates an action for updating a prepared query's filter.
func UpdatePreparedQueryAction(ledger, name string, filter *ledgerpb.QueryFilter) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_UpdatePreparedQuery{
			UpdatePreparedQuery: &ledgerpb.UpdatePreparedQueryRequest{
				Ledger: ledger,
				Name:   name,
				Filter: filter,
			},
		},
	}
}

// DeletePreparedQueryAction creates an action for removing a prepared query.
func DeletePreparedQueryAction(ledger, name string) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_DeletePreparedQuery{
			DeletePreparedQuery: &ledgerpb.DeletePreparedQueryRequest{
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
func CreateQueryCheckpointAction() *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_CreateQueryCheckpoint{
			CreateQueryCheckpoint: &ledgerpb.CreateQueryCheckpointRequest{},
		},
	}
}

// DeleteQueryCheckpointAction creates an action for removing a query checkpoint.
func DeleteQueryCheckpointAction(checkpointID uint64) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_DeleteQueryCheckpoint{
			DeleteQueryCheckpoint: &ledgerpb.DeleteQueryCheckpointRequest{
				CheckpointId: checkpointID,
			},
		},
	}
}

// WithReference sets the reference on a create transaction request.
func WithReference(req *ledgerpb.Request, reference string) *ledgerpb.Request {
	if reqType, ok := req.GetType().(*ledgerpb.Request_Apply); ok {
		if d, ok := reqType.Apply.GetAction().GetData().(*ledgerpb.LedgerAction_CreateTransaction); ok {
			d.CreateTransaction.Reference = reference
		}
	}

	return req
}

// WithSkippableReasons sets the per-apply skippable_reasons whitelist on an
// Apply request. Each reason is a business-level error the caller accepts to
// see converted into an OrderSkippedLog instead of failing the proposal.
// Validated at admission against a per-action whitelist.
func WithSkippableReasons(req *ledgerpb.Request, reasons ...ledgerpb.ErrorReason) *ledgerpb.Request {
	if reqType, ok := req.GetType().(*ledgerpb.Request_Apply); ok {
		reqType.Apply.SkippableReasons = reasons
	}

	return req
}

// WithIdempotencyKey wraps requests into an unsigned ApplyRequest under the
// given idempotency key — idempotency is keyed per atomic batch.
func WithIdempotencyKey(key string, reqs ...*ledgerpb.Request) *ledgerpb.ApplyRequest {
	return ledgerpb.UnsignedApplyRequest(key, reqs...)
}

// GetCreatedTransactionID extracts the first created transaction ID from an ApplyResponse.
// Returns (id, true) on success or (0, false) if no transaction was found.
func GetCreatedTransactionID(resp *ledgerpb.ApplyResponse) (uint64, bool) {
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
func GetCreatedQueryCheckpoint(resp *ledgerpb.ApplyResponse) (checkpointID, maxSequence uint64, ok bool) {
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
func GetAllCreatedTransactionIDs(resp *ledgerpb.ApplyResponse) []uint64 {
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
