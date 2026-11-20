package cmdutil

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"maps"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

// LoadSigningKey loads the signing key and key ID from command flags.
// Returns empty values if no signing key is configured.
func LoadSigningKey(cmd *cobra.Command) (string, ed25519.PrivateKey, error) {
	keyPath, _ := cmd.Flags().GetString("signing-key")
	if keyPath == "" {
		return "", nil, nil
	}

	keyID, _ := cmd.Flags().GetString("signing-key-id")
	if keyID == "" {
		keyID = "default"
	}

	seed, err := signing.LoadSeedFromFile(keyPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to load signing key: %w", err)
	}

	return keyID, ed25519.NewKeyFromSeed(seed), nil
}

// VerifyApplyResponse binds each authenticated response log to the submitted
// order before a write command reports success. A signed log alone only proves
// that the server produced that log at some point.
func VerifyApplyResponse(cmd *cobra.Command, request *servicepb.ApplyRequest, logs []*commonpb.Log, receipt *signaturepb.SignedResponseReceipt) error {
	key, err := LoadResponseVerifyKey(cmd)
	if err != nil {
		return err
	}
	if key == nil {
		return nil
	}

	var batch *servicepb.ApplyBatch
	switch {
	case request == nil || request.GetSkipResponse():
		return errors.New("cannot verify a response without the submitted batch and full logs")
	case request.GetUnsigned() != nil:
		batch = request.GetUnsigned()
	case request.GetSigned() != nil:
		batch = new(servicepb.ApplyBatch)
		if err := batch.UnmarshalVT(request.GetSigned().GetPayload()); err != nil {
			return fmt.Errorf("decode submitted signed batch: %w", err)
		}
	default:
		return errors.New("submitted Apply request has no batch")
	}
	if len(batch.GetRequests()) == 0 {
		return errors.New("submitted Apply batch has no orders to verify")
	}
	if len(logs) != len(batch.GetRequests()) {
		return fmt.Errorf("expected %d response logs for submitted orders, got %d", len(batch.GetRequests()), len(logs))
	}
	for i, order := range batch.GetRequests() {
		log := logs[i]
		if log == nil {
			return fmt.Errorf("response log %d is nil", i)
		}
		if i > 0 && log.GetSequence() <= logs[i-1].GetSequence() {
			return fmt.Errorf("response logs are out of order at order %d", i)
		}
		if err := signing.VerifyResponseLog(log, key); err != nil {
			return fmt.Errorf("log %d: %w", log.GetSequence(), err)
		}
		if !responseMatchesOrder(order, log.GetPayload()) {
			return fmt.Errorf("response log %d does not match submitted order %d", log.GetSequence(), i)
		}
	}
	if err := signing.VerifyResponseReceipt(request, logs, receipt, key); err != nil {
		return err
	}

	return nil
}

func sameMetadata(a, b map[string]*commonpb.MetadataValue) bool {
	return maps.EqualFunc(a, b, func(x, y *commonpb.MetadataValue) bool { return proto.Equal(x, y) })
}

func containsMetadata(actual, submitted map[string]*commonpb.MetadataValue) bool {
	for key, value := range submitted {
		if !proto.Equal(actual[key], value) {
			return false
		}
	}

	return true
}

func matchesInitialSchema(actual *commonpb.MetadataSchema, commands []*commonpb.SetMetadataFieldTypeCommand) bool {
	expected := &commonpb.MetadataSchema{}
	for _, command := range commands {
		field := &commonpb.MetadataFieldSchema{Type: command.GetType()}
		switch command.GetTargetType() {
		case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
			if expected.AccountFields == nil {
				expected.AccountFields = make(map[string]*commonpb.MetadataFieldSchema)
			}
			expected.AccountFields[command.GetKey()] = field
		case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
			if expected.TransactionFields == nil {
				expected.TransactionFields = make(map[string]*commonpb.MetadataFieldSchema)
			}
			expected.TransactionFields[command.GetKey()] = field
		case commonpb.TargetType_TARGET_TYPE_LEDGER:
			if expected.LedgerFields == nil {
				expected.LedgerFields = make(map[string]*commonpb.MetadataFieldSchema)
			}
			expected.LedgerFields[command.GetKey()] = field
		default:
			return false
		}
	}
	if len(commands) == 0 {
		return actual == nil || proto.Equal(actual, expected)
	}

	return proto.Equal(actual, expected)
}

func matchesInitialAccountTypes(actual, submitted map[string]*commonpb.AccountType) bool {
	return maps.EqualFunc(actual, submitted, func(x, y *commonpb.AccountType) bool {
		return x != nil && y != nil && x.GetPattern() == y.GetPattern() && x.GetPersistence() == y.GetPersistence() && maps.Equal(x.GetSegmentTypes(), y.GetSegmentTypes())
	})
}

func responseMatchesOrder(order *servicepb.Request, log *commonpb.LogPayload) bool {
	if order == nil || log == nil {
		return false
	}
	switch o := order.GetType().(type) {
	case *servicepb.Request_CreateLedger:
		l := log.GetCreateLedger()

		return l != nil && l.GetName() == o.CreateLedger.GetName() && sameMetadata(l.GetMetadata(), o.CreateLedger.GetMetadata()) && matchesInitialSchema(l.GetMetadataSchema(), o.CreateLedger.GetInitialSchema()) && matchesInitialAccountTypes(l.GetAccountTypes(), o.CreateLedger.GetAccountTypes()) && l.GetMode() == o.CreateLedger.GetMode() && proto.Equal(l.GetMirrorSource(), o.CreateLedger.GetMirrorSource()) && l.GetDefaultEnforcementMode() == o.CreateLedger.GetDefaultEnforcementMode()
	case *servicepb.Request_DeleteLedger:
		l := log.GetDeleteLedger()

		return l != nil && l.GetName() == o.DeleteLedger.GetName()
	case *servicepb.Request_PromoteLedger:
		l := log.GetPromoteLedger()

		return l != nil && l.GetName() == o.PromoteLedger.GetLedger()
	case *servicepb.Request_SaveLedgerMetadata:
		l := log.GetSavedLedgerMetadata()

		return l != nil && l.GetLedger() == o.SaveLedgerMetadata.GetLedger() && sameMetadata(l.GetMetadata(), o.SaveLedgerMetadata.GetMetadata())
	case *servicepb.Request_DeleteLedgerMetadata:
		l := log.GetDeletedLedgerMetadata()

		return l != nil && l.GetLedger() == o.DeleteLedgerMetadata.GetLedger() && l.GetKey() == o.DeleteLedgerMetadata.GetKey()
	case *servicepb.Request_CreatePreparedQuery:
		l := log.GetCreatedPreparedQuery()

		return l != nil && l.GetLedger() == o.CreatePreparedQuery.GetLedger() && proto.Equal(l.GetQuery(), o.CreatePreparedQuery.GetQuery())
	case *servicepb.Request_UpdatePreparedQuery:
		l := log.GetUpdatedPreparedQuery()

		return l != nil && l.GetLedger() == o.UpdatePreparedQuery.GetLedger() && l.GetName() == o.UpdatePreparedQuery.GetName() && proto.Equal(l.GetNewFilter(), o.UpdatePreparedQuery.GetFilter()) && l.NewTarget != nil && o.UpdatePreparedQuery.Target != nil && l.GetNewTarget() == o.UpdatePreparedQuery.GetTarget()
	case *servicepb.Request_DeletePreparedQuery:
		l := log.GetDeletedPreparedQuery()

		return l != nil && l.GetLedger() == o.DeletePreparedQuery.GetLedger() && l.GetName() == o.DeletePreparedQuery.GetName()
	case *servicepb.Request_CreateQueryCheckpoint:
		return log.GetCreatedQueryCheckpoint() != nil
	case *servicepb.Request_DeleteQueryCheckpoint:
		l := log.GetDeletedQueryCheckpoint()

		return l != nil && l.GetCheckpointId() == o.DeleteQueryCheckpoint.GetCheckpointId()
	case *servicepb.Request_SetQueryCheckpointSchedule:
		l := log.GetSetQueryCheckpointSchedule()

		return l != nil && l.GetCron() == o.SetQueryCheckpointSchedule.GetCron()
	case *servicepb.Request_DeleteQueryCheckpointSchedule:
		return log.GetDeleteQueryCheckpointSchedule() != nil
	case *servicepb.Request_SaveNumscript:
		l := log.GetSavedNumscript()
		info := l.GetInfo()

		return info != nil && info.GetLedger() == o.SaveNumscript.GetLedger() && info.GetName() == o.SaveNumscript.GetName() && info.GetVersion() == o.SaveNumscript.GetVersion() && info.GetContent() == o.SaveNumscript.GetContent()
	case *servicepb.Request_CreateIndex:
		return matchLedgerIndex(log, o.CreateIndex.GetLedger(), o.CreateIndex.GetId(), true)
	case *servicepb.Request_DropIndex:
		return matchLedgerIndex(log, o.DropIndex.GetLedger(), o.DropIndex.GetId(), false)
	case *servicepb.Request_SetMetadataFieldType:
		l := log.GetApply()
		if l == nil || l.GetLedgerName() != o.SetMetadataFieldType.GetLedger() {
			return false
		}
		m := l.GetLog().GetData().GetSetMetadataFieldType()

		return m != nil && m.GetTargetType() == o.SetMetadataFieldType.GetTargetType() && m.GetKey() == o.SetMetadataFieldType.GetKey() && m.GetType() == o.SetMetadataFieldType.GetType()
	case *servicepb.Request_RemoveMetadataFieldType:
		l := log.GetApply()
		if l == nil || l.GetLedgerName() != o.RemoveMetadataFieldType.GetLedger() {
			return false
		}
		m := l.GetLog().GetData().GetRemovedMetadataFieldType()

		return m != nil && m.GetTargetType() == o.RemoveMetadataFieldType.GetTargetType() && m.GetKey() == o.RemoveMetadataFieldType.GetKey()
	case *servicepb.Request_AddAccountType:
		l := log.GetApply()

		return l != nil && l.GetLedgerName() == o.AddAccountType.GetLedger() && proto.Equal(l.GetLog().GetData().GetAddedAccountType().GetAccountType(), o.AddAccountType.GetAccountType()) && l.GetLog().GetData().GetAddedAccountType() != nil
	case *servicepb.Request_RemoveAccountType:
		l := log.GetApply()

		return l != nil && l.GetLedgerName() == o.RemoveAccountType.GetLedger() && l.GetLog().GetData().GetRemovedAccountType() != nil && l.GetLog().GetData().GetRemovedAccountType().GetName() == o.RemoveAccountType.GetName()
	case *servicepb.Request_SetDefaultEnforcementMode:
		l := log.GetApply()

		return l != nil && l.GetLedgerName() == o.SetDefaultEnforcementMode.GetLedger() && l.GetLog().GetData().GetUpdatedDefaultEnforcementMode() != nil && l.GetLog().GetData().GetUpdatedDefaultEnforcementMode().GetEnforcementMode() == o.SetDefaultEnforcementMode.GetEnforcementMode()
	case *servicepb.Request_Apply:
		return matchLedgerApply(o.Apply, log.GetApply())
	default:
		// A new command must define its response binding before it can be
		// acknowledged under response verification.
		return false
	}
}

func matchLedgerIndex(log *commonpb.LogPayload, ledger string, id *commonpb.IndexID, create bool) bool {
	a := log.GetApply()
	if a == nil || a.GetLedgerName() != ledger {
		return false
	}
	if create {
		l := a.GetLog().GetData().GetCreateIndex()

		return l != nil && proto.Equal(l.GetId(), id)
	}
	l := a.GetLog().GetData().GetDropIndex()

	return l != nil && proto.Equal(l.GetId(), id)
}

func matchLedgerApply(request *servicepb.LedgerApplyRequest, log *commonpb.ApplyLedgerLog) bool {
	if request == nil || log == nil || request.GetLedger() != log.GetLedgerName() {
		return false
	}
	data := log.GetLog().GetData()
	if data == nil || request.GetAction() == nil {
		return false
	}
	switch action := request.GetAction().GetData().(type) {
	case *servicepb.LedgerAction_AddMetadata:
		l := data.GetSavedMetadata()

		return l != nil && proto.Equal(l.GetTarget(), action.AddMetadata.GetTarget()) && sameMetadata(l.GetMetadata(), action.AddMetadata.GetMetadata())
	case *servicepb.LedgerAction_DeleteMetadata:
		l := data.GetDeletedMetadata()

		return l != nil && proto.Equal(l.GetTarget(), action.DeleteMetadata.GetTarget()) && l.GetKey() == action.DeleteMetadata.GetKey()
	case *servicepb.LedgerAction_RevertTransaction:
		l := data.GetRevertedTransaction()

		return l != nil && l.GetRevertedTransactionId() == action.RevertTransaction.GetTransactionId() && sameMetadata(l.GetRevertTransaction().GetMetadata(), action.RevertTransaction.GetMetadata())
	case *servicepb.LedgerAction_CreateTransaction:
		l := data.GetCreatedTransaction()
		if l == nil || l.GetTransaction() == nil {
			return false
		}
		tx, submitted := l.GetTransaction(), action.CreateTransaction
		if tx.GetReference() != submitted.GetReference() || !containsMetadata(tx.GetMetadata(), submitted.GetMetadata()) || (submitted.GetTimestamp() != nil && !proto.Equal(tx.GetTimestamp(), submitted.GetTimestamp())) {
			return false
		}
		for account, metadata := range submitted.GetAccountMetadata() {
			if !containsMetadata(l.GetAccountMetadata()[account].GetValues(), metadata.GetValues()) {
				return false
			}
		}
		if len(submitted.GetPostings()) > 0 {
			if len(tx.GetPostings()) != len(submitted.GetPostings()) {
				return false
			}
			for i, posting := range submitted.GetPostings() {
				if !proto.Equal(tx.GetPostings()[i], posting) {
					return false
				}
			}
		}

		return true
	case *servicepb.LedgerAction_AddAccountType:
		l := data.GetAddedAccountType()

		return l != nil && proto.Equal(l.GetAccountType(), action.AddAccountType.GetAccountType())
	case *servicepb.LedgerAction_RemoveAccountType:
		l := data.GetRemovedAccountType()

		return l != nil && l.GetName() == action.RemoveAccountType.GetName()
	case *servicepb.LedgerAction_SetDefaultEnforcementMode:
		l := data.GetUpdatedDefaultEnforcementMode()

		return l != nil && l.GetEnforcementMode() == action.SetDefaultEnforcementMode.GetEnforcementMode()
	default:
		return false
	}
}

// LoadResponseVerifyKey loads the Ed25519 public key for response signature verification.
// Returns nil if --response-verify-key is not set.
func LoadResponseVerifyKey(cmd *cobra.Command) (ed25519.PublicKey, error) {
	keyPath, _ := cmd.Flags().GetString("response-verify-key")
	if keyPath == "" {
		return nil, nil
	}

	pubKey, err := signing.LoadPublicKeyFromFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load response verify key: %w", err)
	}

	return pubKey, nil
}

// VerifyResponseSignatures verifies the response signatures on the given logs.
// If no verify key is configured (--response-verify-key), this is a no-op.
func VerifyResponseSignatures(cmd *cobra.Command, logs []*commonpb.Log) error {
	pubKey, err := LoadResponseVerifyKey(cmd)
	if err != nil {
		return err
	}

	if pubKey == nil {
		return nil
	}

	for _, log := range logs {
		if log.GetResponseSignature() == nil {
			return fmt.Errorf("log %d: missing response signature (server may not have response signing enabled)", log.GetSequence())
		}

		err := signing.VerifyResponseLog(log, pubKey)
		if err != nil {
			return fmt.Errorf("log %d: %w", log.GetSequence(), err)
		}
	}

	return nil
}

// BuildApplyRequest assembles the requests into one atomic ApplyBatch and
// returns the ApplyRequest to pass to Apply — signed as a whole when a signing
// key is configured on the command flags, unsigned otherwise. Signing the batch
// authenticates its composition and ordering.
func BuildApplyRequest(cmd *cobra.Command, requests ...*servicepb.Request) (*servicepb.ApplyRequest, error) {
	// Commands that expose batch retries register this flag. Resolve it before
	// signing so the authenticated batch includes its idempotency key.
	var idempotencyKey string
	if cmd.Flags().Lookup("idempotency-key") != nil {
		var err error
		idempotencyKey, err = cmd.Flags().GetString("idempotency-key")
		if err != nil {
			return nil, err
		}
	}

	return BuildApplyRequestWithIdempotencyKey(cmd, idempotencyKey, requests...)
}

// BuildApplyRequestWithIdempotencyKey binds the key into the batch before signing.
func BuildApplyRequestWithIdempotencyKey(cmd *cobra.Command, idempotencyKey string, requests ...*servicepb.Request) (*servicepb.ApplyRequest, error) {
	keyID, privKey, err := LoadSigningKey(cmd)
	if err != nil {
		return nil, err
	}

	if privKey == nil {
		request := servicepb.UnsignedApplyRequest(idempotencyKey, requests...)
		verifyKeyPath, _ := cmd.Flags().GetString("response-verify-key")
		if verifyKeyPath == "" {
			return request, nil
		}
		payload, err := request.GetUnsigned().MarshalVT()
		if err != nil {
			return nil, fmt.Errorf("encode batch for response binding: %w", err)
		}
		request.ResponseBindingPayload = payload

		return request, nil
	}

	sb, err := signing.Sign(&servicepb.ApplyBatch{Requests: requests, IdempotencyKey: idempotencyKey}, keyID, privKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign batch: %w", err)
	}

	return servicepb.SignedApplyRequest(sb), nil
}
