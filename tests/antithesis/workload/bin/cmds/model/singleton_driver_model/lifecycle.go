package main

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// validateLifecycleLog checks the top-level payloads that carry no ledger-local
// log. An absent payload is never equivalent to a false maintenance toggle.
func validateLifecycleLog(req *servicepb.Request, payload *commonpb.LogPayload) error {
	switch r := req.GetType().(type) {
	case *servicepb.Request_CreateLedger:
		log := payload.GetCreateLedger()
		if log == nil || log.GetId() == 0 || log.GetCreatedAt() == nil || log.GetName() != r.CreateLedger.GetName() ||
			log.GetMode() != r.CreateLedger.GetMode() || !log.GetMirrorSource().EqualVT(r.CreateLedger.GetMirrorSource()) ||
			!log.GetMetadataSchema().EqualVT(lifecycleMetadataSchema(r.CreateLedger.GetInitialSchema())) ||
			!accountTypesEqual(log.GetAccountTypes(), r.CreateLedger.GetAccountTypes()) ||
			log.GetDefaultEnforcementMode() != r.CreateLedger.GetDefaultEnforcementMode() {
			return fmt.Errorf("create-ledger response does not match request")
		}
	case *servicepb.Request_DeleteLedger:
		log := payload.GetDeleteLedger()
		if log == nil || log.GetName() != r.DeleteLedger.GetName() || log.GetDeletedAt() == nil {
			return fmt.Errorf("delete-ledger response does not match request")
		}
	case *servicepb.Request_PromoteLedger:
		log := payload.GetPromoteLedger()
		if log == nil || log.GetName() != r.PromoteLedger.GetLedger() {
			return fmt.Errorf("promote-ledger response does not match request")
		}
	case *servicepb.Request_SetMaintenanceMode:
		log := payload.GetSetMaintenanceMode()
		if log == nil || log.GetEnabled() != r.SetMaintenanceMode.GetEnabled() {
			return fmt.Errorf("maintenance response does not match requested mode")
		}
	}
	return nil
}

func lifecycleMetadataSchema(commands []*commonpb.SetMetadataFieldTypeCommand) *commonpb.MetadataSchema {
	if len(commands) == 0 {
		return nil
	}
	schema := &commonpb.MetadataSchema{}
	for _, cmd := range commands {
		field := &commonpb.MetadataFieldSchema{Type: cmd.GetType()}
		switch cmd.GetTargetType() {
		case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
			if schema.AccountFields == nil {
				schema.AccountFields = map[string]*commonpb.MetadataFieldSchema{}
			}
			schema.AccountFields[cmd.GetKey()] = field
		case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
			if schema.TransactionFields == nil {
				schema.TransactionFields = map[string]*commonpb.MetadataFieldSchema{}
			}
			schema.TransactionFields[cmd.GetKey()] = field
		case commonpb.TargetType_TARGET_TYPE_LEDGER:
			if schema.LedgerFields == nil {
				schema.LedgerFields = map[string]*commonpb.MetadataFieldSchema{}
			}
			schema.LedgerFields[cmd.GetKey()] = field
		}
	}
	return schema
}

func accountTypesEqual(got, want map[string]*commonpb.AccountType) bool {
	if len(got) != len(want) {
		return false
	}
	for name, typ := range want {
		if !got[name].EqualVT(typ) {
			return false
		}
	}
	return true
}
