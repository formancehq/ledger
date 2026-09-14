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
		if log == nil || log.GetName() != r.CreateLedger.GetName() || log.GetMode() != r.CreateLedger.GetMode() || !log.GetMirrorSource().EqualVT(r.CreateLedger.GetMirrorSource()) {
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
