package auditpb

import "github.com/formancehq/ledger/v3/internal/proto/commonpb"

func (x *AuditFailure) GetFacts() map[string]string {
	return commonpb.FailureDetailsFacts(x.GetDetails())
}
