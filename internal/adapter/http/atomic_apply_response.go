package http

import (
	"github.com/formancehq/ledger/v3/internal/pkg/sensitive"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

// atomicApplyResponse keeps the logs visible to the request-scoped JSON encoder.
// ApplyResponse's legacy opaque marshaler cannot carry bigint options through.
func atomicApplyResponse(response *servicepb.ApplyResponse) any {
	if response == nil {
		panic("atomic Apply returned no response")
	}
	logs := make([]any, len(response.GetLogs()))
	for i, log := range response.GetLogs() {
		if log == nil || log.GetPayload() == nil {
			panic("atomic Apply returned a log without a payload")
		}
		logs[i] = log
		if added := log.GetPayload().GetAddedEventsSink(); added != nil {
			// The service signature contains the original protobuf log, including
			// credentials. Return a detached public view without that envelope.
			public := log.CloneVT()
			public.Payload.GetAddedEventsSink().Config = sensitive.Redact(added.GetConfig())
			public.ResponseSignature = nil
			logs[i] = public
		}
		if created := log.GetPayload().GetCreateLedger(); created != nil {
			// The shared creation-log JSON projection predates assigned IDs and
			// initial metadata. Include both here without changing CLI/read JSON.
			logs[i] = struct {
				Sequence          uint64                 `json:"sequence"`
				Payload           any                    `json:"payload"`
				ResponseSignature *signaturepb.SignedLog `json:"responseSignature,omitempty"`
			}{Sequence: log.GetSequence(), Payload: map[string]any{"createLedger": struct {
				Name                   string                           `json:"name"`
				CreatedAt              *commonpb.Timestamp              `json:"createdAt,omitempty"`
				MetadataSchema         *commonpb.MetadataSchema         `json:"metadataSchema,omitempty"`
				Mode                   commonpb.LedgerMode              `json:"mode"`
				MirrorSource           *commonpb.MirrorSourceConfig     `json:"mirrorSource,omitempty"`
				AccountTypes           map[string]*commonpb.AccountType `json:"accountTypes,omitempty"`
				DefaultEnforcementMode commonpb.ChartEnforcementMode    `json:"defaultEnforcementMode"`
				ID                     uint32                           `json:"id"`
				Metadata               map[string]any                   `json:"metadata,omitempty"`
			}{
				Name: created.GetName(), CreatedAt: created.GetCreatedAt(), MetadataSchema: created.GetMetadataSchema(), Mode: created.GetMode(), MirrorSource: sensitive.Redact(created.GetMirrorSource()), AccountTypes: created.GetAccountTypes(), DefaultEnforcementMode: created.GetDefaultEnforcementMode(), ID: created.GetId(), Metadata: commonpb.MetadataToAnyMap(created.GetMetadata()),
			}}, ResponseSignature: func() *signaturepb.SignedLog {
				if created.GetMirrorSource() != nil {
					// The signed protobuf log contains the original mirror credentials.
					return nil
				}

				return log.GetResponseSignature()
			}()}
		}
	}

	return struct {
		Logs []any `json:"logs"`
	}{Logs: logs}
}
