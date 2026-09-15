package ledgerv3

import (
	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"google.golang.org/protobuf/proto"
)

// publicLedgerInfo clones a product response before removing credentials that
// Ledger persists for mirror workers but must never expose through fctl.
func publicLedgerInfo(info *commonpb.LedgerInfo) *commonpb.LedgerInfo {
	if info == nil {
		return nil
	}
	public := proto.Clone(info).(*commonpb.LedgerInfo)
	if credentials := public.GetMirrorSource().GetHttp().GetOauth2ClientCredentials(); credentials != nil {
		credentials.ClientSecret = ""
	}
	if postgres := public.GetMirrorSource().GetPostgres(); postgres != nil {
		// A DSN may contain credentials in several syntaxes. Clearing the entire
		// value is the only fail-closed projection.
		postgres.Dsn = ""
	}
	return public
}

func emitLedgerInfo(host sdk.Host, operationID string, info *commonpb.LedgerInfo) error {
	return emitProto(host, operationID, publicLedgerInfo(info))
}

func emitLedgerInfoList(host sdk.Host, operationID string, messages []proto.Message, page *sdk.PageInfo) error {
	public := make([]proto.Message, 0, len(messages))
	for _, message := range messages {
		info, ok := message.(*commonpb.LedgerInfo)
		if !ok {
			return v3Failure("sanitize %q response: expected LedgerInfo, got %T", operationID, message)
		}
		public = append(public, publicLedgerInfo(info))
	}
	return emitProtoList(host, operationID, public, page)
}
