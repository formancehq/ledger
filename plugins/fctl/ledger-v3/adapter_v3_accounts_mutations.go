package ledgerv3

import (
	"context"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func executeV3AccountMutations(ctx context.Context, _ sdk.ExecuteRequest, decoded input, command sdk.Command, host sdk.Host) (bool, error) {
	var action *servicepb.LedgerAction
	var operation string
	switch command.ID {
	case "ledger.v3.accounts.set-metadata":
		metadata, err := parseMetadata(decoded.list(flagMetadata))
		if err != nil {
			return true, err
		}
		operation = opApplyAddMetadata.id
		action = &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{AddMetadata: &commonpb.SaveMetadataCommand{Target: &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: decoded.text(argAddress)}}}, Metadata: metadata}}}
	case "ledger.v3.accounts.delete-metadata":
		operation = opApplyDeleteMetadata.id
		action = &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{DeleteMetadata: &commonpb.DeleteMetadataCommand{Target: &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: decoded.text(argAddress)}}}, Key: decoded.text(argKey)}}}
	default:
		return false, nil
	}
	request := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: decoded.text(argLedger), Action: action}}}
	_, err := applyV3(ctx, host, command, operation, decoded.text(flagIdempotencyKey), request)
	if err != nil {
		return true, err
	}
	return true, emitEmpty(host, operation)
}
