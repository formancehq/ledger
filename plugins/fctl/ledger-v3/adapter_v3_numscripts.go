package ledgerv3

import (
	"context"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"google.golang.org/protobuf/proto"
)

func executeV3Numscripts(ctx context.Context, request sdk.ExecuteRequest, decoded input, command sdk.Command, host sdk.Host) (bool, error) {
	switch command.ID {
	case "ledger.v3.numscripts.get":
		read, err := v3ReadOptions(decoded)
		if err != nil {
			return true, err
		}
		response, err := unaryV3(ctx, host, command, opGetNumscript.id, &servicepb.GetNumscriptRequest{Ledger: decoded.text(argLedger), Name: decoded.text(argName), Version: decoded.text(flagVersion), Read: read}, &commonpb.NumscriptInfo{})
		if err != nil {
			return true, v3Failure("get numscript: %v", err)
		}
		return true, emitProto(host, opGetNumscript.id, response)
	case "ledger.v3.numscripts.versions":
		read, err := v3ReadOptions(decoded)
		if err != nil {
			return true, err
		}
		response, err := unaryV3(ctx, host, command, opListNumscriptVersions.id, &servicepb.ListNumscriptVersionsRequest{Ledger: decoded.text(argLedger), Name: decoded.text(argName), Read: read}, &servicepb.ListNumscriptVersionsResponse{})
		if err != nil {
			return true, v3Failure("list numscript versions: %v", err)
		}
		return true, emitProto(host, opListNumscriptVersions.id, response)
	case "ledger.v3.numscripts.save":
		content, err := readArtifact(ctx, host, decoded.text(flagScript), maxNumscriptBytes)
		if err != nil {
			return true, err
		}
		response, err := applyV3(ctx, host, command, opApplySaveNumscript.id, decoded.text(flagIdempotencyKey), &servicepb.Request{Type: &servicepb.Request_SaveNumscript{SaveNumscript: &servicepb.SaveNumscriptRequest{Ledger: decoded.text(argLedger), Name: decoded.text(argName), Version: decoded.text(flagVersion), Content: string(content)}}})
		if err != nil {
			return true, err
		}
		return true, emitProto(host, opApplySaveNumscript.id, response)
	case "ledger.v3.numscripts.list":
		read, err := v3ReadOptions(decoded)
		if err != nil {
			return true, err
		}
		items, page, err := collectV3Pages(request.Continuation, decoded.text(flagCursor), func(cursor string) ([]proto.Message, string, error) {
			stream, err := streamV3(ctx, host, command, opListNumscripts.id, &servicepb.ListNumscriptsRequest{Ledger: decoded.text(argLedger), Options: &commonpb.ListOptions{PageSize: uint32(decoded.int32(flagPageSize)), Cursor: cursor, Reverse: decoded.boolean(flagReverse), Read: read}}, func() *commonpb.NumscriptInfo { return &commonpb.NumscriptInfo{} })
			if err != nil {
				return nil, "", v3Failure("list numscripts: %v", err)
			}
			return drainV3Stream(stream)
		})
		if err != nil {
			return true, err
		}
		return true, emitProtoList(host, opListNumscripts.id, items, page)
	default:
		return false, nil
	}
}
