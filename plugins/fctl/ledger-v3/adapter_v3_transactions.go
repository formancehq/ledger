package ledgerv3

import (
	"context"
	"errors"
	"io"
	"math/big"
	"strings"
	"time"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"google.golang.org/protobuf/proto"
)

func executeV3Transactions(ctx context.Context, request sdk.ExecuteRequest, decoded input, command sdk.Command, host sdk.Host) (bool, error) {
	switch request.CommandID {
	case "ledger.v3.transactions.analyze":
		return true, executeV3AnalyzeTransactions(ctx, decoded, command, host)
	case "ledger.v3.transactions.create":
		return true, executeV3CreateTransaction(ctx, decoded, command, host)
	case "ledger.v3.transactions.delete-metadata":
		return true, executeV3DeleteTransactionMetadata(ctx, decoded, command, host)
	case "ledger.v3.transactions.get":
		return true, executeV3GetTransaction(ctx, decoded, command, host)
	case "ledger.v3.transactions.list":
		return true, executeV3ListTransactions(ctx, request.Continuation, decoded, command, host)
	case "ledger.v3.transactions.revert":
		return true, executeV3RevertTransaction(ctx, decoded, command, host)
	case "ledger.v3.transactions.set-metadata":
		return true, executeV3SetTransactionMetadata(ctx, decoded, command, host)
	default:
		return false, nil
	}
}

func executeV3AnalyzeTransactions(ctx context.Context, decoded input, command sdk.Command, host sdk.Host) error {
	threshold := decoded.int32(flagVariableThreshold)
	if threshold < 0 {
		return invalidArgument("flag %q must not be negative", flagVariableThreshold)
	}
	stream, err := streamV3(ctx, host, command, opAnalyzeTransactions.id, &servicepb.AnalyzeTransactionsRequest{
		Ledger: decoded.text(argLedger), VariableThreshold: uint32(threshold),
	}, func() *servicepb.AnalyzeTransactionsEvent { return &servicepb.AnalyzeTransactionsEvent{} })
	if err != nil {
		return v3Failure("analyze transactions: %v", err)
	}
	var result *servicepb.AnalyzeTransactionsResponse
	for {
		event := stream.newItem()
		recvErr := stream.stream.RecvInto(event)
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return v3Failure("receive transaction analysis: %v", recvErr)
		}
		if progress := event.GetProgress(); progress != nil {
			if emitErr := emitAnalyzeProgress(host, progress); emitErr != nil {
				return emitErr
			}
		}
		if candidate := event.GetResult(); candidate != nil {
			if result != nil {
				return v3Failure("transaction analysis returned more than one result")
			}
			result = candidate
		}
	}
	if result == nil {
		return v3Failure("transaction analysis stream ended without a result")
	}
	return emitAnalyzeTransactionsResult(host, opAnalyzeTransactions.id, result)
}

func executeV3GetTransaction(ctx context.Context, decoded input, command sdk.Command, host sdk.Host) error {
	transactionID, err := decoded.uint64(argTransactionID)
	if err != nil {
		return err
	}
	checkpointID, err := decoded.uint64(flagCheckpointID)
	if err != nil {
		return err
	}
	response, err := unaryV3(ctx, host, command, opGetTransaction.id, &servicepb.GetTransactionRequest{
		Ledger: decoded.text(argLedger), TransactionId: transactionID, CheckpointId: checkpointID,
	}, &servicepb.GetTransactionResponse{})
	if err != nil {
		return v3Failure("get transaction: %v", err)
	}
	return emitProto(host, opGetTransaction.id, response)
}

func executeV3ListTransactions(ctx context.Context, continuation sdk.ContinuationControl, decoded input, command sdk.Command, host sdk.Host) error {
	base, err := v3ListOptions(decoded, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)
	if err != nil {
		return err
	}
	items, page, err := collectV3Pages(continuation, base.Cursor, func(cursor string) ([]proto.Message, string, error) {
		options := proto.Clone(base).(*commonpb.ListOptions)
		options.Cursor = cursor
		stream, callErr := streamV3(ctx, host, command, opListTransactions.id, &servicepb.ListTransactionsRequest{
			Ledger: decoded.text(argLedger), Options: options,
		}, func() *commonpb.Transaction { return &commonpb.Transaction{} })
		if callErr != nil {
			return nil, "", v3Failure("list transactions: %v", callErr)
		}
		return drainV3Stream(stream)
	})
	if err != nil {
		return err
	}
	return emitProtoList(host, opListTransactions.id, items, page)
}

func executeV3CreateTransaction(ctx context.Context, decoded input, command sdk.Command, host sdk.Host) error {
	postings, script, err := transactionSource(ctx, decoded, host)
	if err != nil {
		return err
	}
	metadata, err := parseMetadata(decoded.list(flagMetadata))
	if err != nil {
		return err
	}
	var timestamp *commonpb.Timestamp
	if raw := decoded.text(flagTimestamp); raw != "" {
		parsed, parseErr := time.Parse(time.RFC3339Nano, raw)
		if parseErr != nil || parsed.UnixMicro() < 0 {
			return invalidArgument("flag %q expects an RFC 3339 timestamp on or after the Unix epoch", flagTimestamp)
		}
		timestamp = &commonpb.Timestamp{Data: uint64(parsed.UnixMicro())}
	}
	action := &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: &servicepb.CreateTransactionPayload{
		Postings: postings, Script: script, Timestamp: timestamp, Reference: decoded.text(flagReference), Metadata: metadata, Force: decoded.boolean(flagForce),
	}}}
	response, err := applyV3TransactionAction(ctx, decoded, command, host, opApplyCreateTransaction.id, action)
	if err != nil {
		return err
	}
	created := firstCreatedTransaction(response)
	if created == nil {
		return v3Failure("create transaction returned no created transaction")
	}
	return emitProto(host, opApplyCreateTransaction.id, created)
}

func executeV3RevertTransaction(ctx context.Context, decoded input, command sdk.Command, host sdk.Host) error {
	transactionID, err := decoded.uint64(argTransactionID)
	if err != nil {
		return err
	}
	metadata, err := parseMetadata(decoded.list(flagMetadata))
	if err != nil {
		return err
	}
	action := &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RevertTransaction{RevertTransaction: &servicepb.RevertTransactionPayload{
		TransactionId: transactionID, Force: decoded.boolean(flagForce), AtEffectiveDate: decoded.boolean(flagAtEffectiveDate), Metadata: metadata,
	}}}
	response, err := applyV3TransactionAction(ctx, decoded, command, host, opApplyRevertTransaction.id, action)
	if err != nil {
		return err
	}
	reverted := firstRevertedTransaction(response)
	if reverted == nil {
		return v3Failure("revert transaction returned no reverted transaction")
	}
	return emitProto(host, opApplyRevertTransaction.id, reverted)
}

func executeV3SetTransactionMetadata(ctx context.Context, decoded input, command sdk.Command, host sdk.Host) error {
	transactionID, err := decoded.uint64(argTransactionID)
	if err != nil {
		return err
	}
	metadata, err := parseMetadata(decoded.list(flagMetadata))
	if err != nil {
		return err
	}
	action := &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{AddMetadata: &commonpb.SaveMetadataCommand{
		Target: &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: transactionID}}, Metadata: metadata,
	}}}
	if _, err := applyV3TransactionAction(ctx, decoded, command, host, opApplyAddMetadata.id, action); err != nil {
		return err
	}
	return emitEmpty(host, opApplyAddMetadata.id)
}

func executeV3DeleteTransactionMetadata(ctx context.Context, decoded input, command sdk.Command, host sdk.Host) error {
	transactionID, err := decoded.uint64(argTransactionID)
	if err != nil {
		return err
	}
	action := &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{DeleteMetadata: &commonpb.DeleteMetadataCommand{
		Target: &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: transactionID}}, Key: decoded.text(argKey),
	}}}
	if _, err := applyV3TransactionAction(ctx, decoded, command, host, opApplyDeleteMetadata.id, action); err != nil {
		return err
	}
	return emitEmpty(host, opApplyDeleteMetadata.id)
}

func applyV3TransactionAction(ctx context.Context, decoded input, command sdk.Command, host sdk.Host, operationID string, action *servicepb.LedgerAction) (*servicepb.ApplyResponse, error) {
	response, err := applyV3(ctx, host, command, operationID, decoded.text(flagIdempotencyKey), &servicepb.Request{
		Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: decoded.text(argLedger), Action: action}},
	})
	if err != nil {
		return nil, v3Failure("apply transaction action: %v", err)
	}
	return response, nil
}

func transactionSource(ctx context.Context, decoded input, host sdk.Host) ([]*commonpb.Posting, *commonpb.Script, error) {
	postingValues := decoded.list(flagPosting)
	scriptHandle := decoded.text(flagScript)
	variableValues := decoded.list(flagScriptVar)
	if scriptHandle != "" && len(postingValues) != 0 {
		return nil, nil, invalidArgument("flags %q and %q are mutually exclusive", flagScript, flagPosting)
	}
	if scriptHandle == "" && len(variableValues) != 0 {
		return nil, nil, invalidArgument("flag %q requires flag %q", flagScriptVar, flagScript)
	}
	if scriptHandle != "" {
		source, err := readTransactionScript(ctx, host, scriptHandle)
		if err != nil {
			return nil, nil, err
		}
		variables, err := parseStringMap(variableValues, "script variable")
		if err != nil {
			return nil, nil, err
		}
		return nil, &commonpb.Script{Plain: string(source), Vars: variables}, nil
	}
	if len(postingValues) == 0 {
		return nil, nil, invalidArgument("either flag %q or flag %q is required", flagPosting, flagScript)
	}
	postings := make([]*commonpb.Posting, 0, len(postingValues))
	for _, value := range postingValues {
		posting, err := parseV3Posting(value)
		if err != nil {
			return nil, nil, err
		}
		postings = append(postings, posting)
	}
	return postings, nil, nil
}

func readTransactionScript(ctx context.Context, host sdk.Host, handle string) ([]byte, error) {
	if handle == "" || len(handle) > sdk.MaxInputArtifactHandleBytes {
		return nil, invalidArgument("flag %q has an invalid input handle", flagScript)
	}
	var source []byte
	for {
		chunk, err := sdk.ReadInput(ctx, host, handle)
		if err != nil {
			return nil, v3Failure("read transaction script: %v", err)
		}
		if len(chunk.Bytes) == 0 && !chunk.Final {
			return nil, v3Failure("read transaction script: host returned an empty non-final chunk")
		}
		if int64(len(source))+int64(len(chunk.Bytes)) > maxNumscriptBytes {
			return nil, invalidArgument("flag %q exceeds its declared byte limit", flagScript)
		}
		source = append(source, chunk.Bytes...)
		if chunk.Final {
			return source, nil
		}
	}
}

func parseV3Posting(value string) (*commonpb.Posting, error) {
	parts := strings.Split(value, ",")
	if len(parts) == 4 || len(parts) == 5 {
		color := ""
		if len(parts) == 5 {
			color = strings.TrimSpace(parts[4])
		}
		return newV3Posting(parts[0], parts[1], parts[2], parts[3], color)
	}
	colon := strings.Split(value, ":")
	for amountIndex := 2; amountIndex < len(colon)-1; amountIndex++ {
		if _, ok := new(big.Int).SetString(strings.TrimSpace(colon[amountIndex]), 10); !ok {
			continue
		}
		return newV3Posting(strings.Join(colon[:amountIndex-1], ":"), strings.Join(colon[amountIndex+1:], ":"), colon[amountIndex], colon[amountIndex-1], "")
	}
	return nil, invalidArgument("flag %q expects source,destination,amount,asset[,color]", flagPosting)
}

func newV3Posting(source, destination, amountValue, asset, color string) (*commonpb.Posting, error) {
	source, destination = strings.TrimSpace(source), strings.TrimSpace(destination)
	asset, color = strings.TrimSpace(asset), strings.TrimSpace(color)
	amount, ok := new(big.Int).SetString(strings.TrimSpace(amountValue), 10)
	if source == "" || destination == "" || asset == "" || !ok || amount.Sign() <= 0 || amount.BitLen() > 256 {
		return nil, invalidArgument("flag %q contains an invalid posting", flagPosting)
	}
	return commonpb.NewColoredPosting(source, destination, asset, color, amount), nil
}

func parseStringMap(values []string, field string) (map[string]string, error) {
	parsed := make(map[string]string, len(values))
	for _, value := range values {
		key, raw, ok := strings.Cut(value, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			return nil, invalidArgument("%s expects name=value", field)
		}
		if _, duplicate := parsed[key]; duplicate {
			return nil, invalidArgument("%s %q is repeated", field, key)
		}
		parsed[key] = strings.TrimSpace(raw)
	}
	return parsed, nil
}

func firstCreatedTransaction(response *servicepb.ApplyResponse) *commonpb.CreatedTransaction {
	if len(response.GetLogs()) == 0 {
		return nil
	}
	return response.GetLogs()[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction()
}

func firstRevertedTransaction(response *servicepb.ApplyResponse) *commonpb.RevertedTransaction {
	if len(response.GetLogs()) == 0 {
		return nil
	}
	return response.GetLogs()[0].GetPayload().GetApply().GetLog().GetData().GetRevertedTransaction()
}
