package actions

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// CreateMetadataIndexAction creates a request for creating a metadata index on the given target type.
func CreateMetadataIndexAction(ledger string, target commonpb.TargetType, key string) *servicepb.Request {
	switch target {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		return CreateAccountMetadataIndexAction(ledger, key)
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		return &servicepb.Request{
			Type: &servicepb.Request_CreateIndex{
				CreateIndex: &servicepb.CreateIndexRequest{
					Ledger: ledger,
					Index: &servicepb.CreateIndexRequest_Transaction{
						Transaction: &commonpb.TransactionIndex{
							Kind: &commonpb.TransactionIndex_MetadataKey{MetadataKey: key},
						},
					},
				},
			},
		}
	default:
		panic("unsupported target type for metadata index")
	}
}

// DropMetadataIndexAction creates a request for dropping a metadata index on the given target type.
func DropMetadataIndexAction(ledger string, target commonpb.TargetType, key string) *servicepb.Request {
	switch target {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		return DropAccountMetadataIndexAction(ledger, key)
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		return &servicepb.Request{
			Type: &servicepb.Request_DropIndex{
				DropIndex: &servicepb.DropIndexRequest{
					Ledger: ledger,
					Index: &servicepb.DropIndexRequest_Transaction{
						Transaction: &commonpb.TransactionIndex{
							Kind: &commonpb.TransactionIndex_MetadataKey{MetadataKey: key},
						},
					},
				},
			},
		}
	default:
		panic("unsupported target type for metadata index")
	}
}

// AddressRoleToBuiltinIndex maps an AddressRole to its corresponding TransactionBuiltinIndex.
func AddressRoleToBuiltinIndex(role commonpb.AddressRole) commonpb.TransactionBuiltinIndex {
	switch role {
	case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
		return commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS
	case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
		return commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS
	default:
		return commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS
	}
}

// CreateAddressIndexAction creates a request for creating an address index on a ledger.
func CreateAddressIndexAction(ledger string, role commonpb.AddressRole) *servicepb.Request {
	return CreateBuiltinTxIndexAction(ledger, AddressRoleToBuiltinIndex(role))
}

// DropAddressIndexAction creates a request for dropping an address index.
func DropAddressIndexAction(ledger string, role commonpb.AddressRole) *servicepb.Request {
	return DropBuiltinTxIndexAction(ledger, AddressRoleToBuiltinIndex(role))
}

// CreateLogBuiltinIndexAction creates a request for creating a log builtin index.
func CreateLogBuiltinIndexAction(ledger string, index commonpb.LogBuiltinIndex) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_CreateIndex{
			CreateIndex: &servicepb.CreateIndexRequest{
				Ledger: ledger,
				Index: &servicepb.CreateIndexRequest_LogBuiltin{
					LogBuiltin: index,
				},
			},
		},
	}
}

// WaitForMetadataIndexReady polls until a metadata index reaches READY status or the timeout expires.
func WaitForMetadataIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, target commonpb.TargetType, key string) error {
	return poll(ctx, 10*time.Second, 200*time.Millisecond, func() error {
		info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
		if err != nil {
			return err
		}
		if info.GetMetadataSchema() == nil {
			return errors.New("metadata schema is nil")
		}
		var fields map[string]*commonpb.MetadataFieldSchema
		switch target {
		case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
			fields = info.GetMetadataSchema().GetAccountFields()
		case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
			fields = info.GetMetadataSchema().GetTransactionFields()
		}
		f, ok := fields[key]
		if !ok {
			return fmt.Errorf("field %q not found", key)
		}
		if f.GetIndexBuildStatus() != commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY {
			return fmt.Errorf("index status is %v, want READY", f.GetIndexBuildStatus())
		}

		return nil
	})
}

// WaitForBuiltinIndexReady polls until a builtin transaction index reaches READY status.
func WaitForBuiltinIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, index commonpb.TransactionBuiltinIndex) error {
	return poll(ctx, 10*time.Second, 200*time.Millisecond, func() error {
		info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
		if err != nil {
			return err
		}
		if info.GetBuiltinIndexes() == nil {
			return errors.New("builtin indexes is nil")
		}
		var st commonpb.IndexBuildStatus
		switch index {
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE:
			st = info.GetBuiltinIndexes().GetReferenceStatus()
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:
			st = info.GetBuiltinIndexes().GetTimestampStatus()
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS:
			st = info.GetBuiltinIndexes().GetAddressStatus()
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS:
			st = info.GetBuiltinIndexes().GetSourceAddressStatus()
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS:
			st = info.GetBuiltinIndexes().GetDestAddressStatus()
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT:
			st = info.GetBuiltinIndexes().GetInsertedAtStatus()
		}
		if st != commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY {
			return fmt.Errorf("index status is %v, want READY", st)
		}

		return nil
	})
}

// WaitForAddressIndexReady polls until an address index reaches READY status.
func WaitForAddressIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, role commonpb.AddressRole) error {
	return WaitForBuiltinIndexReady(ctx, client, ledger, AddressRoleToBuiltinIndex(role))
}

// WaitForLogBuiltinIndexReady polls until a log builtin index reaches READY status.
func WaitForLogBuiltinIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, index commonpb.LogBuiltinIndex) error {
	return poll(ctx, 10*time.Second, 200*time.Millisecond, func() error {
		info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
		if err != nil {
			return err
		}
		if info.GetLogBuiltinIndexes() == nil {
			return errors.New("log builtin indexes is nil")
		}
		var st commonpb.IndexBuildStatus
		switch index {
		case commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER:
			st = info.GetLogBuiltinIndexes().GetLedgerStatus()
		case commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE:
			st = info.GetLogBuiltinIndexes().GetDateStatus()
		}
		if st != commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY {
			return fmt.Errorf("index status is %v, want READY", st)
		}

		return nil
	})
}

// poll repeatedly calls check until it returns nil or the timeout expires.
func poll(ctx context.Context, timeout, interval time.Duration, check func() error) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		lastErr = check()
		if lastErr == nil {
			return nil
		}
		time.Sleep(interval)
	}

	return fmt.Errorf("timed out after %v: %w", timeout, lastErr)
}
