package controller

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

const bucketServiceApply = "/ledger.BucketService/Apply"

// LedgerClient manages ledger lifecycle via gRPC calls to BucketService/Apply.
// It uses raw protobuf encoding to avoid importing the main module's proto types.
type LedgerClient struct{}

// CreateLedger creates a ledger via BucketService/Apply. AlreadyExists is ignored.
func (c *LedgerClient) CreateLedger(ctx context.Context, endpoint, name string) error {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", endpoint, err)
	}
	defer conn.Close() //nolint:errcheck // best-effort

	req := encodeApplyRequest(encodeCreateLedgerRequest(name))
	var resp []byte

	if err := conn.Invoke(ctx, bucketServiceApply, &req, &resp, grpc.ForceCodec(rawCodec{})); err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.AlreadyExists {
			return nil
		}

		return fmt.Errorf("create ledger %q: %w", name, err)
	}

	return nil
}

// DeleteLedger deletes a ledger via BucketService/Apply. NotFound is ignored.
func (c *LedgerClient) DeleteLedger(ctx context.Context, endpoint, name string) error {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", endpoint, err)
	}
	defer conn.Close() //nolint:errcheck // best-effort

	req := encodeApplyRequest(encodeDeleteLedgerRequest(name))
	var resp []byte

	if err := conn.Invoke(ctx, bucketServiceApply, &req, &resp, grpc.ForceCodec(rawCodec{})); err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			return nil
		}

		return fmt.Errorf("delete ledger %q: %w", name, err)
	}

	return nil
}

// rawCodec passes raw byte slices without marshaling, used with grpc.ForceCodec.
type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.(*[]byte)
	if !ok {
		return nil, fmt.Errorf("rawCodec: expected *[]byte, got %T", v)
	}

	return *b, nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("rawCodec: expected *[]byte, got %T", v)
	}

	*b = data

	return nil
}

func (rawCodec) Name() string { return "proto" }

// Protobuf encoding helpers for BucketService/Apply messages.
// Proto schema (from misc/proto/bucket.proto):
//   ApplyRequest  { repeated Request requests = 1; }
//   Request       { oneof action { CreateLedgerRequest create_ledger = 3; DeleteLedgerRequest delete_ledger = 4; } }
//   CreateLedgerRequest { string name = 1; }
//   DeleteLedgerRequest { string name = 1; }

func encodeCreateLedgerRequest(name string) []byte {
	// CreateLedgerRequest: field 1 = name (string)
	var inner []byte
	inner = protowire.AppendTag(inner, 1, protowire.BytesType)
	inner = protowire.AppendString(inner, name)

	// Request: field 3 = create_ledger (CreateLedgerRequest)
	var req []byte
	req = protowire.AppendTag(req, 3, protowire.BytesType)
	req = protowire.AppendBytes(req, inner)

	return req
}

func encodeDeleteLedgerRequest(name string) []byte {
	// DeleteLedgerRequest: field 1 = name (string)
	var inner []byte
	inner = protowire.AppendTag(inner, 1, protowire.BytesType)
	inner = protowire.AppendString(inner, name)

	// Request: field 4 = delete_ledger (DeleteLedgerRequest)
	var req []byte
	req = protowire.AppendTag(req, 4, protowire.BytesType)
	req = protowire.AppendBytes(req, inner)

	return req
}

func encodeApplyRequest(requests ...[]byte) []byte {
	// ApplyRequest: field 1 = requests (repeated Request)
	var msg []byte
	for _, r := range requests {
		msg = protowire.AppendTag(msg, 1, protowire.BytesType)
		msg = protowire.AppendBytes(msg, r)
	}

	return msg
}
