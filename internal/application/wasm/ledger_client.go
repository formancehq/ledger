package wasm

import (
	"context"
	"fmt"
	"net"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/wasmpb"
)

const (
	ledgerCallBufferSize  = 1 << 20
	ledgerCallMessageSize = 32 << 20
)

// GRPCLedgerCaller exposes the existing BucketService to WASM through an
// in-process gRPC transport. It does not bind a socket and therefore avoids
// routing module calls through the public API while preserving the normal
// service implementation and protobuf contracts.
type GRPCLedgerCaller struct {
	server *grpc.Server
	conn   *grpc.ClientConn
	lis    *bufconn.Listener
}

func NewGRPCLedgerCaller(service servicepb.BucketServiceServer) (*GRPCLedgerCaller, error) {
	if service == nil {
		return nil, fmt.Errorf("nil bucket service")
	}
	lis := bufconn.Listen(ledgerCallBufferSize)
	// WASM modules execute as leader-owned background workers. Their calls use
	// the in-process transport, so stamp the context with the same system actor
	// attribution that other internal workers use before entering admission.
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(ledgerCallMessageSize),
		grpc.MaxSendMsgSize(ledgerCallMessageSize),
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			return handler(internalauth.WithSystemActor(ctx, commands.ComponentWasm), req)
		}),
	)
	servicepb.RegisterBucketServiceServer(server, service)
	go func() { _ = server.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///wasm-ledger", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(ledgerCallMessageSize),
		grpc.MaxCallSendMsgSize(ledgerCallMessageSize),
	))
	if err != nil {
		server.Stop()
		_ = lis.Close()
		return nil, fmt.Errorf("create in-process ledger client: %w", err)
	}
	return &GRPCLedgerCaller{server: server, conn: conn, lis: lis}, nil
}

func (c *GRPCLedgerCaller) Close() error {
	if c == nil {
		return nil
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
	if c.server != nil {
		c.server.Stop()
	}
	if c.lis != nil {
		_ = c.lis.Close()
	}
	return nil
}

func (c *GRPCLedgerCaller) Call(ctx context.Context, request []byte) ([]byte, error) {
	var envelope wasmpb.LedgerCallRequest
	if err := proto.Unmarshal(request, &envelope); err != nil {
		return nil, fmt.Errorf("decode ledger call: %w", err)
	}
	method := envelope.GetMethod()
	if !strings.HasPrefix(method, "/") {
		method = "/ledger.BucketService/" + method
	}
	parts := strings.Split(strings.TrimPrefix(method, "/"), "/")
	if len(parts) != 2 || parts[0] != "ledger.BucketService" {
		return nil, fmt.Errorf("unsupported ledger call method %q", method)
	}
	descriptor := servicepb.File_bucket_proto.Services().ByName("BucketService").Methods().ByName(protoreflect.Name(parts[1]))
	if descriptor == nil {
		return nil, fmt.Errorf("unknown ledger call method %q", method)
	}
	if descriptor.IsStreamingClient() || descriptor.IsStreamingServer() {
		return nil, fmt.Errorf("streaming ledger call method %q is not supported by this ABI", method)
	}

	input := dynamicpb.NewMessage(descriptor.Input())
	if err := proto.Unmarshal(envelope.GetPayload(), input); err != nil {
		return nil, fmt.Errorf("decode %s request: %w", method, err)
	}
	output := dynamicpb.NewMessage(descriptor.Output())
	if err := c.conn.Invoke(ctx, method, input, output); err != nil {
		return nil, err
	}
	payload, err := proto.Marshal(output)
	if err != nil {
		return nil, fmt.Errorf("encode %s response: %w", method, err)
	}
	return proto.Marshal(&wasmpb.LedgerCallResponse{Payload: payload})
}
