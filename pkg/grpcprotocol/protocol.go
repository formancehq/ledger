// Package grpcprotocol identifies the client-facing Ledger gRPC contract.
// It is independent of release versions and is required on every business RPC.
package grpcprotocol

import (
	"context"

	"google.golang.org/grpc"
)

const (
	// Version must change when the client-facing wire format or semantics break.
	// See docs/technical/architecture/subsystems/api/protocol-compatibility.md.
	// A compiled constant also identifies local builds without release ldflags.
	Version = "5"
	// MetadataKey carries the protocol version, not authentication credentials.
	MetadataKey = "ledger-protocol-version"
)

// ClientOption declares this client's protocol on every unary and streaming
// RPC, including retries and calls made after the connection changes backend.
// Other clients must send MetadataKey with their own supported version.
func ClientOption() grpc.DialOption {
	return grpc.WithPerRPCCredentials(protocolCredentials{})
}

type protocolCredentials struct{}

func (protocolCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{MetadataKey: Version}, nil
}

func (protocolCredentials) RequireTransportSecurity() bool {
	// The version is public metadata; plaintext deployments remain supported.
	return false
}
