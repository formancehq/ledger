package grpc_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestPublishedContractIsTheRegisteredPublicClosure(t *testing.T) {
	protoDir := filepath.Join("..", "proto")
	bytes, err := os.ReadFile(filepath.Join(protoDir, "ledger-public.protoset"))
	require.NoError(t, err)
	manifestBytes, err := os.ReadFile(filepath.Join("..", "contract.json"))
	require.NoError(t, err)
	var manifest struct {
		ClientModule             string   `json:"clientModule"`
		ClientVersion            string   `json:"clientVersion"`
		ContractDescriptorSHA256 string   `json:"contractDescriptorSHA256"`
		ProtocolVersion          string   `json:"protocolVersion"`
		CompatibleServerVersions []string `json:"compatibleServerVersions"`
	}
	require.NoError(t, json.Unmarshal(manifestBytes, &manifest))
	require.Equal(t, "github.com/formancehq/ledger/pkg/client/v3", manifest.ClientModule)
	require.NotEmpty(t, manifest.ClientVersion)
	require.NotEmpty(t, manifest.ProtocolVersion)
	require.NotEmpty(t, manifest.CompatibleServerVersions)
	digest := sha256.Sum256(bytes)
	require.Equal(t, hex.EncodeToString(digest[:]), manifest.ContractDescriptorSHA256)

	var set descriptorpb.FileDescriptorSet
	require.NoError(t, proto.Unmarshal(bytes, &set))
	public := map[string]protoreflect.FileDescriptor{
		"common.proto":    ledgerpb.File_common_proto,
		"signature.proto": ledgerpb.File_signature_proto,
		"audit.proto":     ledgerpb.File_audit_proto,
		"bucket.proto":    ledgerpb.File_bucket_proto,
		"cluster.proto":   ledgerpb.File_cluster_proto,
		"restore.proto":   ledgerpb.File_restore_proto,
	}
	private := map[string]struct{}{
		"raft_transport.proto":   {},
		"cluster_bootstrap.proto": {},
		"raft_cmd.proto":         {},
		"snapshot.proto":          {},
		"events.proto":            {},
		"proposal.proto":          {},
		"internal_state.proto":    {},
	}
	methodCount := 0
	for _, file := range set.File {
		if file.GetName() == "google/protobuf/descriptor.proto" {
			continue
		}
		_, isPrivate := private[file.GetName()]
		require.False(t, isPrivate, "private descriptor leaked into public contract: %s", file.GetName())
		registered, ok := public[file.GetName()]
		require.True(t, ok, "unexpected public descriptor %s", file.GetName())
		require.True(t, proto.Equal(file, protodesc.ToFileDescriptorProto(registered)), file.GetName())
		for _, dependency := range file.Dependency {
			_, isPrivate := private[dependency]
			require.False(t, isPrivate, "public descriptor depends on private proto %s: %s", dependency, file.GetName())
		}
		source, err := os.ReadFile(filepath.Join(protoDir, file.GetName()))
		require.NoError(t, err)
		require.NotEmpty(t, source)
		for _, service := range file.Service {
			methodCount += len(service.Method)
		}
		delete(public, file.GetName())
	}
	require.Empty(t, public)
	require.Equal(t, 55, methodCount)
}

func TestInternalCallerAttributionReasonIsAbsentFromPublicDescriptor(t *testing.T) {
	reasons := ledgerpb.File_common_proto.Enums().ByName("ErrorReason")
	require.NotNil(t, reasons)
	require.Nil(t, reasons.Values().ByName("ERROR_REASON_INVALID_CALLER_ATTRIBUTION"))
	require.Nil(t, reasons.Values().ByNumber(72))
}
