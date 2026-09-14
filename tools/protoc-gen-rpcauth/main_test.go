package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const commonFixture = `syntax = "proto3";
package common;
option go_package = "example/internal/proto/commonpb";
import "google/protobuf/descriptor.proto";

enum AuthScope {
  AUTH_SCOPE_UNSPECIFIED = 0;
  AUTH_SCOPE_LEDGER_READ = 1;
}

enum DynamicAuthResolver {
  DYNAMIC_AUTH_RESOLVER_UNSPECIFIED = 0;
  DYNAMIC_AUTH_RESOLVER_APPLY = 1;
}

message MethodAuthPolicy {
  oneof policy {
    bool public = 1;
    AuthScope fixed_scope = 2;
    DynamicAuthResolver dynamic_resolver = 3;
  }
}

extend google.protobuf.MethodOptions {
  MethodAuthPolicy auth_policy = 71002;
}
`

const clusterFixture = `syntax = "proto3";
package cluster;
option go_package = "example/internal/proto/clusterpb";
import "common.proto";

service ClusterService {
  rpc GetClusterState(Request) returns (Response) {
    option (common.auth_policy) = { fixed_scope: AUTH_SCOPE_LEDGER_READ };
  }
}
message Request {}
message Response {}
`

func TestGeneratorAcceptsCompletePoliciesAndEmitsRuntimeRegistry(t *testing.T) {
	t.Parallel()

	output, err := runGenerator(t, `syntax = "proto3";
package ledger;
option go_package = "example/internal/proto/servicepb";
import "common.proto";

service BucketService {
  rpc Discovery(Request) returns (Response) {
    option (common.auth_policy) = { public: true };
  }
  rpc GetLedger(Request) returns (Response) {
    option (common.auth_policy) = { fixed_scope: AUTH_SCOPE_LEDGER_READ };
  }
  rpc Apply(Request) returns (Response) {
    option (common.auth_policy) = { dynamic_resolver: DYNAMIC_AUTH_RESOLVER_APPLY };
  }
}
message Request {}
message Response {}
`)
	require.NoError(t, err)
	require.Contains(t, output, `"/ledger.BucketService/Discovery"`)
	require.Contains(t, output, `"/ledger.BucketService/GetLedger"`)
	require.Contains(t, output, `"/ledger.BucketService/Apply"`)
	require.Contains(t, output, "RPCAuthPolicyForMethod")
}

func TestGeneratorRejectsInvalidPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		methodBody string
		wantError  string
	}{
		{name: "missing", wantError: "has no auth policy"},
		{name: "public false", methodBody: `option (common.auth_policy) = { public: false };`, wantError: "public=true"},
		{name: "unspecified fixed scope", methodBody: `option (common.auth_policy) = { fixed_scope: AUTH_SCOPE_UNSPECIFIED };`, wantError: "unspecified fixed scope"},
		{name: "unknown fixed scope", methodBody: `option (common.auth_policy) = { fixed_scope: 99 };`, wantError: "unknown fixed scope"},
		{name: "unspecified dynamic resolver", methodBody: `option (common.auth_policy) = { dynamic_resolver: DYNAMIC_AUTH_RESOLVER_UNSPECIFIED };`, wantError: "unspecified dynamic resolver"},
		{name: "unknown dynamic resolver", methodBody: `option (common.auth_policy) = { dynamic_resolver: 99 };`, wantError: "unknown dynamic resolver"},
		{name: "contradictory", methodBody: `option (common.auth_policy) = { public: true fixed_scope: AUTH_SCOPE_LEDGER_READ };`, wantError: "another member of oneof"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := runGenerator(t, fmt.Sprintf(`syntax = "proto3";
package ledger;
option go_package = "example/internal/proto/servicepb";
import "common.proto";

service BucketService {
  rpc Method(Request) returns (Response) { %s }
}
message Request {}
message Response {}
`, test.methodBody))
			require.Error(t, err)
			require.Contains(t, err.Error(), test.wantError)
		})
	}
}

func TestGeneratorRejectsMissingPublicService(t *testing.T) {
	t.Parallel()

	_, err := runGeneratorWithCluster(t, `syntax = "proto3";
package ledger;
option go_package = "example/internal/proto/servicepb";
import "common.proto";

service BucketService {
  rpc Discovery(Request) returns (Response) {
    option (common.auth_policy) = { public: true };
  }
}
message Request {}
message Response {}
`, false)
	require.ErrorContains(t, err, `Ledger public service "cluster.ClusterService" not found`)
}

func runGenerator(t *testing.T, serviceProto string) (string, error) {
	return runGeneratorWithCluster(t, serviceProto, true)
}

func runGeneratorWithCluster(t *testing.T, serviceProto string, includeCluster bool) (string, error) {
	t.Helper()

	dir := t.TempDir()
	plugin := filepath.Join(dir, "protoc-gen-rpcauth")

	build := exec.Command("go", "build", "-o", plugin, ".")
	build.Dir = "."
	build.Env = append(os.Environ(), "GOWORK=off")
	combined, err := build.CombinedOutput()
	require.NoError(t, err, string(combined))

	require.NoError(t, os.WriteFile(filepath.Join(dir, "common.proto"), []byte(commonFixture), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "service.proto"), []byte(serviceProto), 0o600))
	if includeCluster {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "cluster.proto"), []byte(clusterFixture), 0o600))
	}

	googleInclude := protobufIncludeDir(t)
	protocArgs := []string{
		"--plugin=protoc-gen-rpcauth=" + plugin,
		"--rpcauth_out=" + dir,
		"--rpcauth_opt=module=example",
		"-I", dir,
		"-I", googleInclude,
		filepath.Join(dir, "common.proto"),
		filepath.Join(dir, "service.proto"),
	}
	if includeCluster {
		protocArgs = append(protocArgs, filepath.Join(dir, "cluster.proto"))
	}
	cmd := exec.Command("protoc", protocArgs...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("protoc: %w: %s", err, stderr.String())
	}

	generated, err := os.ReadFile(filepath.Join(dir, "internal/proto/commonpb/common_rpc_auth_policy.pb.go"))
	if err != nil {
		return "", err
	}

	return string(generated), nil
}

func protobufIncludeDir(t *testing.T) string {
	t.Helper()

	cmd := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "google.golang.org/protobuf")
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.Output()
	require.NoError(t, err)

	return filepath.Join(strings.TrimSpace(string(out)), "src")
}
