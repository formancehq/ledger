package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckGoSourceDetectsImportedCallsWithoutTextFalsePositives(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

import clock "time"

func TestExample() {
	clock.Sleep(1)
	_ = "time.Sleep(1)"
	// time.Sleep(1)
}
`)

	findings, err := checkGoSource("sample_test.go", source)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Equal(t, 6, findings[0].line)
}

func TestCheckGoSourceDetectsDotImportedEnvironmentReads(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

import . "os"

func read() string {
	return Getenv("LOCAL_POLICY")
}
`)

	findings, err := checkGoSource("internal/infra/state/nested/sample.go", source)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Equal(t, 6, findings[0].line)
}

func TestCheckGoSourceIgnoresShadowedPackageNames(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

import "time"

type sleeper struct{}

func (s sleeper) Sleep(int) {}

func TestExample() {
	time := sleeper{}
	time.Sleep(1)
}

var _ = time.Second
`)

	findings, err := checkGoSource("sample_test.go", source)
	require.NoError(t, err)
	require.Empty(t, findings)
}

func TestCheckProtoSourceDetectsMultilineAndInlineDeclarations(t *testing.T) {
	t.Parallel()

	source := []byte(`syntax = "proto3";

message Example {
	reserved
		2,
		4;
	string value = 1; reserved "old_value";
}
`)

	findings := checkProtoSource("misc/proto/example.proto", source)
	require.Len(t, findings, 2)
	require.Equal(t, 4, findings[0].line)
	require.Equal(t, 7, findings[1].line)
}

func TestCheckProtoSourceIgnoresCommentsAndStrings(t *testing.T) {
	t.Parallel()

	source := []byte(`syntax = "proto3";

// reserved 1;
/*
reserved 2;
*/
message Example {
	string note = 1 [default = "reserved 3;"];
}
`)

	require.Empty(t, checkProtoSource("misc/proto/example.proto", source))
}

func TestIsDeterministicFSMPathIncludesNestedPackages(t *testing.T) {
	t.Parallel()

	require.True(t, isDeterministicFSMPath("internal/domain/processing/numscript/example.go"))
	require.True(t, isDeterministicFSMPath("internal/infra/plan/planerr/example.go"))
	require.False(t, isDeterministicFSMPath("internal/application/admission/example.go"))
}

func TestCheckGoSourceDetectsBoundaryImportInTheBusinessCore(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

import (
	"github.com/formancehq/ledger/v3/internal/adapter/apierr"
	"github.com/formancehq/ledger/v3/internal/domain"
)

var _ = apierr.Remote{}
var _ = domain.ErrNotFound
`)

	findings, err := checkGoSource("internal/application/admission/sample.go", source)
	require.NoError(t, err)
	require.Len(t, findings, 1, "only the apierr import may be reported")
	require.Equal(t, 4, findings[0].line)
	require.Contains(t, findings[0].message, "must not import")
}

func TestCheckGoSourceDetectsBoundaryImportInTests(t *testing.T) {
	t.Parallel()

	// A test inside the business core is covered too: constructing a decoded
	// remote failure there is exactly the confusion the rule prevents, and the
	// sleep check must not displace the import check.
	source := []byte(`package sample

import "github.com/formancehq/ledger/v3/internal/adapter/apierr"

var _ = apierr.Remote{}
`)

	findings, err := checkGoSource("internal/infra/state/sample_test.go", source)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Equal(t, 3, findings[0].line)
}

func TestCheckGoSourceAllowsBoundaryImportOutsideTheBusinessCore(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

import "github.com/formancehq/ledger/v3/internal/adapter/apierr"

var _ = apierr.Remote{}
`)

	for _, path := range []string{
		"internal/adapter/http/sample.go",
		"internal/adapter/grpcerr/sample.go",
		"cmd/ledgerctl/cmdutil/sample.go",
	} {
		findings, err := checkGoSource(path, source)
		require.NoError(t, err)
		require.Empty(t, findings, "the boundary representation is legitimate at %s", path)
	}
}

func TestIsBusinessCorePathCoversRaiseFreezeAndAuditTrees(t *testing.T) {
	t.Parallel()

	require.True(t, isBusinessCorePath("internal/domain/errors.go"))
	require.True(t, isBusinessCorePath("internal/domain/processing/numscript/example.go"))
	require.True(t, isBusinessCorePath("internal/application/admission/example.go"))
	require.True(t, isBusinessCorePath("internal/infra/state/audit_failure.go"))
	require.True(t, isBusinessCorePath("internal/infra/plan/planerr/example.go"))
	require.True(t, isBusinessCorePath("internal/infra/preload/example.go"))

	require.False(t, isBusinessCorePath("internal/adapter/http/error_handler.go"))
	require.False(t, isBusinessCorePath("internal/adapter/apierr/apierr.go"))
	require.False(t, isBusinessCorePath("cmd/ledgerctl/cmdutil/errors.go"))
}

func TestCheckGoSourceRestrictsAttributionCapabilityMinting(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

import "github.com/formancehq/ledger/v3/internal/domain/attribution"

func build() { _, _ = attribution.New(nil) }
`)

	findings, err := checkGoSource("internal/application/newproducer/worker.go", source)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].message, "capabilities may only be minted")
}

func TestCheckGoSourceRestrictsProposalAttributionSites(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

import "github.com/formancehq/ledger/v3/internal/pkg/commands"

func build(cmd struct{ CallerSnapshot any }) {
	cmd.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentBackup)
	_ = commands.NewCommand()
}
`)

	findings, err := checkGoSource("internal/application/newproducer/worker.go", source)
	require.NoError(t, err)
	require.Len(t, findings, 3)
}

func TestCheckGoSourceRestrictsForwardedAttributionSetter(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

import auth "github.com/formancehq/ledger/v3/internal/adapter/auth"

func attach(ctx any, capability any) { _ = auth.WithForwardedAttribution(ctx, capability) }
`)

	findings, err := checkGoSource("internal/application/newproducer/worker.go", source)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].message, "cluster-peer authentication")
}

func TestCheckGoSourceRestrictsDotImportedAttributionSites(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		pkg     string
		call    string
		message string
	}{
		{"capability", "internal/domain/attribution", "New(nil)", "capabilities may only be minted"},
		{"system capability", "internal/domain/attribution", "NewSystem(ComponentBackup)", "capabilities may only be minted"},
		{"system snapshot", "internal/pkg/commands", "SystemCallerSnapshot(ComponentBackup)", "named producer allowlist"},
		{"proposal", "internal/pkg/commands", "NewCommand()", "write proposals may only be built"},
		{"system context", "internal/adapter/auth", "WithSystemActor(nil, nil)", "system actor contexts"},
		{"forwarded context", "internal/adapter/auth", "WithForwardedAttribution(nil, nil)", "cluster-peer authentication"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := []byte("package sample\nimport . \"github.com/formancehq/ledger/v3/" + tc.pkg + "\"\nfunc build() { " + tc.call + " }")
			findings, err := checkGoSource("internal/application/newproducer/worker.go", source)
			require.NoError(t, err)
			require.Len(t, findings, 1)
			require.Contains(t, findings[0].message, tc.message)
		})
	}
}
