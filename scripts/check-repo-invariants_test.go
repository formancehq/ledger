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

func TestCheckGoSourceAllowsSleepOnlyInsideSynctestBubble(t *testing.T) {
	t.Parallel()

	source := []byte(`package sample

import (
	"testing"
	bubble "testing/synctest"
	"time"
)

func sleepHelper() {
	time.Sleep(1)
}

func register(callback func()) {}

func TestExample(t *testing.T) {
	outer := t
	var escaped func()
	bubble.Test(t, func(t *testing.T) {
		time.Sleep(1)
		go func() { time.Sleep(1) }()
		defer func() { time.Sleep(1) }()
		t.Cleanup(func() { time.Sleep(1) })
		sleepHelper()
		escaped = func() { time.Sleep(1) }
		register(func() { time.Sleep(1) })
		outer.Cleanup(func() { time.Sleep(1) })
		{
			t := outer
			t.Cleanup(func() { time.Sleep(1) })
		}
	})
	escaped()
	time.Sleep(1)
}
`)

	findings, err := checkGoSource("sample_test.go", source)
	require.NoError(t, err)
	lines := make([]int, 0, len(findings))
	for _, finding := range findings {
		lines = append(lines, finding.line)
	}
	require.Equal(t, []int{
		10, // a helper is not provably inside a bubble
		24, // a stored closure may run after the bubble returns
		25, // a callee may retain its callback and run it after the bubble
		26, // the outer t runs its cleanups after the bubble returns
		29, // a t shadowed by the outer test is not the bubble's t
		33, // a sleep after the bubble returns uses the real clock
	}, lines)
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

func TestProposalCodecMayOnlyCopyExistingAttribution(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		value   string
		allowed bool
	}{
		{"p.GetCallerSnapshot()", true},
		{"nil", false},
		{"other.GetCallerSnapshot()", false},
		{"newCaller()", false},
	} {
		source := []byte("package sample\nimport \"github.com/formancehq/ledger/v3/internal/proto/commonpb\"\nfunc header(p any) any { return &Proposal{CallerSnapshot: " + test.value + "} }")
		findings, err := checkGoSource("internal/pkg/proposals/payload.go", source)
		require.NoError(t, err)
		if test.allowed {
			require.Empty(t, findings)
		} else {
			require.Len(t, findings, 1)
		}
	}
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

func TestCheckGoSourceAllowsOnlyNamedDrainProducer(t *testing.T) {
	t.Parallel()
	source := []byte(`package sample
import "github.com/formancehq/ledger/v3/internal/pkg/commands"
func build() {
    cmd := commands.NewCommand()
    cmd.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterPeer)
}`)
	allowed, err := checkGoSource("internal/infra/node/drain.go", source)
	require.NoError(t, err)
	require.Empty(t, allowed)
	rejected, err := checkGoSource("internal/infra/node/other.go", source)
	require.NoError(t, err)
	require.Len(t, rejected, 3)
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
