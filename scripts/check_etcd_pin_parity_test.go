package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func etcdModule(name, require_, replace string) []byte {
	source := "module " + name + "\n\ngo 1.26\n\nrequire (\n\tgo.etcd.io/etcd/server/v3 " + require_ + "\n)\n"
	if replace != "" {
		source += "\n" + replace + "\n"
	}

	return []byte(source)
}

const (
	pinnedReplace = "replace go.etcd.io/etcd/server/v3 v3.7.1 => github.com/formancehq/etcd/server/v3 v3.7.2-0.20260918095251-b5dd36e4493c"
	otherReplace  = "replace go.etcd.io/etcd/server/v3 v3.7.1 => github.com/someone/etcd/server/v3 v3.7.2-0.20260101000000-000000000000"
)

func TestEtcdPinParityAcceptsMatchingPins(t *testing.T) {
	t.Parallel()

	findings, err := checkEtcdPinParity(
		etcdModule("root", "v3.7.1", pinnedReplace),
		etcdModule("workload", "v3.7.1", pinnedReplace),
	)
	require.NoError(t, err)
	require.Empty(t, findings)
}

func TestEtcdPinParityRejectsARequireThatMovedInOneModuleOnly(t *testing.T) {
	t.Parallel()

	// The shape the version-scoped replace is meant to make loud: the root moves
	// to a released etcd and escapes its replace, while the workload keeps both.
	findings, err := checkEtcdPinParity(
		etcdModule("root", "v3.8.0", pinnedReplace),
		etcdModule("workload", "v3.7.1", pinnedReplace),
	)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].message, "ETCD_PIN_DIVERGED")
	require.Contains(t, findings[0].message, "v3.8.0")
}

func TestEtcdPinParityRejectsAReplaceDroppedFromOneModuleOnly(t *testing.T) {
	t.Parallel()

	findings, err := checkEtcdPinParity(
		etcdModule("root", "v3.7.1", pinnedReplace),
		etcdModule("workload", "v3.7.1", ""),
	)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].message, "no replace")
}

func TestEtcdPinParityRejectsDifferentReplacementTargets(t *testing.T) {
	t.Parallel()

	findings, err := checkEtcdPinParity(
		etcdModule("root", "v3.7.1", pinnedReplace),
		etcdModule("workload", "v3.7.1", otherReplace),
	)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].message, "someone/etcd")
}

func TestEtcdPinParityRejectsARequireDroppedFromOneModuleOnly(t *testing.T) {
	t.Parallel()

	plain := []byte("module workload\n\ngo 1.26\n")

	findings, err := checkEtcdPinParity(etcdModule("root", "v3.7.1", pinnedReplace), plain)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].message, "no requirement")

	// and the other way round
	findings, err = checkEtcdPinParity(plain, etcdModule("workload", "v3.7.1", pinnedReplace))
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].message, "no requirement")
}

func TestEtcdPinParityRendersALocalPathReplacementWithoutATrailingAt(t *testing.T) {
	t.Parallel()

	local := "replace go.etcd.io/etcd/server/v3 v3.7.1 => ../../etcd/server"

	findings, err := checkEtcdPinParity(
		etcdModule("root", "v3.7.1", pinnedReplace),
		etcdModule("workload", "v3.7.1", local),
	)
	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].message, "../../etcd/server\"")
	require.NotContains(t, findings[0].message, "etcd/server@")
}

func TestEtcdPinParityIgnoresModulesWithoutEtcd(t *testing.T) {
	t.Parallel()

	plain := []byte("module plain\n\ngo 1.26\n")

	findings, err := checkEtcdPinParity(plain, plain)
	require.NoError(t, err)
	require.Empty(t, findings)
}

func TestEtcdPinParityHoldsForTheRepositoryItself(t *testing.T) {
	// Not parallel: the paths are repository-relative and this test runs from the
	// repository root, where the checker itself runs.
	t.Chdir("..")

	findings, err := checkEtcdPinParityFiles()
	require.NoError(t, err)
	require.Empty(t, findings, "the checked-in modules must agree on the etcd pin")
}
