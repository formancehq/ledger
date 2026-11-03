package controller

import (
	"fmt"
	"strings"

	"golang.org/x/mod/semver"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// validateLedgerVersion compares declared release tags before the operator
// publishes a server image. Development builds may use opaque image tags;
// release operators require a comparable tag.
func validateLedgerVersion(ledger *ledgerv1alpha1.Cluster, operatorVersion string) (reason string, err error) {
	ledgerVersion := normalizeReleaseVersion(ledger.Spec.Image.Tag)
	operatorRelease := normalizeReleaseVersion(operatorVersion)
	if !semver.IsValid(ledgerVersion) {
		if !semver.IsValid(operatorRelease) {
			return "DevelopmentVersions", nil
		}

		return "LedgerVersionUnknown", fmt.Errorf("cannot deploy Ledger image tag %q with Ledger Operator %s: use a semantic Ledger release tag", ledger.Spec.Image.Tag, operatorVersion)
	}

	if !semver.IsValid(operatorRelease) {
		return "OperatorVersionUnknown", fmt.Errorf("cannot deploy Ledger %s: Ledger Operator version %q is not a semantic version", ledger.Spec.Image.Tag, operatorVersion)
	}
	if semver.Compare(ledgerVersion, operatorRelease) > 0 {
		return "LedgerVersionAheadOfOperator", fmt.Errorf("cannot deploy Ledger %s with Ledger Operator %s: upgrade the operator first", ledger.Spec.Image.Tag, operatorVersion)
	}

	return "Compatible", nil
}

func normalizeReleaseVersion(version string) string {
	if version == "" || strings.HasPrefix(version, "v") {
		return version
	}

	return "v" + version
}
