package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// This annotation belongs to the Cluster, so an operator can supply historical
// inputs without mutating the installed Pod template. The source identity and
// released hash authenticate the inputs; template equality alone cannot prove
// that external Pod-admission inputs (such as ServiceAccount annotations) match.
const annotationLegacySpecHashProof = "ledger.formance.com/legacy-spec-hash-proof"

type legacySpecHashProof struct {
	Version     int                         `json:"version"`
	SourceUID   types.UID                   `json:"sourceUID"`
	OldSpecHash string                      `json:"oldSpecHash"`
	Spec        *ledgerv1alpha1.ClusterSpec `json:"spec"`
}

type storageHashMigrationError struct {
	reason string
	detail string
}

func (e *storageHashMigrationError) Error() string { return e.detail }

func readLegacySpecHashProof(ledger *ledgerv1alpha1.Cluster, existing *appsv1.StatefulSet) (*ledgerv1alpha1.ClusterSpec, error) {
	raw := ledger.Annotations[annotationLegacySpecHashProof]
	if raw == "" {
		return nil, &storageHashMigrationError{reason: "HashMigrationRequired", detail: fmt.Sprintf("cannot reconstruct the released Pod spec hash; supply Cluster annotation %s with version 1, sourceUID %q, oldSpecHash %q and the fully-effective historical ClusterSpec", annotationLegacySpecHashProof, existing.UID, existing.Spec.Template.Annotations[annotationSpecHash])}
	}
	invalid := func(detail string) error {
		return &storageHashMigrationError{reason: "HashMigrationInvalid", detail: "invalid legacy spec-hash proof: " + detail}
	}
	var proof legacySpecHashProof
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&proof); err != nil {
		return nil, invalid(err.Error())
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, invalid("expected one JSON object")
	}
	if proof.Version != 1 || proof.Spec == nil {
		return nil, invalid("version must be 1 and spec must contain the fully-effective historical ClusterSpec")
	}
	installed := existing.Spec.Template.Annotations[annotationSpecHash]
	if existing.UID == "" || proof.SourceUID != existing.UID || installed == "" || proof.OldSpecHash != installed {
		return nil, invalid("sourceUID and oldSpecHash must match the installed StatefulSet and Pod template")
	}
	// Do not apply current defaults: the proof must reproduce exactly the inputs
	// that the released operator hashed, including omitted quantities and fields.
	if computePodSpecHash(proof.Spec, false) != installed {
		return nil, invalid("historical spec does not reproduce the installed released hash")
	}

	return proof.Spec, nil
}

func setStorageSpecHashFailure(ledger *ledgerv1alpha1.Cluster, err error) {
	reason := "AdmissionCheckFailed"
	if migrationErr, ok := errors.AsType[*storageHashMigrationError](err); ok {
		reason = migrationErr.reason
	}
	setClaimTemplatesCondition(ledger, false, reason, err.Error())
}
