package controller

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

// computeSpecHash returns a SHA-256 hash of the serialized Ledger spec.
// This is used as a pod template annotation to trigger rolling updates on spec changes.
func computeSpecHash(spec *ledgerv1alpha1.LedgerSpec) string {
	data, _ := json.Marshal(spec) //nolint:errchkjson // spec is always serializable
	return fmt.Sprintf("%x", sha256.Sum256(data))
}
