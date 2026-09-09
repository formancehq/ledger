package audit_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/formancehq/ledger/plugins/fctl/internal/audit"
)

const root = "../.."

func load(t *testing.T) audit.Inputs {
	t.Helper()

	var in audit.Inputs
	var err error

	if in.V2Inventory, in.V2InvRaw, err = audit.LoadInventory(
		filepath.Join(root, "ledger-v2", "inventory.json")); err != nil {
		t.Fatalf("load v2 inventory: %v", err)
	}

	if in.V2Manifest, in.V2ManRaw, err = audit.LoadManifest(
		filepath.Join(root, "ledger-v2", "manifest.json")); err != nil {
		t.Fatalf("load v2 manifest: %v", err)
	}

	if in.V3Inventory, in.V3InvRaw, err = audit.LoadInventory(
		filepath.Join(root, "ledger-v3", "inventory.json")); err != nil {
		t.Fatalf("load v3 inventory: %v", err)
	}

	if in.V3Manifest, in.V3ManRaw, err = audit.LoadManifest(
		filepath.Join(root, "ledger-v3", "manifest.json")); err != nil {
		t.Fatalf("load v3 manifest: %v", err)
	}

	return in
}

// TestCommittedPreparationPassesAudit is the gate: the committed documents
// must satisfy every recorded invariant.
func TestCommittedPreparationPassesAudit(t *testing.T) {
	report := audit.Run(load(t))

	for _, f := range report.Findings {
		t.Errorf("[%s] %s: %s", f.Scope, f.Check, f.Detail)
	}

	if report.Checks == 0 {
		t.Fatal("audit ran zero checks")
	}

	t.Logf("%d checks passed", report.Checks)
}

func TestV2Denominator(t *testing.T) {
	inv := load(t).V2Inventory

	if got := len(inv.Commands); got != audit.V2BaselineCommands {
		t.Errorf("baseline commands = %d, want %d", got, audit.V2BaselineCommands)
	}

	included := inv.IncludedCommands()
	if got := len(included); got != audit.V2Denominator {
		t.Errorf("included commands = %d, want %d", got, audit.V2Denominator)
	}

	if got := len(inv.Commands) - len(included); got != audit.V2V1OnlyExcluded {
		t.Errorf("excluded commands = %d, want %d", got, audit.V2V1OnlyExcluded)
	}

	for _, c := range included {
		if c.APIMajor != "V2" {
			t.Errorf("%s: published with api_major %q, want V2", c.Path, c.APIMajor)
		}

		if !strings.HasPrefix(c.SDKMethod, "V2.") {
			t.Errorf("%s: sdk_method %q is not a V2 call", c.Path, c.SDKMethod)
		}
	}
}

// TestV1OnlyCommandsNeverLeakIntoV2Catalogue is the invariant Task 7 names
// explicitly.
func TestV1OnlyCommandsNeverLeakIntoV2Catalogue(t *testing.T) {
	for _, c := range load(t).V2Inventory.Commands {
		if c.APIMajor != "V1" {
			continue
		}

		if c.Included == nil || *c.Included {
			t.Errorf("%s is V1-only (%s) but is published by ledger-v2", c.Path, c.SDKMethod)
		}

		if c.OperationID != nil {
			t.Errorf("%s is V1-only but records a v2 operationId %q", c.Path, *c.OperationID)
		}
	}
}

func TestV3ClassificationIsTotal(t *testing.T) {
	inv := load(t).V3Inventory

	got := inv.Tally()
	want := audit.Counts{
		ExecutableTotal:  audit.V3ExecutableCommands,
		Product:          audit.V3Product,
		Operator:         audit.V3Operator,
		HostLocal:        audit.V3HostLocal,
		SigningEventSink: audit.V3SigningEventSink,
	}

	if got != want {
		t.Errorf("tally = %+v, want %+v", got, want)
	}

	sum := got.Product + got.Operator + got.HostLocal + got.SigningEventSink
	if sum != len(inv.Commands) {
		t.Errorf("buckets sum to %d but there are %d commands: a command is unclassified",
			sum, len(inv.Commands))
	}
}

// TestPluginsShareNoContractSurface encodes the 2026-09-10 two-plugin
// decision.
func TestPluginsShareNoContractSurface(t *testing.T) {
	in := load(t)

	if in.V2Inventory.Plugin == in.V3Inventory.Plugin {
		t.Fatal("both inventories claim the same plugin name")
	}

	if in.V2Inventory.ProductMajor == in.V3Inventory.ProductMajor {
		t.Error("plugins must declare disjoint product majors")
	}

	if in.V2Manifest.PluginModulePath == in.V3Manifest.PluginModulePath {
		t.Error("plugins must have distinct module paths")
	}

	v3 := make(map[string]bool, len(in.V3Inventory.Commands))
	for _, c := range in.V3Inventory.Commands {
		v3[c.Path] = true
	}

	for _, c := range in.V2Inventory.Commands {
		if v3[c.Path] {
			t.Errorf("command path %q appears in both catalogues", c.Path)
		}
	}
}

func TestOnlyV3DeclaresSigningCapability(t *testing.T) {
	in := load(t)

	if in.V2Inventory.SigningCapability != nil {
		t.Errorf("ledger-v2 declares signing capability %q", *in.V2Inventory.SigningCapability)
	}

	if len(in.V2Manifest.CapabilitiesDeclared) != 0 {
		t.Errorf("ledger-v2 declares capabilities %v", in.V2Manifest.CapabilitiesDeclared)
	}

	if in.V3Inventory.SigningCapability == nil ||
		*in.V3Inventory.SigningCapability != audit.SigningCapability {
		t.Errorf("ledger-v3 must declare %q", audit.SigningCapability)
	}

	if in.V3Manifest.ProductMajors[0] != 3 {
		t.Errorf("signing capability must be bound to product major 3, got %v",
			in.V3Manifest.ProductMajors)
	}
}

// TestNoRuntimeArtifactIsDeclared keeps the closed 4B/4C/4D gates honest: this
// preparation must not claim a component, install record or dual-host artifact.
func TestNoRuntimeArtifactIsDeclared(t *testing.T) {
	in := load(t)

	if in.V2Manifest.RuntimeArtifact != nil {
		t.Errorf("ledger-v2 declares a runtime artifact %q", *in.V2Manifest.RuntimeArtifact)
	}

	if in.V3Manifest.RuntimeArtifact != nil {
		t.Errorf("ledger-v3 declares a runtime artifact %q", *in.V3Manifest.RuntimeArtifact)
	}

	if in.V2Manifest.Status != "preparation" || in.V3Manifest.Status != "preparation" {
		t.Error("both manifests must be marked status=preparation")
	}
}

// TestV3ModulePathReachesGeneratedStubs guards the empirically verified
// constraint: the generated v3 stubs live in an internal package, so only a
// module path under github.com/formancehq/ledger/v3/ can import them.
func TestV3ModulePathReachesGeneratedStubs(t *testing.T) {
	m := load(t).V3Manifest

	const prefix = "github.com/formancehq/ledger/v3/"
	if !strings.HasPrefix(m.PluginModulePath, prefix) {
		t.Errorf("plugin module path %q is not under %q, so it cannot import the internal generated stubs",
			m.PluginModulePath, prefix)
	}
}

func TestCommittedDocumentsAreCanonical(t *testing.T) {
	in := load(t)

	for _, f := range []struct {
		name string
		raw  []byte
	}{
		{"ledger-v2/inventory.json", in.V2InvRaw},
		{"ledger-v2/manifest.json", in.V2ManRaw},
		{"ledger-v3/inventory.json", in.V3InvRaw},
		{"ledger-v3/manifest.json", in.V3ManRaw},
	} {
		ok, _, err := audit.IsCanonical(f.raw)
		if err != nil {
			t.Errorf("%s: %v", f.name, err)
			continue
		}

		if !ok {
			t.Errorf("%s is not canonical; regenerate with fctl-plugin-audit -write", f.name)
		}
	}
}

// TestCanonicalizeIsDeterministicAndIdempotent is the determinism property the
// generation gate relies on: key order in the input must not change the output,
// and canonicalizing twice must not drift.
func TestCanonicalizeIsDeterministicAndIdempotent(t *testing.T) {
	a := []byte(`{"b":1,"a":{"z":[3,2,1],"y":"x"},"c":null}`)
	b := []byte(`{"a":{"y":"x","z":[3,2,1]},"c":null,"b":1}`)

	ca, err := audit.Canonicalize(a)
	if err != nil {
		t.Fatalf("canonicalize a: %v", err)
	}

	cb, err := audit.Canonicalize(b)
	if err != nil {
		t.Fatalf("canonicalize b: %v", err)
	}

	if string(ca) != string(cb) {
		t.Errorf("key order changed the canonical form:\n%s\n%s", ca, cb)
	}

	again, err := audit.Canonicalize(ca)
	if err != nil {
		t.Fatalf("canonicalize twice: %v", err)
	}

	if string(again) != string(ca) {
		t.Errorf("canonicalize is not idempotent:\n%s\n%s", ca, again)
	}
}

// TestCanonicalizePreservesLargeIntegers proves the canonical form does not
// round-trip integers through float64, which would silently corrupt sequence
// numbers and 64-bit identifiers.
func TestCanonicalizePreservesLargeIntegers(t *testing.T) {
	const big = "9007199254740993"

	out, err := audit.Canonicalize([]byte(`{"n":` + big + `}`))
	if err != nil {
		t.Fatalf("canonicalize: %v", err)
	}

	if !strings.Contains(string(out), big) {
		t.Errorf("large integer not preserved: got %s", out)
	}
}

// TestAuditRejectsAMergedCatalogue proves the audit actually fails when the
// two-plugin boundary is violated, rather than passing vacuously.
func TestAuditRejectsAMergedCatalogue(t *testing.T) {
	in := load(t)

	// Simulate a merged surface: publish a v3 command path from the v2 plugin.
	victim := in.V3Inventory.Commands[0]
	included := true
	in.V2Inventory.Commands = append(in.V2Inventory.Commands, audit.Command{
		Path:     victim.Path,
		APIMajor: "V2",
		Included: &included,
	})

	report := audit.Run(in)
	if report.OK() {
		t.Fatal("audit passed a merged catalogue; the boundary check is vacuous")
	}

	var found bool

	for _, f := range report.Findings {
		if f.Check == "no_shared_command_path" {
			found = true
		}
	}

	if !found {
		t.Errorf("expected a no_shared_command_path finding, got %v", report.Findings)
	}
}

// TestAuditRejectsSigningCapabilityOnV2 proves the RFC 0009 major-3-only
// invariant is enforced, not merely documented.
func TestAuditRejectsSigningCapabilityOnV2(t *testing.T) {
	in := load(t)

	cap := audit.SigningCapability
	in.V2Inventory.SigningCapability = &cap
	in.V2Manifest.CapabilitiesDeclared = []string{cap}

	report := audit.Run(in)
	if report.OK() {
		t.Fatal("audit allowed ledger-v2 to declare the signing capability")
	}

	var found bool

	for _, f := range report.Findings {
		if f.Check == "no_signing_capability" || f.Check == "v2_cannot_name_signing_capability" {
			found = true
		}
	}

	if !found {
		t.Errorf("expected a signing-capability finding, got %v", report.Findings)
	}
}

func TestLoadRejectsMissingAndMalformedDocuments(t *testing.T) {
	dir := t.TempDir()

	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte(`{"schema_version": `), 0o600); err != nil {
		t.Fatal(err)
	}

	missing := filepath.Join(dir, "absent.json")

	if _, _, err := audit.LoadInventory(missing); err == nil {
		t.Error("LoadInventory accepted a missing file")
	}

	if _, _, err := audit.LoadInventory(bad); err == nil {
		t.Error("LoadInventory accepted malformed JSON")
	}

	if _, _, err := audit.LoadManifest(missing); err == nil {
		t.Error("LoadManifest accepted a missing file")
	}

	if _, _, err := audit.LoadManifest(bad); err == nil {
		t.Error("LoadManifest accepted malformed JSON")
	}
}

func TestCanonicalizeRejectsMalformedJSON(t *testing.T) {
	if _, err := audit.Canonicalize([]byte(`{"a":`)); err == nil {
		t.Error("Canonicalize accepted malformed JSON")
	}

	if _, _, err := audit.IsCanonical([]byte(`nope`)); err == nil {
		t.Error("IsCanonical accepted malformed JSON")
	}
}

// TestAuditRejectsAnUnclassifiedV3Command proves the totality check is not
// vacuous: dropping a command's classification must fail the audit.
func TestAuditRejectsAnUnclassifiedV3Command(t *testing.T) {
	in := load(t)
	in.V3Inventory.Commands[0].Classification = ""

	report := audit.Run(in)
	if report.OK() {
		t.Fatal("audit passed an unclassified command")
	}

	var totality, valid bool

	for _, f := range report.Findings {
		switch f.Check {
		case "classification_totality":
			totality = true
		case "classification_valid":
			valid = true
		}
	}

	if !totality || !valid {
		t.Errorf("expected totality and validity findings, got %v", report.Findings)
	}
}

// TestReportIsSerialisable keeps the CLI contract: the report must encode as
// one JSON document on stdout.
func TestReportIsSerialisable(t *testing.T) {
	out, err := json.Marshal(audit.Run(load(t)))
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}

	var back audit.Report
	if err := json.Unmarshal(out, &back); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}

	if !back.OK() {
		t.Errorf("round-tripped report reports errors: %s", out)
	}
}
