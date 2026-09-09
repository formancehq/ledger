package audit

import (
	"fmt"
	"sort"
	"strings"
)

// Expected denominators and tallies, fixed by the programme plan and verified
// against source at the pinned revisions. They are constants so that changing
// a denominator is a reviewed edit here, not a side effect of editing data.
const (
	V2BaselineCommands = 23 // old-fctl cmd/ledger executable commands at 693c58e2
	V2V1OnlyExcluded   = 9  // baseline commands whose only SDK call is V1.*
	V2Denominator      = 14 // V2BaselineCommands - V2V1OnlyExcluded

	V3ExecutableCommands = 108 // cmd/ledgerctl executable commands at bb0297cc
	V3Product            = 54
	V3Operator           = 34
	V3HostLocal          = 13
	V3SigningEventSink   = 7
)

// Finding is one audit result. Severity is "error" when an invariant the
// preparation claims is violated.
type Finding struct {
	Severity string `json:"severity"`
	Scope    string `json:"scope"`
	Check    string `json:"check"`
	Detail   string `json:"detail"`
}

// Report is the audit outcome.
type Report struct {
	Checks   int       `json:"checks"`
	Errors   int       `json:"errors"`
	Findings []Finding `json:"findings"`
}

// OK reports whether every check passed.
func (r Report) OK() bool { return r.Errors == 0 }

type collector struct {
	report *Report
	scope  string
}

func (c *collector) check(name string, ok bool, format string, args ...any) {
	c.report.Checks++

	if ok {
		return
	}

	c.report.Errors++
	c.report.Findings = append(c.report.Findings, Finding{
		Severity: "error",
		Scope:    c.scope,
		Check:    name,
		Detail:   fmt.Sprintf(format, args...),
	})
}

// Inputs are the four preparation documents plus their raw bytes, so the audit
// can check both semantics and canonical form.
type Inputs struct {
	V2Inventory Inventory
	V2InvRaw    []byte
	V2Manifest  Manifest
	V2ManRaw    []byte
	V3Inventory Inventory
	V3InvRaw    []byte
	V3Manifest  Manifest
	V3ManRaw    []byte
}

// Run executes every invariant and returns the report.
func Run(in Inputs) Report {
	rep := &Report{}

	auditV2(&collector{report: rep, scope: "ledger-v2"}, in.V2Inventory, in.V2Manifest)
	auditV3(&collector{report: rep, scope: "ledger-v3"}, in.V3Inventory, in.V3Manifest)
	auditBoundary(&collector{report: rep, scope: "boundary"}, in)
	auditCanonical(&collector{report: rep, scope: "canonical"}, in)

	return *rep
}

func auditV2(c *collector, inv Inventory, man Manifest) {
	c.check("schema_version", inv.SchemaVersion == 1, "got %d, want 1", inv.SchemaVersion)
	c.check("plugin_name", inv.Plugin == "ledger-v2", "got %q", inv.Plugin)
	c.check("product_major", inv.ProductMajor == 2, "got %d, want 2", inv.ProductMajor)
	c.check("no_signing_capability", inv.SigningCapability == nil,
		"ledger-v2 must declare no signing capability, got %v", inv.SigningCapability)

	c.check("baseline_command_count", len(inv.Commands) == V2BaselineCommands,
		"got %d, want %d", len(inv.Commands), V2BaselineCommands)

	included := inv.IncludedCommands()
	c.check("denominator", len(included) == V2Denominator,
		"got %d included, want %d", len(included), V2Denominator)

	var excluded, v1Only int

	for _, cmd := range inv.Commands {
		c.check("inclusion_recorded", cmd.Included != nil,
			"%s has no explicit included flag", cmd.Path)

		if cmd.Included == nil || *cmd.Included {
			continue
		}

		excluded++

		c.check("exclusion_reason_named", cmd.ExclusionReason != nil && *cmd.ExclusionReason != "",
			"%s is excluded with no named reason", cmd.Path)
	}

	c.check("excluded_count", excluded == V2V1OnlyExcluded,
		"got %d excluded, want %d", excluded, V2V1OnlyExcluded)

	for _, cmd := range inv.Commands {
		if cmd.APIMajor == "V1" {
			v1Only++

			c.check("v1_only_not_published", cmd.Included != nil && !*cmd.Included,
				"%s is backed only by %s but is published by the v2 plugin", cmd.Path, cmd.SDKMethod)
		}
	}

	c.check("v1_only_count", v1Only == V2V1OnlyExcluded,
		"got %d V1-only commands, want %d", v1Only, V2V1OnlyExcluded)

	for _, cmd := range included {
		c.check("included_is_v2_backed", cmd.APIMajor == "V2",
			"%s is published but api_major is %q", cmd.Path, cmd.APIMajor)
		c.check("included_has_operation_id", cmd.OperationID != nil && *cmd.OperationID != "",
			"%s has no operationId", cmd.Path)
		c.check("included_has_http_binding", cmd.HTTPMethod != nil && cmd.HTTPPath != nil,
			"%s has no method/path binding", cmd.Path)
		c.check("included_has_scope", len(cmd.Scopes) > 0,
			"%s declares no authorization scope", cmd.Path)
	}

	c.check("manifest_majors", len(man.ProductMajors) == 1 && man.ProductMajors[0] == 2,
		"got %v, want [2]", man.ProductMajors)
	c.check("manifest_declares_no_capability", len(man.CapabilitiesDeclared) == 0,
		"ledger-v2 must declare no capability, got %v", man.CapabilitiesDeclared)
	c.check("manifest_denominator", man.CoverageDenominator == V2Denominator,
		"got %d, want %d", man.CoverageDenominator, V2Denominator)
	c.check("manifest_no_runtime_artifact", man.RuntimeArtifact == nil,
		"runtime gates 4B/4C/4D are closed; no artifact may be declared")
}

func auditV3(c *collector, inv Inventory, man Manifest) {
	c.check("schema_version", inv.SchemaVersion == 1, "got %d, want 1", inv.SchemaVersion)
	c.check("plugin_name", inv.Plugin == "ledger-v3", "got %q", inv.Plugin)
	c.check("product_major", inv.ProductMajor == 3, "got %d, want 3", inv.ProductMajor)
	c.check("signing_capability", inv.SigningCapability != nil && *inv.SigningCapability == SigningCapability,
		"want %q, got %v", SigningCapability, inv.SigningCapability)

	c.check("executable_command_count", len(inv.Commands) == V3ExecutableCommands,
		"got %d, want %d", len(inv.Commands), V3ExecutableCommands)

	got := inv.Tally()
	want := Counts{
		ExecutableTotal:  V3ExecutableCommands,
		Product:          V3Product,
		Operator:         V3Operator,
		HostLocal:        V3HostLocal,
		SigningEventSink: V3SigningEventSink,
	}

	c.check("classification_tally", got == want, "got %+v, want %+v", got, want)

	// Totality: the four buckets must account for every command, so no source
	// command can be dropped without the sum failing.
	sum := got.Product + got.Operator + got.HostLocal + got.SigningEventSink
	c.check("classification_totality", sum == len(inv.Commands),
		"buckets sum to %d but there are %d commands", sum, len(inv.Commands))

	if inv.Counts != nil {
		c.check("recorded_counts_match_data", *inv.Counts == want,
			"recorded %+v, recomputed %+v", *inv.Counts, want)
	} else {
		c.check("recorded_counts_present", false, "inventory records no counts block")
	}

	allowed := map[string]bool{
		ClassProduct: true, ClassOperator: true,
		ClassHostLocal: true, ClassSigningEventSink: true,
	}

	for _, cmd := range inv.Commands {
		c.check("classification_valid", allowed[cmd.Classification],
			"%s has classification %q", cmd.Path, cmd.Classification)
		c.check("source_file_recorded", cmd.SourceFile != nil && *cmd.SourceFile != "",
			"%s records no source file", cmd.Path)
	}

	// Every published command needs RPC evidence, so a command cannot be
	// claimed as covered without naming the call that implements it.
	for _, cmd := range inv.IncludedCommands() {
		c.check("product_has_rpc_evidence", len(cmd.RPCs) > 0,
			"%s is published but names no RPC", cmd.Path)
	}

	c.check("manifest_majors", len(man.ProductMajors) == 1 && man.ProductMajors[0] == 3,
		"got %v, want [3]", man.ProductMajors)
	c.check("manifest_declares_signing", contains(man.CapabilitiesDeclared, SigningCapability),
		"want %q in %v", SigningCapability, man.CapabilitiesDeclared)
	c.check("manifest_denominator", man.CoverageDenominator == V3Product,
		"got %d, want %d", man.CoverageDenominator, V3Product)
	c.check("manifest_module_path_under_v3",
		strings.HasPrefix(man.PluginModulePath, "github.com/formancehq/ledger/v3/"),
		"plugin module path %q cannot reach the internal generated stubs", man.PluginModulePath)
	c.check("manifest_no_runtime_artifact", man.RuntimeArtifact == nil,
		"runtime gates 4B/4C/4D are closed; no artifact may be declared")
}

// auditBoundary enforces the 2026-09-10 decision: two separate plugins that
// share no contract or packaging surface.
func auditBoundary(c *collector, in Inputs) {
	c.check("distinct_plugin_names", in.V2Inventory.Plugin != in.V3Inventory.Plugin,
		"both inventories claim %q", in.V2Inventory.Plugin)
	c.check("disjoint_product_majors", in.V2Inventory.ProductMajor != in.V3Inventory.ProductMajor,
		"both plugins claim major %d", in.V2Inventory.ProductMajor)
	c.check("distinct_manifest_names", in.V2Manifest.Name != in.V3Manifest.Name,
		"both manifests claim %q", in.V2Manifest.Name)
	c.check("distinct_plugin_module_paths", in.V2Manifest.PluginModulePath != in.V3Manifest.PluginModulePath,
		"both manifests claim module path %q", in.V2Manifest.PluginModulePath)

	// A shared command path would mean one user-visible command served by two
	// plugins, which is exactly the merged surface the decision forbids.
	v3 := make(map[string]bool, len(in.V3Inventory.Commands))
	for _, cmd := range in.V3Inventory.Commands {
		v3[cmd.Path] = true
	}

	var shared []string

	for _, cmd := range in.V2Inventory.Commands {
		if v3[cmd.Path] {
			shared = append(shared, cmd.Path)
		}
	}

	sort.Strings(shared)
	c.check("no_shared_command_path", len(shared) == 0,
		"command paths appear in both catalogues: %v", shared)

	// The v2 side must not be able to name the signing capability anywhere.
	c.check("v2_cannot_name_signing_capability",
		!inventoryMentions(in.V2Inventory, SigningCapability) &&
			!contains(in.V2Manifest.CapabilitiesDeclared, SigningCapability) &&
			!containsSubstring(in.V2Manifest.Invariants, "declares "+SigningCapability),
		"ledger-v2 references %q", SigningCapability)

	c.check("v3_declares_signing_capability",
		in.V3Inventory.SigningCapability != nil && *in.V3Inventory.SigningCapability == SigningCapability,
		"ledger-v3 must declare %q", SigningCapability)
}

func auditCanonical(c *collector, in Inputs) {
	for _, f := range []struct {
		name string
		raw  []byte
	}{
		{"ledger-v2/inventory.json", in.V2InvRaw},
		{"ledger-v2/manifest.json", in.V2ManRaw},
		{"ledger-v3/inventory.json", in.V3InvRaw},
		{"ledger-v3/manifest.json", in.V3ManRaw},
	} {
		ok, _, err := IsCanonical(f.raw)
		if err != nil {
			c.check("canonical_form", false, "%s: %v", f.name, err)
			continue
		}

		c.check("canonical_form", ok,
			"%s is not in canonical form; run the audit with -write", f.name)
	}
}

func inventoryMentions(inv Inventory, needle string) bool {
	if inv.SigningCapability != nil && strings.Contains(*inv.SigningCapability, needle) {
		return true
	}

	for _, cmd := range inv.Commands {
		if strings.Contains(cmd.Path, needle) || contains(cmd.Scopes, needle) {
			return true
		}

		if cmd.ExclusionReason != nil && strings.Contains(*cmd.ExclusionReason, needle) {
			return true
		}
	}

	return false
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}

	return false
}

func containsSubstring(haystack []string, needle string) bool {
	for _, s := range haystack {
		if strings.Contains(s, needle) {
			return true
		}
	}

	return false
}
