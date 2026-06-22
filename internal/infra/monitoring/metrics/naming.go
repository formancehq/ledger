// Package metrics wraps an OpenTelemetry MeterProvider so the names of
// instruments created from the application's meters can be rewritten
// according to a configured naming policy.
//
// The wrapper intercepts only the instrument-registration call sites;
// once an instrument is created the hot path records values directly
// on the underlying SDK instrument with no extra indirection.
//
// Scope of the policy: every meter we hand out goes through the
// wrapper, so every instrument our code creates is subject to the
// rename. OpenTelemetry auto-instrumentation (`go.*`, `process.*`,
// `system.*`, `http.*`) uses the *global* MeterProvider — which we
// leave as the raw SDK provider — and therefore bypasses this
// wrapper entirely, preserving the upstream semantic-convention
// names.
package metrics

import (
	"fmt"
	"strings"
)

// Naming selects the convention used to format instrument names
// emitted by the application's code. The auto-instrumented OTel
// semantic-convention metrics are not affected (see package doc).
type Naming string

const (
	// NamingOTel preserves the canonical OpenTelemetry dot-notation
	// names produced at the call sites (e.g.
	// "admission.command.duration"). No prefix is added. This is the
	// historical behaviour and the default.
	NamingOTel Naming = "otel"

	// NamingProm rewrites our metric names to the Prometheus
	// convention: a "ledger_" prefix followed by the original name
	// with every "." replaced by "_" (e.g.
	// "ledger_admission_command_duration"). Use this when the
	// OTLP→Prometheus collector sanitises dots and you want the
	// emitted names to be unambiguous in a Prometheus instance that
	// also scrapes other services.
	NamingProm Naming = "prom"
)

// DefaultNaming is the policy applied when --metrics-naming is not
// set. Keeping the OTel convention as the default avoids a breaking
// change for existing dashboards that reference the dot-notation
// names.
const DefaultNaming = NamingOTel

// Prefix is the single-word application prefix applied to every
// instrument created via this factory in NamingProm mode. Following
// the Prometheus naming recommendation
// (https://prometheus.io/docs/practices/naming/), it disambiguates
// names that would otherwise be too generic (cache_size,
// wal_append_save_duration, …) when several services share the same
// Prometheus instance.
const Prefix = "ledger"

// ParseNaming validates a user-supplied naming value. The empty
// string is treated as [DefaultNaming] so test fixtures and other
// call sites that construct Config literals without going through
// the CLI parser don't have to special-case this field.
func ParseNaming(s string) (Naming, error) {
	switch Naming(s) {
	case "":
		return DefaultNaming, nil
	case NamingOTel:
		return NamingOTel, nil
	case NamingProm:
		return NamingProm, nil
	default:
		return "", fmt.Errorf("invalid metrics naming %q: expected %q or %q",
			s, NamingOTel, NamingProm)
	}
}

// transformName applies the naming policy to an instrument name. It
// is a package-level pure function so it can be reused both by the
// runtime wrapper and by tests / external tooling that needs to
// predict what a given instrument will look like after the rewrite.
//
// Instrument names that already carry the application prefix
// (`ledger.…` in OTel form, `ledger_…` post-collector form) keep
// the existing prefix instead of getting a second one — call sites
// that hand-rolled the namespace stay backward-compatible with
// existing alerts and dashboards.
func transformName(instrumentName string, naming Naming) string {
	if naming == NamingOTel {
		return instrumentName
	}
	underscored := strings.ReplaceAll(instrumentName, ".", "_")
	if strings.HasPrefix(underscored, Prefix+"_") {
		return underscored
	}

	return Prefix + "_" + underscored
}
