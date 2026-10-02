// Package metrics wraps an OpenTelemetry MeterProvider so the names of
// instruments created from the application's meters can be rewritten
// according to a configured naming policy and namespace prefix.
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
	"regexp"
	"strings"
)

// Naming selects the convention used to format instrument names
// emitted by the application's code. The auto-instrumented OTel
// semantic-convention metrics are not affected (see package doc).
type Naming string

const (
	// NamingOTel keeps the OpenTelemetry dot-notation produced at the
	// call sites, joined to the prefix with a "." (e.g.
	// "formance.ledger.admission.command.duration"). This is the
	// default.
	NamingOTel Naming = "otel"

	// NamingProm rewrites our metric names to the Prometheus
	// convention: the prefixed name with every "." replaced by "_"
	// (e.g. "formance_ledger_admission_command_duration"). Use this
	// when the OTLP→Prometheus path preserves dots but you want
	// Prometheus-style names.
	NamingProm Naming = "prom"
)

// DefaultNaming is the policy applied when --metrics-naming is not
// set.
const DefaultNaming = NamingOTel

// DefaultPrefix is the namespace prepended to every instrument created
// via this factory when --metrics-prefix is not set. Following the
// OpenTelemetry naming recommendation for application-specific names
// (https://opentelemetry.io/docs/specs/semconv/general/naming/), it
// disambiguates names that would otherwise be too generic
// (cache.size, wal.append.save.duration, …) when several services
// share the same metrics backend. [NoPrefix] disables the namespace.
const DefaultPrefix = "formance.ledger"

// NoPrefix is the --metrics-prefix value that disables the namespace.
// A non-empty sentinel is needed because an empty METRICS_PREFIX
// environment variable is ignored by the flag binder, and the
// operator cannot pass an empty value either. An explicit empty flag
// value disables the namespace too.
const NoPrefix = "none"

// prefixPattern accepts dot-separated segments that stay valid both as
// an OpenTelemetry instrument-name prefix and, once dots are replaced
// by underscores, as a Prometheus metric-name prefix. Each segment
// starts with a letter and ends with a letter or digit, so the joined
// name never contains "..", "._" or "_.".
var prefixPattern = regexp.MustCompile(`^[A-Za-z]([A-Za-z0-9_]*[A-Za-z0-9])?(\.[A-Za-z]([A-Za-z0-9_]*[A-Za-z0-9])?)*$`)

// MaxPrefixLength bounds the prefix so that prefixed instrument names
// stay well under the 255-character limit the OpenTelemetry SDK
// enforces at instrument creation.
const MaxPrefixLength = 64

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

// ParsePrefix validates a user-supplied metrics prefix and returns the
// effective one: [NoPrefix] and the empty string both yield "", which
// disables the namespace.
func ParsePrefix(s string) (string, error) {
	if s == "" || s == NoPrefix {
		return "", nil
	}
	if len(s) > MaxPrefixLength {
		return "", fmt.Errorf("invalid metrics prefix %q: longer than %d characters", s, MaxPrefixLength)
	}
	if !prefixPattern.MatchString(s) {
		return "", fmt.Errorf("invalid metrics prefix %q: expected %q or dot-separated segments of letters, digits and underscores, each starting with a letter and ending with a letter or digit", s, NoPrefix)
	}

	return s, nil
}

// transformName applies the naming policy and prefix to an instrument
// name. It is a package-level pure function so it can be reused both
// by the runtime wrapper and by tests / external tooling that needs to
// predict what a given instrument will look like after the rewrite.
func transformName(instrumentName string, naming Naming, prefix string) string {
	name := instrumentName
	if prefix != "" {
		name = prefix + "." + instrumentName
	}
	if naming == NamingProm {
		return strings.ReplaceAll(name, ".", "_")
	}

	return name
}
