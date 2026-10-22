package domain

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
)

// FailureFacts is a stable reason, code, and fact view used at public API
// boundaries. Audited errors also persist this outcome in typed form;
// public-only errors never enter the audit or idempotency projections.
// Human-readable diagnostic text is deliberately absent.
type FailureFacts struct {
	Reason ledgerpb.ErrorReason
	Code   string
	Facts  map[string]string
}

// FailureFactsOf renders the typed audited outcome for public API boundaries.
func FailureFactsOf(d SerializableError) FailureFacts {
	details := d.FailureDetails()

	return FailureFacts{
		Reason: ReasonCode(d.Reason()), Code: ledgerpb.FailureDetailsCode(details),
		Facts: ledgerpb.FailureDetailsFacts(details),
	}
}

// TypedFailureOf reads the audited representation directly from its producer.
// Public map facts are rendered only at API boundaries.
func TypedFailureOf(d SerializableError) *ledgerpb.FailureDetails {
	reason := ReasonCode(d.Reason())
	if reason == ledgerpb.ErrorReason_ERROR_REASON_UNSPECIFIED {
		panic(fmt.Errorf("error %T has an unknown failure reason", d))
	}
	details := d.FailureDetails()
	code := ledgerpb.FailureDetailsCode(details)
	if code == "" {
		panic(fmt.Errorf("error %T has no typed failure details", d))
	}
	if ledgerpb.FailureDetailsReason(details) != reason {
		panic(fmt.Errorf("error %T emits code %q for reason %q", d, code, d.Reason()))
	}
	if err := ledgerpb.ValidateRequiredFailureFacts(details); err != nil {
		panic(fmt.Errorf("error %T emits incomplete failure details: %w", d, err))
	}

	return details
}

// NewFailureFacts copies the values supplied by an error for public rendering.
// Audited producers use the typed FailureDetails representation instead.
func NewFailureFacts(reason, code string, values map[string]string) FailureFacts {
	facts := make(map[string]string, len(values))
	maps.Copy(facts, values)

	return FailureFacts{Reason: ReasonCode(reason), Code: code, Facts: facts}
}

func FailureFactsFromTyped(details *ledgerpb.FailureDetails) FailureFacts {
	return FailureFacts{
		Reason: ledgerpb.FailureDetailsReason(details), Code: ledgerpb.FailureDetailsCode(details),
		Facts: ledgerpb.FailureDetailsFacts(details),
	}
}

// FailureFactsFromDetails validates the persisted variant before exposing its
// derived reason, public code, and fact map.
func FailureFactsFromDetails(details *ledgerpb.FailureDetails) (FailureFacts, error) {
	if details == nil {
		return FailureFacts{}, errors.New("failure details are missing")
	}
	parent := details.ProtoReflect()
	if len(parent.GetUnknown()) != 0 {
		return FailureFacts{}, errors.New("failure details contain unknown fields")
	}
	arm := parent.WhichOneof(parent.Descriptor().Oneofs().ByName("kind"))
	if arm != nil && len(parent.Get(arm).Message().GetUnknown()) != 0 {
		return FailureFacts{}, errors.New("failure detail variant contains unknown fields")
	}
	f := FailureFactsFromTyped(details)
	if err := ValidateFailureFacts(f); err != nil {
		return FailureFacts{}, err
	}

	return f, nil
}

// ValidateFailureFacts validates public map facts against the protobuf variant.
// The oneof fields and their failure_reason annotations are the sole schema.
func ValidateFailureFacts(f FailureFacts) error {
	if f.Reason == ledgerpb.ErrorReason_ERROR_REASON_UNSPECIFIED || ReasonString(f.Reason) == "" {
		return fmt.Errorf("failure reason is unknown: %d", f.Reason)
	}
	details, err := ledgerpb.NewFailureDetails(f.Code, f.Facts)
	if err != nil {
		return err
	}
	if ledgerpb.FailureDetailsReason(details) != f.Reason {
		return fmt.Errorf("failure code %q has reason %q, not %q", f.Code, ReasonString(ledgerpb.FailureDetailsReason(details)), ReasonString(f.Reason))
	}
	if err := ledgerpb.ValidateRequiredFailureFacts(details); err != nil {
		return err
	}

	return nil
}

// RenderFailureFacts supplies a deterministic presentation at read/API edges.
// A caller may attach its own transient diagnostic to a live error, but that
// text is never part of the audited identity.
func RenderFailureFacts(f FailureFacts) (string, map[string]string) {
	name := strings.ToLower(strings.ReplaceAll(f.Code, "_", " "))
	facts := maps.Clone(f.Facts)
	if len(facts) == 0 {
		return name, facts
	}
	keys := slices.Sorted(maps.Keys(facts))
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+strconv.Quote(facts[key]))
	}

	return name + ": " + strings.Join(parts, ", "), facts
}
