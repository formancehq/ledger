package internal

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"google.golang.org/grpc/metadata"
)

const (
	// These keys mirror the test-only Antithesis gRPC probe. The ordinary SUT
	// build compiles a no-op and never interprets them.
	PITIdempotencyProbeMetadataKey        = "x-formance-antithesis-idempotency-probe"
	PITIdempotencyProbeReachedMetadataKey = "x-formance-antithesis-idempotency-probe-reached"
	PITIdempotencyMaximumKeyLength        = 256
)

// PITIdempotencyKeyLengths returns the configured-limit family used by the
// property: a representative key plus just-below and at the admission limit.
func PITIdempotencyKeyLengths() []int {
	return []int{64, PITIdempotencyMaximumKeyLength - 1, PITIdempotencyMaximumKeyLength}
}

// PITIdempotencyKey creates a unique valid UTF-8 key at an exact wire length.
func PITIdempotencyKey(seedOne, seedTwo uint64, length int) (string, error) {
	base := fmt.Sprintf("pit-idem-%016x%016x", seedOne, seedTwo)
	if length < len(base) || length > PITIdempotencyMaximumKeyLength {
		return "", fmt.Errorf(
			"idempotency key length %d is outside property range [%d,%d]",
			length,
			len(base),
			PITIdempotencyMaximumKeyLength,
		)
	}

	return base + strings.Repeat("x", length-len(base)), nil
}

// PITIdempotencyProbeReached authenticates the post-commit response header for
// one workload-supplied probe ID.
func PITIdempotencyProbeReached(header metadata.MD, probeID string) bool {
	values := header.Get(PITIdempotencyProbeReachedMetadataKey)

	return probeID != "" && len(values) == 1 && values[0] == probeID
}

// ValidatePITKeyedAudit proves that one key owns exactly one successful audit
// entry and exactly one fresh log reference.
func ValidatePITKeyedAudit(
	entries []*auditpb.AuditEntry,
	idempotencyKey string,
	logSequence uint64,
) (uint64, error) {
	if len(entries) != 1 {
		return 0, fmt.Errorf("expected one keyed audit entry, got %d", len(entries))
	}
	entry := entries[0]
	if entry == nil {
		return 0, fmt.Errorf("keyed audit entry is nil")
	}
	if entry.GetIdempotency().GetKey() != idempotencyKey {
		return 0, fmt.Errorf("keyed audit entry carries a different idempotency key")
	}
	success := entry.GetSuccess()
	if success == nil {
		return 0, fmt.Errorf("keyed audit entry is not a success")
	}
	if success.GetMinLogSequence() != logSequence || success.GetMaxLogSequence() != logSequence {
		return 0, fmt.Errorf(
			"keyed audit range [%d,%d] differs from committed log %d",
			success.GetMinLogSequence(),
			success.GetMaxLogSequence(),
			logSequence,
		)
	}
	if entry.GetOrderCount() != 1 || len(entry.GetItems()) != 1 {
		return 0, fmt.Errorf(
			"keyed audit contains %d orders and %d detailed items",
			entry.GetOrderCount(),
			len(entry.GetItems()),
		)
	}
	item := entry.GetItems()[0]
	if item.GetOrderIndex() != 0 || item.GetLogSequence() != logSequence {
		return 0, fmt.Errorf(
			"keyed audit item (%d,%d) differs from expected (0,%d)",
			item.GetOrderIndex(),
			item.GetLogSequence(),
			logSequence,
		)
	}

	return entry.GetSequence(), nil
}

// ValidatePITSingleInput proves that the property-owned destination received
// exactly one posting and no output under the requested immutable PIT view.
func ValidatePITSingleInput(
	result *commonpb.AggregateResult,
	asset string,
	amount *big.Int,
) error {
	if amount == nil || amount.Sign() <= 0 {
		return fmt.Errorf("expected amount must be positive")
	}
	canonical, err := CanonicalFlatAggregate(result)
	if err != nil {
		return err
	}
	if len(canonical) != 1 {
		return fmt.Errorf("expected one PIT volume bucket, got %d", len(canonical))
	}
	volume := canonical[0]
	if volume.Asset != asset || volume.Color != "" ||
		volume.Input != amount.String() || volume.Output != "0" {
		return fmt.Errorf(
			"PIT bucket %+v differs from expected asset=%s input=%s output=0",
			volume,
			asset,
			amount,
		)
	}

	return nil
}
