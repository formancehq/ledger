package domain

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestBusinessError(t *testing.T) {
	t.Parallel()

	inner := &ErrLedgerNotFound{Name: "missing"}
	bErr := &BusinessError{Err: inner}

	require.Equal(t, "ledger does not exist: missing", bErr.Error())
	require.ErrorIs(t, bErr, inner)
	require.Equal(t, inner, bErr.Unwrap())
	require.Equal(t, KindNotFound, Kind(bErr))
	require.Equal(t, ErrReasonLedgerNotFound, bErr.Reason())
	require.Equal(t, map[string]string{"name": "missing"}, bErr.Metadata())
}

func TestIsFreezableFailure(t *testing.T) {
	t.Parallel()

	// Deterministic business rejections are frozen so a retry under the same
	// idempotency key replays the same outcome.
	freezable := []Describable{
		&ErrInsufficientFunds{},
		&ErrTransactionNotFound{},
		&ErrLedgerAlreadyExists{},
	}
	for _, d := range freezable {
		require.Truef(t, IsFreezableFailure(Kind(d)), "%T should be freezable", d)
	}

	// A preload miss is a transient server-side gap, not a definitive business
	// outcome — it must never be frozen, or a retry replays the cache miss
	// until TTL instead of rebuilding preload and re-executing.
	require.False(t, IsFreezableFailure(Kind(new(ErrBalanceNotPreloaded))),
		"preload miss must not be freezable")
}

func TestErrDependencyDiscoveryFailed_Error(t *testing.T) {
	t.Parallel()

	cause := errors.New("parse failed")
	err := &ErrDependencyDiscoveryFailed{Cause: cause}

	require.Contains(t, err.Error(), "numscript dependency discovery failed")
	require.Contains(t, err.Error(), "parse failed")
	require.ErrorIs(t, err, cause)
}

func TestErrDependencyDiscoveryFailed_ReasonPreservesDescribableCause(t *testing.T) {
	t.Parallel()

	err := &ErrDependencyDiscoveryFailed{
		Cause: &ErrNumscriptParse{Details: "syntax error"},
	}

	require.Equal(t, ErrReasonNumscriptParseError, err.Reason())
}

func TestErrorTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "ErrLedgerAlreadyExists",
			err:      &ErrLedgerAlreadyExists{Name: "default"},
			expected: "ledger already exists: default",
		},
		{
			name:     "ErrLedgerNotFound",
			err:      &ErrLedgerNotFound{Name: "default"},
			expected: "ledger does not exist: default",
		},
		{
			name:     "ErrIdempotencyKeyConflict",
			err:      &ErrIdempotencyKeyConflict{Key: "ik-001"},
			expected: `idempotency key conflict: key "ik-001" used with different request content`,
		},
		{
			name:     "ErrTransactionReferenceConflict",
			err:      &ErrTransactionReferenceConflict{Ledger: "test", Reference: "ref-001"},
			expected: `transaction reference "ref-001" already exists in ledger test`,
		},
		{
			name:     "ErrTransactionNotFound",
			err:      &ErrTransactionNotFound{TransactionID: 42},
			expected: "transaction 42 does not exist",
		},
		{
			name:     "ErrTransactionAlreadyReverted",
			err:      &ErrTransactionAlreadyReverted{TransactionID: 42},
			expected: "transaction 42 is already reverted",
		},
		{
			name:     "ErrTransactionReferenceNotFound",
			err:      &ErrTransactionReferenceNotFound{Reference: "invoice:42"},
			expected: `transaction with reference "invoice:42" does not exist`,
		},
		{
			name:     "ErrTransactionTargetMissing",
			err:      ErrTransactionTargetMissing,
			expected: "transaction target requires either id or reference",
		},
		{
			name:     "ErrInsufficientFunds",
			err:      &ErrInsufficientFunds{Account: "bank", Asset: "USD", Amount: "1000", Balance: "500"},
			expected: `insufficient funds on account "bank" for asset USD: needed 1000, available 500`,
		},
		{
			name:     "ErrSinkAlreadyExists",
			err:      &ErrSinkAlreadyExists{Name: "nats-1"},
			expected: "event sink already exists: nats-1",
		},
		{
			name:     "ErrSinkNotFound",
			err:      &ErrSinkNotFound{Name: "nats-1"},
			expected: "event sink not found: nats-1",
		},
		{
			name:     "ErrMetadataNotFound",
			err:      &ErrMetadataNotFound{Target: "users:123", Key: "status"},
			expected: `metadata key "status" not found on users:123`,
		},
		{
			name:     "ErrChapterNotFound",
			err:      &ErrChapterNotFound{ChapterID: 99},
			expected: "chapter 99 not found",
		},
		{
			name:     "ErrChapterNotClosing",
			err:      &ErrChapterNotClosing{ChapterID: 5},
			expected: "chapter 5 is not in CLOSING state",
		},
		{
			name:     "ErrChapterNotClosed",
			err:      &ErrChapterNotClosed{ChapterID: 5},
			expected: "chapter 5 is not in CLOSED state",
		},
		{
			name:     "ErrChapterNotArchiving",
			err:      &ErrChapterNotArchiving{ChapterID: 5},
			expected: "chapter 5 is not in ARCHIVING state",
		},
		{
			name:     "ErrInvalidCronExpression",
			err:      &ErrInvalidCronExpression{Expression: "bad", Details: "parse failed"},
			expected: `invalid cron expression "bad": parse failed`,
		},
		{
			name:     "ErrInvalidReceipt",
			err:      &ErrInvalidReceipt{Detail: "expired"},
			expected: "invalid receipt: expired",
		},
		{
			name: "ErrTransientAccountNonZero",
			err: &ErrTransientAccountNonZero{Accounts: []AccountAssetKey{
				{Account: "staging:a", Asset: "USD"},
				{Account: "staging:b", Asset: "EUR"},
			}},
			expected: "transient accounts with non-zero balance at end of batch (input != output): staging:a/USD, staging:b/EUR",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.expected, tc.err.Error())
		})
	}
}

// TestErrTransientAccountNonZeroMetadata pins the structured "accounts" context
// (comma-separated account/asset) hashed into the AuditFailure, and that the
// zero value renders without panicking.
func TestErrTransientAccountNonZeroMetadata(t *testing.T) {
	t.Parallel()

	err := &ErrTransientAccountNonZero{Accounts: []AccountAssetKey{
		{Account: "staging:a", Asset: "USD"},
		{Account: "staging:b", Asset: "EUR"},
	}}
	require.Equal(t, map[string]string{"accounts": "staging:a/USD, staging:b/EUR"}, err.Metadata())

	// Zero value (nil slice) must render an empty list, never panic.
	empty := &ErrTransientAccountNonZero{}
	require.Equal(t, map[string]string{"accounts": ""}, empty.Metadata())
	require.Equal(t, "transient accounts with non-zero balance at end of batch (input != output): ", empty.Error())
}

func TestWrapCompileError(t *testing.T) {
	t.Parallel()

	t.Run("raw error passes through unchanged (sanitiser handles it)", func(t *testing.T) {
		t.Parallel()

		// After flemzord's review: raw errors at the WrapCompileError
		// boundary are no longer re-wrapped — they pass through so the
		// convertToGRPCError sanitiser default returns codes.Unknown with
		// a correlation ID instead of leaking the message via
		// ErrFilterCompilation.Detail. Client-actionable validation
		// errors are typed at source (compile.go validation helpers via
		// NewFilterCompilationError).
		raw := errors.New(`creating account iterator: pebble: not found at /var/lib/ledger/...`)
		wrapped := WrapCompileError(raw)

		require.Same(t, raw, wrapped)
	})

	t.Run("existing BusinessError passes through unchanged", func(t *testing.T) {
		t.Parallel()

		// ErrIndexNotFound is already wrapped by query.Compile and has its own
		// gRPC mapping (FailedPrecondition). Re-wrapping it as
		// ErrFilterCompilation would shadow the specific mapping with the
		// generic InvalidArgument — regression caught by E2E
		// indexes_test.go:63.
		original := &BusinessError{Err: &ErrIndexNotFound{Index: `metadata["category"] on accounts`}}
		got := WrapCompileError(original)
		require.Same(t, original, got)

		var notFound *ErrIndexNotFound
		require.ErrorAs(t, got, &notFound)
		require.Equal(t, `metadata["category"] on accounts`, notFound.Index)
	})

	t.Run("wrapped BusinessError passes through unchanged", func(t *testing.T) {
		t.Parallel()

		// Defensive: even if a caller wraps with fmt.Errorf, errors.As must
		// still find the BusinessError underneath and we must NOT re-wrap.
		original := &BusinessError{Err: &ErrIndexNotFound{Index: "idx"}}
		wrapped := errors.Join(original)
		got := WrapCompileError(wrapped)
		require.Equal(t, wrapped, got)
	})
}

func TestNewFilterCompilationError(t *testing.T) {
	t.Parallel()

	err := NewFilterCompilationError("field %q is declared as %s, cannot use string condition", "age", "INT64")

	var biz *BusinessError
	require.ErrorAs(t, err, &biz)

	var compile *ErrFilterCompilation
	require.ErrorAs(t, err, &compile)
	require.Equal(t, `field "age" is declared as INT64, cannot use string condition`, compile.Detail)
}

// TestEveryDomainErrorImplementsDescribable parses every Go file in this
// package and asserts that every `type Err... struct` declaration has a
// corresponding *T or T that satisfies the Describable interface. This is
// the structural backstop for #431: adding a new domain error without
// implementing Describable fails this test even if the new type is never
// reached via a BusinessError construction site (which would catch it at
// compile time). The reflection-based discovery means the test catches new
// additions automatically — no hand-maintained list to forget about.
func TestEveryDomainErrorImplementsDescribable(t *testing.T) {
	t.Parallel()

	matches, err := filepath.Glob("*.go")
	require.NoError(t, err)

	describableType := reflect.TypeFor[Describable]()

	// Pre-built registry: for each known type in this package, a zero
	// pointer instance the test can run reflection against. New entries
	// must be added here when a new typed error is introduced — but the
	// AST scan below verifies the list is complete, so forgetting fails
	// the test.
	instances := map[string]any{
		"ErrLedgerAlreadyExists":           &ErrLedgerAlreadyExists{},
		"ErrLedgerNotFound":                &ErrLedgerNotFound{},
		"ErrLedgerDeleted":                 &ErrLedgerDeleted{},
		"ErrIdempotencyKeyConflict":        &ErrIdempotencyKeyConflict{},
		"ErrTransactionReferenceConflict":  &ErrTransactionReferenceConflict{},
		"ErrInvalidSkippableReason":        &ErrInvalidSkippableReason{},
		"ErrTransactionReferenceNotFound":  &ErrTransactionReferenceNotFound{},
		"ErrTransactionNotFound":           &ErrTransactionNotFound{},
		"ErrTransactionAlreadyReverted":    &ErrTransactionAlreadyReverted{},
		"ErrInsufficientFunds":             &ErrInsufficientFunds{},
		"ErrVolumeOverflow":                &ErrVolumeOverflow{},
		"ErrSinkAlreadyExists":             &ErrSinkAlreadyExists{},
		"ErrSinkBatchSizeTooLarge":         &ErrSinkBatchSizeTooLarge{},
		"ErrMetadataNotFound":              &ErrMetadataNotFound{},
		"ErrSinkNotFound":                  &ErrSinkNotFound{},
		"ErrChapterNotFound":               &ErrChapterNotFound{},
		"ErrChapterNotClosing":             &ErrChapterNotClosing{},
		"ErrChapterNotClosed":              &ErrChapterNotClosed{},
		"ErrChapterNotArchiving":           &ErrChapterNotArchiving{},
		"ErrInvalidCronExpression":         &ErrInvalidCronExpression{},
		"ErrLedgerInMirrorMode":            &ErrLedgerInMirrorMode{},
		"ErrLedgerNotInMirrorMode":         &ErrLedgerNotInMirrorMode{},
		"ErrMirrorV2LogIDGap":              &ErrMirrorV2LogIDGap{},
		"ErrMirrorV2LogIDInvalid":          &ErrMirrorV2LogIDInvalid{},
		"ErrPreparedQueryAlreadyExists":    &ErrPreparedQueryAlreadyExists{},
		"ErrPreparedQueryNotFound":         &ErrPreparedQueryNotFound{},
		"ErrIndexNotFound":                 &ErrIndexNotFound{},
		"ErrMetadataFieldNotInSchema":      &ErrMetadataFieldNotInSchema{},
		"ErrIndexBuilding":                 &ErrIndexBuilding{},
		"ErrCheckpointNotReady":            &ErrCheckpointNotReady{},
		"ErrIndexInconsistent":             &ErrIndexInconsistent{},
		"ErrInvalidReceipt":                &ErrInvalidReceipt{},
		"ErrNumscriptNotFound":             &ErrNumscriptNotFound{},
		"ErrNumscriptVersionAlreadyExists": &ErrNumscriptVersionAlreadyExists{},
		"ErrNumscriptInvalidVersion":       &ErrNumscriptInvalidVersion{},
		"ErrAccountNotMatchingType":        &ErrAccountNotMatchingType{},
		"ErrAccountTypeNotFound":           &ErrAccountTypeNotFound{},
		"ErrAccountTypeAlreadyExists":      &ErrAccountTypeAlreadyExists{},
		"ErrAccountTypeConflict":           &ErrAccountTypeConflict{},
		"ErrInvalidPattern":                &ErrInvalidPattern{},
		"ErrAccountTypeHasAccounts":        &ErrAccountTypeHasAccounts{},
		"ErrNumscriptParse":                &ErrNumscriptParse{},
		"ErrDependencyDiscoveryFailed":     &ErrDependencyDiscoveryFailed{},
		"ErrBalanceNotPreloaded":           &ErrBalanceNotPreloaded{},
		"ErrTransientAccountNonZero":       &ErrTransientAccountNonZero{},
		"ErrFilterCompilation":             &ErrFilterCompilation{},
		"ErrInvalidOrderType":              &ErrInvalidOrderType{},
		"ErrIdempotencyCheckFailed":        &ErrIdempotencyCheckFailed{},
		"ErrAccountValidation":             &ErrAccountValidation{},
		"ErrMetadataKeyValidation":         &ErrMetadataKeyValidation{},
		"ErrInvalidApplyType":              &ErrInvalidApplyType{},
		"ErrInvalidExecutionPlan":          &ErrInvalidExecutionPlan{},
		"ErrExecutionPlanTooLarge":         &ErrExecutionPlanTooLarge{},
		"ErrStorageOperation":              &ErrStorageOperation{},
		"ErrTransactionStateInconsistent":  &ErrTransactionStateInconsistent{},
		"ErrNumscriptRuntime":              &ErrNumscriptRuntime{},
		"ErrVolumeNotMaterialized":         &ErrVolumeNotMaterialized{},
		"errCheckpointIDRequired":          errCheckpointIDRequired{},
		// Unexported sentinel struct types; each is exposed once via a
		// package-level Describable var (ErrColdStorageDisabled, etc.)
		// and must implement the interface.
		"validationSentinel":        &validationSentinel{},
		"errValidation":             &errValidation{},
		"errColdStorageDisabled":    errColdStorageDisabled{},
		"errAuditDisabled":          errAuditDisabled{},
		"errMaintenanceMode":        errMaintenanceMode{},
		"errStaleProposal":          errStaleProposal{},
		"errStaleInputsResolution":  errStaleInputsResolution{},
		"errPreloadUnavailable":     errPreloadUnavailable{},
		"errNoChapterOpen":          errNoChapterOpen{},
		"errWritesBlockedDiskFull":  errWritesBlockedDiskFull{},
		"errWritesBlockedClockSkew": errWritesBlockedClockSkew{},
	}

	// Walk the AST to discover every type declaration that starts with
	// "Err" in this package. If a new type is added without a matching
	// instances entry, the test fails — the boundary that turns
	// "forgot to add a Describable method" into a CI failure.
	discovered := make(map[string]bool)

	for _, path := range matches {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}

		fset := token.NewFileSet()

		f, parseErr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		require.NoError(t, parseErr, "parsing %s", path)

		for _, decl := range f.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}

			for _, spec := range gen.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}

				name := typeSpec.Name.Name

				// Match Err<UpperCase>... (typed errors) or err<lowercase>...
				// (unexported sentinel helpers). Skip "ErrorKind",
				// "Describable", "BusinessError", and validationSentinel
				// (covered by name='validationSentinel' below).
				switch {
				case strings.HasPrefix(name, "Err") && len(name) > 3 && name[3] >= 'A' && name[3] <= 'Z':
				case strings.HasPrefix(name, "err") && len(name) > 3 && name[3] >= 'A' && name[3] <= 'Z':
				case name == "validationSentinel":
				default:
					continue
				}

				discovered[name] = true
			}
		}
	}

	for name := range discovered {
		// Some types are pointer-receiver Describables and some are
		// value-receiver. The map above stores pointer instances so
		// both forms satisfy Describable through implicit pointer
		// promotion.
		inst, ok := instances[name]
		require.True(t, ok,
			"type %s declared in internal/domain has no entry in TestEveryDomainErrorImplementsDescribable.instances — add one (and a Describable implementation) to keep the structural check tight",
			name)

		require.True(t,
			reflect.TypeOf(inst).Implements(describableType),
			"type %s in internal/domain does not implement Describable — every domain error type must declare Kind(), Reason(), and Metadata() so the gRPC and HTTP adapters can route it",
			name)

		// Every domain reason must resolve to an ErrorReason enum value — kind
		// is derived from it (domain.Kind / KindForReason), so a reason with no
		// enum value would silently classify as Internal. The two wrapper types
		// delegate Reason() to a Cause and panic on a nil zero-value; their
		// reason is the cause's, covered by the cause's own entry.
		switch name {
		case "ErrMetadataKeyValidation", "ErrAccountValidation":
			continue
		}

		d := inst.(Describable)

		require.NotEqualf(t, commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED, ReasonCode(d.Reason()),
			"reason %q of %s has no ErrorReason enum value — add ERROR_REASON_%s to common.proto", d.Reason(), name, d.Reason())
	}

	// And conversely: every entry in instances must correspond to a
	// declared type — catches stale list entries after a type is
	// renamed or removed.
	for name := range instances {
		require.True(t, discovered[name], "instances entry %s does not match any type declared in internal/domain — remove it", name)
	}
}

func TestWriteGateErrorsDescribable(t *testing.T) {
	t.Parallel()

	// Kind is derived from the reason via the KindForReason switch (master's
	// single source of truth), not declared per type — read it through Kind().
	require.Equal(t, KindResourceExhausted, Kind(ErrWritesBlockedDiskFull))
	require.Equal(t, ErrReasonWritesBlockedDiskFull, ErrWritesBlockedDiskFull.Reason())
	require.Equal(t, KindUnavailable, Kind(ErrWritesBlockedClockSkew))
	require.Equal(t, ErrReasonWritesBlockedClockSkew, ErrWritesBlockedClockSkew.Reason())

	wrapped := fmt.Errorf("admission: %w", ErrWritesBlockedDiskFull)
	require.ErrorIs(t, wrapped, ErrWritesBlockedDiskFull)
}

// TestKindForReason_DeprecatedRetainedReasons pins the replay classification of
// reasons that are still in the wire enum but no longer carried by any typed
// domain error. TestEveryDomainErrorImplementsDescribable only iterates typed
// errors, so once the typed error is deleted nothing else asserts the reason's
// Kind — yet KindForReason is a persisted-replay invariant (CLAUDE.md #8: the
// checker re-derives a frozen failure's kind from the chain-bound reason).
// `//exhaustive:enforce` prevents removing a case, but not silently MOVING it to
// another Kind* block, which would reclassify every historical frozen failure of
// that reason on replay/verify. These assertions pin the mapping directly.
func TestKindForReason_DeprecatedRetainedReasons(t *testing.T) {
	t.Parallel()

	// Deprecated (no typed error emits it) but retained for replay of frozen
	// pre-upgrade failures — must stay a definitive validation failure.
	require.Equal(t, KindValidation, KindForReason(commonpb.ErrorReason_ERROR_REASON_NON_DETERMINISTIC_SCRIPT))
}
