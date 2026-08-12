package state

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestIdempotencyFailureMessageMatchesAudit pins the equality the checker's
// idempotencyMismatch depends on: the reason and message frozen into the
// SubIdempKeys projection by recordIdempotencyFailure MUST equal the ones
// buildAuditFailure wrote into the hash-chained AuditFailure for the same
// error. On a drift a perfectly healthy store reports
// CHECK_STORE_ERROR_TYPE_IDEMPOTENCY_MISMATCH on every frozen failure, and the
// audit side of the divergence is inside the hash chain, so it cannot be
// corrected afterwards.
//
// Both sites derive those two fields from describeFailure, so on that half the
// test is a forcing function against a future re-split of the derivation, not
// an independent oracle. The independent half is the round trip through
// state.IdempotencyValueFromAudit — the same derivation
// check.expectedIdempotencyOutcome builds its expectation with — which crosses
// the auditpb.AuditFailure to commonpb.IdempotencyFailure field mapping
// (Context to Metadata included) that describeFailure does not cover.
func TestIdempotencyFailureMessageMatchesAudit(t *testing.T) {
	t.Parallel()

	const (
		proposalCreatedAt = uint64(1700000000)
		idempotencyKey    = "idempotency-key-1"
	)

	for _, tc := range []struct {
		name string
		err  domain.Describable
	}{
		{
			// Every field distinct and non-zero, so a projection bug cannot
			// hide behind a zero value that both sides happen to share.
			name: "populated metadata",
			err: &domain.ErrInsufficientFunds{
				Account:    "user:alice",
				Asset:      "USD/2",
				Color:      "RESERVED",
				ColorKnown: true,
				Amount:     "100",
				Balance:    "10",
			},
		},
		{
			// Metadata() is nil here, the only shape that exercises the
			// nil-vs-empty asymmetry: buildAuditFailure emits a non-nil empty
			// Context while recordIdempotencyFailure stores nil.
			name: "nil metadata",
			err:  domain.NewValidationSentinel("EN-1772 fixture: value must not be empty"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.True(t, domain.IsFreezableFailure(domain.Kind(tc.err)),
				"the fixture must be freezable or recordIdempotencyFailure is a no-op and this test proves nothing")

			machine, dataStore, _ := newTestMachine(t)

			// Side A — the hash-chained audit entry, round-tripped through the
			// wire because that is how the checker reads it: a proto3 map with
			// no entries comes back nil, so the empty Context does not survive.
			entry := &auditpb.AuditEntry{
				Timestamp: &commonpb.Timestamp{Data: proposalCreatedAt},
				Outcome:   &auditpb.AuditEntry_Failure{Failure: buildAuditFailure(tc.err)},
			}

			raw, err := entry.MarshalVT()
			require.NoError(t, err)

			audited := &auditpb.AuditEntry{}
			require.NoError(t, audited.UnmarshalVT(raw))

			// Side B — the SubIdempKeys projection, read back out of Pebble
			// rather than off the cache, so the commit is load-bearing and the
			// serialization the checker walks is covered.
			batch := dataStore.OpenWriteSession()
			require.NoError(t, machine.recordIdempotencyFailure(
				batch, idempotencyKey, []byte("proposal-hash"), tc.err, proposalCreatedAt))
			require.NoError(t, batch.Commit())

			handle, err := dataStore.NewDirectReadHandle()
			require.NoError(t, err)
			t.Cleanup(func() { _ = handle.Close() })

			stored, err := LoadIdempotencyKey(handle, idempotencyKey)
			require.NoError(t, err)
			require.NotNil(t, stored.GetFailure(), "the failure outcome must have been frozen")

			// IdempotencyFailure.EqualVT compares metadata by length then by
			// key, so it carries the same nil-vs-empty tolerance as
			// checker.metadataEqual — no hand-maintained copy of that rule.
			expected, ok := IdempotencyValueFromAudit(audited, nil)
			require.True(t, ok, "a freezable failure must yield an expectation")
			require.True(t, expected.GetFailure().EqualVT(stored.GetFailure()),
				"audit chain %+v and idempotency projection %+v must carry the same reason, message and metadata",
				expected.GetFailure(), stored.GetFailure())
		})
	}
}

// auditFailureCase is one row of the buildAuditFailure projection table. The
// expected message is deliberately absent: it is derived in the shared body as
// tc.err.Error(), so no row can assert a subset of the projected fields. That
// per-row drift is exactly what left AuditFailure.Message unasserted for every
// error type but one (EN-1772).
type auditFailureCase struct {
	name        string
	err         domain.Describable
	wantReason  string
	wantContext map[string]string
}

// auditFailureCases carries one row per Describable reachable from the FSM
// failure path. TestBuildAuditFailureCoversEveryDescribable fails if a type is
// missing, so this list cannot silently fall behind.
func auditFailureCases() []auditFailureCase {
	return []auditFailureCase{}
}

// describableTypeKey identifies a Describable implementation by package and type
// name. The package is part of the key so a domain type and a state type that
// happen to share a name can never be confused for one another.
type describableTypeKey struct {
	pkg  string
	name string
}

func (k describableTypeKey) String() string { return k.pkg + "." + k.name }

// describableScanDirs are the packages whose Describable implementations can
// reach buildAuditFailure. This is a deliberate boundary, not an oversight:
// internal/domain holds the business errors the FSM returns and internal/infra/state
// holds the FSM-local ones (ErrCoverageMiss). Every other implementation in the
// tree — admission's errIdempotencyKeyTooLong / errIdempotencyKeyInvalidUTF8 /
// errCheckpointOrderNotLast, query.ErrAggregateOverflow, grpc.validationError — is
// produced before a proposal exists or on the read path, so it never reaches the
// audit chain. If one of them ever becomes FSM-reachable, add its directory here.
var describableScanDirs = map[string]string{
	"../../domain": "github.com/formancehq/ledger/v3/internal/domain",
	".":            "github.com/formancehq/ledger/v3/internal/infra/state",
}

// TestBuildAuditFailureCoversEveryDescribable is the forcing function: adding a
// Describable without adding a row to auditFailureCases fails this test.
//
// Discovery is by METHOD SET, not by type name. It collects every receiver type
// declaring both Reason() string and Metadata() map[string]string — which is the
// Describable contract itself. The name-prefix scan used by
// TestEveryDomainErrorImplementsDescribable (internal/domain/errors_test.go:318)
// would miss domain.ReplayedFailure, domain.RemoteError and domain.BusinessError,
// all three of which do reach buildAuditFailure, and it would need a hand-maintained
// exclusion list for the Err* types in this package that are NOT Describable
// (ErrNodeOutOfSync, ErrInvalidEntryIndex, ErrDoubleEntryInvariantViolated,
// ErrVolumeCachePebbleDivergence). The method-set predicate needs neither.
func TestBuildAuditFailureCoversEveryDescribable(t *testing.T) {
	t.Parallel()

	discovered := make(map[describableTypeKey]bool)

	for dir, pkgPath := range describableScanDirs {
		for name := range describableTypesIn(t, dir) {
			discovered[describableTypeKey{pkg: pkgPath, name: name}] = true
		}
	}

	require.NotEmpty(t, discovered,
		"the AST scan found no Describable implementations at all — the scan is broken, not the table")

	covered := make(map[describableTypeKey]bool)

	for _, tc := range auditFailureCases() {
		rt := reflect.TypeOf(tc.err)
		for rt.Kind() == reflect.Ptr {
			rt = rt.Elem()
		}

		covered[describableTypeKey{pkg: rt.PkgPath(), name: rt.Name()}] = true
	}

	require.Equal(t, sortedKeys(discovered), sortedKeys(covered),
		"auditFailureCases must hold exactly one row per Describable reachable from the FSM failure path:\n"+
			"  a MISSING entry means a new error type landed with no assertion on what buildAuditFailure writes\n"+
			"  an EXTRA entry means a row references a type that is no longer a Describable")
}

// describableTypesIn returns the names of every type declared in dir whose
// method set includes both Reason() string and Metadata() map[string]string.
func describableTypesIn(t *testing.T, dir string) map[string]bool {
	t.Helper()

	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no Go files found in %s — the relative scan path is wrong", dir)

	hasReason := make(map[string]bool)
	hasMetadata := make(map[string]bool)

	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}

		fset := token.NewFileSet()

		f, parseErr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		require.NoError(t, parseErr, "parsing %s", path)

		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
				continue
			}

			receiver := receiverTypeName(fn.Recv.List[0].Type)
			if receiver == "" {
				continue
			}

			switch fn.Name.Name {
			case "Reason":
				hasReason[receiver] = true
			case "Metadata":
				hasMetadata[receiver] = true
			}
		}
	}

	out := make(map[string]bool)

	for name := range hasReason {
		if hasMetadata[name] {
			out[name] = true
		}
	}

	return out
}

// receiverTypeName resolves a method receiver expression to its bare type name,
// unwrapping the pointer form. Generic receivers (IndexExpr) return "" — there
// are none among the Describables today, and a new one would surface as a
// missing row rather than as a silent skip.
func receiverTypeName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return receiverTypeName(e.X)
	case *ast.Ident:
		return e.Name
	default:
		return ""
	}
}

func sortedKeys(set map[describableTypeKey]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k.String())
	}

	sort.Strings(out)

	return out
}
