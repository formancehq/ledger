package internal

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"google.golang.org/protobuf/proto"
)

const (
	PITScopeReservedAddress = "pitscope:never-created"
	pitScopePairAttempts    = 32
)

// PITScopeCase is one fixed axis/transformation combination for the redundant
// scope oracle. The returned selector is immutable for the lifetime of a
// command invocation.
type PITScopeCase struct {
	Name            string
	AxisName        string
	Selector        *servicepb.HistoricalBalanceSelector
	UseMaxPrecision bool
	CollapseColors  bool
}

// PITScopeLedgerName returns the fixed ledger seeded by first_default_ledger.
func PITScopeLedgerName() string {
	return PrefixPITScope.WithSuffix("oracle")
}

// ResolvedPITScopeTargets removes predeclared addresses that do not currently
// correspond to a Raft member. Kubernetes advertises spare pod ordinals for
// scaling, and DialPerNode intentionally preserves those unresolved targets.
func ResolvedPITScopeTargets(conns PerNodeConns) PerNodeConns {
	resolved := make(PerNodeConns, 0, len(conns))
	for _, conn := range conns {
		if conn != nil && conn.NodeID != 0 {
			resolved = append(resolved, conn)
		}
	}

	return resolved
}

// PITScopeCases returns the complete bounded menu: both temporal axes crossed
// with every legal pair of aggregate transformation booleans.
func PITScopeCases() []PITScopeCase {
	effectiveAt := uint64(time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC).UnixMicro())
	type selector struct {
		name string
		at   uint64
		axis servicepb.HistoricalBalanceTemporality
	}
	selectors := []selector{
		{name: "effective", at: effectiveAt, axis: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE},
		{name: "insertion", at: math.MaxUint64, axis: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION},
	}
	type mode struct {
		name           string
		maxPrecision   bool
		collapseColors bool
	}
	modes := []mode{
		{name: "plain"},
		{name: "max-precision", maxPrecision: true},
		{name: "collapse-colors", collapseColors: true},
		{name: "max-precision-and-collapse-colors", maxPrecision: true, collapseColors: true},
	}

	cases := make([]PITScopeCase, 0, len(selectors)*len(modes))
	for _, selector := range selectors {
		for _, mode := range modes {
			cases = append(cases, PITScopeCase{
				Name:     selector.name + "/" + mode.name,
				AxisName: selector.name,
				Selector: &servicepb.HistoricalBalanceSelector{
					At:          &commonpb.Timestamp{Data: selector.at},
					Temporality: selector.axis,
				},
				UseMaxPrecision: mode.maxPrecision,
				CollapseColors:  mode.collapseColors,
			})
		}
	}

	return cases
}

// SeedPITScopeFixture creates the fixed, driver-owned history used by both the
// fault-time and quiescent scope-equivalence commands. It deliberately never
// creates PITScopeReservedAddress, making NOT(address == reserved) exhaustive.
func SeedPITScopeFixture(ctx context.Context, client servicepb.BucketServiceClient) error {
	ledger := PITScopeLedgerName()
	if err := CreateLedger(ctx, client, ledger); err != nil {
		return err
	}
	if err := ConfigureHistoricalBalances(ctx, client, ledger); err != nil {
		return fmt.Errorf("configuring historical balances for scope fixture: %w", err)
	}

	fixtureAt := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	response, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(
		"pit-scope-fixture-transactions-v1",
		actions.WithTimestamp(actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{
			actions.NewColoredPosting("world", "pitscope:users:a", big.NewInt(100), "USD/2", "RED"),
		}, nil), fixtureAt),
		actions.WithTimestamp(actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{
			actions.NewColoredPosting("pitscope:users:a", "pitscope:users:b", big.NewInt(25), "USD/2", "BLUE"),
		}, nil), fixtureAt),
		actions.WithTimestamp(actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{
			actions.NewColoredPosting("world", "pitscope:merchants:shop", big.NewInt(10_000), "USD/4", "RED"),
		}, nil), fixtureAt),
		actions.WithTimestamp(actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{
			actions.NewPosting("pitscope:merchants:shop", "pitscope:users:b", big.NewInt(50), "EUR/2"),
		}, nil), fixtureAt),
	))
	if err != nil {
		return fmt.Errorf("applying point-in-time scope fixture: %w", err)
	}
	created := ExtractCreatedTransaction(response)
	if created == nil || created.GetTransaction() == nil {
		return fmt.Errorf("point-in-time scope fixture returned no created transaction")
	}

	_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest(
		"pit-scope-fixture-revert-v1",
		actions.RevertTransactionAction(
			ledger,
			created.GetTransaction().GetId(),
			true,
			true,
			nil,
		),
	))
	if err != nil {
		return fmt.Errorf("reverting point-in-time scope fixture transaction: %w", err)
	}

	return nil
}

// ComparePITScopeCase tries to obtain two successful responses from the same
// immutable view, then checks the ledger-wide summary against an exhaustive
// per-account row fold. Classified fail-closed outcomes and token movement are
// inconclusive, not safety failures.
func ComparePITScopeCase(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	nodeAddress string,
	nodeID uint32,
	testCase PITScopeCase,
) bool {
	ledger := PITScopeLedgerName()
	baseDetails := Details{
		"ledger":            ledger,
		"node_address":      nodeAddress,
		"node_id":           nodeID,
		"case":              testCase.Name,
		"axis":              testCase.AxisName,
		"requested_at":      testCase.Selector.GetAt().GetData(),
		"use_max_precision": testCase.UseMaxPrecision,
		"collapse_colors":   testCase.CollapseColors,
	}
	ledgerInfo, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
	if err != nil || ledgerInfo.GetId() == 0 {
		if IsTransient(err) || IsCanceled(err) {
			return false
		}
		assert.Unreachable("pit: scope fixture ledger identity was unavailable", baseDetails.With(Details{
			"ledger_id": ledgerInfo.GetId(),
			"error":     err,
		}))

		return false
	}
	expectedLedgerID := ledgerInfo.GetId()
	base := servicepb.AggregateVolumesRequest{
		Ledger:            ledger,
		UseMaxPrecision:   testCase.UseMaxPrecision,
		CollapseColors:    testCase.CollapseColors,
		HistoricalBalance: testCase.Selector,
	}
	for range pitScopePairAttempts {
		unfilteredRequest := base
		foldRequest := base
		foldRequest.Filter = actions.NotFilter(actions.AddressExactFilter(PITScopeReservedAddress))

		unfiltered, unfilteredView, err := AggregatePointInTime(ctx, client, &unfilteredRequest)
		if err != nil {
			if IsTransient(err) || IsCanceled(err) || IsClassifiedPointInTimeFailure(err) {
				continue
			}
			assert.Unreachable(
				"pit: redundant unfiltered aggregate returned unexpected error",
				baseDetails.With(Details{"error": err}),
			)

			return false
		}

		folded, foldedView, err := AggregatePointInTime(ctx, client, &foldRequest)
		if err != nil {
			if IsTransient(err) || IsCanceled(err) || IsClassifiedPointInTimeFailure(err) {
				continue
			}
			assert.Unreachable(
				"pit: exhaustive account-fold aggregate returned unexpected error",
				baseDetails.With(Details{"error": err}),
			)

			return false
		}

		if unfilteredView.GetViewToken() != foldedView.GetViewToken() {
			continue
		}

		details := baseDetails.With(Details{
			"ledger_id":                  expectedLedgerID,
			"view_token":                 unfilteredView.GetViewToken(),
			"unfiltered_audit_watermark": unfilteredView.GetAuditWatermark(),
			"unfiltered_log_watermark":   unfilteredView.GetLogWatermark(),
			"folded_audit_watermark":     foldedView.GetAuditWatermark(),
			"folded_log_watermark":       foldedView.GetLogWatermark(),
			"unfiltered_manifest":        unfilteredView.GetManifestVersion(),
			"folded_manifest":            foldedView.GetManifestVersion(),
		})
		assert.AlwaysOrUnreachable(
			proto.Equal(unfilteredView, foldedView),
			"pit: matching redundant scope tokens carry identical view provenance",
			details,
		)

		canonicalUnfiltered, unfilteredErr := CanonicalFlatAggregate(unfiltered)
		canonicalFolded, foldedErr := CanonicalFlatAggregate(folded)
		canonical := unfilteredErr == nil && foldedErr == nil
		assert.AlwaysOrUnreachable(
			canonical,
			"pit: redundant aggregate scopes return canonical flat volume buckets",
			details.With(Details{
				"unfiltered_error": unfilteredErr,
				"fold_error":       foldedErr,
			}),
		)
		if !canonical {
			return true
		}

		assert.AlwaysOrUnreachable(
			reflect.DeepEqual(canonicalUnfiltered, canonicalFolded),
			"pit: unfiltered asset summaries equal the exhaustive account fold",
			details.With(Details{
				"unfiltered": canonicalUnfiltered,
				"folded":     canonicalFolded,
			}),
		)

		return true
	}

	return false
}
