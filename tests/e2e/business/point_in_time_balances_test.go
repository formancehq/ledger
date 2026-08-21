//go:build e2e

package business

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const pointInTimeViewTrailerKey = "x-historical-balance-view-bin"

var _ = Describe("Point-in-time balances", Serial, func() {
	It("serves v2-compatible historical monetary views through the real gRPC server", func() {
		ctx, node := testutil.SetupSingleNode()
		client := node.Client

		By("separating effective time from insertion time and honoring read-after-write")
		const axesLedger = "pit-e2e-axes"
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(axesLedger, nil)))
		Expect(err).To(Succeed())
		Expect(testutil.ConfigureHistoricalBalances(ctx, client, axesLedger, true)).To(Succeed())
		testutil.WaitForHistoricalBalancesReady(ctx, client, axesLedger)

		firstEffective := time.Date(2020, time.January, 10, 12, 0, 0, 0, time.UTC)
		firstRequest := actions.WithTimestamp(actions.CreateForceTransactionAction(axesLedger, []*commonpb.Posting{
			actions.NewPosting("world", "accounts:alice", big.NewInt(100), "USD"),
		}, nil), firstEffective)
		firstResponse, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", firstRequest))
		Expect(err).To(Succeed())
		firstTransaction := pointInTimeCreatedTransaction(firstResponse)
		Expect(firstTransaction.GetInsertedAt()).NotTo(BeNil())

		secondEffective := time.Date(2020, time.January, 20, 12, 0, 0, 0, time.UTC)
		secondRequest := actions.WithTimestamp(actions.CreateForceTransactionAction(axesLedger, []*commonpb.Posting{
			actions.NewPosting("world", "accounts:alice", big.NewInt(50), "USD"),
		}, nil), secondEffective)
		secondResponse, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", secondRequest))
		Expect(err).To(Succeed())
		secondTransaction := pointInTimeCreatedTransaction(secondResponse)
		Expect(secondTransaction.GetInsertedAt()).NotTo(BeNil())
		Expect(secondTransaction.GetInsertedAt().GetData()).To(BeNumerically(">", firstTransaction.GetInsertedAt().GetData()))
		secondLogSequence := pointInTimeLastLogSequence(secondResponse)

		// This is intentionally the first read after the write. min_log_sequence
		// must wait for the asynchronous history builder instead of returning a
		// partial historical view.
		readAfterWriteCtx, cancelReadAfterWrite := context.WithTimeout(ctx, 15*time.Second)
		afterBoth, firstView, err := pointInTimeAggregate(readAfterWriteCtx, client, &servicepb.AggregateVolumesRequest{
			Ledger:         axesLedger,
			Filter:         actions.AddressExactFilter("accounts:alice"),
			MinLogSequence: secondLogSequence,
			HistoricalBalance: pointInTimeSelector(
				secondEffective.Add(time.Hour),
				servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
			),
		})
		cancelReadAfterWrite()
		Expect(err).To(Succeed())
		expectPointInTimeVolume(afterBoth, "USD", "", 150, 0)
		expectHistoricalBalanceView(firstView, uint64(secondEffective.Add(time.Hour).UnixMicro()), servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE, secondLogSequence)

		beforeAll, _ := pointInTimeAggregateEventually(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:         axesLedger,
			Filter:         actions.AddressExactFilter("accounts:alice"),
			MinLogSequence: secondLogSequence,
			HistoricalBalance: pointInTimeSelector(
				firstEffective.Add(-time.Hour),
				servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
			),
		})
		Expect(beforeAll.GetVolumes()).To(BeEmpty())

		betweenTransactions, _ := pointInTimeAggregateEventually(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:         axesLedger,
			Filter:         actions.AddressExactFilter("accounts:alice"),
			MinLogSequence: secondLogSequence,
			HistoricalBalance: pointInTimeSelector(
				firstEffective.Add(time.Hour),
				servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
			),
		})
		expectPointInTimeVolume(betweenTransactions, "USD", "", 100, 0)

		beforeFirstInsertion, _ := pointInTimeAggregateEventually(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:         axesLedger,
			Filter:         actions.AddressExactFilter("accounts:alice"),
			MinLogSequence: secondLogSequence,
			HistoricalBalance: &servicepb.HistoricalBalanceSelector{
				At:          &commonpb.Timestamp{Data: firstTransaction.GetInsertedAt().GetData() - 1},
				Temporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION,
			},
		})
		Expect(beforeFirstInsertion.GetVolumes()).To(BeEmpty())

		atFirstInsertion, _ := pointInTimeAggregateEventually(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:         axesLedger,
			Filter:         actions.AddressExactFilter("accounts:alice"),
			MinLogSequence: secondLogSequence,
			HistoricalBalance: &servicepb.HistoricalBalanceSelector{
				At:          firstTransaction.GetInsertedAt(),
				Temporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION,
			},
		})
		expectPointInTimeVolume(atFirstInsertion, "USD", "", 100, 0)

		atSecondInsertion, _ := pointInTimeAggregateEventually(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:         axesLedger,
			Filter:         actions.AddressExactFilter("accounts:alice"),
			MinLogSequence: secondLogSequence,
			HistoricalBalance: &servicepb.HistoricalBalanceSelector{
				At:          secondTransaction.GetInsertedAt(),
				Temporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION,
			},
		})
		expectPointInTimeVolume(atSecondInsertion, "USD", "", 150, 0)

		// With no intervening write, the same selector is served from the same
		// immutable manifest and therefore has a stable provenance token.
		_, repeatedView, err := pointInTimeAggregate(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:         axesLedger,
			Filter:         actions.AddressExactFilter("accounts:alice"),
			MinLogSequence: secondLogSequence,
			HistoricalBalance: pointInTimeSelector(
				secondEffective.Add(time.Hour),
				servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
			),
		})
		Expect(err).To(Succeed())
		Expect(repeatedView.GetViewToken()).To(Equal(firstView.GetViewToken()))
		Expect(repeatedView.GetManifestVersion()).To(Equal(firstView.GetManifestVersion()))

		_, err = client.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{
			Ledger:       axesLedger,
			CheckpointId: 1,
			HistoricalBalance: pointInTimeSelector(
				secondEffective,
				servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
			),
		})
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))

		By("placing normal and at-effective-date reversals on the correct axes")
		const reversalLedger = "pit-e2e-reversals"
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(reversalLedger, nil)))
		Expect(err).To(Succeed())
		Expect(testutil.ConfigureHistoricalBalances(ctx, client, reversalLedger, true)).To(Succeed())
		testutil.WaitForHistoricalBalancesReady(ctx, client, reversalLedger)

		normalEffective := time.Date(2021, time.February, 10, 8, 0, 0, 0, time.UTC)
		normalResponse, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.WithTimestamp(
			actions.CreateForceTransactionAction(reversalLedger, []*commonpb.Posting{
				actions.NewPosting("world", "accounts:normal", big.NewInt(100), "USD"),
			}, nil),
			normalEffective,
		)))
		Expect(err).To(Succeed())
		normalTransaction := pointInTimeCreatedTransaction(normalResponse)

		normalRevertResponse, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.RevertTransactionAction(reversalLedger, normalTransaction.GetId(), false, false, nil),
		))
		Expect(err).To(Succeed())
		normalRevert := pointInTimeRevertTransaction(normalRevertResponse)
		Expect(normalRevert.GetTimestamp().GetData()).To(BeNumerically(">", normalTransaction.GetTimestamp().GetData()))
		Expect(normalRevert.GetInsertedAt().GetData()).To(BeNumerically(">", normalTransaction.GetInsertedAt().GetData()))
		normalRevertLogSequence := pointInTimeLastLogSequence(normalRevertResponse)

		beforeNormalRevert, _ := pointInTimeAggregateEventually(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:         reversalLedger,
			Filter:         actions.AddressExactFilter("accounts:normal"),
			MinLogSequence: normalRevertLogSequence,
			HistoricalBalance: &servicepb.HistoricalBalanceSelector{
				At:          &commonpb.Timestamp{Data: normalRevert.GetTimestamp().GetData() - 1},
				Temporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
			},
		})
		expectPointInTimeVolume(beforeNormalRevert, "USD", "", 100, 0)

		atNormalRevert, _ := pointInTimeAggregateEventually(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:         reversalLedger,
			Filter:         actions.AddressExactFilter("accounts:normal"),
			MinLogSequence: normalRevertLogSequence,
			HistoricalBalance: &servicepb.HistoricalBalanceSelector{
				At:          normalRevert.GetTimestamp(),
				Temporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
			},
		})
		expectPointInTimeVolume(atNormalRevert, "USD", "", 100, 100)

		atNormalInsertion, _ := pointInTimeAggregateEventually(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:         reversalLedger,
			Filter:         actions.AddressExactFilter("accounts:normal"),
			MinLogSequence: normalRevertLogSequence,
			HistoricalBalance: &servicepb.HistoricalBalanceSelector{
				At:          normalRevert.GetInsertedAt(),
				Temporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION,
			},
		})
		expectPointInTimeVolume(atNormalInsertion, "USD", "", 100, 100)

		atEffectiveDate := time.Date(2022, time.March, 15, 9, 0, 0, 0, time.UTC)
		datedResponse, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.WithTimestamp(
			actions.CreateForceTransactionAction(reversalLedger, []*commonpb.Posting{
				actions.NewPosting("world", "accounts:dated", big.NewInt(200), "EUR"),
			}, nil),
			atEffectiveDate,
		)))
		Expect(err).To(Succeed())
		datedTransaction := pointInTimeCreatedTransaction(datedResponse)

		datedRevertResponse, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.RevertTransactionAction(reversalLedger, datedTransaction.GetId(), false, true, nil),
		))
		Expect(err).To(Succeed())
		datedRevert := pointInTimeRevertTransaction(datedRevertResponse)
		Expect(datedRevert.GetTimestamp().GetData()).To(Equal(datedTransaction.GetTimestamp().GetData()))
		Expect(datedRevert.GetInsertedAt().GetData()).To(BeNumerically(">", datedTransaction.GetInsertedAt().GetData()))
		datedRevertLogSequence := pointInTimeLastLogSequence(datedRevertResponse)

		beforeDatedEffective, _ := pointInTimeAggregateEventually(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:         reversalLedger,
			Filter:         actions.AddressExactFilter("accounts:dated"),
			MinLogSequence: datedRevertLogSequence,
			HistoricalBalance: &servicepb.HistoricalBalanceSelector{
				At:          &commonpb.Timestamp{Data: datedTransaction.GetTimestamp().GetData() - 1},
				Temporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
			},
		})
		Expect(beforeDatedEffective.GetVolumes()).To(BeEmpty())

		atDatedEffective, _ := pointInTimeAggregateEventually(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:         reversalLedger,
			Filter:         actions.AddressExactFilter("accounts:dated"),
			MinLogSequence: datedRevertLogSequence,
			HistoricalBalance: &servicepb.HistoricalBalanceSelector{
				At:          datedTransaction.GetTimestamp(),
				Temporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
			},
		})
		expectPointInTimeVolume(atDatedEffective, "EUR", "", 200, 200)

		beforeDatedRevertInsertion, _ := pointInTimeAggregateEventually(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:         reversalLedger,
			Filter:         actions.AddressExactFilter("accounts:dated"),
			MinLogSequence: datedRevertLogSequence,
			HistoricalBalance: &servicepb.HistoricalBalanceSelector{
				At:          datedTransaction.GetInsertedAt(),
				Temporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION,
			},
		})
		expectPointInTimeVolume(beforeDatedRevertInsertion, "EUR", "", 200, 0)

		atDatedRevertInsertion, _ := pointInTimeAggregateEventually(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:         reversalLedger,
			Filter:         actions.AddressExactFilter("accounts:dated"),
			MinLogSequence: datedRevertLogSequence,
			HistoricalBalance: &servicepb.HistoricalBalanceSelector{
				At:          datedRevert.GetInsertedAt(),
				Temporality: servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION,
			},
		})
		expectPointInTimeVolume(atDatedRevertInsertion, "EUR", "", 200, 200)

		By("preserving colors while grouping prefixes and rescaling precision")
		const aggregateLedger = "pit-e2e-aggregate-options"
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(aggregateLedger, nil)))
		Expect(err).To(Succeed())
		Expect(testutil.ConfigureHistoricalBalances(ctx, client, aggregateLedger, true)).To(Succeed())
		testutil.WaitForHistoricalBalancesReady(ctx, client, aggregateLedger)
		aggregateAt := time.Date(2023, time.April, 20, 10, 0, 0, 0, time.UTC)
		aggregateResponse, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.WithTimestamp(actions.CreateForceTransactionAction(aggregateLedger, []*commonpb.Posting{
				actions.NewColoredPosting("world", "users:alice", big.NewInt(100), "USD/2", "RED"),
			}, nil), aggregateAt),
			actions.WithTimestamp(actions.CreateForceTransactionAction(aggregateLedger, []*commonpb.Posting{
				actions.NewColoredPosting("world", "users:bob", big.NewInt(10000), "USD/4", "RED"),
			}, nil), aggregateAt),
			actions.WithTimestamp(actions.CreateForceTransactionAction(aggregateLedger, []*commonpb.Posting{
				actions.NewColoredPosting("world", "users:carol", big.NewInt(50), "USD/2", "BLUE"),
			}, nil), aggregateAt),
		))
		Expect(err).To(Succeed())
		aggregateLogSequence := pointInTimeLastLogSequence(aggregateResponse)

		grouped, _ := pointInTimeAggregateEventually(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:          aggregateLedger,
			MinLogSequence:  aggregateLogSequence,
			UseMaxPrecision: true,
			GroupByPrefixes: []string{"users:"},
			HistoricalBalance: pointInTimeSelector(
				aggregateAt,
				servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
			),
		})
		Expect(grouped.GetVolumes()).To(BeEmpty())
		Expect(grouped.GetGroups()).To(HaveLen(1))
		Expect(grouped.GetGroups()[0].GetPrefix()).To(Equal("users:"))
		expectPointInTimeVolumeList(grouped.GetGroups()[0].GetVolumes(), "USD/4", "RED", 20000, 0)
		expectPointInTimeVolumeList(grouped.GetGroups()[0].GetVolumes(), "USD/4", "BLUE", 5000, 0)

		collapsed, _ := pointInTimeAggregateEventually(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:          aggregateLedger,
			MinLogSequence:  aggregateLogSequence,
			UseMaxPrecision: true,
			CollapseColors:  true,
			GroupByPrefixes: []string{"users:"},
			HistoricalBalance: pointInTimeSelector(
				aggregateAt,
				servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
			),
		})
		Expect(collapsed.GetGroups()).To(HaveLen(1))
		Expect(collapsed.GetGroups()[0].GetVolumes()).To(HaveLen(1))
		expectPointInTimeVolumeList(collapsed.GetGroups()[0].GetVolumes(), "USD/4", "", 25000, 0)

		By("evaluating metadata filters from current state, not historical state")
		const metadataLedger = "pit-e2e-current-metadata"
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.CreateLedgerWithSchemaAction(metadataLedger, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "segment",
					Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
				},
			}),
			actions.CreateAccountMetadataIndexAction(metadataLedger, "segment"),
		))
		Expect(err).To(Succeed())
		Expect(testutil.ConfigureHistoricalBalances(ctx, client, metadataLedger, true)).To(Succeed())
		testutil.WaitForHistoricalBalancesReady(ctx, client, metadataLedger)
		Expect(actions.WaitForMetadataIndexReady(ctx, client, metadataLedger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "segment")).To(Succeed())

		metadataAt := time.Date(2024, time.May, 5, 11, 0, 0, 0, time.UTC)
		metadataTransactions, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.WithTimestamp(actions.CreateForceTransactionAction(metadataLedger, []*commonpb.Posting{
				actions.NewPosting("world", "accounts:alice", big.NewInt(100), "GBP"),
			}, nil), metadataAt),
			actions.WithTimestamp(actions.CreateForceTransactionAction(metadataLedger, []*commonpb.Posting{
				actions.NewPosting("world", "accounts:bob", big.NewInt(200), "GBP"),
			}, nil), metadataAt),
		))
		Expect(err).To(Succeed())

		initialMetadata, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.SaveAccountMetadataAction(metadataLedger, "accounts:alice", map[string]string{"segment": "selected"}),
			actions.SaveAccountMetadataAction(metadataLedger, "accounts:bob", map[string]string{"segment": "other"}),
		))
		Expect(err).To(Succeed())
		initialMetadataLogSequence := pointInTimeLastLogSequence(initialMetadata)
		Expect(initialMetadataLogSequence).To(BeNumerically(">=", pointInTimeLastLogSequence(metadataTransactions)))

		initialFiltered, _ := pointInTimeAggregateEventually(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:         metadataLedger,
			Filter:         actions.StringMetadataFilter("segment", "selected"),
			MinLogSequence: initialMetadataLogSequence,
			HistoricalBalance: pointInTimeSelector(
				metadataAt,
				servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
			),
		})
		expectPointInTimeVolume(initialFiltered, "GBP", "", 100, 0)

		changedMetadata, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.SaveAccountMetadataAction(metadataLedger, "accounts:alice", map[string]string{"segment": "other"}),
			actions.SaveAccountMetadataAction(metadataLedger, "accounts:bob", map[string]string{"segment": "selected"}),
		))
		Expect(err).To(Succeed())
		changedMetadataLogSequence := pointInTimeLastLogSequence(changedMetadata)

		changedFiltered, _ := pointInTimeAggregateEventually(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:         metadataLedger,
			Filter:         actions.StringMetadataFilter("segment", "selected"),
			MinLogSequence: changedMetadataLogSequence,
			HistoricalBalance: pointInTimeSelector(
				metadataAt,
				servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
			),
		})
		expectPointInTimeVolume(changedFiltered, "GBP", "", 200, 0)
	})

	It("gates historical-balance reads by client-configured ledger name", func() {
		ctx, node := testutil.SetupSingleNode()
		client := node.Client

		const (
			allowedLedger = "pit-canary"
			deniedLedger  = "pit-denied"
		)
		for _, ledger := range []string{allowedLedger, deniedLedger} {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil)))
			Expect(err).To(Succeed())
		}
		Expect(testutil.ConfigureHistoricalBalances(ctx, client, allowedLedger, true)).To(Succeed())
		testutil.WaitForHistoricalBalancesReady(ctx, client, allowedLedger)

		effectiveAt := time.Date(2025, time.January, 2, 3, 4, 5, 0, time.UTC)
		allowedWrite, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.WithTimestamp(
			actions.CreateForceTransactionAction(allowedLedger, []*commonpb.Posting{
				actions.NewPosting("world", "accounts:canary", big.NewInt(42), "USD"),
			}, nil),
			effectiveAt,
		)))
		Expect(err).To(Succeed())

		allowed, _ := pointInTimeAggregateEventually(ctx, client, &servicepb.AggregateVolumesRequest{
			Ledger:         allowedLedger,
			Filter:         actions.AddressExactFilter("accounts:canary"),
			MinLogSequence: pointInTimeLastLogSequence(allowedWrite),
			HistoricalBalance: pointInTimeSelector(
				effectiveAt,
				servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
			),
		})
		expectPointInTimeVolume(allowed, "USD", "", 42, 0)

		_, err = client.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{
			Ledger: deniedLedger,
			HistoricalBalance: pointInTimeSelector(
				effectiveAt,
				servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
			),
		})
		Expect(err).To(HaveOccurred())
		errorInfo := actions.ExtractGRPCErrorInfo(err)
		Expect(errorInfo).NotTo(BeNil())
		Expect(errorInfo.GetReason()).To(Equal("HISTORY_SOURCE_MISSING"))
	})
})

func pointInTimeSelector(at time.Time, axis servicepb.HistoricalBalanceTemporality) *servicepb.HistoricalBalanceSelector {
	return &servicepb.HistoricalBalanceSelector{
		At:          &commonpb.Timestamp{Data: uint64(at.UnixMicro())},
		Temporality: axis,
	}
}

func pointInTimeAggregateEventually(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	request *servicepb.AggregateVolumesRequest,
) (*commonpb.AggregateResult, *servicepb.HistoricalBalanceView) {
	var (
		result *commonpb.AggregateResult
		view   *servicepb.HistoricalBalanceView
	)
	Eventually(func(g Gomega) {
		var err error
		result, view, err = pointInTimeAggregate(ctx, client, request)
		g.Expect(err).To(Succeed())
		g.Expect(result).NotTo(BeNil())
		g.Expect(view).NotTo(BeNil())
	}).Within(15 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())

	return result, view
}

func pointInTimeAggregate(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	request *servicepb.AggregateVolumesRequest,
) (*commonpb.AggregateResult, *servicepb.HistoricalBalanceView, error) {
	var trailer metadata.MD
	result, err := client.AggregateVolumes(ctx, request, grpc.Trailer(&trailer))
	if err != nil {
		return nil, nil, err
	}

	view, err := pointInTimeViewFromTrailer(trailer)
	if err != nil {
		return nil, nil, err
	}

	return result, view, nil
}

func pointInTimeViewFromTrailer(trailer metadata.MD) (*servicepb.HistoricalBalanceView, error) {
	values := trailer.Get(pointInTimeViewTrailerKey)
	if len(values) != 1 {
		return nil, fmt.Errorf("expected one %s trailer, got %d", pointInTimeViewTrailerKey, len(values))
	}

	view := &servicepb.HistoricalBalanceView{}
	if err := view.UnmarshalVT([]byte(values[0])); err != nil {
		return nil, fmt.Errorf("decoding point-in-time view trailer: %w", err)
	}

	return view, nil
}

func pointInTimeCreatedTransaction(response *servicepb.ApplyResponse) *commonpb.Transaction {
	Expect(response).NotTo(BeNil())
	Expect(response.GetLogs()).To(HaveLen(1))
	transaction := response.GetLogs()[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction()
	Expect(transaction).NotTo(BeNil())

	return transaction
}

func pointInTimeRevertTransaction(response *servicepb.ApplyResponse) *commonpb.Transaction {
	Expect(response).NotTo(BeNil())
	Expect(response.GetLogs()).To(HaveLen(1))
	transaction := response.GetLogs()[0].GetPayload().GetApply().GetLog().GetData().GetRevertedTransaction().GetRevertTransaction()
	Expect(transaction).NotTo(BeNil())

	return transaction
}

func pointInTimeLastLogSequence(response *servicepb.ApplyResponse) uint64 {
	Expect(response).NotTo(BeNil())
	Expect(response.GetLogs()).NotTo(BeEmpty())

	var sequence uint64
	for _, log := range response.GetLogs() {
		if log.GetSequence() > sequence {
			sequence = log.GetSequence()
		}
	}
	Expect(sequence).NotTo(BeZero())

	return sequence
}

func expectHistoricalBalanceView(
	view *servicepb.HistoricalBalanceView,
	requestedAt uint64,
	axis servicepb.HistoricalBalanceTemporality,
	minimumLogSequence uint64,
) {
	Expect(view).NotTo(BeNil())
	Expect(view.GetRequestedAt().GetData()).To(Equal(requestedAt))
	Expect(view.GetTemporality()).To(Equal(axis))
	Expect(view.GetLedger()).NotTo(BeEmpty())
	Expect(view.GetAuditWatermark()).NotTo(BeZero())
	Expect(view.GetLogWatermark()).To(BeNumerically(">=", minimumLogSequence))
	Expect(view.GetManifestVersion()).NotTo(BeZero())
	Expect(view.GetViewToken()).NotTo(BeEmpty())
}

func expectPointInTimeVolume(result *commonpb.AggregateResult, asset, color string, input, output int64) {
	Expect(result).NotTo(BeNil())
	expectPointInTimeVolumeList(result.GetVolumes(), asset, color, input, output)
}

func expectPointInTimeVolumeList(volumes []*commonpb.AggregatedVolume, asset, color string, input, output int64) {
	var found *commonpb.AggregatedVolume
	for _, volume := range volumes {
		if volume.GetAsset() == asset && volume.GetColor() == color {
			found = volume
			break
		}
	}
	Expect(found).NotTo(BeNil(), "expected (%s,%s) in %+v", asset, color, volumes)
	Expect(found.GetInput().ToBigInt().Int64()).To(Equal(input))
	Expect(found.GetOutput().ToBigInt().Int64()).To(Equal(output))
}
