package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	antirandom "github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const propertyAccount = "pitidem:destination"

const pitOracleMaximumAttempts = 8

type pitAxisObservation struct {
	axis   servicepb.HistoricalBalanceTemporality
	result *commonpb.AggregateResult
	view   *servicepb.HistoricalBalanceView
}

func main() {
	log.Println("composer: parallel_driver_idempotency")

	ctx, cancel := internal.DriverContext()
	defer cancel()

	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("creating retrying client: %s", err)

		return
	}
	defer conn.Close()

	probeClient, probeConn, probeAddress, err := newDirectNoRetryClient()
	if err != nil {
		log.Printf("creating direct no-retry probe client: %s", err)

		return
	}
	defer probeConn.Close()

	seedOne := internal.Rand().Uint64()
	seedTwo := internal.Rand().Uint64()
	ledger := internal.PrefixPITIdempotency.WithSuffix(fmt.Sprintf("%016x%016x", seedOne, seedTwo))
	if err := internal.CreateLedger(ctx, client, ledger); err != nil {
		return
	}
	if err := internal.ConfigureHistoricalBalances(ctx, client, ledger); err != nil {
		return
	}
	ledgerInfo, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
	if err != nil || ledgerInfo.GetId() == 0 {
		if !internal.IsTolerated(err) {
			assert.Unreachable("pit: idempotency property ledger identity was unavailable", internal.Details{
				"ledger": ledger,
				"error":  err,
			})
		}

		return
	}

	keyLength := antirandom.RandomChoice(internal.PITIdempotencyKeyLengths())
	idempotencyKey, err := internal.PITIdempotencyKey(seedOne, seedTwo, keyLength)
	if err != nil {
		assert.Unreachable("pit: idempotency property generated an invalid bounded key", internal.Details{
			"key_length": keyLength,
			"error":      err,
		})

		return
	}
	amountValue := antirandom.RandomChoice([]uint64{1, math.MaxUint8, math.MaxUint8 + 1, math.MaxUint16, math.MaxUint16 + 1})
	amount := new(big.Int).SetUint64(amountValue)
	asset := antirandom.RandomChoice([]string{"USD/2", "USD/4", "COIN"})
	probeID := fmt.Sprintf("pit-idem-probe-%016x", internal.Rand().Uint64())
	request := servicepb.UnsignedApplyRequest(
		idempotencyKey,
		actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{
			actions.NewPosting("world", propertyAccount, amount, asset),
		}, nil),
	)

	probeTimeout := antirandom.RandomChoice([]time.Duration{
		250 * time.Millisecond,
		time.Second,
		2 * time.Second,
	})
	probeCtx, probeCancel := context.WithTimeout(ctx, probeTimeout)
	probeCtx = metadata.AppendToOutgoingContext(
		probeCtx,
		internal.PITIdempotencyProbeMetadataKey,
		probeID,
	)
	var probeHeader metadata.MD
	firstResponse, firstErr := probeClient.Apply(probeCtx, request, grpc.Header(&probeHeader))
	probeCancel()
	postCommitReached := internal.PITIdempotencyProbeReached(probeHeader, probeID)
	if firstErr != nil && !internal.IsClassified(firstErr) {
		assert.Unreachable("pit: post-commit idempotency probe returned an unclassified error", internal.Details{
			"ledger":        ledger,
			"probe_address": probeAddress,
			"error":         firstErr,
		})

		return
	}

	secondResponse, secondErr := client.Apply(ctx, request)
	if secondErr != nil {
		if !internal.IsTolerated(secondErr) {
			assert.Unreachable("pit: keyed retry returned a definitive non-idempotent error", internal.Details{
				"ledger": ledger,
				"error":  secondErr,
			})
		}

		return
	}
	thirdResponse, thirdErr := client.Apply(ctx, request)
	if thirdErr != nil {
		if !internal.IsTolerated(thirdErr) {
			assert.Unreachable("pit: keyed replay returned a definitive non-idempotent error", internal.Details{
				"ledger": ledger,
				"error":  thirdErr,
			})
		}

		return
	}

	secondTransaction := internal.ExtractCreatedTransaction(secondResponse)
	thirdTransaction := internal.ExtractCreatedTransaction(thirdResponse)
	if secondTransaction == nil || thirdTransaction == nil || len(secondResponse.GetLogs()) != 1 {
		assert.Unreachable("pit: keyed retries returned an incomplete committed transaction response", internal.Details{
			"ledger":           ledger,
			"second_log_count": len(secondResponse.GetLogs()),
			"third_log_count":  len(thirdResponse.GetLogs()),
		})

		return
	}
	secondTransactionID := secondTransaction.GetTransaction().GetId()
	thirdTransactionID := thirdTransaction.GetTransaction().GetId()
	details := internal.Details{
		"ledger":                 ledger,
		"ledger_id":              ledgerInfo.GetId(),
		"idempotency_key":        idempotencyKey,
		"idempotency_key_length": keyLength,
		"probe_id":               probeID,
		"probe_address":          probeAddress,
		"probe_timeout_ms":       probeTimeout.Milliseconds(),
		"post_commit_reached":    postCommitReached,
		"first_error":            firstErr,
		"first_response_present": firstResponse != nil,
		"second_transaction_id":  secondTransactionID,
		"third_transaction_id":   thirdTransactionID,
		"asset":                  asset,
		"amount":                 amount.String(),
	}
	assert.Always(
		secondTransactionID == thirdTransactionID,
		"pit: keyed retries return the original committed transaction identity",
		details,
	)
	if secondTransactionID != thirdTransactionID {
		return
	}

	logSequence := secondResponse.GetLogs()[0].GetSequence()
	entries, auditReadErr := collectKeyedAuditEntries(ctx, client, ledger, idempotencyKey, logSequence)
	if auditReadErr != nil {
		if !internal.IsTolerated(auditReadErr) {
			assert.Unreachable("pit: keyed audit correlation returned an unexpected error", details.With(internal.Details{
				"error": auditReadErr,
			}))
		}

		return
	}
	auditSequence, auditErr := internal.ValidatePITKeyedAudit(entries, idempotencyKey, logSequence)
	assert.Always(
		auditErr == nil,
		"pit: keyed retries have one authoritative audit and log outcome",
		details.With(internal.Details{
			"audit_entry_count": len(entries),
			"audit_error":       auditErr,
			"log_sequence":      logSequence,
		}),
	)
	if auditErr != nil {
		return
	}

	axes := []servicepb.HistoricalBalanceTemporality{
		servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_EFFECTIVE,
		servicepb.HistoricalBalanceTemporality_HISTORICAL_BALANCE_TEMPORALITY_INSERTION,
	}
	observations := make([]pitAxisObservation, 0, len(axes))
	for attempt := 1; attempt <= pitOracleMaximumAttempts; attempt++ {
		observations = observations[:0]
		retryPair := false
		for _, axis := range axes {
			result, view, pitErr := internal.AggregatePointInTime(ctx, client, &servicepb.AggregateVolumesRequest{
				Ledger:         ledger,
				Filter:         actions.AddressExactFilter(propertyAccount),
				MinLogSequence: logSequence,
				HistoricalBalance: &servicepb.HistoricalBalanceSelector{
					At:          &commonpb.Timestamp{Data: math.MaxUint64},
					Temporality: axis,
				},
			})
			if pitErr == nil {
				observations = append(observations, pitAxisObservation{
					axis:   axis,
					result: result,
					view:   view,
				})

				continue
			}
			if internal.IsCanceled(pitErr) {
				return
			}
			if internal.IsClassifiedPointInTimeFailure(pitErr) {
				assert.Reachable(
					"pit: keyed retry oracle observes fail-closed history before convergence",
					details.With(internal.Details{
						"axis":    axis.String(),
						"attempt": attempt,
						"error":   pitErr,
					}),
				)
				retryPair = true

				break
			}
			if internal.IsTransient(pitErr) {
				retryPair = true

				break
			}

			assert.Unreachable("pit: keyed retry aggregate returned an unexpected error", details.With(internal.Details{
				"axis":    axis.String(),
				"attempt": attempt,
				"error":   pitErr,
			}))

			return
		}
		if !retryPair {
			break
		}
		if ctx.Err() != nil {
			return
		}
	}
	if len(observations) != len(axes) {
		return
	}

	allAxesExact := true
	for _, observation := range observations {
		watermarkExact := observation.view.GetAuditWatermark() >= auditSequence &&
			observation.view.GetLogWatermark() >= logSequence
		assert.AlwaysOrUnreachable(
			watermarkExact,
			"pit: keyed retry view covers its authoritative audit and log outcome",
			details.With(internal.Details{
				"axis":            observation.axis.String(),
				"audit_sequence":  auditSequence,
				"audit_watermark": observation.view.GetAuditWatermark(),
				"log_sequence":    logSequence,
				"log_watermark":   observation.view.GetLogWatermark(),
				"view_token":      observation.view.GetViewToken(),
			}),
		)
		if !watermarkExact {
			return
		}

		monetaryErr := internal.ValidatePITSingleInput(observation.result, asset, amount)
		assert.Always(
			monetaryErr == nil,
			"pit: keyed apply retries contribute one committed monetary effect",
			details.With(internal.Details{
				"axis":           observation.axis.String(),
				"audit_sequence": auditSequence,
				"log_sequence":   logSequence,
				"monetary_error": monetaryErr,
				"view_token":     observation.view.GetViewToken(),
			}),
		)
		allAxesExact = allAxesExact && monetaryErr == nil
	}

	ambiguousReconciled := postCommitReached && firstErr != nil &&
		internal.IsTransient(firstErr) && allAxesExact
	assert.Sometimes(
		ambiguousReconciled,
		"pit: post-commit ambiguous keyed retry reconciles through the original outcome",
		details.With(internal.Details{
			"audit_sequence": auditSequence,
			"log_sequence":   logSequence,
		}),
	)
}

func newDirectNoRetryClient() (
	servicepb.BucketServiceClient,
	*grpc.ClientConn,
	string,
	error,
) {
	targets := os.Getenv("LEDGER_PER_NODE_GRPC_ADDR")
	if targets == "" {
		targets = os.Getenv("LEDGER_GRPC_ADDR")
	}
	if targets == "" {
		targets = "localhost:15100"
	}

	addresses := make([]string, 0, len(strings.Split(targets, ",")))
	for address := range strings.SplitSeq(targets, ",") {
		if address = strings.TrimSpace(address); address != "" {
			addresses = append(addresses, address)
		}
	}
	if len(addresses) == 0 {
		return nil, nil, "", errors.New("no direct ledger address configured")
	}
	address := antirandom.RandomChoice(addresses)
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, address, err
	}

	return servicepb.NewBucketServiceClient(conn), conn, address, nil
}

func collectKeyedAuditEntries(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	ledger string,
	idempotencyKey string,
	minLogSequence uint64,
) ([]*auditpb.AuditEntry, error) {
	stream, err := client.ListAuditEntries(ctx, &servicepb.ListAuditEntriesRequest{
		Options: &commonpb.ListOptions{
			Read: &commonpb.ReadOptions{MinLogSequence: minLogSequence},
			Filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{
				Audit: &commonpb.AuditCondition{
					Field: commonpb.AuditField_AUDIT_FIELD_LEDGER,
					Condition: &commonpb.AuditCondition_StringCond{StringCond: &commonpb.StringCondition{
						Value: &commonpb.StringCondition_Hardcoded{Hardcoded: ledger},
					}},
				},
			}},
		},
	})
	if err != nil {
		return nil, err
	}

	var entries []*auditpb.AuditEntry
	for {
		entry, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return nil, recvErr
		}
		if entry.GetIdempotency().GetKey() != idempotencyKey {
			continue
		}

		detailed, getErr := client.GetAuditEntry(ctx, &servicepb.GetAuditEntryRequest{
			Sequence: entry.GetSequence(),
		})
		if getErr != nil {
			return nil, getErr
		}
		entries = append(entries, detailed)
	}

	return entries, nil
}
