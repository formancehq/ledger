package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

type oracleConfig struct {
	Attempts          int
	ConvergenceWindow time.Duration
	Wait              func(context.Context) error
}

type attemptReport struct {
	Qualified    bool                      `json:"qualified"`
	Horizon      uint64                    `json:"horizon"`
	Witness      string                    `json:"witness"`
	Expected     map[string]expectedLedger `json:"expected,omitempty"`
	Observations []observation             `json:"observations,omitempty"`
	Error        string                    `json:"error,omitempty"`
}

// runOracle retries the complete source/witness/observation lifecycle, never
// reuses an invalidated expectation, and retains failed attempts for diagnosis.
// Exhausting the budget yields an explicit unqualified report even if no final
// barrier can run. Only a qualified attempt can publish a counter verdict.
func runOracle(ctx context.Context, conns internal.PerNodeConns, config oracleConfig) []attemptReport {
	var reports []attemptReport
	for range config.Attempts {
		report, err := runAttempt(ctx, conns, config)
		if err != nil {
			report.Error = err.Error()
		}
		reports = append(reports, report)
		if err == nil || ctx.Err() != nil {
			break
		}
		if !isSourceChanged(err) && !internal.IsTransient(err) && !errors.Is(err, context.DeadlineExceeded) {
			break
		}
		if err := config.Wait(ctx); err != nil {
			break
		}
	}
	return reports
}

func runAttempt(ctx context.Context, conns internal.PerNodeConns, config oracleConfig) (attemptReport, error) {
	var report attemptReport
	sourceConn, err := selectSource(ctx, conns)
	if err != nil {
		return report, fmt.Errorf("selecting source: %w", err)
	}
	source := sourceConn.Bucket
	report.Witness = internal.PrefixStatsWitness.WithSuffix(fmt.Sprintf("%016x", internal.Rand().Uint64()))
	witness, err := createWitness(ctx, source, report.Witness)
	if err != nil {
		return report, fmt.Errorf("creating witness: %w", err)
	}
	report.Expected, report.Horizon, err = captureSource(ctx, source)
	if err != nil {
		return report, fmt.Errorf("capturing source: %w", err)
	}
	delete(report.Expected, witness.GetName())
	if len(report.Expected) == 0 {
		return report, errors.New("no source ledger to observe")
	}
	replicas, err := activeReplicas(ctx, sourceConn, conns)
	if err != nil {
		return report, fmt.Errorf("identifying source membership: %w", err)
	}
	report.Horizon, err = writeWitness(ctx, source, witness.GetName(), report.Horizon)
	if err != nil {
		return report, fmt.Errorf("writing usage witness: %w", err)
	}
	report.Observations = make([]observation, len(replicas))
	pollCtx, stop := context.WithTimeout(ctx, config.ConvergenceWindow)
	var workers sync.WaitGroup
	for i, conn := range replicas {
		workers.Go(func() {
			report.Observations[i] = awaitReplica(pollCtx, conn, report.Horizon, witness, report.Expected, config.Wait)
			report.Observations[i].Node = conn.Addr
			report.Observations[i].NodeID = conn.NodeID
		})
	}
	workers.Wait()
	stop()
	if err := confirmSource(ctx, source, report.Horizon+1); err != nil {
		return report, fmt.Errorf("final source fence: %w", err)
	}
	report.Qualified = true
	return report, nil
}
