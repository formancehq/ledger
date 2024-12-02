package generate

import (
	"context"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"golang.org/x/sync/errgroup"
	"math/big"
)

type GeneratorSet struct {
	vus            int
	script         string
	targetedLedger string
	client         *client.Formance
	untilLogID     uint64
}

func (s *GeneratorSet) Run(ctx context.Context) error {
	parallelContext, cancel := context.WithCancel(ctx)
	defer cancel()

	errGroup, ctx := errgroup.WithContext(parallelContext)

	for vu := 0; vu < s.vus; vu++ {
		generator, err := NewGenerator(s.script, WithGlobals(map[string]any{
			"vu": vu,
		}))
		if err != nil {
			return fmt.Errorf("failed to create generator: %w", err)
		}

		errGroup.Go(func() error {
			defer cancel()

			iteration := 0

			for {
				logging.FromContext(ctx).Debugf("Run iteration %d/%d", vu, iteration)

				action, err := generator.Next(vu)
				if err != nil {
					return fmt.Errorf("iteration %d/%d failed: %w", vu, iteration, err)
				}

				ret, err := action.Apply(ctx, s.client.Ledger.V2, s.targetedLedger)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return nil
					}
					return fmt.Errorf("iteration %d/%d failed: %w", vu, iteration, err)
				}

				if s.untilLogID != 0 {
					maxLogID := collectionutils.Reduce(ret, func(acc int64, r components.V2BulkElementResult) int64 {
						var logID int64
						switch r.Type {
						case components.V2BulkElementResultTypeCreateTransaction:
							logID = r.V2BulkElementResultCreateTransaction.LogID
						case components.V2BulkElementResultTypeAddMetadata:
							logID = r.V2BulkElementResultAddMetadata.LogID
						case components.V2BulkElementResultTypeDeleteMetadata:
							logID = r.V2BulkElementResultDeleteMetadata.LogID
						case components.V2BulkElementResultTypeRevertTransaction:
							logID = r.V2BulkElementResultRevertTransaction.LogID
						default:
							panic(fmt.Sprintf("unexpected result type: %s", r.Type))
						}

						if logID > acc {
							return logID
						}
						return acc
					}, 0)

					if maxLogID == 0 { // version < 2.2.0
						// notes(gfyrag): avoid list logs for each parallel runner by checking only on the first vu
						if vu == 0 {
							logs, err := s.client.Ledger.V2.ListLogs(ctx, operations.V2ListLogsRequest{
								Ledger:   s.targetedLedger,
								PageSize: pointer.For(int64(1)),
							})
							if err != nil {
								return fmt.Errorf("failed to list logs: %w", err)
							}
							if logs.V2LogsCursorResponse.Cursor.Data[0].ID.Cmp(big.NewInt(int64(s.untilLogID))) > 0 {
								logging.FromContext(ctx).Infof("Log %s reached, stopping generator", logs.V2LogsCursorResponse.Cursor.Data[0].ID.String())
								return nil
							}
						}
					} else {
						if uint64(maxLogID) >= s.untilLogID {
							logging.FromContext(ctx).Infof("Log %d reached, stopping generator", maxLogID)
							return nil
						}
					}
				}

				iteration++
			}
		})
	}

	return errGroup.Wait()
}

func NewGeneratorSet(vus int, script string, targetedLedger string, client *client.Formance, untilLogID uint64) *GeneratorSet {
	return &GeneratorSet{
		vus:            vus,
		script:         script,
		targetedLedger: targetedLedger,
		client:         client,
		untilLogID:     untilLogID,
	}
}
