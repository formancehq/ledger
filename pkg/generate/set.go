package generate

import (
	"context"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"golang.org/x/sync/errgroup"
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

				if s.untilLogID != 0 && uint64(maxLogID) >= s.untilLogID {
					return nil
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
