package generate

import (
	"context"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/pkg/client"
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
				if s.untilLogID != 0 && uint64(ret.GetLogID()) >= s.untilLogID {
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
