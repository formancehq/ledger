package state

import (
	"fmt"
	"testing"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Measures the extra FSM work after posting processing, with all incarnation
// values already in the cache. It deliberately excludes the existing posting
// processor and Pebble commit so the new cost is visible on its own.
func BenchmarkEnsureAccountIncarnationsHot(b *testing.B) {
	for _, accounts := range []int{2, 20, 100} {
		b.Run(fmt.Sprintf("accounts=%d", accounts), func(b *testing.B) {
			c, err := cache.New(1000, nil)
			if err != nil {
				b.Fatal(err)
			}
			registry := NewStateRegistry(c, attributes.New())
			ws := NewWriteSet(&Machine{Registry: registry, State: &FSMState{}})
			ws.gatedLedgerTypes = map[string]gatedLedgerType{"ledger": {found: true}}
			plan := &raftcmdpb.ExecutionPlan{}
			keys := make([]domain.VolumeKey, accounts)
			for i := range keys {
				account := domain.AccountKey{LedgerName: "ledger", Account: fmt.Sprintf("account:%d", i)}
				keys[i] = domain.NewVolumeKey(account.LedgerName, account.Account, "USD/2", "")
				id, tag := attributes.MakeKey(account.Bytes())
				plan.Attributes = append(plan.Attributes, declareTestPlan(id, dal.SubAttrAccountIncarnation))
				c.AccountIncarnations.Put(id, attributes.Entry[*commonpb.AccountIncarnation]{Tag: tag, Data: &commonpb.AccountIncarnation{}})
			}
			factory := NewScopeFactory(ws, plan, logging.FromContext(logging.TestingContext()), nil, 42)
			scope, err := factory.NewProposalScope()
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				ws.Derived.Reset()
				for _, key := range keys {
					ws.Derived.Volumes.Put(key, &raftcmdpb.VolumePair{})
				}
				if err := ws.ensureAccountIncarnations(scope); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
