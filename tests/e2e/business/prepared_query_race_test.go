//go:build e2e

package business

import (
	"fmt"
	"strings"
	"sync"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// PreparedQueryDeleteCreateRace exercises the admission/apply race where two
// concurrent operations target the same prepared-query name with one of them
// being a delete.
//
// Scenario: q-N exists in the leader's cache. Two operations arrive together:
//   - delete q-N
//   - create q-N again
//
// Both admit while the cache holds the entry, so CheckCache returns
// CacheHit and neither admission emits a preload for q-N. If the
// delete is processed first in the batch, it clears the in-memory cache.
// The create then runs in the same batch with no preload and an empty
// cache: GetPreparedQuery returns ErrNotFound, which leaks out of
// processCreatePreparedQuery and surfaces to the client as
// `Internal: not found`.
var _ = Describe("PreparedQueryDeleteCreateRace", Ordered, func() {
	const (
		ledgerName = "prepared-query-race"
		iterations = 30
	)

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())
	})

	It("Should not fail with 'not found' when concurrent delete and re-create race", func() {
		query := func(name string) *commonpb.PreparedQuery {
			return &commonpb.PreparedQuery{
				Name:   name,
				Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
				Filter: &commonpb.QueryFilter{
					Filter: &commonpb.QueryFilter_Address{
						Address: &commonpb.AddressMatch{
							Match: &commonpb.AddressMatch_HardcodedPrefix{
								HardcodedPrefix: "users:",
							},
						},
					},
				},
			}
		}

		var raceHits []string

		for i := 0; i < iterations; i++ {
			name := fmt.Sprintf("race-q-%d", i)

			// Seed: pre-create the query so it lives in the leader's cache
			// at the moment admission inspects CheckCache for the racing ops.
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Ledger: ledgerName,
				Query:  query(name),
			})
			Expect(err).To(Succeed())

			// Issue delete + create through a barrier so they reach admission
			// together and co-batch on apply.
			barrier := make(chan struct{})
			var (
				wg                   sync.WaitGroup
				errDelete, errCreate error
			)
			wg.Add(2)

			go func() {
				defer wg.Done()
				<-barrier
				_, errDelete = sharedClient.DeletePreparedQuery(sharedCtx, &servicepb.DeletePreparedQueryRequest{
					Ledger: ledgerName,
					Name:   name,
				})
			}()

			go func() {
				defer wg.Done()
				<-barrier
				_, errCreate = sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
					Ledger: ledgerName,
					Query:  query(name),
				})
			}()

			close(barrier)
			wg.Wait()

			// Race signature: ErrNotFound leaks from the create as `Internal: not found`.
			// AlreadyExists from the create is benign (delete didn't apply first).
			// Delete returning PreparedQueryNotFound would mean the create lost and
			// cleanup ran against an absent entry — also benign.
			if errCreate != nil && strings.Contains(errCreate.Error(), "not found") {
				raceHits = append(raceHits, fmt.Sprintf("iter %d create: %v", i, errCreate))
			}
			_ = errDelete
		}

		Expect(raceHits).To(BeEmpty(),
			"prepared-query delete/create race fired in %d/%d iterations:\n%s",
			len(raceHits), iterations, strings.Join(raceHits, "\n"))
	})
})
