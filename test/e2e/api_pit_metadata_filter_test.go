//go:build it

package test_suite

import (
	"math/big"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	. "github.com/formancehq/go-libs/v5/pkg/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v5/pkg/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"

	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

// A metadata filter has to select the same accounts on every endpoint that
// filters accounts, as of a pit as well as now.
//
// An account carrying no metadata as of the pit must read as having empty
// metadata, so a containment test is false for it and a negated one is true. The
// hazard is the metadata column being left NULL instead: in SQL a containment
// test against NULL is NULL rather than false, NOT NULL is NULL rather than true,
// and a WHERE clause keeps only what is true — so every such account silently
// disappears from the result instead of being selected.
var _ = Context("Ledger accounts filtered on metadata as of a pit", func() {
	var (
		db         = UseTemplatedDatabase()
		ctx        = logging.TestingContext()
		testServer = ginkgo.DeferTestServer(
			DeferMap(db, (*pgtesting.Database).ConnectionOptions),
			testservice.WithInstruments(
				testservice.DebugInstrumentation(debug),
				testservice.OutputInstrumentation(GinkgoWriter),
			),
			testservice.WithLogger(GinkgoT()),
		)

		now = time.Now().UTC().Round(time.Second)
		// The transactions are effective before the pit and the metadata is written
		// after it, so as of the pit the accounts exist and none of them carries
		// metadata.
		effective = now.Add(-2 * time.Hour)
		pit       = now.Add(-time.Hour)
	)

	// Only bank1 ends up tagged, so a filter negating the tag selects bank2 and
	// world now, and all three as of the pit.
	notPremium := map[string]any{
		"$not": map[string]any{
			"$match": map[string]any{"metadata[category]": "premium"},
		},
	}
	// The same predicate reached through a conjunction, which is where a NULL also
	// propagates, narrowed to one account so the aggregate is a non-zero number.
	notPremiumBank1 := map[string]any{
		"$and": []any{
			notPremium,
			map[string]any{"$match": map[string]any{"address": "bank1"}},
		},
	}

	BeforeEach(func(specContext SpecContext) {
		client := Wait(specContext, DeferClient(testServer))

		_, err := client.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())

		for _, posting := range []struct {
			destination string
			amount      int64
		}{{"bank1", 100}, {"bank2", 50}} {
			_, err := client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
				Ledger: "default",
				V2PostTransaction: components.V2PostTransaction{
					Timestamp: pointer.For(effective),
					Postings: []components.V2Posting{{
						Amount:      big.NewInt(posting.amount),
						Asset:       "USD/2",
						Source:      "world",
						Destination: posting.destination,
					}},
				},
			})
			Expect(err).To(BeNil())
		}

		// Undated, so the server stamps it now — after the pit.
		_, err = client.Ledger.V2.AddMetadataToAccount(ctx, operations.V2AddMetadataToAccountRequest{
			Ledger:      "default",
			Address:     "bank1",
			RequestBody: map[string]string{"category": "premium"},
		})
		Expect(err).To(BeNil())
	})

	listAccounts := func(specContext SpecContext, filter map[string]any, at *time.Time) []string {
		response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListAccounts(ctx, operations.V2ListAccountsRequest{
			Ledger:      "default",
			Pit:         at,
			RequestBody: filter,
		})
		Expect(err).To(BeNil())

		addresses := make([]string, 0, len(response.V2AccountsCursorResponse.Cursor.Data))
		for _, account := range response.V2AccountsCursorResponse.Cursor.Data {
			addresses = append(addresses, account.Address)
		}

		return addresses
	}

	listVolumes := func(specContext SpecContext, filter map[string]any, at *time.Time) []string {
		// The volumes endpoint spells its point in time endTime, which the handler
		// resolves to the same PIT the other two take.
		response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetVolumesWithBalances(ctx, operations.V2GetVolumesWithBalancesRequest{
			Ledger:      "default",
			EndTime:     at,
			RequestBody: filter,
		})
		Expect(err).To(BeNil())

		accounts := make([]string, 0, len(response.V2VolumesWithBalanceCursorResponse.Cursor.Data))
		for _, volumes := range response.V2VolumesWithBalanceCursorResponse.Cursor.Data {
			accounts = append(accounts, volumes.Account)
		}

		return accounts
	}

	aggregate := func(specContext SpecContext, filter map[string]any, at *time.Time) map[string]*big.Int {
		response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetBalancesAggregated(ctx, operations.V2GetBalancesAggregatedRequest{
			Ledger:      "default",
			Pit:         at,
			RequestBody: filter,
		})
		Expect(err).To(BeNil())

		return response.V2AggregateBalancesResponse.Data
	}

	When("no account carries the metadata yet", func() {
		It("should select every account on all three endpoints", func(specContext SpecContext) {
			Expect(listAccounts(specContext, notPremium, &pit)).To(ConsistOf("bank1", "bank2", "world"))
			Expect(listVolumes(specContext, notPremium, &pit)).To(ConsistOf("bank1", "bank2", "world"))
			// Every account is selected, so the aggregate is the whole ledger: the
			// asset is reported and nets to zero.
			Expect(aggregate(specContext, notPremium, &pit)).To(Equal(map[string]*big.Int{"USD/2": big.NewInt(0)}))
		})

		It("should select the account a conjunction narrows to on all three endpoints", func(specContext SpecContext) {
			Expect(listAccounts(specContext, notPremiumBank1, &pit)).To(ConsistOf("bank1"))
			Expect(listVolumes(specContext, notPremiumBank1, &pit)).To(ConsistOf("bank1"))
			Expect(aggregate(specContext, notPremiumBank1, &pit)).To(Equal(map[string]*big.Int{"USD/2": big.NewInt(100)}))
		})

		It("should report an absent key as absent rather than unknown", func(specContext SpecContext) {
			// $exists compiles to a null check instead of a containment test, so it is
			// false for an account with no metadata either way. It pins the contrast:
			// only the containment form can turn into NULL.
			existsFilter := map[string]any{
				"$not": map[string]any{"$exists": map[string]any{"metadata": "category"}},
			}

			Expect(listAccounts(specContext, existsFilter, &pit)).To(ConsistOf("bank1", "bank2", "world"))
			Expect(listVolumes(specContext, existsFilter, &pit)).To(ConsistOf("bank1", "bank2", "world"))
			Expect(aggregate(specContext, existsFilter, &pit)).To(Equal(map[string]*big.Int{"USD/2": big.NewInt(0)}))
		})
	})

	// Same filters without a pit, where the metadata column comes straight from the
	// accounts table and can never be NULL. This isolates the pit as the variable:
	// it has to pass whether or not the pit paths coalesce.
	When("the metadata is in place and no pit is given", func() {
		It("should exclude the tagged account on all three endpoints", func(specContext SpecContext) {
			Expect(listAccounts(specContext, notPremium, nil)).To(ConsistOf("bank2", "world"))
			Expect(listVolumes(specContext, notPremium, nil)).To(ConsistOf("bank2", "world"))
			Expect(aggregate(specContext, notPremium, nil)).To(Equal(map[string]*big.Int{"USD/2": big.NewInt(-100)}))
		})
	})
})
