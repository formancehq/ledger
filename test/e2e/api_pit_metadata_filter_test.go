//go:build it

package test_suite

import (
	"math/big"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"

	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

// A metadata filter has to select the same accounts on every endpoint that filters
// accounts, and as of a pit it has to test the metadata as it stood at that point.
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
	)

	// A filter negating the tag selects every account as of a point before the tag
	// was written, and only the untagged ones after it.
	notPremium := map[string]any{
		"$not": map[string]any{
			"$match": map[string]any{"metadata[category]": "premium"},
		},
	}
	premium := map[string]any{
		"$match": map[string]any{"metadata[category]": "premium"},
	}

	// Two accounts funded well before any pit these specs use, so the accounts and
	// their volumes are visible throughout and only metadata is in question.
	fund := func(specContext SpecContext) {
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
					Timestamp: pointer.For(time.Now().UTC().Add(-2 * time.Hour)),
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
	}

	tag := func(specContext SpecContext, address string, metadata map[string]string) {
		_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(ctx, operations.V2AddMetadataToAccountRequest{
			Ledger:      "default",
			Address:     address,
			RequestBody: metadata,
		})
		Expect(err).To(BeNil())
	}

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

	// Every account is tagged before the pit, so none of them lacks a metadata row
	// and the only question is whether a revision written afterwards is in scope.
	When("a metadata revision is written after the pit", func() {
		var pit time.Time

		BeforeEach(func(specContext SpecContext) {
			fund(specContext)

			for _, address := range []string{"world", "bank1", "bank2"} {
				tag(specContext, address, map[string]string{"tier": "basic"})
			}

			pit = time.Now().UTC()
			// Wide enough that the revision below is unambiguously after the pit.
			time.Sleep(time.Second)

			tag(specContext, "bank1", map[string]string{"category": "premium"})
		})

		It("should not see the later revision on any endpoint", func(specContext SpecContext) {
			Expect(listAccounts(specContext, notPremium, &pit)).To(ConsistOf("bank1", "bank2", "world"))
			Expect(listVolumes(specContext, notPremium, &pit)).To(ConsistOf("bank1", "bank2", "world"))
			// Every account is selected, so the aggregate is the whole ledger and
			// nets to zero.
			Expect(aggregate(specContext, notPremium, &pit)).To(Equal(map[string]*big.Int{"USD/2": big.NewInt(0)}))

			Expect(listAccounts(specContext, premium, &pit)).To(BeEmpty())
			Expect(listVolumes(specContext, premium, &pit)).To(BeEmpty())
			Expect(aggregate(specContext, premium, &pit)).To(BeEmpty())
		})

		It("should see it with no pit", func(specContext SpecContext) {
			// The control: without a pit the revision is in scope everywhere, which
			// holds whether or not a pit is resolved correctly.
			Expect(listAccounts(specContext, notPremium, nil)).To(ConsistOf("bank2", "world"))
			Expect(listVolumes(specContext, notPremium, nil)).To(ConsistOf("bank2", "world"))
			Expect(aggregate(specContext, notPremium, nil)).To(Equal(map[string]*big.Int{"USD/2": big.NewInt(-100)}))
		})
	})

	// The pit falls between the transactions' effective date and the moment they
	// were written, so the accounts are visible while no metadata revision is —
	// not even the empty one an account starts out with, which is dated when the
	// account is created. The filter has to read metadata it cannot find as
	// metadata the account does not carry, rather than as metadata it might.
	When("no metadata revision exists as of the pit", func() {
		pit := time.Now().UTC().Add(-time.Hour)

		BeforeEach(func(specContext SpecContext) {
			fund(specContext)
			tag(specContext, "bank1", map[string]string{"category": "premium"})
		})

		It("should select every account on all three endpoints", func(specContext SpecContext) {
			Expect(listAccounts(specContext, notPremium, &pit)).To(ConsistOf("bank1", "bank2", "world"))
			Expect(listVolumes(specContext, notPremium, &pit)).To(ConsistOf("bank1", "bank2", "world"))
			Expect(aggregate(specContext, notPremium, &pit)).To(Equal(map[string]*big.Int{"USD/2": big.NewInt(0)}))
		})

		It("should select the account a conjunction narrows to on all three endpoints", func(specContext SpecContext) {
			// The same predicate reached through a conjunction, which is where an
			// unknown also propagates, narrowed so the aggregate is a non-zero number
			// rather than a conservation zero.
			narrowed := map[string]any{
				"$and": []any{
					notPremium,
					map[string]any{"$match": map[string]any{"address": "bank1"}},
				},
			}

			Expect(listAccounts(specContext, narrowed, &pit)).To(ConsistOf("bank1"))
			Expect(listVolumes(specContext, narrowed, &pit)).To(ConsistOf("bank1"))
			Expect(aggregate(specContext, narrowed, &pit)).To(Equal(map[string]*big.Int{"USD/2": big.NewInt(100)}))
		})

		It("should report an absent key as absent rather than unknown", func(specContext SpecContext) {
			// An existence check is a null check, so it is false for an account with
			// no metadata either way. It pins the contrast: only a containment test
			// can turn into an unknown.
			existsFilter := map[string]any{
				"$not": map[string]any{"$exists": map[string]any{"metadata": "category"}},
			}

			Expect(listAccounts(specContext, existsFilter, &pit)).To(ConsistOf("bank1", "bank2", "world"))
			Expect(listVolumes(specContext, existsFilter, &pit)).To(ConsistOf("bank1", "bank2", "world"))
			Expect(aggregate(specContext, existsFilter, &pit)).To(Equal(map[string]*big.Int{"USD/2": big.NewInt(0)}))
		})
	})
})
