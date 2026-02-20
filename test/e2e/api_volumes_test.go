//go:build it

package test_suite

import (
	"math/big"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/go-libs/v4/logging"
	"github.com/formancehq/go-libs/v4/pointer"
	. "github.com/formancehq/go-libs/v4/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v4/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v4/testing/testservice"

	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

type Transaction struct {
	Amount        int64
	Asset         string
	Source        string
	Destination   string
	EffectiveDate time.Time
}

var now = time.Now()

var _ = Context("Ledger accounts list API tests", func() {
	var (
		db  = UseTemplatedDatabase()
		ctx = logging.TestingContext()
	)

	testServer := DeferTestServer(
		DeferMap(db, (*pgtesting.Database).ConnectionOptions),
		testservice.WithInstruments(
			testservice.DebugInstrumentation(debug),
			testservice.OutputInstrumentation(GinkgoWriter),
		),
		testservice.WithLogger(GinkgoT()),
	)

	transactions := []Transaction{
		{Amount: 100, Asset: "USD", Source: "world", Destination: "account:user1", EffectiveDate: now.Add(-4 * time.Hour)},        //user1:100, world:-100
		{Amount: 125, Asset: "USD", Source: "world", Destination: "account:user2", EffectiveDate: now.Add(-3 * time.Hour)},        //user1:100, user2:125, world:-225
		{Amount: 75, Asset: "USD", Source: "account:user1", Destination: "account:user2", EffectiveDate: now.Add(-2 * time.Hour)}, //user1:25, user2:200, world:-200
		{Amount: 175, Asset: "USD", Source: "world", Destination: "account:user1", EffectiveDate: now.Add(-1 * time.Hour)},        //user1:200, user2:200, world:-400
		{Amount: 50, Asset: "USD", Source: "account:user2", Destination: "bank", EffectiveDate: now},                              //user1:200, user2:150, world:-400, bank:50
		{Amount: 100, Asset: "USD", Source: "account:user2", Destination: "account:user1", EffectiveDate: now.Add(1 * time.Hour)}, //user1:300, user2:50, world:-400, bank:50
		{Amount: 150, Asset: "USD", Source: "account:user1", Destination: "bank", EffectiveDate: now.Add(2 * time.Hour)},          //user1:150, user2:50, world:-400, bank:200
	}

	BeforeEach(func(specContext SpecContext) {
		_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())

		for _, transaction := range transactions {
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(
				ctx,
				operations.V2CreateTransactionRequest{
					V2PostTransaction: components.V2PostTransaction{
						Metadata: map[string]string{},
						Postings: []components.V2Posting{
							{
								Amount:      big.NewInt(transaction.Amount),
								Asset:       transaction.Asset,
								Source:      transaction.Source,
								Destination: transaction.Destination,
							},
						},
						Timestamp: &transaction.EffectiveDate,
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
		}
	})

	When("Get current volumes and balances from origin of time till now (insertion-date)", func() {
		It("should be ok", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetVolumesWithBalances(
				ctx,
				operations.V2GetVolumesWithBalancesRequest{
					InsertionDate: pointer.For(true),
					Ledger:        "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(len(response.V2VolumesWithBalanceCursorResponse.Cursor.Data)).To(Equal(4))
			for _, volume := range response.V2VolumesWithBalanceCursorResponse.Cursor.Data {
				if volume.Account == "account:user1" {
					Expect(volume.Balance).To(Equal(big.NewInt(150)))
				}
				if volume.Account == "account:user2" {
					Expect(volume.Balance).To(Equal(big.NewInt(50)))
				}
				if volume.Account == "bank" {
					Expect(volume.Balance).To(Equal(big.NewInt(200)))
				}
				if volume.Account == "world" {
					Expect(volume.Balance).To(Equal(big.NewInt(-400)))
				}
			}
		})
	})

	When("Get volumes and balances from oot til oot+2 hours (effectiveDate) ", func() {
		It("should be ok", func(specContext SpecContext) {

			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetVolumesWithBalances(
				ctx,
				operations.V2GetVolumesWithBalancesRequest{
					StartTime: pointer.For(now.Add(-4 * time.Hour)),
					EndTime:   pointer.For(now.Add(-2 * time.Hour)),
					Ledger:    "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(len(response.V2VolumesWithBalanceCursorResponse.Cursor.Data)).To(Equal(3))
			for _, volume := range response.V2VolumesWithBalanceCursorResponse.Cursor.Data {
				if volume.Account == "account:user1" {
					Expect(volume.Balance).To(Equal(big.NewInt(25)))
				}
				if volume.Account == "account:user2" {
					Expect(volume.Balance).To(Equal(big.NewInt(200)))
				}
				if volume.Account == "world" {
					Expect(volume.Balance).To(Equal(big.NewInt(-225)))
				}
			}
		})
	})

	When("Get volumes and balances filter by address account", func() {
		It("should be ok", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetVolumesWithBalances(
				ctx,
				operations.V2GetVolumesWithBalancesRequest{
					InsertionDate: pointer.For(true),
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"account": "account:",
						},
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.V2VolumesWithBalanceCursorResponse.Cursor.Data).To(HaveLen(2))
			for _, volume := range response.V2VolumesWithBalanceCursorResponse.Cursor.Data {
				if volume.Account == "account:user1" {
					Expect(volume.Balance).To(Equal(big.NewInt(150)))
				}
				if volume.Account == "account:user2" {
					Expect(volume.Balance).To(Equal(big.NewInt(50)))
				}
			}
		})
	})

	When("Get volumes and balances filter by address account a,d and end-time now effective", func() {
		It("should be ok", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetVolumesWithBalances(
				ctx,
				operations.V2GetVolumesWithBalancesRequest{
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"account": "account:",
						},
					},
					EndTime: pointer.For(now),
					Ledger:  "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.V2VolumesWithBalanceCursorResponse.Cursor.Data).To(HaveLen(2))

			for _, volume := range response.V2VolumesWithBalanceCursorResponse.Cursor.Data {
				if volume.Account == "account:user1" {
					Expect(volume.Balance).To(Equal(big.NewInt(200)))
				}
				if volume.Account == "account:user2" {
					Expect(volume.Balance).To(Equal(big.NewInt(150)))
				}
			}
		})
	})

	When("Get volumes and balances filter by address account which doesn't exist", func() {
		It("should be ok", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetVolumesWithBalances(
				ctx,
				operations.V2GetVolumesWithBalancesRequest{
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"account": "foo:",
						},
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.V2VolumesWithBalanceCursorResponse.Cursor.Data).To(HaveLen(0))
		})
	})
	When("Get volumes and balances filter by $in operator on account", func() {
		It("should be ok", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetVolumesWithBalances(
				ctx,
				operations.V2GetVolumesWithBalancesRequest{
					InsertionDate: pointer.For(true),
					RequestBody: map[string]interface{}{
						"$in": map[string]any{
							"account": []any{"account:user1", "account:user2"},
						},
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.V2VolumesWithBalanceCursorResponse.Cursor.Data).To(HaveLen(2))
			for _, volume := range response.V2VolumesWithBalanceCursorResponse.Cursor.Data {
				if volume.Account == "account:user1" {
					Expect(volume.Balance).To(Equal(big.NewInt(150)))
				}
				if volume.Account == "account:user2" {
					Expect(volume.Balance).To(Equal(big.NewInt(50)))
				}
			}
		})
		It("should return empty when filtering with non-existing accounts", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetVolumesWithBalances(
				ctx,
				operations.V2GetVolumesWithBalancesRequest{
					InsertionDate: pointer.For(true),
					RequestBody: map[string]interface{}{
						"$in": map[string]any{
							"account": []any{"not_existing", "also_not_existing"},
						},
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.V2VolumesWithBalanceCursorResponse.Cursor.Data).To(HaveLen(0))
		})
	})

	When("Get volumes and balances filter With futures dates empty", func() {
		It("should be ok", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetVolumesWithBalances(
				ctx,
				operations.V2GetVolumesWithBalancesRequest{
					StartTime: pointer.For(time.Now().Add(8 * time.Hour)),
					EndTime:   pointer.For(time.Now().Add(12 * time.Hour)),
					Ledger:    "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(response.V2VolumesWithBalanceCursorResponse.Cursor.Data)).To(Equal(0))
		})
	})

	When("Get volumes and balances filter by address account aggregation by level 1", func() {
		It("should be ok", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetVolumesWithBalances(
				ctx,
				operations.V2GetVolumesWithBalancesRequest{
					InsertionDate: pointer.For(true),
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"account": "account:",
						},
					},
					GroupBy: pointer.For(int64(1)),
					Ledger:  "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(response.V2VolumesWithBalanceCursorResponse.Cursor.Data)).To(Equal(1))
			for _, volume := range response.V2VolumesWithBalanceCursorResponse.Cursor.Data {
				if volume.Account == "account" {
					Expect(volume.Balance).To(Equal(big.NewInt(200)))
				}
			}
		})
	})

	// Test case to reproduce bug: column "account_array" does not exist (SQLSTATE 42703)
	// This happens when using $or with both exact addresses AND partial addresses with PIT
	When("Get volumes with PIT and $or filter mixing exact and partial addresses", func() {
		It("should not fail with account_array column error", func(specContext SpecContext) {
			// This test reproduces the bug where using $or with:
			// - exact addresses (e.g., "account:user1")
			// - partial addresses (e.g., "account:")
			// combined with PIT causes "column account_array does not exist" error
			//
			// The bug occurs because validateFilters only stores ONE value per field name,
			// so if the last address in the $or is exact, needAddressSegments becomes false,
			// but ResolveFilter still generates filters on account_array for partial addresses.
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetVolumesWithBalances(
				ctx,
				operations.V2GetVolumesWithBalancesRequest{
					EndTime: pointer.For(now),
					RequestBody: map[string]interface{}{
						"$or": []any{
							map[string]any{
								"$match": map[string]any{
									"account": "account:", // partial address - requires account_array
								},
							},
							map[string]any{
								"$match": map[string]any{
									"account": "bank", // exact address - does NOT require account_array
								},
							},
						},
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.V2VolumesWithBalanceCursorResponse.Cursor.Data).To(HaveLen(3)) // account:user1, account:user2, bank
		})
	})
})
