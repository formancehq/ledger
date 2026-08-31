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

	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

// Deleting a metadata key must not touch history: as of an instant at which the
// key was set and not yet deleted, reads still see it. Each revision of an
// account's metadata is dated when it is written, and a pit read resolves to the
// newest revision at or before the pit — so the delete has to carry its own
// date. A delete dated at the previous write's instant instead erases the key
// from every pit at or after the set, as if it had never existed.
var _ = Context("Ledger account metadata deleted after a pit", func() {
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

		// The set's own instant, read back from its log entry. A pit placed exactly
		// there is the earliest instant at which the key is visible, with no clock
		// assumptions in the test.
		pit time.Time
	)

	BeforeEach(func(specContext SpecContext) {
		client := Wait(specContext, DeferClient(testServer))

		_, err := client.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())

		_, err = client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
			Ledger: "default",
			V2PostTransaction: components.V2PostTransaction{
				Postings: []components.V2Posting{{
					Amount:      big.NewInt(100),
					Asset:       "USD/2",
					Source:      "world",
					Destination: "bank1",
				}},
			},
		})
		Expect(err).To(BeNil())

		_, err = client.Ledger.V2.AddMetadataToAccount(ctx, operations.V2AddMetadataToAccountRequest{
			Ledger:      "default",
			Address:     "bank1",
			RequestBody: map[string]string{"category": "premium"},
		})
		Expect(err).To(BeNil())

		logs, err := client.Ledger.V2.ListLogs(ctx, operations.V2ListLogsRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())
		Expect(logs.V2LogsCursorResponse.Cursor.Data).ToNot(BeEmpty())
		// Newest first: the head is the metadata write just made.
		pit = logs.V2LogsCursorResponse.Cursor.Data[0].Date

		_, err = client.Ledger.V2.DeleteAccountMetadata(ctx, operations.V2DeleteAccountMetadataRequest{
			Ledger:  "default",
			Address: "bank1",
			Key:     "category",
		})
		Expect(err).To(BeNil())
	})

	It("should still read the key as of the pit", func(specContext SpecContext) {
		response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListAccounts(ctx, operations.V2ListAccountsRequest{
			Ledger: "default",
			Pit:    &pit,
		})
		Expect(err).To(BeNil())

		metadata := map[string]map[string]string{}
		for _, account := range response.V2AccountsCursorResponse.Cursor.Data {
			metadata[account.Address] = account.Metadata
		}
		Expect(metadata["bank1"]).To(Equal(map[string]string{"category": "premium"}))
	})

	It("should leave the account untouched when deleting an absent key", func(specContext SpecContext) {
		client := Wait(specContext, DeferClient(testServer))

		before, err := client.Ledger.V2.GetAccount(ctx, operations.V2GetAccountRequest{
			Ledger:  "default",
			Address: "bank1",
		})
		Expect(err).To(BeNil())

		_, err = client.Ledger.V2.DeleteAccountMetadata(ctx, operations.V2DeleteAccountMetadataRequest{
			Ledger:  "default",
			Address: "bank1",
			Key:     "never-set",
		})
		Expect(err).To(BeNil())

		after, err := client.Ledger.V2.GetAccount(ctx, operations.V2GetAccountRequest{
			Ledger:  "default",
			Address: "bank1",
		})
		Expect(err).To(BeNil())
		Expect(after.V2AccountResponse.Data.UpdatedAt).To(Equal(before.V2AccountResponse.Data.UpdatedAt))
	})

	It("should preserve both instants through an export and import", func(specContext SpecContext) {
		// The replay path dates the revisions from the imported logs' own dates
		// (importLog passes log.Date), where the live path lets the store stamp
		// them — so a replica must answer the same pit reads as the original.
		client := Wait(specContext, DeferClient(testServer))

		export, err := client.Ledger.V2.ExportLogs(ctx, operations.V2ExportLogsRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())

		_, err = client.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: "replica",
		})
		Expect(err).To(BeNil())

		_, err = client.Ledger.V2.ImportLogs(ctx, operations.V2ImportLogsRequest{
			Ledger:              "replica",
			V2ImportLogsRequest: export.Bytes,
		})
		Expect(err).To(BeNil())

		atPit, err := client.Ledger.V2.ListAccounts(ctx, operations.V2ListAccountsRequest{
			Ledger: "replica",
			Pit:    &pit,
		})
		Expect(err).To(BeNil())
		metadata := map[string]map[string]string{}
		for _, account := range atPit.V2AccountsCursorResponse.Cursor.Data {
			metadata[account.Address] = account.Metadata
		}
		Expect(metadata["bank1"]).To(Equal(map[string]string{"category": "premium"}))

		now, err := client.Ledger.V2.GetAccount(ctx, operations.V2GetAccountRequest{
			Ledger:  "replica",
			Address: "bank1",
		})
		Expect(err).To(BeNil())
		Expect(now.V2AccountResponse.Data.Metadata).To(BeEmpty())

		// The discriminating instant: as of the delete's own original date the key
		// is already gone. A replay dating the delete at import time instead of the
		// log's date would still show it here, since the import happened later.
		logs, err := client.Ledger.V2.ListLogs(ctx, operations.V2ListLogsRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())
		deleteDate := logs.V2LogsCursorResponse.Cursor.Data[0].Date

		atDelete, err := client.Ledger.V2.ListAccounts(ctx, operations.V2ListAccountsRequest{
			Ledger: "replica",
			Pit:    &deleteDate,
		})
		Expect(err).To(BeNil())
		for _, account := range atDelete.V2AccountsCursorResponse.Cursor.Data {
			if account.Address == "bank1" {
				Expect(account.Metadata).To(BeEmpty())
			}
		}
	})

	It("should still select the key's account as of the pit, and none now", func(specContext SpecContext) {
		client := Wait(specContext, DeferClient(testServer))
		hasCategory := map[string]any{
			"$exists": map[string]any{"metadata": "category"},
		}

		response, err := client.Ledger.V2.ListAccounts(ctx, operations.V2ListAccountsRequest{
			Ledger:      "default",
			Pit:         &pit,
			RequestBody: hasCategory,
		})
		Expect(err).To(BeNil())
		addresses := make([]string, 0, len(response.V2AccountsCursorResponse.Cursor.Data))
		for _, account := range response.V2AccountsCursorResponse.Cursor.Data {
			addresses = append(addresses, account.Address)
		}
		Expect(addresses).To(ConsistOf("bank1"))

		now, err := client.Ledger.V2.ListAccounts(ctx, operations.V2ListAccountsRequest{
			Ledger:      "default",
			RequestBody: hasCategory,
		})
		Expect(err).To(BeNil())
		Expect(now.V2AccountsCursorResponse.Cursor.Data).To(BeEmpty())
	})
})
