//go:build e2e

package business

import (
	"errors"
	"io"
	"math/big"
	"slices"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Exact address search with current ephemeral account attributes", func() {
	for _, withMetadata := range []bool{false, true} {
		name, ledger := "without account metadata", "history-no-metadata"
		if withMetadata {
			name, ledger = "with account metadata", "history-with-metadata"
		}

		It("requires current account attributes "+name, func() {
			const address = "clearing:1"
			setupHistoricalAddressLedger(ledger)
			if withMetadata {
				_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
					actions.SaveAccountMetadataAction(ledger, address, map[string]string{"owner": "customer"})))
				Expect(err).To(Succeed())
			}

			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.WithReference(actions.CreateTransactionAction(ledger, []*commonpb.Posting{
					actions.NewPosting("world", address, big.NewInt(100), "USD"),
				}, map[string]string{"phase": "funding"}, nil), "funding")))
			Expect(err).To(Succeed())
			// A separate committed apply proves the live funding mapping exists
			// before the later drain purges the account's final volume cell.
			Expect(historicalAddressTransactionIDs(ledger, actions.AddressExactRoleFilter(address,
				commonpb.AddressRole_ADDRESS_ROLE_DESTINATION), false)).To(Equal([]uint64{1}))

			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.WithReference(actions.CreateTransactionAction(ledger, []*commonpb.Posting{
					actions.NewPosting(address, "bank", big.NewInt(100), "USD"),
				}, map[string]string{"phase": "draining"}, nil), "draining")))
			Expect(err).To(Succeed())

			account, err := actions.GetAccount(sharedCtx, sharedClient, ledger, address)
			Expect(err).To(Succeed())
			Expect(account.GetVolumes()).To(BeEmpty())
			if !withMetadata {
				Expect(account.GetMetadata()).To(BeEmpty())
				accounts, err := actions.ListAccountsFiltered(sharedCtx, sharedClient, ledger, 10, "",
					actions.AddressExactFilter(address))
				Expect(err).To(Succeed())
				Expect(accounts).To(BeEmpty(), "the no-metadata fixture must lack current account attributes")
			}

			if withMetadata {
				Expect(account.GetMetadata()).To(HaveKey("owner"))
				Expect(account.GetMetadata()["owner"].GetStringValue()).To(Equal("customer"))
				assertHistoricalAddressRoles(ledger, address)
			} else {
				assertAbsentHistoricalAddressRoles(ledger, address)
			}
			for _, reverse := range []bool{false, true} {
				Expect(historicalAddressTransactionIDs(ledger, actions.ReferenceFilter("draining"), reverse)).To(Equal([]uint64{2}))
				Expect(historicalAddressTransactionIDs(ledger, actions.StringMetadataFilter("phase", "draining"), reverse)).To(Equal([]uint64{2}))
				matching := historicalAddressTransactionIDs(ledger, actions.AndFilter(
					actions.AddressExactFilter(address), actions.StringMetadataFilter("phase", "draining")), reverse)
				if withMetadata {
					Expect(matching).To(Equal([]uint64{2}))
				} else {
					Expect(matching).To(BeEmpty())
				}
				union := historicalAddressTransactionIDs(ledger, actions.OrFilter(
					actions.AddressExactFilter(address), actions.ReferenceFilter("draining")), reverse)
				if withMetadata {
					want := []uint64{2, 1}
					if reverse {
						slices.Reverse(want)
					}
					Expect(union).To(Equal(want))
				} else {
					Expect(union).To(Equal([]uint64{2}))
				}
				if !withMetadata {
					Expect(historicalAddressTransactionIDs(ledger, actions.AddressPrefixFilter("clearing:"), reverse)).To(BeEmpty())
				}
			}
		})
	}

	It("retains historical address roles when only one asset/color cell is purged", func() {
		const ledger, address = "history-partial-purge", "clearing:partial"
		setupHistoricalAddressLedger(ledger)
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
			actions.CreateTransactionAction(ledger, []*commonpb.Posting{
				actions.NewColoredPosting("world", address, big.NewInt(100), "USD", "RED"),
				actions.NewColoredPosting("world", address, big.NewInt(50), "USD", "BLUE"),
				actions.NewPosting("world", address, big.NewInt(25), "EUR"),
			}, nil, nil)))
		Expect(err).To(Succeed())
		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
			actions.CreateTransactionAction(ledger, []*commonpb.Posting{
				actions.NewColoredPosting(address, "bank", big.NewInt(100), "USD", "RED"),
			}, nil, nil)))
		Expect(err).To(Succeed())
		account, err := actions.GetAccount(sharedCtx, sharedClient, ledger, address)
		Expect(err).To(Succeed())
		Expect(account.FindVolume("USD", "RED")).To(BeNil())
		Expect(account.FindVolume("USD", "BLUE").GetBalance().DecimalString()).To(Equal("50"))
		Expect(account.FindVolume("EUR", "").GetBalance().DecimalString()).To(Equal("25"))
		assertHistoricalAddressRoles(ledger, address)
	})
})

func setupHistoricalAddressLedger(ledger string) {
	_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
		actions.CreateLedgerAction(ledger, nil),
		actions.AddEphemeralAccountTypeAction(ledger, "clearing", "clearing:{id}"),
		actions.AddAccountTypeAction(ledger, "bank", "bank"),
		actions.CreateAddressIndexAction(ledger, commonpb.AddressRole_ADDRESS_ROLE_ANY),
		actions.CreateAddressIndexAction(ledger, commonpb.AddressRole_ADDRESS_ROLE_SOURCE),
		actions.CreateAddressIndexAction(ledger, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION),
		actions.SetMetadataFieldTypeAction(ledger, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "phase", commonpb.MetadataType_METADATA_TYPE_STRING),
		actions.CreateTransactionMetadataIndexAction(ledger, "phase"),
		actions.CreateBuiltinTxIndexAction(ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)))
	Expect(err).To(Succeed())
	// Wait for the live versions before writing, so the scenario cannot pass
	// merely because a later backfill reconstructs the mappings.
	for _, role := range []commonpb.AddressRole{
		commonpb.AddressRole_ADDRESS_ROLE_ANY,
		commonpb.AddressRole_ADDRESS_ROLE_SOURCE,
		commonpb.AddressRole_ADDRESS_ROLE_DESTINATION,
	} {
		Expect(actions.WaitForAddressIndexReady(sharedCtx, sharedClient, ledger, role)).To(Succeed())
	}
	Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledger, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "phase")).To(Succeed())
	Expect(actions.WaitForBuiltinIndexReady(sharedCtx, sharedClient, ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)).To(Succeed())
}

func assertAbsentHistoricalAddressRoles(ledger, address string) {
	for _, reverse := range []bool{false, true} {
		for _, role := range []commonpb.AddressRole{
			commonpb.AddressRole_ADDRESS_ROLE_ANY,
			commonpb.AddressRole_ADDRESS_ROLE_SOURCE,
			commonpb.AddressRole_ADDRESS_ROLE_DESTINATION,
		} {
			filter := actions.AddressExactRoleFilter(address, role)
			Expect(historicalAddressTransactionIDs(ledger, filter, reverse)).To(BeEmpty(),
				"ledger %s role %s reverse %t", ledger, role, reverse)
			Expect(historicalAddressTransactionIDs(ledger, actions.AndFilter(
				filter, actions.StringMetadataFilter("phase", "draining")), reverse)).To(BeEmpty())
			Expect(historicalAddressTransactionIDs(ledger, actions.OrFilter(
				filter, actions.ReferenceFilter("draining")), reverse)).To(Equal([]uint64{2}))
		}
	}
}

func assertHistoricalAddressRoles(ledger, address string) {
	for _, reverse := range []bool{false, true} {
		for _, role := range []struct {
			role commonpb.AddressRole
			want []uint64
		}{
			{commonpb.AddressRole_ADDRESS_ROLE_ANY, []uint64{2, 1}},
			{commonpb.AddressRole_ADDRESS_ROLE_SOURCE, []uint64{2}},
			{commonpb.AddressRole_ADDRESS_ROLE_DESTINATION, []uint64{1}},
		} {
			want := slices.Clone(role.want)
			if reverse {
				slices.Reverse(want)
			}
			Expect(historicalAddressTransactionIDs(ledger, actions.AddressExactRoleFilter(address, role.role), reverse)).To(Equal(want),
				"ledger %s role %s reverse %t", ledger, role.role, reverse)
		}
	}
}

// Read page-size-one results through the transport's actual cursor trailers.
func historicalAddressTransactionIDs(ledger string, filter *commonpb.QueryFilter, reverse bool) []uint64 {
	var ids []uint64
	var cursor string
	for range 10 {
		stream, err := sharedClient.ListTransactions(sharedCtx, &servicepb.ListTransactionsRequest{
			Ledger: ledger,
			Options: &commonpb.ListOptions{
				Filter: filter, PageSize: 1, Cursor: cursor, Reverse: reverse,
			},
		})
		Expect(err).To(Succeed())
		for {
			tx, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			Expect(err).To(Succeed())
			ids = append(ids, tx.GetId())
		}
		next := stream.Trailer().Get("x-next-cursor")
		if len(next) == 0 || next[0] == "" {
			return ids
		}
		Expect(next[0]).NotTo(Equal(cursor), "pagination must advance")
		cursor = next[0]
	}
	Fail("historical address pagination did not terminate")
	return nil
}
