//go:build e2e

package business

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// MetadataRetypeRoundTripPreservesOriginal pins the headline lossless
// round-trip invariant: stored metadata values are the literal bytes the
// client wrote. Retyping the field does not transform stored values. The
// declared_type is an index hint, not an API contract — reads return the
// raw client value verbatim.
var _ = Describe("MetadataRetypeRoundTripPreservesOriginal", Ordered, func() {
	const (
		ledgerName = "retype-round-trip"
		account    = "users:alice"
		key        = "score"
	)

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
			{TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, Key: key, Type: commonpb.MetadataType_METADATA_TYPE_STRING},
		})))
		Expect(err).To(Succeed())
	})

	It("preserves the original string across STRING → UINT64 → STRING retypes", func() {
		// Write a leading-zero string under declared=STRING.
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, account, map[string]string{key: "030"})))
		Expect(err).To(Succeed())

		// Read back: the raw STRING "030" is returned verbatim.
		acct, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
			Ledger:  ledgerName,
			Address: account,
		})
		Expect(err).To(Succeed())
		Expect(acct.GetMetadata()[key].GetStringValue()).To(Equal("030"))

		// Retype to UINT64 — O(1) on the apply path.
		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SetMetadataFieldTypeAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, key, commonpb.MetadataType_METADATA_TYPE_UINT64)))
		Expect(err).To(Succeed(), "retype must succeed immediately under the no-converter model")

		// Read under declared=UINT64: still returns the raw STRING value.
		// The declared type only affects index encoding, not API reads.
		acct, err = sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
			Ledger:  ledgerName,
			Address: account,
		})
		Expect(err).To(Succeed())
		Expect(acct.GetMetadata()[key].GetStringValue()).To(Equal("030"),
			"declared_type is an index hint, not an API contract — reads return the raw client value")

		// Retype back to STRING — also O(1).
		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SetMetadataFieldTypeAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, key, commonpb.MetadataType_METADATA_TYPE_STRING)))
		Expect(err).To(Succeed())

		// The original "030" is still there — never mutated.
		acct, err = sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
			Ledger:  ledgerName,
			Address: account,
		})
		Expect(err).To(Succeed())
		Expect(acct.GetMetadata()[key].GetStringValue()).To(Equal("030"),
			"leading-zero string must survive a STRING → UINT64 → STRING round-trip")
	})
})
