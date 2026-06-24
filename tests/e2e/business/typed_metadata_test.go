//go:build e2e

package business

import (
	"github.com/formancehq/ledger/v3/pkg/actions"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("TypedMetadata", Ordered, func() {

	Context("Schema Declaration Lifecycle", Ordered, func() {
		const ledgerName = "typed-meta-lifecycle"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should declare a field type and verify via schema status", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SetMetadataFieldTypeAction(ledgerName,
				commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age",
				commonpb.MetadataType_METADATA_TYPE_INT64)))
			Expect(err).To(Succeed())

			resp, err := sharedClient.GetMetadataSchemaStatus(sharedCtx, &servicepb.GetMetadataSchemaStatusRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(resp.AccountFields).To(HaveKey("age"))
			Expect(resp.AccountFields["age"].DeclaredType).To(Equal(commonpb.MetadataType_METADATA_TYPE_INT64))
		})

		It("Should remove a field type and verify it is absent", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.RemoveMetadataFieldTypeAction(ledgerName,
				commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age")))
			Expect(err).To(Succeed())

			// Removal triggers a background conversion to STRING then deletes
			// the field on completion — wait for it to be fully removed.
			Eventually(func(g Gomega) {
				resp, err := sharedClient.GetMetadataSchemaStatus(sharedCtx, &servicepb.GetMetadataSchemaStatusRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.AccountFields).NotTo(HaveKey("age"))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("Initial Schema at Ledger Creation", Ordered, func() {
		const ledgerName = "typed-meta-initial-schema"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "verified",
					Type:       commonpb.MetadataType_METADATA_TYPE_BOOL,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
					Key:        "amount_cents",
					Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
				},
			})))
			Expect(err).To(Succeed())
		})

		It("Should have both fields present with status COMPLETE", func() {
			resp, err := sharedClient.GetMetadataSchemaStatus(sharedCtx, &servicepb.GetMetadataSchemaStatusRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())

			Expect(resp.AccountFields).To(HaveKey("verified"))
			Expect(resp.AccountFields["verified"].DeclaredType).To(Equal(commonpb.MetadataType_METADATA_TYPE_BOOL))

			Expect(resp.TransactionFields).To(HaveKey("amount_cents"))
			Expect(resp.TransactionFields["amount_cents"].DeclaredType).To(Equal(commonpb.MetadataType_METADATA_TYPE_INT64))
		})
	})

	Context("Schema Enforcement on Account Metadata", Ordered, func() {
		const ledgerName = "typed-meta-account-enforce"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "age",
					Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
				},
			})))
			Expect(err).To(Succeed())
		})

		It("Should convert string metadata to int64 on write", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, "user1", map[string]string{"age": "42"})))
			Expect(err).To(Succeed())

			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "user1",
			})
			Expect(err).To(Succeed())

			// declared_type is an index hint, not an API contract — the API
			// returns the raw string the client wrote, not a coerced int.
			v := actions.FindMetadataValue(account.Metadata, "age")
			Expect(v).NotTo(BeNil())
			strVal, ok := v.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value (raw client write), got %T", v.Type)
			Expect(strVal.StringValue).To(Equal("42"))

			// String projection still surfaces the same value.
			Expect(commonpb.MetadataToGoMap(account.Metadata)["age"]).To(Equal("42"))
		})
	})

	Context("Schema Enforcement on Transaction Metadata", Ordered, func() {
		const ledgerName = "typed-meta-tx-enforce"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
					Key:        "priority",
					Type:       commonpb.MetadataType_METADATA_TYPE_UINT64,
				},
			})))
			Expect(err).To(Succeed())
		})

		It("Should convert string metadata to uint64 on transaction creation", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "user1", big.NewInt(100), "USD"),
			}, map[string]string{"priority": "100"})))
			Expect(err).To(Succeed())

			txID := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			txResp, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: txID,
			})
			Expect(err).To(Succeed())

			v := actions.FindMetadataValue(txResp.Transaction.Metadata, "priority")
			Expect(v).NotTo(BeNil())
			_, ok := v.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value, got %T", v.Type)
		})
	})

	Context("Bool Conversion", Ordered, func() {
		const ledgerName = "typed-meta-bool-conv"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "active",
					Type:       commonpb.MetadataType_METADATA_TYPE_BOOL,
				},
			})))
			Expect(err).To(Succeed())
		})

		It("Should convert 'true' string to bool_value true", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, "acct1", map[string]string{"active": "true"})))
			Expect(err).To(Succeed())

			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "acct1",
			})
			Expect(err).To(Succeed())

			v := actions.FindMetadataValue(account.Metadata, "active")
			Expect(v).NotTo(BeNil())
			_, ok := v.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value, got %T", v.Type)
		})

		It("Should convert '0' string to bool_value false", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, "acct2", map[string]string{"active": "0"})))
			Expect(err).To(Succeed())

			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "acct2",
			})
			Expect(err).To(Succeed())

			v := actions.FindMetadataValue(account.Metadata, "active")
			Expect(v).NotTo(BeNil())
			_, ok := v.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value, got %T", v.Type)
		})
	})

	Context("Failed Conversion (NullValue)", Ordered, func() {
		const ledgerName = "typed-meta-null-conv"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "age",
					Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
				},
			})))
			Expect(err).To(Succeed())
		})

		It("Should produce null_value preserving the original string", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, "bad-data", map[string]string{"age": "not-a-number"})))
			Expect(err).To(Succeed())

			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "bad-data",
			})
			Expect(err).To(Succeed())

			// declared_type is an index hint, not an API contract — the API
			// returns the raw string the client wrote, even when it can't be
			// coerced to the declared type. The indexer surfaces uncoercible
			// values under a Null sentinel inside its forward index (so range
			// queries skip them cleanly), but that's a forward-index concern,
			// not an API-side transformation.
			v := actions.FindMetadataValue(account.Metadata, "age")
			Expect(v).NotTo(BeNil())
			strVal, ok := v.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value (raw client write), got %T", v.Type)
			Expect(strVal.StringValue).To(Equal("not-a-number"))

			// ToMap surfaces the same raw value.
			Expect(commonpb.MetadataToGoMap(account.Metadata)["age"]).To(Equal("not-a-number"))
		})
	})

	Context("Background Conversion of Existing Data", Ordered, func() {
		const ledgerName = "typed-meta-bg-convert"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should convert existing string data after schema declaration", func() {
			// Save metadata before any schema exists (stored as string)
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, "scored-user", map[string]string{"score": "99"})))
			Expect(err).To(Succeed())

			// Declare the type
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SetMetadataFieldTypeAction(ledgerName,
				commonpb.TargetType_TARGET_TYPE_ACCOUNT, "score",
				commonpb.MetadataType_METADATA_TYPE_INT64)))
			Expect(err).To(Succeed())

			// Wait for background conversion to complete
			Eventually(func(g Gomega) {
				resp, err := sharedClient.GetMetadataSchemaStatus(sharedCtx, &servicepb.GetMetadataSchemaStatusRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.AccountFields).To(HaveKey("score"))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// Verify the value was converted
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "scored-user",
			})
			Expect(err).To(Succeed())

			v := actions.FindMetadataValue(account.Metadata, "score")
			Expect(v).NotTo(BeNil())
			_, ok := v.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value after background conversion, got %T", v.Type)
		})
	})

	Context("Numscript set_account_meta with Schema", Ordered, func() {
		const ledgerName = "typed-meta-numscript"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "account_type",
					Type:       commonpb.MetadataType_METADATA_TYPE_BOOL,
				},
			})))
			Expect(err).To(Succeed())
		})

		It("Should enforce schema on Numscript set_account_meta", func() {
			script := `
send [USD/2 100] (
  source = @world
  destination = @user
)
set_account_meta(@user, "account_type", "true")
`
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateForceScriptTransactionAction(ledgerName, script, nil, nil)))
			Expect(err).To(Succeed())

			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "user",
			})
			Expect(err).To(Succeed())

			v := actions.FindMetadataValue(account.Metadata, "account_type")
			Expect(v).NotTo(BeNil())
			_, ok := v.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value, got %T", v.Type)
		})
	})

	Context("Mixed Typed and Untyped Metadata", Ordered, func() {
		const ledgerName = "typed-meta-mixed"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "age",
					Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
				},
			})))
			Expect(err).To(Succeed())
		})

		It("Should convert typed fields and keep untyped as strings", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{
				"age":  "25",
				"name": "Alice",
			})))
			Expect(err).To(Succeed())

			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "alice",
			})
			Expect(err).To(Succeed())

			// "age" should be converted to int_value
			ageVal := actions.FindMetadataValue(account.Metadata, "age")
			Expect(ageVal).NotTo(BeNil())
			_, ok := ageVal.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value for age, got %T", ageVal.Type)

			// "name" should remain as string_value
			nameVal := actions.FindMetadataValue(account.Metadata, "name")
			Expect(nameVal).NotTo(BeNil())
			strVal, ok := nameVal.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value for name, got %T", nameVal.Type)
			Expect(strVal.StringValue).To(Equal("Alice"))
		})
	})

	Context("Multiple Field Types", Ordered, func() {
		const ledgerName = "typed-meta-multi-types"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "count",
					Type:       commonpb.MetadataType_METADATA_TYPE_UINT64,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "enabled",
					Type:       commonpb.MetadataType_METADATA_TYPE_BOOL,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "label",
					Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
				},
			})))
			Expect(err).To(Succeed())
		})

		It("Should convert each field to the correct type", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, "multi", map[string]string{
				"count":   "42",
				"enabled": "true",
				"label":   "test",
			})))
			Expect(err).To(Succeed())

			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "multi",
			})
			Expect(err).To(Succeed())

			// count → uint_value
			countVal := actions.FindMetadataValue(account.Metadata, "count")
			Expect(countVal).NotTo(BeNil())
			_, ok := countVal.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value for count, got %T", countVal.Type)

			// enabled → bool_value
			enabledVal := actions.FindMetadataValue(account.Metadata, "enabled")
			Expect(enabledVal).NotTo(BeNil())
			_, ok = enabledVal.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value for enabled, got %T", enabledVal.Type)

			// label → string_value
			labelVal := actions.FindMetadataValue(account.Metadata, "label")
			Expect(labelVal).NotTo(BeNil())
			strVal, ok := labelVal.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value for label, got %T", labelVal.Type)
			Expect(strVal.StringValue).To(Equal("test"))
		})
	})

	Context("Small Integer Types Conversion", Ordered, func() {
		const ledgerName = "typed-meta-small-ints"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "field_int8",
					Type:       commonpb.MetadataType_METADATA_TYPE_INT8,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "field_int16",
					Type:       commonpb.MetadataType_METADATA_TYPE_INT16,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "field_int32",
					Type:       commonpb.MetadataType_METADATA_TYPE_INT32,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "field_uint8",
					Type:       commonpb.MetadataType_METADATA_TYPE_UINT8,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "field_uint16",
					Type:       commonpb.MetadataType_METADATA_TYPE_UINT16,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "field_uint32",
					Type:       commonpb.MetadataType_METADATA_TYPE_UINT32,
				},
			})))
			Expect(err).To(Succeed())
		})

		It("Should convert string values to correct proto types", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, "small-ints", map[string]string{
				"field_int8":   "-42",
				"field_int16":  "1000",
				"field_int32":  "100000",
				"field_uint8":  "200",
				"field_uint16": "50000",
				"field_uint32": "3000000000",
			})))
			Expect(err).To(Succeed())

			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "small-ints",
			})
			Expect(err).To(Succeed())

			// Signed types -> int_value
			int8Val := actions.FindMetadataValue(account.Metadata, "field_int8")
			Expect(int8Val).NotTo(BeNil())
			_, ok := int8Val.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value for int8, got %T", int8Val.Type)

			int16Val := actions.FindMetadataValue(account.Metadata, "field_int16")
			Expect(int16Val).NotTo(BeNil())
			_, ok = int16Val.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value for int16, got %T", int16Val.Type)

			int32Val := actions.FindMetadataValue(account.Metadata, "field_int32")
			Expect(int32Val).NotTo(BeNil())
			_, ok = int32Val.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value for int32, got %T", int32Val.Type)

			// Unsigned types -> uint_value
			uint8Val := actions.FindMetadataValue(account.Metadata, "field_uint8")
			Expect(uint8Val).NotTo(BeNil())
			_, ok = uint8Val.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value for uint8, got %T", uint8Val.Type)

			uint16Val := actions.FindMetadataValue(account.Metadata, "field_uint16")
			Expect(uint16Val).NotTo(BeNil())
			_, ok = uint16Val.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value for uint16, got %T", uint16Val.Type)

			uint32Val := actions.FindMetadataValue(account.Metadata, "field_uint32")
			Expect(uint32Val).NotTo(BeNil())
			_, ok = uint32Val.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value for uint32, got %T", uint32Val.Type)
		})

		It("Should return correct strings via ToMap()", func() {
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "small-ints",
			})
			Expect(err).To(Succeed())

			m := commonpb.MetadataToGoMap(account.Metadata)
			Expect(m["field_int8"]).To(Equal("-42"))
			Expect(m["field_int16"]).To(Equal("1000"))
			Expect(m["field_int32"]).To(Equal("100000"))
			Expect(m["field_uint8"]).To(Equal("200"))
			Expect(m["field_uint16"]).To(Equal("50000"))
			Expect(m["field_uint32"]).To(Equal("3000000000"))
		})
	})

	Context("Initial Schema with All Types", Ordered, func() {
		const ledgerName = "typed-meta-all-types"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "f_string",
					Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "f_bool",
					Type:       commonpb.MetadataType_METADATA_TYPE_BOOL,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "f_int8",
					Type:       commonpb.MetadataType_METADATA_TYPE_INT8,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "f_int16",
					Type:       commonpb.MetadataType_METADATA_TYPE_INT16,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "f_int32",
					Type:       commonpb.MetadataType_METADATA_TYPE_INT32,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "f_int64",
					Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "f_uint8",
					Type:       commonpb.MetadataType_METADATA_TYPE_UINT8,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "f_uint16",
					Type:       commonpb.MetadataType_METADATA_TYPE_UINT16,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "f_uint32",
					Type:       commonpb.MetadataType_METADATA_TYPE_UINT32,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "f_uint64",
					Type:       commonpb.MetadataType_METADATA_TYPE_UINT64,
				},
			})))
			Expect(err).To(Succeed())
		})

		It("Should have all 10 fields present with correct types and COMPLETE status", func() {
			resp, err := sharedClient.GetMetadataSchemaStatus(sharedCtx, &servicepb.GetMetadataSchemaStatusRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())

			expectedFields := map[string]commonpb.MetadataType{
				"f_string": commonpb.MetadataType_METADATA_TYPE_STRING,
				"f_bool":   commonpb.MetadataType_METADATA_TYPE_BOOL,
				"f_int8":   commonpb.MetadataType_METADATA_TYPE_INT8,
				"f_int16":  commonpb.MetadataType_METADATA_TYPE_INT16,
				"f_int32":  commonpb.MetadataType_METADATA_TYPE_INT32,
				"f_int64":  commonpb.MetadataType_METADATA_TYPE_INT64,
				"f_uint8":  commonpb.MetadataType_METADATA_TYPE_UINT8,
				"f_uint16": commonpb.MetadataType_METADATA_TYPE_UINT16,
				"f_uint32": commonpb.MetadataType_METADATA_TYPE_UINT32,
				"f_uint64": commonpb.MetadataType_METADATA_TYPE_UINT64,
			}

			Expect(resp.AccountFields).To(HaveLen(len(expectedFields)))
			for key, expectedType := range expectedFields {
				Expect(resp.AccountFields).To(HaveKey(key))
				Expect(resp.AccountFields[key].DeclaredType).To(Equal(expectedType),
					"field %s: expected type %v", key, expectedType)
			}
		})
	})

	Context("Schema Declaration Lifecycle for Ledger Metadata", Ordered, func() {
		const ledgerName = "typed-meta-ledger-lifecycle"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should declare a ledger field type and verify via schema status", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SetMetadataFieldTypeAction(ledgerName,
				commonpb.TargetType_TARGET_TYPE_LEDGER, "env",
				commonpb.MetadataType_METADATA_TYPE_STRING)))
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				resp, err := sharedClient.GetMetadataSchemaStatus(sharedCtx, &servicepb.GetMetadataSchemaStatusRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.LedgerFields).To(HaveKey("env"))
				g.Expect(resp.LedgerFields["env"].DeclaredType).To(Equal(commonpb.MetadataType_METADATA_TYPE_STRING))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should remove a ledger field type and verify it is absent", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.RemoveMetadataFieldTypeAction(ledgerName,
				commonpb.TargetType_TARGET_TYPE_LEDGER, "env")))
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				resp, err := sharedClient.GetMetadataSchemaStatus(sharedCtx, &servicepb.GetMetadataSchemaStatusRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.LedgerFields).NotTo(HaveKey("env"))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("Schema Enforcement on Ledger Metadata", Ordered, func() {
		const ledgerName = "typed-meta-ledger-enforce"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_LEDGER,
					Key:        "max_tx",
					Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
				},
			})))
			Expect(err).To(Succeed())
		})

		It("Should convert string ledger metadata to int64 on write", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveLedgerMetadataAction(ledgerName, map[string]string{"max_tx": "1000"})))
			Expect(err).To(Succeed())

			info, err := actions.GetLedger(sharedCtx, sharedClient, ledgerName)
			Expect(err).To(Succeed())

			v := actions.FindMetadataValue(info.Metadata, "max_tx")
			Expect(v).NotTo(BeNil())
			_, ok := v.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value, got %T", v.Type)
		})

		It("Should return raw string for inconvertible ledger metadata", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveLedgerMetadataAction(ledgerName, map[string]string{"max_tx": "not-a-number"})))
			Expect(err).To(Succeed())

			info, err := actions.GetLedger(sharedCtx, sharedClient, ledgerName)
			Expect(err).To(Succeed())

			// declared_type is an index hint, not an API contract — the API
			// returns the raw string. Inconvertible values get the Null
			// sentinel in the forward index only.
			v := actions.FindMetadataValue(info.Metadata, "max_tx")
			Expect(v).NotTo(BeNil())
			strVal, ok := v.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value (raw client write), got %T", v.Type)
			Expect(strVal.StringValue).To(Equal("not-a-number"))
		})

		It("Should keep untyped ledger metadata as strings", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveLedgerMetadataAction(ledgerName, map[string]string{"label": "production"})))
			Expect(err).To(Succeed())

			info, err := actions.GetLedger(sharedCtx, sharedClient, ledgerName)
			Expect(err).To(Succeed())

			v := actions.FindMetadataValue(info.Metadata, "label")
			Expect(v).NotTo(BeNil())
			strVal, ok := v.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value, got %T", v.Type)
			Expect(strVal.StringValue).To(Equal("production"))
		})
	})

	Context("Background Conversion of Existing Ledger Metadata", Ordered, func() {
		const ledgerName = "typed-meta-ledger-bg"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should convert existing ledger metadata after schema declaration", func() {
			// Save metadata before any schema exists (stored as string)
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveLedgerMetadataAction(ledgerName, map[string]string{"version": "42"})))
			Expect(err).To(Succeed())

			// Declare the type
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SetMetadataFieldTypeAction(ledgerName,
				commonpb.TargetType_TARGET_TYPE_LEDGER, "version",
				commonpb.MetadataType_METADATA_TYPE_INT64)))
			Expect(err).To(Succeed())

			// Wait for background conversion to complete
			Eventually(func(g Gomega) {
				resp, err := sharedClient.GetMetadataSchemaStatus(sharedCtx, &servicepb.GetMetadataSchemaStatusRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.LedgerFields).To(HaveKey("version"))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// Verify the value was converted
			info, err := actions.GetLedger(sharedCtx, sharedClient, ledgerName)
			Expect(err).To(Succeed())

			v := actions.FindMetadataValue(info.Metadata, "version")
			Expect(v).NotTo(BeNil())
			_, ok := v.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value after background conversion, got %T", v.Type)
		})
	})

	Context("Initial Schema with Ledger Fields", Ordered, func() {
		const ledgerName = "typed-meta-initial-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "role",
					Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_LEDGER,
					Key:        "env",
					Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
				},
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_LEDGER,
					Key:        "version",
					Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
				},
			})))
			Expect(err).To(Succeed())
		})

		It("Should have all fields present with status COMPLETE", func() {
			resp, err := sharedClient.GetMetadataSchemaStatus(sharedCtx, &servicepb.GetMetadataSchemaStatusRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())

			Expect(resp.AccountFields).To(HaveKey("role"))

			Expect(resp.LedgerFields).To(HaveKey("env"))
			Expect(resp.LedgerFields["env"].DeclaredType).To(Equal(commonpb.MetadataType_METADATA_TYPE_STRING))

			Expect(resp.LedgerFields).To(HaveKey("version"))
			Expect(resp.LedgerFields["version"].DeclaredType).To(Equal(commonpb.MetadataType_METADATA_TYPE_INT64))
		})
	})

	Context("Typed Values via gRPC (Direct Proto Values)", Ordered, func() {
		const ledgerName = "typed-meta-direct-proto"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "score",
					Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
				},
			})))
			Expect(err).To(Succeed())
		})

		It("Should accept and preserve a typed int_value sent directly", func() {
			typedMeta := map[string]*commonpb.MetadataValue{
				"score": commonpb.NewIntValue(42),
			}
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SaveTypedAccountMetadataAction(ledgerName, "proto-user", typedMeta)))
			Expect(err).To(Succeed())

			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "proto-user",
			})
			Expect(err).To(Succeed())

			// Client sent an int_value directly; the new contract returns
			// the raw wire type the client wrote, so we get int_value back.
			v := actions.FindMetadataValue(account.Metadata, "score")
			Expect(v).NotTo(BeNil())
			intVal, ok := v.Type.(*commonpb.MetadataValue_IntValue)
			Expect(ok).To(BeTrue(), "expected int_value (the wire type the client sent), got %T", v.Type)
			Expect(intVal.IntValue).To(Equal(int64(42)))
		})
	})
})
