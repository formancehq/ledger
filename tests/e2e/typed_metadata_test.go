//go:build e2e

package e2e

import (
	"context"
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("TypedMetadata", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const (
		httpPort = testSingleHTTPPort
		grpcPort = testSingleGRPCPort
	)

	BeforeAll(func() {
		ctx, client, _ = setupSingleNode(httpPort, grpcPort)
	})

	Context("Schema Declaration Lifecycle", Ordered, func() {
		const ledgerName = "typed-meta-lifecycle"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should declare a field type and verify via schema status", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					setMetadataFieldTypeAction(ledgerName,
						commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age",
						commonpb.MetadataType_METADATA_TYPE_INT64),
				},
			})
			Expect(err).To(Succeed())

			resp, err := client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(resp.AccountFields).To(HaveKey("age"))
			Expect(resp.AccountFields["age"].DeclaredType).To(Equal(commonpb.MetadataType_METADATA_TYPE_INT64))
		})

		It("Should remove a field type and verify it is absent", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					removeMetadataFieldTypeAction(ledgerName,
						commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age"),
				},
			})
			Expect(err).To(Succeed())

			// Removal triggers a background conversion to STRING then deletes
			// the field on completion — wait for it to be fully removed.
			Eventually(func(g Gomega) {
				resp, err := client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
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
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should have both fields present with status COMPLETE", func() {
			resp, err := client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())

			Expect(resp.AccountFields).To(HaveKey("verified"))
			Expect(resp.AccountFields["verified"].DeclaredType).To(Equal(commonpb.MetadataType_METADATA_TYPE_BOOL))
			Expect(resp.AccountFields["verified"].Status).To(Equal(
				commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE))

			Expect(resp.TransactionFields).To(HaveKey("amount_cents"))
			Expect(resp.TransactionFields["amount_cents"].DeclaredType).To(Equal(commonpb.MetadataType_METADATA_TYPE_INT64))
			Expect(resp.TransactionFields["amount_cents"].Status).To(Equal(
				commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE))
		})
	})

	Context("Schema Enforcement on Account Metadata", Ordered, func() {
		const ledgerName = "typed-meta-account-enforce"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "age",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should convert string metadata to int64 on write", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "user1", map[string]string{"age": "42"}),
				},
			})
			Expect(err).To(Succeed())

			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "user1",
			})
			Expect(err).To(Succeed())

			// Raw proto value should be int_value
			v := findMetadataValue(account.Metadata, "age")
			Expect(v).NotTo(BeNil())
			intVal, ok := v.Type.(*commonpb.MetadataValue_IntValue)
			Expect(ok).To(BeTrue(), "expected int_value, got %T", v.Type)
			Expect(intVal.IntValue).To(Equal(int64(42)))

			// ToMap should still return string representation
			Expect(account.Metadata.ToMap()["age"]).To(Equal("42"))
		})
	})

	Context("Schema Enforcement on Transaction Metadata", Ordered, func() {
		const ledgerName = "typed-meta-tx-enforce"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
							Key:        "priority",
							Type:       commonpb.MetadataType_METADATA_TYPE_UINT64,
						},
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should convert string metadata to uint64 on transaction creation", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "user1", big.NewInt(100), "USD"),
					}, map[string]string{"priority": "100"}),
				},
			})
			Expect(err).To(Succeed())

			txID := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			txResp, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        ledgerName,
				TransactionId: txID,
			})
			Expect(err).To(Succeed())

			v := findMetadataValue(txResp.Transaction.Metadata, "priority")
			Expect(v).NotTo(BeNil())
			uintVal, ok := v.Type.(*commonpb.MetadataValue_UintValue)
			Expect(ok).To(BeTrue(), "expected uint_value, got %T", v.Type)
			Expect(uintVal.UintValue).To(Equal(uint64(100)))
		})
	})

	Context("Bool Conversion", Ordered, func() {
		const ledgerName = "typed-meta-bool-conv"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "active",
							Type:       commonpb.MetadataType_METADATA_TYPE_BOOL,
						},
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should convert 'true' string to bool_value true", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "acct1", map[string]string{"active": "true"}),
				},
			})
			Expect(err).To(Succeed())

			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "acct1",
			})
			Expect(err).To(Succeed())

			v := findMetadataValue(account.Metadata, "active")
			Expect(v).NotTo(BeNil())
			boolVal, ok := v.Type.(*commonpb.MetadataValue_BoolValue)
			Expect(ok).To(BeTrue(), "expected bool_value, got %T", v.Type)
			Expect(boolVal.BoolValue).To(BeTrue())
		})

		It("Should convert '0' string to bool_value false", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "acct2", map[string]string{"active": "0"}),
				},
			})
			Expect(err).To(Succeed())

			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "acct2",
			})
			Expect(err).To(Succeed())

			v := findMetadataValue(account.Metadata, "active")
			Expect(v).NotTo(BeNil())
			boolVal, ok := v.Type.(*commonpb.MetadataValue_BoolValue)
			Expect(ok).To(BeTrue(), "expected bool_value, got %T", v.Type)
			Expect(boolVal.BoolValue).To(BeFalse())
		})
	})

	Context("Failed Conversion (NullValue)", Ordered, func() {
		const ledgerName = "typed-meta-null-conv"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "age",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should produce null_value preserving the original string", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "bad-data", map[string]string{"age": "not-a-number"}),
				},
			})
			Expect(err).To(Succeed())

			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "bad-data",
			})
			Expect(err).To(Succeed())

			v := findMetadataValue(account.Metadata, "age")
			Expect(v).NotTo(BeNil())
			nullVal, ok := v.Type.(*commonpb.MetadataValue_NullValue)
			Expect(ok).To(BeTrue(), "expected null_value, got %T", v.Type)
			Expect(nullVal.NullValue.Original).To(Equal("not-a-number"))

			// ToMap should preserve the original string
			Expect(account.Metadata.ToMap()["age"]).To(Equal("not-a-number"))
		})
	})

	Context("Background Conversion of Existing Data", Ordered, func() {
		const ledgerName = "typed-meta-bg-convert"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should convert existing string data after schema declaration", func() {
			// Save metadata before any schema exists (stored as string)
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "scored-user", map[string]string{"score": "99"}),
				},
			})
			Expect(err).To(Succeed())

			// Declare the type
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					setMetadataFieldTypeAction(ledgerName,
						commonpb.TargetType_TARGET_TYPE_ACCOUNT, "score",
						commonpb.MetadataType_METADATA_TYPE_INT64),
				},
			})
			Expect(err).To(Succeed())

			// Wait for background conversion to complete
			Eventually(func(g Gomega) {
				resp, err := client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.AccountFields).To(HaveKey("score"))
				g.Expect(resp.AccountFields["score"].Status).To(Equal(
					commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// Verify the value was converted
			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "scored-user",
			})
			Expect(err).To(Succeed())

			v := findMetadataValue(account.Metadata, "score")
			Expect(v).NotTo(BeNil())
			intVal, ok := v.Type.(*commonpb.MetadataValue_IntValue)
			Expect(ok).To(BeTrue(), "expected int_value after background conversion, got %T", v.Type)
			Expect(intVal.IntValue).To(Equal(int64(99)))
		})
	})

	Context("Numscript set_account_meta with Schema", Ordered, func() {
		const ledgerName = "typed-meta-numscript"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "account_type",
							Type:       commonpb.MetadataType_METADATA_TYPE_BOOL,
						},
					}),
				},
			})
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceScriptTransactionAction(ledgerName, script, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "user",
			})
			Expect(err).To(Succeed())

			v := findMetadataValue(account.Metadata, "account_type")
			Expect(v).NotTo(BeNil())
			boolVal, ok := v.Type.(*commonpb.MetadataValue_BoolValue)
			Expect(ok).To(BeTrue(), "expected bool_value, got %T", v.Type)
			Expect(boolVal.BoolValue).To(BeTrue())
		})
	})

	Context("Mixed Typed and Untyped Metadata", Ordered, func() {
		const ledgerName = "typed-meta-mixed"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "age",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should convert typed fields and keep untyped as strings", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "alice", map[string]string{
						"age":  "25",
						"name": "Alice",
					}),
				},
			})
			Expect(err).To(Succeed())

			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "alice",
			})
			Expect(err).To(Succeed())

			// "age" should be converted to int_value
			ageVal := findMetadataValue(account.Metadata, "age")
			Expect(ageVal).NotTo(BeNil())
			intVal, ok := ageVal.Type.(*commonpb.MetadataValue_IntValue)
			Expect(ok).To(BeTrue(), "expected int_value for age, got %T", ageVal.Type)
			Expect(intVal.IntValue).To(Equal(int64(25)))

			// "name" should remain as string_value
			nameVal := findMetadataValue(account.Metadata, "name")
			Expect(nameVal).NotTo(BeNil())
			strVal, ok := nameVal.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value for name, got %T", nameVal.Type)
			Expect(strVal.StringValue).To(Equal("Alice"))
		})
	})

	Context("Multiple Field Types", Ordered, func() {
		const ledgerName = "typed-meta-multi-types"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
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
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should convert each field to the correct type", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "multi", map[string]string{
						"count":   "42",
						"enabled": "true",
						"label":   "test",
					}),
				},
			})
			Expect(err).To(Succeed())

			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "multi",
			})
			Expect(err).To(Succeed())

			// count → uint_value
			countVal := findMetadataValue(account.Metadata, "count")
			Expect(countVal).NotTo(BeNil())
			uintVal, ok := countVal.Type.(*commonpb.MetadataValue_UintValue)
			Expect(ok).To(BeTrue(), "expected uint_value for count, got %T", countVal.Type)
			Expect(uintVal.UintValue).To(Equal(uint64(42)))

			// enabled → bool_value
			enabledVal := findMetadataValue(account.Metadata, "enabled")
			Expect(enabledVal).NotTo(BeNil())
			boolVal, ok := enabledVal.Type.(*commonpb.MetadataValue_BoolValue)
			Expect(ok).To(BeTrue(), "expected bool_value for enabled, got %T", enabledVal.Type)
			Expect(boolVal.BoolValue).To(BeTrue())

			// label → string_value
			labelVal := findMetadataValue(account.Metadata, "label")
			Expect(labelVal).NotTo(BeNil())
			strVal, ok := labelVal.Type.(*commonpb.MetadataValue_StringValue)
			Expect(ok).To(BeTrue(), "expected string_value for label, got %T", labelVal.Type)
			Expect(strVal.StringValue).To(Equal("test"))
		})
	})

	Context("Typed Values via gRPC (Direct Proto Values)", Ordered, func() {
		const ledgerName = "typed-meta-direct-proto"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "score",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should accept and preserve a typed int_value sent directly", func() {
			typedMeta := &commonpb.MetadataSet{
				Metadata: []*commonpb.Metadata{
					{Key: "score", Value: commonpb.NewIntValue(42)},
				},
			}
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveTypedAccountMetadataAction(ledgerName, "proto-user", typedMeta),
				},
			})
			Expect(err).To(Succeed())

			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "proto-user",
			})
			Expect(err).To(Succeed())

			v := findMetadataValue(account.Metadata, "score")
			Expect(v).NotTo(BeNil())
			intVal, ok := v.Type.(*commonpb.MetadataValue_IntValue)
			Expect(ok).To(BeTrue(), "expected int_value, got %T", v.Type)
			Expect(intVal.IntValue).To(Equal(int64(42)))
		})
	})
})
