//go:build e2e

package e2e

import (
	"context"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// simpleChart returns a chart with:
//
//	bank (account)
//	users -> {id} (account)
func simpleChart() *commonpb.ChartOfAccounts {
	return &commonpb.ChartOfAccounts{
		Roots: map[string]*commonpb.ChartSegment{
			"bank": {Account: true},
			"users": {
				Variable: &commonpb.ChartVariable{
					Name:    "id",
					Account: true,
				},
			},
		},
	}
}

// regexChart returns a chart with a variable constrained by a regex pattern:
//
//	accounts -> {id:[0-9]+} (account)
func regexChart() *commonpb.ChartOfAccounts {
	return &commonpb.ChartOfAccounts{
		Roots: map[string]*commonpb.ChartSegment{
			"accounts": {
				Variable: &commonpb.ChartVariable{
					Name:    "id",
					Pattern: `^[0-9]+$`,
					Account: true,
				},
			},
		},
	}
}

// deepChart returns a chart with nested fixed and variable segments:
//
//	org -> {orgId} -> department -> {deptId} (account)
func deepChart() *commonpb.ChartOfAccounts {
	return &commonpb.ChartOfAccounts{
		Roots: map[string]*commonpb.ChartSegment{
			"org": {
				Variable: &commonpb.ChartVariable{
					Name: "orgId",
					Children: map[string]*commonpb.ChartSegment{
						"department": {
							Variable: &commonpb.ChartVariable{
								Name:    "deptId",
								Account: true,
							},
						},
					},
				},
			},
		},
	}
}

var _ = Describe("ChartOfAccounts", Ordered, func() {
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

	Context("Ledger Creation with Chart", Ordered, func() {
		const ledgerName = "chart-create"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithChartAction(ledgerName, simpleChart(), commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should allow transactions with valid addresses", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(1000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("bank", "users:123", big.NewInt(100), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject transactions with invalid addresses in strict mode", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("bank", "invalid:addr:here", big.NewInt(50), "USD"),
					}, nil),
				},
			})
			Expect(err).To(HaveOccurred())

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(processing.ErrReasonAccountNotInChart))
		})
	})

	Context("Set Chart on Existing Ledger", Ordered, func() {
		const ledgerName = "chart-set-later"

		BeforeAll(func() {
			// Create ledger without chart
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should allow any address before chart is set", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "anything:goes:here", big.NewInt(100), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should set chart and enforce it", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					setChartOfAccountsAction(ledgerName, simpleChart()),
				},
			})
			Expect(err).To(Succeed())

			// Now invalid addresses should be rejected (default is STRICT)
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "not:in:chart", big.NewInt(10), "USD"),
					}, nil),
				},
			})
			Expect(err).To(HaveOccurred())

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(processing.ErrReasonAccountNotInChart))
		})

		It("Should allow valid addresses after chart is set with no warnings", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(500), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Valid addresses should produce no warnings
			Expect(resp.Logs).NotTo(BeEmpty())
			created := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(created).NotTo(BeNil())
			Expect(created.Warnings).To(BeEmpty())
		})
	})

	Context("Enforcement Mode Switching", Ordered, func() {
		const ledgerName = "chart-mode-switch"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithChartAction(ledgerName, simpleChart(), commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject invalid addresses in strict mode", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "unknown:addr", big.NewInt(10), "USD"),
					}, nil),
				},
			})
			Expect(err).To(HaveOccurred())

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(processing.ErrReasonAccountNotInChart))
		})

		It("Should switch to audit mode and allow invalid addresses with warnings", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					setChartEnforcementModeAction(ledgerName, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT),
				},
			})
			Expect(err).To(Succeed())

			// Now invalid addresses should be allowed (audit mode) with warnings
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "unknown:addr", big.NewInt(10), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Verify warnings are present in the log payload
			Expect(resp.Logs).NotTo(BeEmpty())
			created := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
			Expect(created).NotTo(BeNil())
			Expect(created.Warnings).To(HaveLen(1))
			Expect(created.Warnings[0].Address).To(Equal("unknown:addr"))
		})

		It("Should switch back to strict and reject again", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					setChartEnforcementModeAction(ledgerName, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "unknown:addr", big.NewInt(10), "USD"),
					}, nil),
				},
			})
			Expect(err).To(HaveOccurred())

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(processing.ErrReasonAccountNotInChart))
		})
	})

	Context("Variable Segments with Regex", Ordered, func() {
		const ledgerName = "chart-regex"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithChartAction(ledgerName, regexChart(), commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should allow addresses matching the regex pattern", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "accounts:42", big.NewInt(100), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject addresses not matching the regex pattern", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "accounts:abc", big.NewInt(100), "USD"),
					}, nil),
				},
			})
			Expect(err).To(HaveOccurred())

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(processing.ErrReasonAccountNotInChart))
		})
	})

	Context("Deep Nested Chart", Ordered, func() {
		const ledgerName = "chart-deep"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithChartAction(ledgerName, deepChart(), commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should allow deeply nested valid addresses", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "org:acme:department:engineering", big.NewInt(100), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject incomplete nested addresses", func() {
			// org:acme is not a valid account (account=false on the variable node)
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "org:acme", big.NewInt(10), "USD"),
					}, nil),
				},
			})
			Expect(err).To(HaveOccurred())

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(processing.ErrReasonAccountNotInChart))
		})
	})

	Context("World Account Always Valid", Ordered, func() {
		const ledgerName = "chart-world"

		BeforeAll(func() {
			// Create a very restrictive chart (only "bank" is valid)
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithChartAction(ledgerName, &commonpb.ChartOfAccounts{
						Roots: map[string]*commonpb.ChartSegment{
							"bank": {Account: true},
						},
					}, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should always allow world as source even if not in chart", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank", big.NewInt(1000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
		})
	})

	Context("Invalid Chart Rejection", Ordered, func() {
		const ledgerName = "chart-invalid"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject a chart with no account nodes", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					setChartOfAccountsAction(ledgerName, &commonpb.ChartOfAccounts{
						Roots: map[string]*commonpb.ChartSegment{
							"bank": {Account: false}, // No account=true anywhere
						},
					}),
				},
			})
			Expect(err).To(HaveOccurred())

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(processing.ErrReasonInvalidChart))
		})

		It("Should reject a chart with empty roots", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					setChartOfAccountsAction(ledgerName, &commonpb.ChartOfAccounts{
						Roots: map[string]*commonpb.ChartSegment{},
					}),
				},
			})
			Expect(err).To(HaveOccurred())

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(processing.ErrReasonInvalidChart))
		})
	})

	Context("Chart Validation on Account Metadata", Ordered, func() {
		const ledgerName = "chart-metadata"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithChartAction(ledgerName, simpleChart(), commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should allow metadata on valid accounts", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "bank", map[string]string{"type": "main"}),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "users:42", map[string]string{"name": "Alice"}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject metadata on invalid accounts in strict mode", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "invalid:addr:here", map[string]string{"key": "val"}),
				},
			})
			Expect(err).To(HaveOccurred())

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(processing.ErrReasonAccountNotInChart))
		})

		It("Should allow metadata on invalid accounts in audit mode with warnings", func() {
			// Switch to audit mode
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					setChartEnforcementModeAction(ledgerName, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT),
				},
			})
			Expect(err).To(Succeed())

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "invalid:addr:here", map[string]string{"key": "val"}),
				},
			})
			Expect(err).To(Succeed())

			// Verify warnings are present in the log payload
			Expect(resp.Logs).NotTo(BeEmpty())
			savedMeta := resp.Logs[0].Payload.GetApply().Log.Data.GetSavedMetadata()
			Expect(savedMeta).NotTo(BeNil())
			Expect(savedMeta.Warnings).To(HaveLen(1))
			Expect(savedMeta.Warnings[0].Address).To(Equal("invalid:addr:here"))
		})
	})

	Context("Numscript with Chart Validation", Ordered, func() {
		const ledgerName = "chart-numscript"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithChartAction(ledgerName, simpleChart(), commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should allow Numscript transactions to valid accounts", func() {
			script := `
send [USD/2 100] (
  source = @world
  destination = @users:abc
)`
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceScriptTransactionAction(ledgerName, script, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject Numscript transactions to invalid accounts", func() {
			script := `
send [USD/2 100] (
  source = @world
  destination = @forbidden:path:deep
)`
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceScriptTransactionAction(ledgerName, script, nil, nil),
				},
			})
			Expect(err).To(HaveOccurred())

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(processing.ErrReasonAccountNotInChart))
		})
	})

	Context("Invalid Chart at Ledger Creation", func() {
		It("Should reject ledger creation with invalid chart", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithChartAction("chart-invalid-create", &commonpb.ChartOfAccounts{
						Roots: map[string]*commonpb.ChartSegment{
							"nope": {Account: false},
						},
					}, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
				},
			})
			Expect(err).To(HaveOccurred())

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(processing.ErrReasonInvalidChart))
		})
	})
})
