//go:build e2e

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/accounts"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/ledgers"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/transactions"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// newTestRootCommand builds a root command tree identical to ledgerctl's main.go
// but importable from tests.
func newTestRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:          "ledgerctl",
		SilenceUsage: true,
	}

	rootCmd.PersistentFlags().String("server", "localhost:8888", "gRPC server address")
	rootCmd.PersistentFlags().Bool("insecure", false, "Use insecure connection (no TLS)")
	rootCmd.PersistentFlags().String("tls-ca-cert", "", "Path to CA certificate file")
	rootCmd.PersistentFlags().String("signing-key", "", "Path to Ed25519 private key file")
	rootCmd.PersistentFlags().String("signing-key-id", "", "Key ID for request signatures")
	rootCmd.PersistentFlags().String("response-verify-key", "", "Path to Ed25519 seed file for verifying server response signatures")

	rootCmd.AddCommand(ledgers.NewCommand())
	rootCmd.AddCommand(accounts.NewCommand())
	rootCmd.AddCommand(transactions.NewCommand())

	return rootCmd
}

// captureStdout runs fn while capturing os.Stdout output.
// Returns the captured bytes and fn's error.
func captureStdout(fn func() error) ([]byte, error) {
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("creating pipe: %w", err)
	}
	os.Stdout = w
	// Also redirect pterm's default writer to the pipe
	pterm.SetDefaultOutput(w)

	fnErr := fn()

	_ = w.Close()
	os.Stdout = oldStdout
	pterm.SetDefaultOutput(oldStdout)

	captured, readErr := io.ReadAll(r)
	_ = r.Close()
	if readErr != nil {
		return nil, fmt.Errorf("reading pipe: %w", readErr)
	}
	return captured, fnErr
}

// runCLI executes a ledgerctl command with the given args against a test server.
// Returns the combined stdout output and any error.
func runCLI(grpcPort int, args ...string) (string, error) {
	cmd := newTestRootCommand()
	buf := new(bytes.Buffer)
	cmd.SetErr(buf)

	fullArgs := append([]string{
		"--server", fmt.Sprintf("localhost:%d", grpcPort),
		"--insecure",
	}, args...)
	cmd.SetArgs(fullArgs)

	captured, err := captureStdout(func() error {
		return cmd.Execute()
	})
	// Combine captured stdout with any stderr from cobra
	output := string(captured) + buf.String()
	return output, err
}

// runCLIJSON executes a command with --json and unmarshals the output into dest.
func runCLIJSON(grpcPort int, dest any, args ...string) error {
	cmd := newTestRootCommand()
	errBuf := new(bytes.Buffer)
	cmd.SetErr(errBuf)

	fullArgs := append([]string{
		"--server", fmt.Sprintf("localhost:%d", grpcPort),
		"--insecure",
	}, args...)
	cmd.SetArgs(fullArgs)

	captured, err := captureStdout(func() error {
		return cmd.Execute()
	})
	if err != nil {
		return fmt.Errorf("command failed: %w\nstdout: %s\nstderr: %s", err, string(captured), errBuf.String())
	}

	if msg, ok := dest.(proto.Message); ok {
		return protojson.Unmarshal(captured, msg)
	}
	return json.Unmarshal(captured, dest)
}

var _ = Describe("LedgerctlTypedMetadata", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const (
		httpPort = testutil.TestSingleHTTPPort
		grpcPort = testutil.TestSingleGRPCPort
	)

	BeforeAll(func() {
		// Disable pterm interactive/animated output for test stability
		pterm.DisableColor()
		pterm.DisableStyling()

		ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort)
	})

	Context("ledgers create --schema", Ordered, func() {
		const ledgerName = "cli-create-schema"

		It("Should create a ledger with an initial schema via --schema flags", func() {
			_, err := runCLI(grpcPort,
				"ledgers", "create",
				"--name", ledgerName,
				"--schema", "account:age:int64",
				"--schema", "account:active:bool",
				"--schema", "transaction:priority:uint64",
			)
			Expect(err).To(Succeed())
		})

		It("Should have the schema visible via gRPC GetMetadataSchemaStatus", func() {
			resp, err := client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())

			Expect(resp.AccountFields).To(HaveKey("age"))
			Expect(resp.AccountFields["age"].DeclaredType).To(Equal(commonpb.MetadataType_METADATA_TYPE_INT64))
			Expect(resp.AccountFields).To(HaveKey("active"))
			Expect(resp.AccountFields["active"].DeclaredType).To(Equal(commonpb.MetadataType_METADATA_TYPE_BOOL))

			Expect(resp.TransactionFields).To(HaveKey("priority"))
			Expect(resp.TransactionFields["priority"].DeclaredType).To(Equal(commonpb.MetadataType_METADATA_TYPE_UINT64))
		})

		It("Should show the schema in ledgers get --json", func() {
			var ledger map[string]any
			err := runCLIJSON(grpcPort, &ledger, "ledgers", "get", ledgerName, "--json")
			Expect(err).To(Succeed())

			Expect(ledger["name"]).To(Equal(ledgerName))
			schema, ok := ledger["metadataSchema"].(map[string]any)
			Expect(ok).To(BeTrue(), "metadataSchema should be a map")
			accountFields, ok := schema["accountFields"].(map[string]any)
			Expect(ok).To(BeTrue(), "accountFields should be a map")
			Expect(accountFields).To(HaveKey("age"))
			Expect(accountFields).To(HaveKey("active"))
			txFields, ok := schema["transactionFields"].(map[string]any)
			Expect(ok).To(BeTrue(), "transactionFields should be a map")
			Expect(txFields).To(HaveKey("priority"))
		})
	})

	Context("ledgers set-metadata-type", Ordered, func() {
		const ledgerName = "cli-set-type"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should set a metadata field type via CLI", func() {
			_, err := runCLI(grpcPort,
				"ledgers", "set-metadata-type",
				"--ledger", ledgerName,
				"--target", "account",
				"--key", "score",
				"--type", "int64",
			)
			Expect(err).To(Succeed())
		})

		It("Should verify the type was set via gRPC", func() {
			resp, err := client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(resp.AccountFields).To(HaveKey("score"))
			Expect(resp.AccountFields["score"].DeclaredType).To(Equal(commonpb.MetadataType_METADATA_TYPE_INT64))
		})

		It("Should set a transaction field type via CLI", func() {
			_, err := runCLI(grpcPort,
				"ledgers", "set-metadata-type",
				"--ledger", ledgerName,
				"--target", "transaction",
				"--key", "category",
				"--type", "string",
			)
			Expect(err).To(Succeed())
		})

		It("Should verify both field types are present", func() {
			resp, err := client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(resp.AccountFields).To(HaveKey("score"))
			Expect(resp.TransactionFields).To(HaveKey("category"))
			Expect(resp.TransactionFields["category"].DeclaredType).To(Equal(commonpb.MetadataType_METADATA_TYPE_STRING))
		})
	})

	Context("ledgers remove-metadata-type", Ordered, func() {
		const ledgerName = "cli-rm-type"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "temp_field",
							Type:       commonpb.MetadataType_METADATA_TYPE_BOOL,
						},
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "keep_field",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should remove a metadata field type via CLI with -y", func() {
			_, err := runCLI(grpcPort,
				"ledgers", "remove-metadata-type",
				"--ledger", ledgerName,
				"--target", "account",
				"--key", "temp_field",
				"-y",
			)
			Expect(err).To(Succeed())
		})

		It("Should verify the field was removed but the other remains", func() {
			// Removal triggers background conversion to STRING then deletion —
			// wait for it to complete.
			Eventually(func(g Gomega) {
				resp, err := client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.AccountFields).NotTo(HaveKey("temp_field"))
				g.Expect(resp.AccountFields).To(HaveKey("keep_field"))
				g.Expect(resp.AccountFields["keep_field"].DeclaredType).To(Equal(commonpb.MetadataType_METADATA_TYPE_INT64))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("ledgers get-schema", Ordered, func() {
		const ledgerName = "cli-get-schema"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
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

		It("Should display the schema in text mode without error", func() {
			output, err := runCLI(grpcPort, "ledgers", "get-schema", ledgerName)
			Expect(err).To(Succeed())
			Expect(output).To(Or(BeEmpty(), Not(ContainSubstring("FAIL"))))
		})

		It("Should return correct schema in JSON mode", func() {
			var resp servicepb.GetMetadataSchemaStatusResponse
			err := runCLIJSON(grpcPort, &resp, "ledgers", "get-schema", ledgerName, "--json")
			Expect(err).To(Succeed())

			Expect(resp.AccountFields).To(HaveKey("verified"))
			Expect(resp.AccountFields["verified"].DeclaredType).To(Equal(commonpb.MetadataType_METADATA_TYPE_BOOL))
			Expect(resp.AccountFields["verified"].Status).To(Equal(
				commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE))

			Expect(resp.TransactionFields).To(HaveKey("amount_cents"))
			Expect(resp.TransactionFields["amount_cents"].DeclaredType).To(Equal(commonpb.MetadataType_METADATA_TYPE_INT64))
		})

		It("Should show empty schema for a ledger without schema", func() {
			const emptyLedger = "cli-get-schema-empty"
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(emptyLedger, nil)},
			})
			Expect(err).To(Succeed())

			var resp servicepb.GetMetadataSchemaStatusResponse
			err = runCLIJSON(grpcPort, &resp, "ledgers", "get-schema", emptyLedger, "--json")
			Expect(err).To(Succeed())
			Expect(resp.AccountFields).To(BeEmpty())
			Expect(resp.TransactionFields).To(BeEmpty())
		})
	})

	Context("Full lifecycle via CLI", Ordered, func() {
		const ledgerName = "cli-lifecycle"

		It("Should create ledger with schema", func() {
			_, err := runCLI(grpcPort,
				"ledgers", "create",
				"--name", ledgerName,
				"--schema", "account:age:int64",
				"--schema", "account:active:bool",
			)
			Expect(err).To(Succeed())
		})

		It("Should add another field type via set-metadata-type", func() {
			_, err := runCLI(grpcPort,
				"ledgers", "set-metadata-type",
				"--ledger", ledgerName,
				"--target", "account",
				"--key", "score",
				"--type", "uint64",
			)
			Expect(err).To(Succeed())
		})

		It("Should show all three account fields in get-schema --json", func() {
			var resp servicepb.GetMetadataSchemaStatusResponse
			err := runCLIJSON(grpcPort, &resp, "ledgers", "get-schema", ledgerName, "--json")
			Expect(err).To(Succeed())

			Expect(resp.AccountFields).To(HaveLen(3))
			Expect(resp.AccountFields).To(HaveKey("age"))
			Expect(resp.AccountFields).To(HaveKey("active"))
			Expect(resp.AccountFields).To(HaveKey("score"))
		})

		It("Should enforce typed metadata on account writes", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveAccountMetadataAction(ledgerName, "user1", map[string]string{
						"age":    "25",
						"active": "true",
						"score":  "100",
					}),
				},
			})
			Expect(err).To(Succeed())

			account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "user1",
			})
			Expect(err).To(Succeed())

			// age → int_value
			ageVal := actions.FindMetadataValue(account.Metadata, "age")
			Expect(ageVal).NotTo(BeNil())
			intVal, ok := ageVal.Type.(*commonpb.MetadataValue_IntValue)
			Expect(ok).To(BeTrue(), "expected int_value for age, got %T", ageVal.Type)
			Expect(intVal.IntValue).To(Equal(int64(25)))

			// active → bool_value
			activeVal := actions.FindMetadataValue(account.Metadata, "active")
			Expect(activeVal).NotTo(BeNil())
			boolVal, ok := activeVal.Type.(*commonpb.MetadataValue_BoolValue)
			Expect(ok).To(BeTrue(), "expected bool_value for active, got %T", activeVal.Type)
			Expect(boolVal.BoolValue).To(BeTrue())

			// score → uint_value
			scoreVal := actions.FindMetadataValue(account.Metadata, "score")
			Expect(scoreVal).NotTo(BeNil())
			uintVal, ok := scoreVal.Type.(*commonpb.MetadataValue_UintValue)
			Expect(ok).To(BeTrue(), "expected uint_value for score, got %T", scoreVal.Type)
			Expect(uintVal.UintValue).To(Equal(uint64(100)))
		})

		It("Should remove a field type via remove-metadata-type", func() {
			_, err := runCLI(grpcPort,
				"ledgers", "remove-metadata-type",
				"--ledger", ledgerName,
				"--target", "account",
				"--key", "score",
				"-y",
			)
			Expect(err).To(Succeed())
		})

		It("Should show only two fields after removal", func() {
			var resp servicepb.GetMetadataSchemaStatusResponse
			err := runCLIJSON(grpcPort, &resp, "ledgers", "get-schema", ledgerName, "--json")
			Expect(err).To(Succeed())

			Expect(resp.AccountFields).To(HaveLen(2))
			Expect(resp.AccountFields).To(HaveKey("age"))
			Expect(resp.AccountFields).To(HaveKey("active"))
			Expect(resp.AccountFields).NotTo(HaveKey("score"))
		})

		It("Should show schema in ledgers get", func() {
			var ledger map[string]any
			err := runCLIJSON(grpcPort, &ledger, "ledgers", "get", ledgerName, "--json")
			Expect(err).To(Succeed())

			schema, ok := ledger["metadataSchema"].(map[string]any)
			Expect(ok).To(BeTrue(), "metadataSchema should be a map")
			accountFields, ok := schema["accountFields"].(map[string]any)
			Expect(ok).To(BeTrue(), "accountFields should be a map")
			Expect(accountFields).To(HaveKey("age"))
			Expect(accountFields).To(HaveKey("active"))
			Expect(accountFields).NotTo(HaveKey("score"))
		})
	})

	Context("CLI error handling", Ordered, func() {
		It("Should fail set-metadata-type with invalid target", func() {
			_, err := runCLI(grpcPort,
				"ledgers", "set-metadata-type",
				"--ledger", "nonexistent",
				"--target", "invalid",
				"--key", "foo",
				"--type", "int64",
			)
			Expect(err).To(HaveOccurred())
		})

		It("Should fail set-metadata-type with invalid type", func() {
			_, err := runCLI(grpcPort,
				"ledgers", "set-metadata-type",
				"--ledger", "nonexistent",
				"--target", "account",
				"--key", "foo",
				"--type", "float64",
			)
			Expect(err).To(HaveOccurred())
		})

		It("Should fail create with invalid --schema format", func() {
			_, err := runCLI(grpcPort,
				"ledgers", "create",
				"--name", "should-fail",
				"--schema", "invalid-format",
			)
			Expect(err).To(HaveOccurred())
		})

		It("Should fail create with invalid --schema type", func() {
			_, err := runCLI(grpcPort,
				"ledgers", "create",
				"--name", "should-fail-2",
				"--schema", "account:key:float64",
			)
			Expect(err).To(HaveOccurred())
		})

		It("Should fail get-schema without ledger name argument", func() {
			_, err := runCLI(grpcPort, "ledgers", "get-schema")
			Expect(err).To(HaveOccurred())
		})
	})

	Context("CLI set-metadata-type with all supported types", Ordered, func() {
		const ledgerName = "cli-all-types"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		allTypes := []struct {
			typeName string
			expected commonpb.MetadataType
		}{
			{"string", commonpb.MetadataType_METADATA_TYPE_STRING},
			{"int64", commonpb.MetadataType_METADATA_TYPE_INT64},
			{"bool", commonpb.MetadataType_METADATA_TYPE_BOOL},
			{"uint64", commonpb.MetadataType_METADATA_TYPE_UINT64},
			{"int8", commonpb.MetadataType_METADATA_TYPE_INT8},
			{"int16", commonpb.MetadataType_METADATA_TYPE_INT16},
			{"int32", commonpb.MetadataType_METADATA_TYPE_INT32},
			{"uint8", commonpb.MetadataType_METADATA_TYPE_UINT8},
			{"uint16", commonpb.MetadataType_METADATA_TYPE_UINT16},
			{"uint32", commonpb.MetadataType_METADATA_TYPE_UINT32},
		}

		It("Should set all metadata types via CLI and verify each", func() {
			for _, tt := range allTypes {
				By(fmt.Sprintf("Setting type %s", tt.typeName), func() {
					key := fmt.Sprintf("field_%s", tt.typeName)
					_, err := runCLI(grpcPort,
						"ledgers", "set-metadata-type",
						"--ledger", ledgerName,
						"--target", "account",
						"--key", key,
						"--type", tt.typeName,
					)
					Expect(err).To(Succeed())
				})
			}

			By("Verifying all types via get-schema", func() {
				var resp servicepb.GetMetadataSchemaStatusResponse
				err := runCLIJSON(grpcPort, &resp, "ledgers", "get-schema", ledgerName, "--json")
				Expect(err).To(Succeed())

				Expect(resp.AccountFields).To(HaveLen(len(allTypes)))
				for _, tt := range allTypes {
					key := fmt.Sprintf("field_%s", tt.typeName)
					Expect(resp.AccountFields).To(HaveKey(key))
					Expect(resp.AccountFields[key].DeclaredType).To(Equal(tt.expected),
						"field %s should be %s", key, tt.typeName)
				}
			})
		})
	})

	Context("CLI with background conversion monitoring", Ordered, func() {
		const ledgerName = "cli-bg-conversion"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Write untyped metadata before schema exists
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveAccountMetadataAction(ledgerName, "user1", map[string]string{"score": "42"}),
					actions.SaveAccountMetadataAction(ledgerName, "user2", map[string]string{"score": "99"}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should declare a type via CLI triggering background conversion", func() {
			_, err := runCLI(grpcPort,
				"ledgers", "set-metadata-type",
				"--ledger", ledgerName,
				"--target", "account",
				"--key", "score",
				"--type", "int64",
			)
			Expect(err).To(Succeed())
		})

		It("Should eventually show COMPLETE status via get-schema --json", func() {
			Eventually(func(g Gomega) {
				var resp servicepb.GetMetadataSchemaStatusResponse
				err := runCLIJSON(grpcPort, &resp, "ledgers", "get-schema", ledgerName, "--json")
				g.Expect(err).To(Succeed())
				g.Expect(resp.AccountFields).To(HaveKey("score"))
				g.Expect(resp.AccountFields["score"].Status).To(Equal(
					commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should have converted existing data to int64", func() {
			for _, addr := range []string{"user1", "user2"} {
				account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: addr,
				})
				Expect(err).To(Succeed())

				v := actions.FindMetadataValue(account.Metadata, "score")
				Expect(v).NotTo(BeNil())
				_, ok := v.Type.(*commonpb.MetadataValue_IntValue)
				Expect(ok).To(BeTrue(), "expected int_value for %s score, got %T", addr, v.Type)
			}
		})
	})

	Context("CLI aliases", Ordered, func() {
		const ledgerName = "cli-aliases"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "test_field",
							Type:       commonpb.MetadataType_METADATA_TYPE_BOOL,
						},
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should work with 'smt' alias for set-metadata-type", func() {
			_, err := runCLI(grpcPort,
				"ledgers", "smt",
				"--ledger", ledgerName,
				"--target", "account",
				"--key", "alias_field",
				"--type", "uint32",
			)
			Expect(err).To(Succeed())

			resp, err := client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(resp.AccountFields).To(HaveKey("alias_field"))
		})

		It("Should work with 'rmt' alias for remove-metadata-type", func() {
			_, err := runCLI(grpcPort,
				"ledgers", "rmt",
				"--ledger", ledgerName,
				"--target", "account",
				"--key", "test_field",
				"-y",
			)
			Expect(err).To(Succeed())

			resp, err := client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(resp.AccountFields).NotTo(HaveKey("test_field"))
		})

		It("Should work with 'schema' alias for get-schema", func() {
			var resp servicepb.GetMetadataSchemaStatusResponse
			err := runCLIJSON(grpcPort, &resp, "ledgers", "schema", ledgerName, "--json")
			Expect(err).To(Succeed())
			Expect(resp.AccountFields).To(HaveKey("alias_field"))
		})
	})
})
