//go:build e2e

package cluster

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ledgerctl KV", func() {
	It("round-trips opaque bytes through the live ledger and deletes the key", func() {
		ctx, node := testutil.SetupSingleNode()
		const ledger = "ledgerctl-kv-e2e"
		_, err := node.Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil)))
		Expect(err).To(Succeed())

		value := []byte{0, 1, 255, '\n', 0}
		input := filepath.Join(GinkgoT().TempDir(), "input.bin")
		Expect(os.WriteFile(input, value, 0600)).To(Succeed())
		args := []string{"--server", fmt.Sprintf("localhost:%d", node.GRPCPort), "--insecure"}
		run := func(stdin []byte, command ...string) ([]byte, error) {
			cmd := newTestRootCommand()
			out := new(bytes.Buffer)
			cmd.SetOut(out)
			cmd.SetErr(new(bytes.Buffer))
			if stdin != nil {
				cmd.SetIn(bytes.NewReader(stdin))
			}
			cmd.SetArgs(append(append([]string{}, args...), command...))
			err := cmd.Execute()
			return out.Bytes(), err
		}

		_, err = run(nil, "kv", "put", "binary", "--ledger", ledger, "--file", input)
		Expect(err).To(Succeed())
		got, err := run(nil, "kv", "get", "binary", "--ledger", ledger)
		Expect(err).To(Succeed())
		Expect(got).To(Equal(value))

		output := filepath.Join(GinkgoT().TempDir(), "output.bin")
		_, err = run(nil, "kv", "get", "binary", "--ledger", ledger, "--file", output)
		Expect(err).To(Succeed())
		fileValue, err := os.ReadFile(output)
		Expect(err).To(Succeed())
		Expect(fileValue).To(Equal(value))

		_, err = run([]byte{}, "kv", "put", "binary", "--ledger", ledger)
		Expect(err).To(Succeed())
		got, err = run(nil, "kv", "get", "binary", "--ledger", ledger)
		Expect(err).To(Succeed())
		Expect(got).To(BeEmpty())

		_, err = run(nil, "kv", "delete", "binary", "--ledger", ledger)
		Expect(err).To(Succeed())
		_, err = run(nil, "kv", "get", "binary", "--ledger", ledger)
		Expect(err).To(HaveOccurred())
	})
})
