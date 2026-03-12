//go:build e2e

package business

import (
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Backup", Ordered, func() {

	BeforeAll(func() {

		// Create a ledger with some data so the backup is non-trivial
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{testutil.CreateLedgerAction("backup-test", nil)},
		})
		Expect(err).To(Succeed())

		_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				testutil.CreateTransactionAction("backup-test", []*commonpb.Posting{
					testutil.NewPosting("world", "bank", big.NewInt(10000), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())
	})

	It("should stream a valid tar archive with correct SHA256", func() {
		stream, err := sharedClusterClient.Backup(sharedCtx, &clusterpb.BackupRequest{})
		Expect(err).To(Succeed())

		var (
			allData        []byte
			hash           = sha256.New()
			expectedHash   string
			expectedSize   uint64
			gotEOF         bool
			statusMessages []string
		)

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			Expect(err).To(Succeed())

			// Collect status-only messages (preparation phase)
			if resp.StatusMessage != "" && len(resp.Data) == 0 && !resp.Eof {
				statusMessages = append(statusMessages, resp.StatusMessage)
				continue
			}

			if resp.Eof {
				expectedHash = resp.ContentSha256
				expectedSize = resp.ContentSize
				gotEOF = true
				break
			}

			allData = append(allData, resp.Data...)
			_, err = hash.Write(resp.Data)
			Expect(err).To(Succeed())
		}

		// Verify status messages were sent before data
		Expect(statusMessages).NotTo(BeEmpty(), "should receive at least one status message during preparation")
		Expect(statusMessages).To(ContainElement(ContainSubstring("checkpoint")))
		Expect(statusMessages).To(ContainElement(ContainSubstring("ompacting")))

		Expect(gotEOF).To(BeTrue(), "should receive an EOF chunk")
		Expect(allData).NotTo(BeEmpty(), "backup should contain data")
		Expect(expectedSize).To(Equal(uint64(len(allData))))

		// Verify SHA256
		actualHash := hex.EncodeToString(hash.Sum(nil))
		Expect(actualHash).To(Equal(expectedHash))

		// Verify the data is a valid tar archive with at least one entry
		tarReader := tar.NewReader(bytes.NewReader(allData))
		var fileCount int
		for {
			_, err := tarReader.Next()
			if err == io.EOF {
				break
			}
			Expect(err).To(Succeed())
			fileCount++
		}
		Expect(fileCount).To(BeNumerically(">", 0), "tar archive should contain files")
	})

	It("should not interfere with normal cluster operations", func() {
		stream, err := sharedClusterClient.Backup(sharedCtx, &clusterpb.BackupRequest{})
		Expect(err).To(Succeed())

		// Consume the full stream
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			Expect(err).To(Succeed())
			if resp.Eof {
				break
			}
		}

		// Verify the cluster is still healthy and operational after backup
		state, err := sharedClusterClient.GetClusterState(sharedCtx, &clusterpb.GetClusterStateRequest{})
		Expect(err).To(Succeed())
		Expect(state.Leader).NotTo(BeZero())

		// Verify we can still create transactions after backup
		_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				testutil.CreateTransactionAction("backup-test", []*commonpb.Posting{
					testutil.NewPosting("world", "user", big.NewInt(500), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())
	})
})
