//go:build e2e

package e2e

import (
	"archive/tar"
	"bytes"
	"context"
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
	var (
		ctx           context.Context
		client        servicepb.BucketServiceClient
		clusterClient clusterpb.ClusterServiceClient
	)

	const (
		httpPort = testSingleHTTPPort
		grpcPort = testSingleGRPCPort
	)

	BeforeAll(func() {
		ctx, client, clusterClient = setupSingleNode(httpPort, grpcPort)

		// Create a ledger with some data so the backup is non-trivial
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{createLedgerAction("backup-test", nil)},
		})
		Expect(err).To(Succeed())

		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				createTransactionAction("backup-test", []*commonpb.Posting{
					newPosting("world", "bank", big.NewInt(10000), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())
	})

	It("should stream a valid tar archive with correct SHA256", func() {
		stream, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{})
		Expect(err).To(Succeed())

		var (
			allData      []byte
			hash         = sha256.New()
			expectedHash string
			expectedSize uint64
			gotEOF       bool
		)

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			Expect(err).To(Succeed())

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
		stream, err := clusterClient.Backup(ctx, &clusterpb.BackupRequest{})
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
		state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		Expect(err).To(Succeed())
		Expect(state.Leader).NotTo(BeZero())

		// Verify we can still create transactions after backup
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				createTransactionAction("backup-test", []*commonpb.Posting{
					newPosting("world", "user", big.NewInt(500), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())
	})
})
