//go:build e2e

package business

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/ledger-v3-poc/internal/infra/backup"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
)

const (
	fsBackupHTTPPort = 15700
	fsBackupGRPCPort = 15800
)

// readBackupManifest reads the backup manifest from the filesystem.
func readBackupManifest(backupPath string) (*backup.Manifest, error) {
	data, err := os.ReadFile(filepath.Join(backupPath, "test-cluster", "backups", "manifest.json"))
	if err != nil {
		return nil, err
	}

	var manifest backup.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}

	return &manifest, nil
}

// backupDataPath returns the path to the backup data directory.
func backupDataPath(backupPath string) string {
	return filepath.Join(backupPath, "test-cluster", "backups", "data")
}

var _ = Describe("Filesystem Incremental Backup", Ordered, func() {
	var (
		ctx        context.Context
		client     servicepb.BucketServiceClient
		backupPath string
	)

	BeforeAll(func() {
		backupPath = GinkgoT().TempDir()

		ctx, client, _ = testutil.SetupSingleNode(fsBackupHTTPPort, fsBackupGRPCPort,
			testserver.WithBackupFilesystem(backupPath, "@every 3s"),
		)
	})

	It("should create a backup with manifest after the first scheduled cycle", func() {
		// Create a ledger with some data
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateLedgerAction("backup-test", nil),
			},
		})
		Expect(err).To(Succeed())

		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateForceTransactionAction("backup-test",
					[]*commonpb.Posting{
						actions.NewPosting("world", "users:alice", big.NewInt(5000), "USD"),
					},
					nil,
				),
			},
		})
		Expect(err).To(Succeed())

		// Wait for the first backup to complete (manifest appears)
		var manifest *backup.Manifest
		Eventually(func(g Gomega) {
			var err error
			manifest, err = readBackupManifest(backupPath)
			g.Expect(err).To(Succeed())
			g.Expect(manifest.Files).NotTo(BeEmpty())
		}).Within(15 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())

		// Verify all files referenced in the manifest exist on disk
		dataDir := backupDataPath(backupPath)
		for filename := range manifest.Files {
			path := filepath.Join(dataDir, filepath.FromSlash(filename))
			_, err := os.Stat(path)
			Expect(err).To(Succeed(), "backup file %s should exist", filename)
		}
	})

	It("should perform an incremental backup after adding more data", func() {
		// Read the manifest from the first backup
		manifestBefore, err := readBackupManifest(backupPath)
		Expect(err).To(Succeed())

		filesBefore := make(map[string]int64)
		for k, v := range manifestBefore.Files {
			filesBefore[k] = v
		}

		timestampBefore := manifestBefore.Timestamp

		// Add more data to trigger new SST files
		for i := range 5 {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction("backup-test",
						[]*commonpb.Posting{
							actions.NewPosting("world", "users:bob", big.NewInt(int64(100*(i+1))), "EUR"),
						},
						nil,
					),
				},
			})
			Expect(err).To(Succeed())
		}

		// Wait for the next backup cycle to update the manifest
		Eventually(func(g Gomega) {
			manifest, err := readBackupManifest(backupPath)
			g.Expect(err).To(Succeed())
			g.Expect(manifest.Timestamp).NotTo(Equal(timestampBefore),
				"manifest timestamp should be updated after new backup")
		}).Within(15 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())

		// Read updated manifest
		manifestAfter, err := readBackupManifest(backupPath)
		Expect(err).To(Succeed())

		// Verify all files still exist
		dataDir := backupDataPath(backupPath)
		for filename := range manifestAfter.Files {
			path := filepath.Join(dataDir, filepath.FromSlash(filename))
			_, err := os.Stat(path)
			Expect(err).To(Succeed(), "backup file %s should exist after incremental backup", filename)
		}

		// Verify files that were removed from the manifest no longer exist on disk
		for filename := range filesBefore {
			if _, stillExists := manifestAfter.Files[filename]; !stillExists {
				path := filepath.Join(dataDir, filepath.FromSlash(filename))
				_, err := os.Stat(path)
				Expect(os.IsNotExist(err)).To(BeTrue(),
					"stale file %s should have been deleted", filename)
			}
		}
	})
})
