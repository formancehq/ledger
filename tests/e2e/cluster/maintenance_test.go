//go:build e2e

package cluster

import (
	"context"
	"crypto/ed25519"
	"math/big"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// setMaintenanceModeAction creates a SetMaintenanceMode request.
func setMaintenanceModeAction(enabled bool) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SetMaintenanceMode{
			SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{
				Enabled: enabled,
			},
		},
	}
}

var _ = Describe("Maintenance Mode", func() {

	Context("Enable and disable maintenance mode", Ordered, func() {
		var (
			ctx           context.Context
			client        servicepb.BucketServiceClient
			clusterClient clusterpb.ClusterServiceClient
		)

		const (
			httpPort   = 9210
			grpcPort   = 8210
			ledgerName = "maintenance-test"
		)

		BeforeAll(func() {
			ctx, client, clusterClient = testutil.SetupSingleNode(httpPort, grpcPort)

			// Create a ledger before enabling maintenance mode
			resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should allow all operations when maintenance mode is off", func() {
			resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
			}, nil)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should enable maintenance mode", func() {
			resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", setMaintenanceModeAction(true)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should show maintenance mode in cluster status", func() {
			state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
			Expect(err).To(Succeed())
			Expect(state.MaintenanceMode).To(BeTrue())
		})

		It("should reject create ledger requests in maintenance mode", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("should-fail", nil)))
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unavailable))
		})

		It("should reject create transaction requests in maintenance mode", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "bob", big.NewInt(50), "USD"),
			}, nil)))
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unavailable))
		})

		It("should reject delete ledger requests in maintenance mode", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.DeleteLedgerAction(ledgerName)))
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unavailable))
		})

		It("should reject save metadata requests in maintenance mode", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"key": "value"})))
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unavailable))
		})

		It("should allow read operations in maintenance mode", func() {
			// GetLedger is a read operation, should work
			ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(ledger.Name).To(Equal(ledgerName))
		})

		It("should allow disabling maintenance mode while in maintenance mode", func() {
			resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", setMaintenanceModeAction(false)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should show maintenance mode off in cluster status after disabling", func() {
			state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
			Expect(err).To(Succeed())
			Expect(state.MaintenanceMode).To(BeFalse())
		})

		It("should allow write operations after maintenance mode is disabled", func() {
			resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "charlie", big.NewInt(200), "USD"),
			}, nil)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})
	})

	Context("Maintenance mode with bulk requests", Ordered, func() {
		var (
			ctx    context.Context
			client servicepb.BucketServiceClient
		)

		const (
			httpPort   = 9211
			grpcPort   = 8211
			ledgerName = "maintenance-bulk"
		)

		BeforeAll(func() {
			ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort)

			// Create a ledger
			resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Enable maintenance mode
			resp, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", setMaintenanceModeAction(true)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should reject bulk requests containing non-maintenance operations", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", setMaintenanceModeAction(false),
				actions.CreateLedgerAction("should-fail-bulk", nil)))
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unavailable))
		})

		It("should allow bulk requests containing only maintenance mode operations", func() {
			// Disable then re-enable in a single batch
			resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", setMaintenanceModeAction(false)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})
	})

	Context("Maintenance mode with request signing", Ordered, func() {
		var (
			ctx     context.Context
			client  servicepb.BucketServiceClient
			privKey ed25519.PrivateKey
		)

		const (
			httpPort   = 9212
			grpcPort   = 8212
			ledgerName = "maintenance-signing"
			keyID      = "maint-key"
		)

		BeforeAll(func() {
			var pubKey ed25519.PublicKey
			var err error
			pubKey, privKey, err = actions.GenerateTestKeypair()
			Expect(err).To(Succeed())

			ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort)

			// Bootstrap signing key
			resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.RegisterSigningKeyAction(keyID, pubKey)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Create a ledger
			resp, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should accept signed maintenance mode request", func() {
			req := setMaintenanceModeAction(true)
			signedEnv, err := actions.SignBatch(&servicepb.ApplyBatch{Requests: []*servicepb.Request{req}}, keyID, privKey)
			Expect(err).To(Succeed())

			resp, err := client.Apply(ctx, signedEnv)
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should reject signed write requests in maintenance mode", func() {
			req := actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "signed-user", big.NewInt(100), "USD"),
			}, nil)
			signedEnv1, err := actions.SignBatch(&servicepb.ApplyBatch{Requests: []*servicepb.Request{req}}, keyID, privKey)
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, signedEnv1)
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unavailable))
		})

		It("should allow signed disable maintenance mode request", func() {
			req := setMaintenanceModeAction(false)
			signedEnv2, err := actions.SignBatch(&servicepb.ApplyBatch{Requests: []*servicepb.Request{req}}, keyID, privKey)
			Expect(err).To(Succeed())

			resp, err := client.Apply(ctx, signedEnv2)
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should allow signed write requests after maintenance mode is disabled", func() {
			req := actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "signed-user", big.NewInt(200), "USD"),
			}, nil)
			signedEnv3, err := actions.SignBatch(&servicepb.ApplyBatch{Requests: []*servicepb.Request{req}}, keyID, privKey)
			Expect(err).To(Succeed())

			resp, err := client.Apply(ctx, signedEnv3)
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})
	})
})
