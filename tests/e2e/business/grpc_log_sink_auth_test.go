//go:build e2e

package business

import (
	"context"
	"crypto/sha256"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var _ = Describe("gRPC log sink secret authorization", func() {
	It("checks the configured verifier before sending any stream response", func() {
		ctx, node := testutil.SetupSingleNode()
		firstSecret := "first-sink-secret-generated-from-at-least-32-random-bytes"
		otherSecret := "other-sink-secret-generated-from-at-least-32-random-bytes"
		for _, candidate := range []struct {
			name   string
			secret string
		}{
			{name: "first-log-sink", secret: firstSecret},
			{name: "other-log-sink", secret: otherSecret},
		} {
			digest := sha256.Sum256([]byte(candidate.secret))
			config := &commonpb.SinkConfig{Name: candidate.name, Type: &commonpb.SinkConfig_GrpcLog{
				GrpcLog: &commonpb.GrpcLogSinkConfig{ClientId: "consumer", SecretSha256: digest[:]},
			}}
			_, err := node.Client.Apply(ctx, servicepb.UnsignedApplyRequest("", addEventsSinkAction(config)))
			Expect(err).To(Succeed())
		}

		open := func(secret string) (*servicepb.SubscribeLogsResponse, error) {
			GinkgoHelper()
			streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			streamCtx = metadata.AppendToOutgoingContext(streamCtx, "x-ledger-sink-secret", secret)
			stream, err := node.Client.SubscribeLogs(streamCtx)
			Expect(err).To(Succeed())
			defer func() { _ = stream.CloseSend() }()
			Expect(stream.Send(&servicepb.SubscribeLogsRequest{Kind: &servicepb.SubscribeLogsRequest_Open{
				Open: &servicepb.OpenLogSink{SinkName: "first-log-sink", ClientId: "consumer"},
			}})).To(Succeed())
			return stream.Recv()
		}

		response, err := open("wrong-sink-secret-generated-from-at-least-32-random-bytes")
		Expect(response).To(BeNil())
		Expect(status.Code(err)).To(Equal(codes.PermissionDenied))
		response, err = open(otherSecret)
		Expect(response).To(BeNil())
		Expect(status.Code(err)).To(Equal(codes.PermissionDenied))
		response, err = open(firstSecret)
		Expect(err).To(Succeed())
		Expect(response.GetOpened()).NotTo(BeNil())
	})
})
