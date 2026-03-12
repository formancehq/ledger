//go:build e2e

package business

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// addEventsSinkAction creates a request to add a named sink configuration.
func addEventsSinkAction(config *commonpb.SinkConfig) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_AddEventsSink{
			AddEventsSink: &servicepb.AddEventsSinkRequest{
				Config: config,
			},
		},
	}
}

// removeEventsSinkAction creates a request to remove a named sink configuration.
func removeEventsSinkAction(name string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_RemoveEventsSink{
			RemoveEventsSink: &servicepb.RemoveEventsSinkRequest{
				Name: name,
			},
		},
	}
}

func newTestSinkConfig(name, topic string) *commonpb.SinkConfig {
	return &commonpb.SinkConfig{
		Name:         name,
		Format:       "json",
		BatchSize:    32,
		BatchDelayMs: 50,
		Type: &commonpb.SinkConfig_Nats{
			Nats: &commonpb.NatsSinkConfig{
				Url:   "nats://localhost:4222",
				Topic: topic,
			},
		},
	}
}

var _ = Describe("Events Sinks", Ordered, func() {

	It("Should return empty sinks when none are configured", func() {
		resp, err := sharedClient.GetEventsSinks(sharedCtx, &servicepb.GetEventsSinksRequest{})
		Expect(err).To(Succeed())
		Expect(resp.Sinks).To(BeEmpty())
		Expect(resp.SinkStatuses).To(BeEmpty())
	})

	It("Should add a sink configuration via Apply", func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				addEventsSinkAction(newTestSinkConfig("test-sink", "ledger.events")),
			},
		})
		Expect(err).To(Succeed())

		resp, err := sharedClient.GetEventsSinks(sharedCtx, &servicepb.GetEventsSinksRequest{})
		Expect(err).To(Succeed())
		Expect(resp.Sinks).To(HaveLen(1))
		Expect(resp.Sinks[0].Name).To(Equal("test-sink"))
		Expect(resp.Sinks[0].Format).To(Equal("json"))
		Expect(resp.Sinks[0].BatchSize).To(Equal(int32(32)))
		Expect(resp.Sinks[0].BatchDelayMs).To(Equal(int64(50)))

		nats := resp.Sinks[0].GetNats()
		Expect(nats).NotTo(BeNil())
		Expect(nats.Url).To(Equal("nats://localhost:4222"))
		Expect(nats.Topic).To(Equal("ledger.events"))
	})

	It("Should reject adding a sink with a duplicate name", func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				addEventsSinkAction(newTestSinkConfig("test-sink", "ledger.events.dup")),
			},
		})
		Expect(err).To(HaveOccurred())
		Expect(status.Code(err)).To(Equal(codes.AlreadyExists))
	})

	It("Should add a second sink with a different name", func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				addEventsSinkAction(newTestSinkConfig("second-sink", "ledger.events.secondary")),
			},
		})
		Expect(err).To(Succeed())

		resp, err := sharedClient.GetEventsSinks(sharedCtx, &servicepb.GetEventsSinksRequest{})
		Expect(err).To(Succeed())
		Expect(resp.Sinks).To(HaveLen(2))

		names := make([]string, len(resp.Sinks))
		for i, s := range resp.Sinks {
			names[i] = s.Name
		}
		Expect(names).To(ContainElements("test-sink", "second-sink"))
	})

	It("Should remove a sink configuration", func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				removeEventsSinkAction("test-sink"),
			},
		})
		Expect(err).To(Succeed())

		resp, err := sharedClient.GetEventsSinks(sharedCtx, &servicepb.GetEventsSinksRequest{})
		Expect(err).To(Succeed())
		Expect(resp.Sinks).To(HaveLen(1))
		Expect(resp.Sinks[0].Name).To(Equal("second-sink"))
	})

	It("Should remove the last sink leaving empty config", func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				removeEventsSinkAction("second-sink"),
			},
		})
		Expect(err).To(Succeed())

		resp, err := sharedClient.GetEventsSinks(sharedCtx, &servicepb.GetEventsSinksRequest{})
		Expect(err).To(Succeed())
		Expect(resp.Sinks).To(BeEmpty())
	})

	It("Should reject removing a non-existent sink", func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				removeEventsSinkAction("does-not-exist"),
			},
		})
		Expect(err).To(HaveOccurred())
		Expect(status.Code(err)).To(Equal(codes.NotFound))
	})

	It("Should add and remove sinks in a single batch Apply", func() {
		// Add two sinks
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				addEventsSinkAction(newTestSinkConfig("batch-sink-1", "batch.events.1")),
				addEventsSinkAction(newTestSinkConfig("batch-sink-2", "batch.events.2")),
			},
		})
		Expect(err).To(Succeed())

		resp, err := sharedClient.GetEventsSinks(sharedCtx, &servicepb.GetEventsSinksRequest{})
		Expect(err).To(Succeed())
		Expect(resp.Sinks).To(HaveLen(2))

		// Remove both in one batch
		_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				removeEventsSinkAction("batch-sink-1"),
				removeEventsSinkAction("batch-sink-2"),
			},
		})
		Expect(err).To(Succeed())

		resp, err = sharedClient.GetEventsSinks(sharedCtx, &servicepb.GetEventsSinksRequest{})
		Expect(err).To(Succeed())
		Expect(resp.Sinks).To(BeEmpty())
	})

	It("Should produce audit log entries for sink operations", func() {
		addResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				addEventsSinkAction(newTestSinkConfig("audited-sink", "audited.events")),
			},
		})
		Expect(err).To(Succeed())
		Expect(addResp.Logs).To(HaveLen(1))
		Expect(addResp.Logs[0].Payload.GetAddedEventsSink()).NotTo(BeNil())
		Expect(addResp.Logs[0].Payload.GetAddedEventsSink().Config.Name).To(Equal("audited-sink"))

		removeResp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				removeEventsSinkAction("audited-sink"),
			},
		})
		Expect(err).To(Succeed())
		Expect(removeResp.Logs).To(HaveLen(1))
		Expect(removeResp.Logs[0].Payload.GetRemovedEventsSink()).NotTo(BeNil())
		Expect(removeResp.Logs[0].Payload.GetRemovedEventsSink().Name).To(Equal("audited-sink"))
	})
})
